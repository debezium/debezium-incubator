/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static io.debezium.connector.oracle.logminer.LogMinerHelper.getLastScnToAbandon;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.logDatabaseState;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.logError;
import static io.debezium.connector.oracle.logminer.LogMinerHelper.setRedoLogFilesForMining;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleTaskContext;
import io.debezium.connector.oracle.logminer.logwriter.CommitLogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.LogWriterFlushStrategy;
import io.debezium.connector.oracle.logminer.logwriter.RacCommitLogWriterFlushStrategy;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Stopwatch;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 * The event handler loop is executed in a separate executor.
 */
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);

    private static final String NO = "NO";
    private static final String ALL_COLUMN_LOGGING = "ALL COLUMN LOGGING";
    private static final String NLS_SESSION_PARAMETERS = "ALTER SESSION SET "
            + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
            + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'"
            + "  NLS_NUMERIC_CHARACTERS = '.,'";

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final String catalogName;
    private final JdbcConfiguration jdbcConfiguration;
    private final OracleConnectorConfig.LogMiningStrategy strategy;
    private final OracleTaskContext taskContext;
    private final ErrorHandler errorHandler;
    private final boolean isContinuousMining;
    private final LogWriterFlushStrategy flushStrategy;

    private OracleConnectorConfig connectorConfig;
    private LogMinerMetrics logMinerMetrics;
    private long startScn;
    private long endScn;
    private Duration archiveLogRetention;

    public LogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext,
                                              OracleConnection jdbcConnection, EventDispatcher<TableId> dispatcher,
                                              ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema,
                                              OracleTaskContext taskContext) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.connectorConfig = connectorConfig;
        this.catalogName = (connectorConfig.getPdbName() != null) ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName();
        this.strategy = connectorConfig.getLogMiningStrategy();
        this.isContinuousMining = connectorConfig.isContinuousMining();
        this.errorHandler = errorHandler;
        this.taskContext = taskContext;
        this.jdbcConfiguration = JdbcConfiguration.adapt(connectorConfig.getConfig().subset("database.", true));
        this.flushStrategy = resolveLogWriterFlushStrategy(connectorConfig, jdbcConfiguration, jdbcConnection);
        this.archiveLogRetention = connectorConfig.getLogMiningArchiveLogRetention();
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context
     *         change event source context
     */
    @Override
    public void execute(ChangeEventSourceContext context) {
        try (TransactionalBuffer transactionalBuffer = new TransactionalBuffer(taskContext, errorHandler)) {
            try {
                // Perform registration
                registerLogMinerMetrics();

                long databaseTimeMs = getTimeDifference().toMillis();
                LOGGER.trace("Current time {} ms, database difference {} ms", System.currentTimeMillis(), databaseTimeMs);
                transactionalBuffer.setDatabaseTimeDifference(databaseTimeMs);

                startScn = offsetContext.getScn();

                if (!isContinuousMining && startScn < getFirstRedoLogScn()) {
                    throw new DebeziumException(
                            "Online REDO LOG files or archive log files do not contain the offset scn " + startScn +
                                    ".  Please perform a new snapshot.");
                }

                setNlsSessionParameters();
                checkSupplementalLogging();

                initializeRedoLogsForMining(jdbcConnection, false);

                HistoryRecorder historyRecorder = connectorConfig.getLogMiningHistoryRecorder();

                Duration processTime = Duration.ZERO;
                try {
                    // todo: why can't OracleConnection be used rather than a Factory+JdbcConfiguration?
                    historyRecorder.prepare(logMinerMetrics, jdbcConfiguration, connectorConfig.getLogMinerHistoryRetentionHours());

                    final LogMinerQueryResultProcessor processor = new LogMinerQueryResultProcessor(context, jdbcConnection,
                            connectorConfig, logMinerMetrics, transactionalBuffer, offsetContext, schema, dispatcher,
                            catalogName, clock, historyRecorder);

                    final String query = SqlUtils.logMinerContentsQuery(connectorConfig.getSchemaName(), jdbcConnection.username(), schema);
                    try (PreparedStatement miningView = jdbcConnection.connection().prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
                        Set<String> currentRedoLogFiles = getCurrentRedoLogFiles();

                        Stopwatch stopwatch = Stopwatch.reusable();
                        while (context.isRunning()) {
                            Instant start = Instant.now();
                            endScn = getEndScn(startScn, connectorConfig.getLogMiningBatchSizeDefault());
                            flushStrategy.flush(jdbcConnection.getCurrentScn());

                            Set<String> possibleNewCurrentLogFile = getCurrentRedoLogFiles();
                            if (!currentRedoLogFiles.equals(possibleNewCurrentLogFile)) {
                                LOGGER.debug("Redo log switch detected, from {} to {}", currentRedoLogFiles, possibleNewCurrentLogFile);

                                // This is the way to mitigate PGA leaks.
                                // With one mining session, it grows and maybe there is another way to flush PGA.
                                // At this point we use a new mining session
                                endMiningSession();

                                initializeRedoLogsForMining(jdbcConnection, true);

                                abandonOldTransactionsIfExist(jdbcConnection, transactionalBuffer);
                                currentRedoLogFiles = getCurrentRedoLogFiles();
                            }

                            startMiningSession();

                            stopwatch.start();
                            miningView.setFetchSize(connectorConfig.getMaxQueueSize());
                            miningView.setFetchDirection(ResultSet.FETCH_FORWARD);
                            miningView.setLong(1, startScn);
                            miningView.setLong(2, endScn);
                            try (ResultSet rs = miningView.executeQuery()) {
                                Duration lastDurationOfBatchCapturing = stopwatch.stop().durations().statistics().getTotal();
                                logMinerMetrics.setLastDurationOfBatchCapturing(lastDurationOfBatchCapturing);
                                processor.processResult(rs);

                                startScn = endScn;

                                if (transactionalBuffer.isEmpty()) {
                                    LOGGER.debug("Transactional buffer empty, updating offset's SCN {}", startScn);
                                    offsetContext.setScn(startScn);
                                }
                            }

                            processTime = processTime.plus(Duration.between(start, Instant.now()));
                            logMinerMetrics.setTotalProcessingTime(processTime);

                            pauseBetweenMiningSessions();
                        }
                    }
                }
                finally {
                    historyRecorder.close();
                    flushStrategy.close();
                }
            }
            catch (Throwable t) {
                logError(transactionalBuffer.getMetrics(), "Mining session stopped due to the {}", t);
                errorHandler.setProducerThrowable(t);
            }
            finally {
                LOGGER.info("startScn={}, endScn={}, offsetContext.getScn()={}", startScn, endScn, offsetContext.getScn());
                LOGGER.info("Transactional buffer metrics dump: {}", transactionalBuffer.getMetrics().toString());
                LOGGER.info("Transactional buffer dump: {}", transactionalBuffer.getMetrics().toString());
                LOGGER.info("LogMiner metrics dump: {}", logMinerMetrics.toString());

                // Perform unregistration
                unregisterLogMinerMetrics();
            }
        }
    }

    private void registerLogMinerMetrics() {
        logMinerMetrics = new LogMinerMetrics(taskContext, connectorConfig);
        logMinerMetrics.register(LOGGER);
        if (connectorConfig.isLogMiningHistoryRecorded()) {
            logMinerMetrics.setRecordMiningHistory(true);
        }
    }

    private void unregisterLogMinerMetrics() {
        if (logMinerMetrics != null) {
            logMinerMetrics.unregister(LOGGER);
        }
    }

    private void abandonOldTransactionsIfExist(OracleConnection connection, TransactionalBuffer transactionalBuffer) {
        Optional<Long> lastScnToAbandonTransactions = getLastScnToAbandon(connection, offsetContext.getScn(),
                connectorConfig.getLogMiningTransactionRetention());
        lastScnToAbandonTransactions.ifPresent(thresholdScn -> {
            transactionalBuffer.abandonLongTransactions(thresholdScn, offsetContext);
            offsetContext.setScn(thresholdScn);
            startScn = endScn;
        });
    }

    private void initializeRedoLogsForMining(OracleConnection connection, boolean postEndMiningSession) throws SQLException {
        if (!postEndMiningSession) {
            if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                buildDataDictionary();
            }
            if (!isContinuousMining) {
                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
            }
        }
        else {
            if (!isContinuousMining) {
                if (OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO.equals(strategy)) {
                    buildDataDictionary();
                }
                setRedoLogFilesForMining(connection, startScn, archiveLogRetention);
            }
        }
    }

    private void pauseBetweenMiningSessions() throws InterruptedException {
        Duration period = Duration.ofMillis(logMinerMetrics.getMillisecondToSleepBetweenMiningQuery());
        Metronome.sleeper(period, clock).pause();
    }

    /**
     * Sets the NLS parameters for the mining session.
     *
     * @throws SQLException if an exception occurred
     */
    private void setNlsSessionParameters() throws SQLException {
        jdbcConnection.executeWithoutCommitting(NLS_SESSION_PARAMETERS);
    }

    /**
     * Checks that the database has at least minimum supplemental logging enabled and that if the database
     * does not use all column logging that each monitored table does.
     *
     * @throws SQLException if a database exception occurs
     */
    private void checkSupplementalLogging() throws SQLException {
        try {
            if (connectorConfig.getPdbName() != null) {
                jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
            }

            // Check if ALL supplemental logging is enabled at the database
            String globalAll = jdbcConnection.querySingleResult(SqlUtils.databaseSupplementalLoggingAllCheckQuery(), NO, (rs) -> rs.getString(2));
            if (NO.equalsIgnoreCase(globalAll)) {
                // Check if MIN supplemental logging is enabled at database
                String globalMin = jdbcConnection.querySingleResult(SqlUtils.databaseSupplementalLoggingMinCheckQuery(), NO, (rs) -> rs.getString(2));
                if (NO.equalsIgnoreCase(globalMin)) {
                    throw new DebeziumException("Supplemental logging not properly configured. " +
                            "Use: ALTER DATABASE ADD SUPPLEMENTAL LOG DATA");
                }

                // If ALL supplemental logging is not enabled, each monitored table should have ALL COLUMNS
                for (TableId tableId : schema.getTables().tableIds()) {
                    boolean tableAll = jdbcConnection.queryAndMap(SqlUtils.tableSupplementalLoggingCheckQuery(tableId), (rs) -> {
                        while (rs.next()) {
                            if (ALL_COLUMN_LOGGING.equals(rs.getString(2))) {
                                return true;
                            }
                        }
                        return false;
                    });
                    if (!tableAll) {
                        throw new DebeziumException("Supplemental logging not configured for table " + tableId + ". " +
                                "Use: ALTER TABLE " + tableId.schema() + "." + tableId.table() + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
                    }
                }
            }
        }
        finally {
            if (connectorConfig.getPdbName() != null) {
                jdbcConnection.resetSessionToCdb();
            }
        }
    }

    /**
     * Builds the data dictionary from the redo log files.  During the build, Oracle does an additional
     * REDO LOG switch.  This call may take some time which leads to delays in delivering changes.
     * With this option, the lag between source and database dispatching events can fluctuate.
     *
     * @throws SQLException if an exception occurred
     */
    private void buildDataDictionary() throws SQLException {
        LOGGER.trace("Building data dictionary");
        final String query = "BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;";
        jdbcConnection.prepareCall(false, query);
    }

    /**
     * Build the LogMiner view to query changes from.  The view is built for online redo log files and
     * any archive logs that were added to the LogMiner session.
     *
     * @throws SQLException if an exception occurred
     */
    private void startMiningSession() throws SQLException {
        LOGGER.trace("Starting log mining session startScn={}, endScn={}, strategy={}, continuous={}",
                startScn, endScn, strategy, isContinuousMining);
        final String statement = SqlUtils.startLogMinerStatement(startScn, endScn, strategy, isContinuousMining);
        try {
            Instant start = Instant.now();
            jdbcConnection.prepareCall(false, statement);
            logMinerMetrics.addCurrentMiningSessionStart(Duration.between(start, Instant.now()));
        }
        catch (SQLException e) {
            // Capture database state before re-throwing exception
            logDatabaseState(jdbcConnection);
            throw e;
        }
    }

    /**
     * Ends the current LogMiner session if one is active.
     */
    private void endMiningSession() {
        LOGGER.trace("Ending log mining startScn={}, endScn={}, offsetContext.getScn={}, strategy={}, continuous={}",
                startScn, endScn, offsetContext.getScn(), strategy, isContinuousMining);
        try {
            jdbcConnection.prepareCall(false, "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;");
        }
        catch (SQLException e) {
            if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                LOGGER.info("LogMiner session has already ended");
            }
            else {
                LOGGER.error("Cannot end LogMiner session gracefully: {}", e.getMessage(), e);
            }
        }
    }

    private Set<String> getCurrentRedoLogFiles() throws SQLException {
        Set<String> fileNames = new LinkedHashSet<>();
        jdbcConnection.query(SqlUtils.currentRedoNameQuery(), (rs) -> {
            while (rs.next()) {
                fileNames.add(rs.getString(1));
            }
            LOGGER.trace(" Current Redo log fileNames: {}", fileNames);
        });

        logMinerMetrics.setRedoLogStatus(getRedoLogStatus());
        logMinerMetrics.setSwitchCount(getRedoLogSwitchCount());
        logMinerMetrics.setCurrentLogFileName(fileNames);

        return fileNames;
    }

    private Map<String, String> getRedoLogStatus() throws SQLException {
        Map<String, String> results = new LinkedHashMap<>();
        jdbcConnection.query(SqlUtils.redoLogStatusQuery(), (rs) -> {
            while (rs.next()) {
                String value = rs.getString(2);
                results.put(rs.getString(1), value == null ? "UNKNOWN" : value);
            }
        });
        return results;
    }

    private int getRedoLogSwitchCount() throws SQLException {
        return jdbcConnection.queryAndMap(SqlUtils.switchHistoryQuery(), (rs) -> {
            return (rs.next() ? rs.getInt(2) : 0);
        });
    }

    /**
     * Get the oldest SCN from the redo logs.
     *
     * @return the oldest SCN from the redo logs.
     * @throws SQLException if an exception occurred
     */
    private long getFirstRedoLogScn() throws SQLException {
        LOGGER.trace("Getting first SCN of all redo logs");
        final String query = SqlUtils.oldestFirstChangeQuery(archiveLogRetention);
        long firstScn = jdbcConnection.querySingleResult(query, 0L, (rs) -> rs.getLong(1));
        LOGGER.trace("First SCN in redo logs is {}", firstScn);
        return firstScn;
    }

    /**
     * Returns the upper bounds SCN for mining and updates the JMX MBean metrics.
     *
     * Uses a configurable limit due to larger mining ranges cause slower queries from the LogMiner view.
     * In addition, capturing unlimited number of changes can impact connector's memory footprint.
     * Using an adaptive batch window helps catch up faster after long delays in mining changes.
     *
     * @param startScn the starting SCN
     * @param defaultBatchSize the default batch size
     * @return the upper bounds SCN for initiating a LogMiner session
     * @throws SQLException if an exception occurred
     */
    private long getEndScn(long startScn, int defaultBatchSize) throws SQLException {
        long currentScn = jdbcConnection.getCurrentScn();
        logMinerMetrics.setCurrentScn(currentScn);

        long topScnToMine = startScn + logMinerMetrics.getBatchSize();

        // adjust batch size
        boolean topMiningScnInFarFuture = false;
        if ((topScnToMine - currentScn) > defaultBatchSize) {
            logMinerMetrics.changeBatchSize(false);
            topMiningScnInFarFuture = true;
        }
        if ((currentScn - topScnToMine) > defaultBatchSize) {
            logMinerMetrics.changeBatchSize(true);
        }

        // adjust sleeping time to reduce database impact
        if (currentScn < topScnToMine) {
            if (!topMiningScnInFarFuture) {
                logMinerMetrics.changeSleepingTime(true);
            }
            return currentScn;
        }
        else {
            logMinerMetrics.changeSleepingTime(false);
            return topScnToMine;
        }
    }

    /**
     * Calculate the time difference between the connector and the database.
     * The resulting duration can be negative if the database time is ahead of the connector.
     *
     * @return the time difference as a duration
     * @throws SQLException if an exception occurred
     */
    private Duration getTimeDifference() throws SQLException {
        Timestamp dbCurrentMs = jdbcConnection.querySingleResult("SELECT CURRENT_TIMESTAMP FROM DUAL", (rs) -> rs.getTimestamp(1));
        if (dbCurrentMs == null) {
            return Duration.ZERO;
        }
        return Duration.between(dbCurrentMs.toInstant(), Instant.now());
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // nothing to do
    }

    private static LogWriterFlushStrategy resolveLogWriterFlushStrategy(OracleConnectorConfig connectorConfig,
                                                                        JdbcConfiguration jdbcConfiguration, OracleConnection connection) {
        if (connectorConfig.isRacSystem()) {
            return new RacCommitLogWriterFlushStrategy(jdbcConfiguration, connectorConfig);
        }
        return new CommitLogWriterFlushStrategy(connection, null);
    }
}
