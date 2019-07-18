/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Map;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 * The event handler loop is executed in a separate executor.
 */
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;
    private final ErrorHandler errorHandler;
    private final TransactionalBuffer transactionalBuffer;
    private final OracleDmlParser dmlParser;
    private final String catalogName;
    //private final int posVersion;
    private OracleConnectorConfig connectorConfig;

    public LogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext, OracleConnection jdbcConnection,
             EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = connectorConfig.getTablenameCaseInsensitive();
        //this.posVersion = connectorConfig.getOracleVersion().getPosVersion();
        OracleChangeRecordValueConverter converters = new OracleChangeRecordValueConverter(jdbcConnection);

        this.connectorConfig = connectorConfig;

        this.dmlParser = new OracleDmlParser(true, connectorConfig.getDatabaseName(), connectorConfig.getSchemaName(),
                converters);
        this.errorHandler = errorHandler;
        this.transactionalBuffer = new TransactionalBuffer(connectorConfig.getLogicalName(), errorHandler);
        this.catalogName = (connectorConfig.getPdbName() != null) ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName();
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context change event source context
     * @throws InterruptedException an exception
     */
    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        Metronome metronome = Metronome.sleeper(Duration.ofMillis(1000L), clock);
        ResultSet res = null;

        try (Connection connection = jdbcConnection.connection();
             PreparedStatement fetchChangesFromMiningView =
                     connection.prepareStatement(SqlUtils.queryLogMinerContents(connectorConfig.getSchemaName(), jdbcConnection.username()))) {
            long lastProcessedScn = offsetContext.getScn(); // last processes Scn
            long oldestScnInOnlineRedo = LogMinerHelper.getFirstOnlineLogScn(connection);
            if (lastProcessedScn < oldestScnInOnlineRedo) {
                throw new RuntimeException("Online REDO LOG files don't contain the offset SCN. Clean offset info and start over");
            }

            LogMinerHelper.setNlsSessionParameters(jdbcConnection);

            // 1. Configure Log Miner to mine online logs
            if (connectorConfig.getPdbName() != null) {
                jdbcConnection.resetSessionToCdb();
            }
            LogMinerHelper.buildDataDictionary(connection);
            LogMinerHelper.addOnlineRedoLogFilesForMining(connection);
            String currentRedoLogFile = LogMinerHelper.getCurrentRedoLogFile(connection);

            // 2. Querying LogMinerRowLcr(s) from Log miner while running
            while (context.isRunning()) {
                LOGGER.trace("Receiving a change from LogMiner");

                long currentScn = LogMinerHelper.getCurrentScn(connection);
                LOGGER.debug("lastProcessedScn: {}, endScn: {}", lastProcessedScn, currentScn);

                String possibleNewCurrentLogFile = LogMinerHelper.getCurrentRedoLogFile(connection);

                // if switching is happening too frequently, increase log file sizes
                if (!currentRedoLogFile.equals(possibleNewCurrentLogFile)) {
                    LogMinerHelper.endMining(connection);
                    LogMinerHelper.buildDataDictionary(connection);
                    LogMinerHelper.addOnlineRedoLogFilesForMining(connection);
                    // Oracle does the switch on building data dictionary in redo logs
                    currentRedoLogFile = LogMinerHelper.getCurrentRedoLogFile(connection);
                }

                LogMinerHelper.startOnlineMining(connection, lastProcessedScn, currentScn);

                fetchChangesFromMiningView.setLong(1, lastProcessedScn);

                traceInfo(connection, "before fetching changes");
                res = fetchChangesFromMiningView.executeQuery();
                traceInfo(connection, "after fetching changes");

                if (res.next()) {
                    res.beforeFirst();
                    processResult(res, context);
                } else {
                    metronome.pause();
                }
                // update SCN in offset context only if buffer is empty
                if (transactionalBuffer.isEmpty()) {
                    offsetContext.setScn(currentScn);
                }
                lastProcessedScn = currentScn;
                res.close();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            // 3. disconnect
            try (Connection connection = jdbcConnection.connection()) {
                LogMinerHelper.endMining(connection);
            } catch (SQLException e) {
                LOGGER.error("Cannot close Log Miner session gracefully: {}", e.getMessage());
            }
            if (res != null) {
                try {
                    res.close();
                } catch (SQLException e) {
                    LOGGER.error("Cannot close result set due to the :{}", e.getMessage());
                }
            }
            try {
                jdbcConnection.close();
                transactionalBuffer.close();
            } catch (SQLException e) {
                LOGGER.error("Cannot close JDBC connection: {}", e.getMessage());
            }
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // todo nothing here?
    }

    private void processResult(ResultSet res, ChangeEventSourceContext context) throws SQLException {
        while (res.next()) {
            BigDecimal scn = RowMapper.getScn(res);
            BigDecimal commitScn = RowMapper.getCommitScn(res);
            String operation = RowMapper.getOperation(res);
            String userName = RowMapper.getUserName(res);
            String redo_sql = RowMapper.getSqlRedo(res);
            int operationCode = RowMapper.getOperationCode(res);
            String tableName = RowMapper.getTableName(res);
            Timestamp changeTime = RowMapper.getChangeTime(res);
            String txId = RowMapper.getTransactionId(res);
            String segOwner = RowMapper.getSegOwner(res);

            String logMessage = String.format("transactionId = %s, actualScn= %s, committed SCN= %s, userName= %s,segOwner=%s, segName=%s, sequence=%s",
                                    txId, scn, commitScn, userName, segOwner, RowMapper.getSegName(res), RowMapper.getSequence(res));
            // Commit
            if (operationCode == RowMapper.COMMIT) {
                LOGGER.info("COMMIT, {}", logMessage);
                transactionalBuffer.commit(txId, changeTime, context);
            }

            //Rollback
            if (operationCode == RowMapper.ROLLBACK) {
                LOGGER.info("ROLLBACK, {}", logMessage);
                transactionalBuffer.rollback(txId);
            }

            if (!isTableWhitelisted(tableName)) {
                LOGGER.debug("table {} is not in the whitelist, skipped", tableName);
                continue;
            }

            // DDL
            if (operationCode == RowMapper.DDL) {
                LOGGER.info("DDL,  {}", logMessage);
                // todo parse, add to the collection.
            }

            // DML
            if (operationCode == RowMapper.INSERT || operationCode == RowMapper.DELETE || operationCode == RowMapper.UPDATE) {
                LOGGER.info("DML,  {}", logMessage);
                try {
                    dmlParser.parse(redo_sql, schema.getTables());
                    LogMinerRowLcr rowLcr = dmlParser.getDmlChange();
                    if (rowLcr == null) {
                        LOGGER.error("Following statement was not parsed: {}", redo_sql);
                        continue;
                    }
                    rowLcr.setObjectOwner(userName);
                    rowLcr.setSourceTime(changeTime);
                    rowLcr.setTransactionId(txId);
                    rowLcr.setObjectName(tableName);
                    rowLcr.setActualCommitScn(commitScn);
                    rowLcr.setActualScn(scn);
                    TableId tableId = RowMapper.getTableId(catalogName, res);

                    transactionalBuffer.registerCommitCallback(txId, scn, (timestamp, smallestScn) -> {
                        // update SCN in offset context only if processed SCN less than SCN among other transactions
                        if (smallestScn == null || scn.compareTo(smallestScn) < 0) {
                            offsetContext.setScn(scn.longValue());
                        }
                        offsetContext.setTransactionId(txId);
                        offsetContext.setSourceTime(timestamp.toInstant());
                        offsetContext.setTableId(tableId);
                        Table table = schema.tableFor(tableId);
                        LOGGER.debug("Processing DML event {} scn {}", rowLcr.toString(), scn);
                        dispatcher.dispatchDataChangeEvent(tableId,
                                new LogMinerChangeRecordEmitter(offsetContext, rowLcr, table, clock)
                        );
                    });

                } catch (Exception e) {
                    LOGGER.error("Following statement: {} cannot be parsed due to the : {}", redo_sql, e.getMessage());
                }
            }
        }
    }

    private boolean isTableWhitelisted(String tableName) {
        if (tableName == null) {
            return false;
        }
        return schema.getTables().tableIds()
                .stream()
                .anyMatch(tableId -> tableId.table().toLowerCase().contains(tableName.toLowerCase()));
    }


    // todo this is temporary debugging info,  remove.
    private void traceInfo(Connection connection, String info) throws SQLException {
        //String checkQuery = "select member, log.status, log.first_change# from v$log log, v$logfile  f  where log.group#=f.group# order by 1 asc";
        String checkQuery = "select f.member, log.status, log.first_change#, f.TYPE, f.status, f.group#, f.IS_RECOVERY_DEST_FILE, log.archived " +
                "from v$log log, v$logfile f  where log.group#=f.group# and log.status='CURRENT'";

        PreparedStatement st = connection.prepareStatement(checkQuery);
        ResultSet result = st.executeQuery();
        while (result.next()) {
            String fileName = result.getString(1);
            String status = result.getString(2);
            long changeScn = result.getLong(3);
            String fileType = result.getString(4);
            String fileStatus = result.getString(5);
            Long fileGroup = result.getLong(6);
            String dest = result.getString(7);
            String archived = result.getString(8);
            LOGGER.trace(info + "= filename = " + fileName + ", status: " + status + ", first SCN= " + changeScn +
                    ", file type: " + fileType + ", file group: " + fileGroup
                    + ", dest: " + dest + ", archived=" + archived);
        }
        st.close();
        result.close();
    }
}
