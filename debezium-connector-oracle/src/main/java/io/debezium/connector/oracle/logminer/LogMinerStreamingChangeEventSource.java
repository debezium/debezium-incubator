/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.OracleValuePreConverter;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerLcr;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 * The event handler loop is executed in a separate executor.
 */
public class LogMinerStreamingChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerStreamingChangeEventSource.class);

    private final JdbcConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;

    //private final int posVersion;

    private OracleConnectorConfig connectorConfig;
    // todo make it configurable, depending on the PGA limit and the DB CPU number.
    private int batchSize = 10;
    private final LogMinerProcessor logMinerProcessor;

    public LogMinerStreamingChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext, JdbcConnection jdbcConnection, EventDispatcher<TableId> dispatcher, Clock clock, OracleDatabaseSchema schema) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = connectorConfig.getTablenameCaseInsensitive();
        //this.posVersion = connectorConfig.getOracleVersion().getPosVersion();
        OracleValueConverters converters = new OracleValueConverters(jdbcConnection);
        OracleValuePreConverter preConverter = new OracleValuePreConverter();

        this.connectorConfig = connectorConfig;

        OracleDmlParser dmlParser = new OracleDmlParser(true, connectorConfig.getPdbName(),
                connectorConfig.getSchemaName(),
                converters,
                preConverter);
        logMinerProcessor = new LogMinerProcessor(schema, dmlParser);
    }

    /**
     * The workflow is:
     * 1. Mine archived log
     *   Check if current offset SCN corresponds to an archived log and mine it.
     * - fetch archived logs info which are older than the offset SCN
     * - Loop them while current offset is in an archived log file.
     *   Each iteration might take time and during this processing more archives could be created.
     * NOTE.  Currently if DDL occurred after a log file was archived, the archived log mining may fail due to the data dictionary changes.
     *          We set mining option having data dictionary in online catalog for this step.
     *          This is due to the performance and to avoid "no log file" exception.
     *          todo We will investigate it further
     * Inside the loop:
     * - add a batch for mining analysis
     * - build mining view
     * - get changes, dispatch them
     * - delete processed archived logs from mining analysis, update offset
     * NOTE.  Processing of an archived files batch may take > 1 minute.
     *          The longer the connector is down, the longer "catch up" time is required.
     * 2. Build data dictionary objects in online redo log files. Add all online redo log files for mining
     * 3. Fetch changes from this view, dispatch them
     * NOTE. Steps 2 and 3 also take time and new archived log(s) could be created
     * todo address this scenario
     * 4. Close resources
     *
     * @param context change event source context
     * @throws InterruptedException an exception
     */
    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {

        ResultSet res = null;
        long startScn;

        try (Connection connection = jdbcConnection.connection();
             PreparedStatement fetchChangesFromMiningView = connection.prepareStatement(SqlUtils.queryLogMinerContents(connectorConfig.getSchemaName()))) {
            // 1. Mine archived logs
            while (true) {

                startScn = offsetContext.getScn(); // last processes Scn
                Map<String, Long> archivedLogFiles = LogMinerHelper.getArchivedLogFiles(startScn, connection);
                if (archivedLogFiles.isEmpty()) {
                    LOGGER.info("All archived log files were processed successfully");
                    break;
                }

                for (Iterator<Map.Entry<String, Long>> archivedFile = archivedLogFiles.entrySet().iterator(); archivedFile.hasNext();) {
                    ResultSet resultset = LogMinerHelper.getArchivedChanges(connection, batchSize, connectorConfig.getSchemaName(), archivedFile);
                    dispatchChanges(resultset);
                    resultset.close();
                }

                // delete archived log from mining
                for (Map.Entry<String, Long> archivedFile : archivedLogFiles.entrySet()) {
                    LogMinerHelper.removeArchiveLogFile(archivedFile.getKey(), connection);
                    offsetContext.setScn(archivedFile.getValue());
                }
            }
            // 2. Configure Log Miner to mine online logs
            LogMinerHelper.buildDataDictionary(connection);// todo should we renew it after a DDL?
            LogMinerHelper.addOnlineRedoLogFilesForMining(connection);

            // 3. Querying LogMinerRowLcr(s) from Log miner while running

            while (context.isRunning()) {
                LOGGER.trace("Receiving LogMinerLcr");

                startScn = offsetContext.getScn();
                long currentScn = LogMinerHelper.getCurrentScn(connection);
                LOGGER.debug("\nstart, startScn: " + startScn + ", endScn: " + currentScn);

                LogMinerHelper.startOnlineMining(connection, startScn, currentScn);

                fetchChangesFromMiningView.setLong(1, startScn);
                fetchChangesFromMiningView.setLong(2, startScn);

                // todo this is temporary debugging info, will be removed. Once we observed a missed DML. This should help to trace if reproducible.
//                String checkQuery = "select member, log.status, log.first_change# from v$log log, v$logfile  f  where log.group#=f.group# order by 1 asc";
                String checkQuery = "select member, log.status, log.first_change# from v$log log, v$logfile  f  where log.group#=f.group# and log.status='CURRENT'";
                PreparedStatement st = connection.prepareStatement(checkQuery);
                ResultSet result = st.executeQuery();
                while (result.next()){
                    String fileName = result.getString(1);
                    String status  = result.getString(2);
                    long changeScn = result.getLong(3);
                    LOGGER.debug("before= filename = " + fileName + ", status: " + status + " SCN= " + changeScn);
                }

                res = fetchChangesFromMiningView.executeQuery();

                // todo remove
                result = st.executeQuery();
                while (result.next()){
                    String fileName = result.getString(1);
                    String status  = result.getString(2);
                    long changeScn = result.getLong(3);
                    LOGGER.debug("after= filename = " + fileName + ", status: " + status + " SCN= " + changeScn);
                }

                dispatchChanges(res);
                offsetContext.setScn(currentScn);

                res.close();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            // 4. disconnect
            try (Connection connection = jdbcConnection.connection()) {
                LogMinerHelper.endMining(connection);
            } catch (SQLException e) {
                LOGGER.error("Cannot close Log Miner session gracefully: " + e.getMessage());
            }
            if (res != null) {
                try {
                    res.close();
                } catch (SQLException e) {
                    LOGGER.error("Cannot close result set due to the :" + e.getMessage());
                }
            }
            try {
                jdbcConnection.close();
            } catch (SQLException e) {
                LOGGER.error("Cannot close JDBC connection: " + e.getMessage());
            }
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // todo nothing here?
    }

    private void dispatchChanges(ResultSet res) throws SQLException {

        LOGGER.debug("dispatching changes..");
        List<LogMinerRowLcr> changes = logMinerProcessor.process(res);

        for (LogMinerRowLcr logMinerRowLcr : changes) {

            long actualScn = logMinerRowLcr.getActualScn();
            offsetContext.setScn(actualScn);
            //offsetContext.setLcrPosition(new LcrPosition());
            offsetContext.setTransactionId(logMinerRowLcr.getTransactionId());// todo this is't included into the SourceRecord
            offsetContext.setSourceTime(logMinerRowLcr.getSourceTime().toInstant());
            offsetContext.setTableId(getTableId(logMinerRowLcr));
            try {
                LOGGER.debug("Processing DML event {}", logMinerRowLcr);
                TableId tableId = getTableId(logMinerRowLcr);
                dispatcher.dispatchDataChangeEvent(
                        tableId,
                        new ChangeRecordEmitter(offsetContext, logMinerRowLcr, schema.tableFor(tableId), clock)
                );
            } catch (InterruptedException e) {
                LOGGER.error("Following SCN: " + logMinerRowLcr.getActualScn() + " cannot be dispatched due to the : " + e);
            }
        }
    }

    private TableId getTableId(LogMinerLcr lcr) {
        if (!this.tablenameCaseInsensitive) {
            return new TableId(lcr.getSourceDatabaseName(), lcr.getObjectOwner(), lcr.getObjectName());
        } else {
            return new TableId(lcr.getSourceDatabaseName().toLowerCase(), lcr.getObjectOwner(), lcr.getObjectName().toLowerCase());
        }
    }
}
