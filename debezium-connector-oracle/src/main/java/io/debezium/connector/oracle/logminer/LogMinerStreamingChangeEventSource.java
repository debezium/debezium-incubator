/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerLcr;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
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
        OracleChangeRecordValueConverter converters = new OracleChangeRecordValueConverter(jdbcConnection);

        this.connectorConfig = connectorConfig;

        OracleDmlParser dmlParser = new OracleDmlParser(true, connectorConfig.getPdbName(), connectorConfig.getSchemaName(),
                converters);
        logMinerProcessor = new LogMinerProcessor(schema, dmlParser);
    }

    /**
     * This is the loop to get changes from LogMiner
     *
     * @param context change event source context
     * @throws InterruptedException an exception
     */
    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {

        ResultSet res = null;

        try (Connection connection = jdbcConnection.connection();
             PreparedStatement fetchChangesFromMiningView =
                     connection.prepareStatement(SqlUtils.queryLogMinerContents(connectorConfig.getSchemaName()))) {
            long lastProcessedScn = offsetContext.getScn(); // last processes Scn
            long oldestScnInOnlineRedo = LogMinerHelper.getFirstOnlineLogScn(connection);
            if (lastProcessedScn < oldestScnInOnlineRedo) {
                throw new RuntimeException("Online REDO LOG files don't contain the offset SCN. Clean offset info and start over");
            }

            LogMinerHelper.setNlsSessionParameters(jdbcConnection);

            // 1. Configure Log Miner to mine online logs
            LogMinerHelper.buildDataDictionary(connection);// todo should we renew it after a DDL?
            LogMinerHelper.addOnlineRedoLogFilesForMining(connection);

            // 2. Querying LogMinerRowLcr(s) from Log miner while running
            while (context.isRunning()) {
                LOGGER.trace("Receiving a change from LogMiner");

                lastProcessedScn = offsetContext.getScn();
                long currentScn = LogMinerHelper.getCurrentScn(connection);
                LOGGER.debug("\nstart, lastProcessedScn: " + lastProcessedScn + ", endScn: " + currentScn);
                if (lastProcessedScn == currentScn) {
                    LOGGER.debug("No changes to process...pausing for a second");
                    Metronome metronome = Metronome.sleeper(Duration.ofSeconds(1L), clock);
                    metronome.pause();
                }

                LogMinerHelper.startOnlineMining(connection, lastProcessedScn, currentScn);

                fetchChangesFromMiningView.setLong(1, lastProcessedScn);
                fetchChangesFromMiningView.setLong(2, lastProcessedScn);

                debugInfo(connection, "before");
                res = fetchChangesFromMiningView.executeQuery();
                debugInfo(connection, "after");

                dispatchChanges(res);
                offsetContext.setScn(currentScn);

                res.close();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            // 3. disconnect
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
            offsetContext.setTransactionId(logMinerRowLcr.getTransactionId());
            offsetContext.setSourceTime(logMinerRowLcr.getSourceTime().toInstant());
            offsetContext.setTableId(getTableId(logMinerRowLcr));
            try {
                LOGGER.debug("Processing DML event {} scn {}", logMinerRowLcr, actualScn);
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

    // todo this is temporary debugging info, will be removed. Once we observed a missed DML. This should help to trace if reproducible.
    private void debugInfo(Connection connection, String info) throws SQLException {
        //String checkQuery = "select member, log.status, log.first_change# from v$log log, v$logfile  f  where log.group#=f.group# order by 1 asc";
        String checkQuery = "select member, log.status, log.first_change# from v$log log, v$logfile  f  where log.group#=f.group# and log.status='CURRENT'";

        PreparedStatement st = connection.prepareStatement(checkQuery);
        ResultSet result = st.executeQuery();
        while (result.next()) {
            String fileName = result.getString(1);
            String status = result.getString(2);
            long changeScn = result.getLong(3);
            LOGGER.debug(info + "= filename = " + fileName + ", status: " + status + " SCN= " + changeScn);
        }
        st.close();
        result.close();
    }
}
