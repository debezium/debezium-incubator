/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.oracle.jsql.OracleDmlParser;
import io.debezium.jdbc.JdbcConnection;
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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Map;

/**
 * A {@link StreamingChangeEventSource} based on Oracle's LogMiner utility.
 */
public class LogMinerChangeEventSource implements StreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerChangeEventSource.class);

    private final OracleConnection jdbcConnection;
    private final EventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final OracleDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final OracleDmlParser parser;
    private final String catalogName;
    private final Duration pollInterval;
    private final LogMiner logMiner;
    private final TransactionalBuffer transactionalBuffer;

    public LogMinerChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext offsetContext, OracleConnection jdbcConnection,
            EventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, OracleDatabaseSchema schema) {
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.parser = new OracleDmlParser();
        this.catalogName = (connectorConfig.getPdbName() != null) ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName();
        this.pollInterval = connectorConfig.getPollInterval();
        this.logMiner = new LogMiner(jdbcConnection);
        this.transactionalBuffer = new TransactionalBuffer(connectorConfig.getLogicalName(), errorHandler);
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        Metronome metronome = Metronome.sleeper(pollInterval, clock);
        logMiner.followTables(schema.tableIds());
        try {
            logMiner.setNlsSessionParameters();
            BigDecimal lastProcessedScn = BigDecimal.valueOf(offsetContext.getScn());
            LOGGER.info("Last SCN recorded in offsets is {}", lastProcessedScn);
            // 1. receive events while running
            while (context.isRunning()) {
                BigDecimal currentScn = jdbcConnection.getCurrentScn();
                // there is no change in the database
                if (currentScn.equals(lastProcessedScn)) {
                    LOGGER.debug("No change in the database");
                    metronome.pause();
                    continue;
                }
                try {
                    logMiner.start(lastProcessedScn, currentScn);
                    LOGGER.trace("Reading LogMiner contents from {} to {} SCN", lastProcessedScn, currentScn);
                    logMiner.getContents(lastProcessedScn, handleLogMinerContents(context));
                }
                finally {
                    // ending this session helps reduce PGA memory usage
                    try {
                        logMiner.end();
                    }
                    catch (SQLException e) {
                        LOGGER.error("Couldn't close LogMiner session", e);
                    }
                }
                // update SCN in offset context only if buffer is empty
                if (transactionalBuffer.isEmpty()) {
                    offsetContext.setOffsetScn(currentScn.longValue());
                }
                lastProcessedScn = currentScn;
            }
        }
        catch (Exception e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {
            // 2. stop
            transactionalBuffer.close();
        }
    }

    private JdbcConnection.BlockingResultSetConsumer handleLogMinerContents(ChangeEventSourceContext context) {
        return rs -> {
            while (rs.next() && context.isRunning()) {
                String transactionId = logMiner.getTransactionId(rs);
                int operationCode = logMiner.getOperationCode(rs);
                if (operationCode == LogMiner.ROLLBACK) {
                    transactionalBuffer.rollback(transactionId);
                }
                else if (operationCode == LogMiner.COMMIT) {
                    Timestamp timestamp = logMiner.getTimestamp(rs);
                    transactionalBuffer.commit(transactionId, timestamp, context);
                }
                else {
                    BigDecimal scn = logMiner.getScn(rs);
                    TableId tableId = logMiner.getTableId(catalogName, rs);
                    String sqlRedo = logMiner.getSqlRedo(rs);
                    LOGGER.trace("DML => transactionId: {}, scn: {}, tableId: {}, sqlRedo: {}", transactionId, scn, tableId, sqlRedo);
                    transactionalBuffer.registerCommitCallback(transactionId, scn, (timestamp, smallestScn) -> {
                        offsetContext.setScn(scn.longValue());
                        // update SCN in offset context only if processed SCN less than SCN among other transactions
                        if (smallestScn == null || scn.compareTo(smallestScn) < 0) {
                            offsetContext.setOffsetScn(scn.longValue());
                        }
                        offsetContext.setTransactionId(transactionId);
                        offsetContext.setSourceTime(timestamp.toInstant());
                        Table table = schema.tableFor(tableId);
                        dispatcher.dispatchDataChangeEvent(tableId,
                                new LogMinerChangeRecordEmitter(offsetContext, parser.parse(sqlRedo), table, clock)
                        );
                    });
                }
            }
        };
    }

    @Override
    public void commitOffset(Map<String, ?> map) {

    }
}
