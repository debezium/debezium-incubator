/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.logwriter;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * A {@link LogWriterFlushStrategy} for Oracle RAC that performs a transaction commit to flush
 * the Oracle LogWriter (LGWR) in-memory redo buffers to disk.
 *
 * @author Chris Cranford
 */
public class RacCommitLogWriterFlushStrategy implements LogWriterFlushStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(RacCommitLogWriterFlushStrategy.class);

    private final Map<String, CommitLogWriterFlushStrategy> flushStrategies = new HashMap<>();
    private final JdbcConfiguration jdbcConfiguration;
    private final Set<String> hosts;

    public RacCommitLogWriterFlushStrategy(JdbcConfiguration jdbcConfiguration, OracleConnectorConfig connectorConfig) {
        this.jdbcConfiguration = jdbcConfiguration;
        this.hosts = connectorConfig.getRacNodes().stream().map(String::toUpperCase).collect(Collectors.toSet());

        recreateRacNodeFlushStrategies();
    }

    @Override
    public void flush(long currentScn) {
        // Oracle RAC has one LogWriter (LGWR) process per node (instance).
        // For this configuration, all LGWR processes across all instances must be flushed.
        // Queries cannot be used such as gv_instance as not all nodes could be load balanced.
        Instant startTime = Instant.now();
        if (flushStrategies.isEmpty()) {
            throw new DebeziumException("No RAC node addresses supplied or currently connected");
        }

        boolean errors = false;
        for (Map.Entry<String, CommitLogWriterFlushStrategy> entry : flushStrategies.entrySet()) {
            try {
                // Flush the node's commit log writer
                entry.getValue().flush(currentScn);
            }
            catch (Exception e) {
                LOGGER.trace("Cannot flush LogWriter (LGWR) buffer of node {}", entry.getKey(), e);
                errors = true;
            }
        }

        if (errors) {
            // If there are errors, its best to cleanup and recreate all rac nodes connections
            // Additionally, we'll wait 3 seconds to let Oracle do the flush itself.
            recreateRacNodeFlushStrategies();
            Metronome metronome = Metronome.sleeper(Duration.ofSeconds(3), Clock.SYSTEM);
            try {
                metronome.pause();
            }
            catch (InterruptedException e) {
                LOGGER.warn("Failed to wait 3 seconds for Oracle LogWriter (LGWR) flush", e);
            }
        }
        LOGGER.trace("RAC node LogWriter (LGWR) flush took {}", Duration.between(startTime, Instant.now()));
    }

    @Override
    public void close() {
        // For each RAC node, close the commit log flush strategy
        flushStrategies.values().forEach(CommitLogWriterFlushStrategy::close);
        // Clear the strategies
        flushStrategies.clear();
    }

    private void recreateRacNodeFlushStrategies() {
        // Close existing flush strategies to RAC nodes
        for (CommitLogWriterFlushStrategy strategy : flushStrategies.values()) {
            try {
                strategy.close();
            }
            catch (Exception e) {
                LOGGER.warn("Cannot close existing RAC flush connection", e);
            }
        }
        // Clear map
        flushStrategies.clear();
        // Create strategies by host
        for (String host : hosts) {
            try {
                flushStrategies.put(host, createRacHostStrategy(jdbcConfiguration, host));
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to create flush strategy for RAC node '" + host + "'", e);
            }
        }
    }

    private CommitLogWriterFlushStrategy createRacHostStrategy(JdbcConfiguration jdbcConfig, String host) throws SQLException {
        JdbcConfiguration hostConfig = JdbcConfiguration.adapt(jdbcConfig.edit().with(JdbcConfiguration.DATABASE, host).build());
        OracleConnection connection = new OracleConnection(hostConfig, () -> getClass().getClassLoader());
        connection.setAutoCommit(false);

        // Oracle RAC node connections are separate than the connection to the primary database.
        // As such, we provide a close handler that is responsible for closing these connections
        // in the event that this strategy's close method is called or if the connections need
        // to be re-established due to some connectivity error.
        return new CommitLogWriterFlushStrategy(connection, RacCommitLogWriterFlushStrategy::closeRacConnectionHandler);
    }

    /**
     * Callback used to handle closing the Oracle RAC node's database connection when the flush strategy
     * is requested to be closed.
     *
     * @param connection the rac node's specific database connection
     */
    private static void closeRacConnectionHandler(OracleConnection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.warn("Cannot close existing RAC flush connection", e);
        }
    }
}
