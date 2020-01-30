/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2.util;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Testing;

/**
 * @author Horia Chiorean (hchiorea@redhat.com), Luis Garcés-Erice
 */
public class TestHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();
    public static final String TEST_DATABASE = "testdb";
    public static final int WAIT_FOR_CDC = 30 * 1000;

    private static final String STATEMENTS_PLACEHOLDER = "#";

    private static final String ENABLE_DB_CDC = "VALUES ASNCDC.ASNCDCSERVICES('start','asncdc')";
    private static final String DISABLE_DB_CDC = "VALUES ASNCDC.ASNCDCSERVICES('stop','asncdc')";
    private static final String ENABLE_TABLE_CDC = "CALL ASNCDC.ADDTABLE('DB2INST1', '#' )";
    private static final String DISABLE_TABLE_CDC = "CALL ASNCDC.REMOVETABLE('DB2INST1', '#' )";
    private static final String RESTART_ASN_CDC = "VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc')";

    public static JdbcConfiguration adminJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, "testdb")
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 50000)
                .withDefault(JdbcConfiguration.USER, "db2inst1")
                .withDefault(JdbcConfiguration.PASSWORD, "admin")
                .build();
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.DATABASE, TEST_DATABASE)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 50000)
                .withDefault(JdbcConfiguration.USER, "db2inst1")
                .withDefault(JdbcConfiguration.PASSWORD, "admin")
                .build();
    }

    /**
     * Returns a default configuration suitable for most test cases. Can be amended/overridden in individual tests as
     * needed.
     */
    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(Db2ConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        return builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, "testdb")
                .with(Db2ConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH);
    }

    public static Db2Connection adminConnection() {
        return new Db2Connection(TestHelper.adminJdbcConfig());
    }

    public static Db2Connection testConnection() {
        return new Db2Connection(TestHelper.defaultJdbcConfig());
    }

    /**
     * Enables CDC for a given database, if not already enabled.
     *
     * @throws SQLException
     *             if anything unexpected fails
     */
    public static void enableDbCdc(Db2Connection connection) throws SQLException {
        connection.execute(ENABLE_DB_CDC);
    }

    /**
     * Disables CDC for a given database, if not already disabled.
     *
     * @throws SQLException
     *             if anything unexpected fails
     */
    protected static void disableDbCdc(Db2Connection connection) throws SQLException {
        connection.execute(DISABLE_DB_CDC);
    }

    /**
     * Enables CDC for a table if not already enabled and generates the wrapper
     * functions for that table.
     *
     * @param name
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void enableTableCdc(Db2Connection connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        String enableCdcForTableStmt = ENABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, name);
        connection.execute(enableCdcForTableStmt);
        connection.execute(RESTART_ASN_CDC);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER  = 'DB2INST1' AND SOURCE_TABLE = '" + name + "'");
    }

    /**
     * Disables CDC for a table for which it was enabled before.
     *
     * @param name
     *            the name of the table, may not be {@code null}
     * @throws SQLException if anything unexpected fails
     */
    public static void disableTableCdc(Db2Connection connection, String name) throws SQLException {
        Objects.requireNonNull(name);
        String disableCdcForTableStmt = DISABLE_TABLE_CDC.replace(STATEMENTS_PLACEHOLDER, name);
        connection.execute(disableCdcForTableStmt);
        connection.execute(RESTART_ASN_CDC);
    }

    public static void waitForSnapshotToBeCompleted() throws InterruptedException {
        int waitForSeconds = 60;
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        final Metronome metronome = Metronome.sleeper(Duration.ofSeconds(1), Clock.system());

        while (true) {
            if (waitForSeconds-- <= 0) {
                Assert.fail("Snapshot was not completed on time");
            }
            try {
                final boolean completed = (boolean) mbeanServer.getAttribute(new ObjectName("debezium.sql_server:type=connector-metrics,context=snapshot,server=server1"),
                        "SnapshotCompleted");
                if (completed) {
                    break;
                }
            }
            catch (InstanceNotFoundException e) {
                // Metrics has not started yet
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            metronome.pause();
        }
    }

    public static void waitForCDC() {
        try {
            Thread.sleep(WAIT_FOR_CDC);
        }
        catch (Exception e) {

        }
    }
}
