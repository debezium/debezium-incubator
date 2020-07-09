/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.util;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectionFactory;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.FileDatabaseHistory;
import io.debezium.util.Testing;

import java.nio.file.Path;
import java.sql.SQLException;

public class TestHelper {

//    private static final String HOST = "10.47.100.32"; //Roby
    private static final String HOST = "10.47.100.62"; //Development
    private static final String SCHEMA_USER = "debezium";
    private static final String SCHEMA_PASS = "dbz";
    private static final String DATABASE = "ORA19C_PDB01"; //dev
//    private static final String DATABASE = "ORCLPDB";//qa
    public static final Path DB_HISTORY_PATH = Testing.Files.createTestingPath("file-db-history-connect.txt").toAbsolutePath();

//    public static final String CONNECTOR_USER = "c##xstrm";// qa
    public static final String CONNECTOR_USER = "c##logminer"; // dev
//    public static final String CONNECTOR_USER_PASS = "xs";         //qa
    public static final String CONNECTOR_USER_PASS = "lm"; //dev

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, 1521)
                .withDefault(JdbcConfiguration.USER, CONNECTOR_USER)
                .withDefault(JdbcConfiguration.PASSWORD, CONNECTOR_USER_PASS)
                .withDefault(JdbcConfiguration.DATABASE, "ORA19C")
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
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value)
        );

        return builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, "server1")
                .with(OracleConnectorConfig.PDB_NAME, DATABASE)
                .with(OracleConnectorConfig.XSTREAM_SERVER_NAME, "dbzxout")
                .with(OracleConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class)
                .with(OracleConnectorConfig.SCHEMA_NAME, SCHEMA_USER)
                .with(FileDatabaseHistory.FILE_PATH, DB_HISTORY_PATH);
    }

    public static OracleConnection defaultConnection() {
        Configuration config = defaultConfig().build();
        Configuration jdbcConfig = config.subset("database.", true);

        OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, new OracleConnectionFactory());

        String pdbName = new OracleConnectorConfig(config).getPdbName();

        if (pdbName != null) {
            jdbcConnection.setSessionToPdb(pdbName);
        }

        return jdbcConnection;
    }

    /**
     * Database level connection.
     * this is PDB level connector with LogMiner adapter
     * @return OracleConnection
     */
    public static OracleConnection logMinerPdbConnection() {
        Configuration jdbcConfig = testJdbcConfig().edit()
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "LogMiner")
                .with(OracleConnectorConfig.DRIVER_TYPE, "thin")
                .build();
        return new OracleConnection(jdbcConfig, new OracleConnectionFactory());
    }

    /**
     * Returns a JDBC configuration for the test data schema and user (NOT the XStream user).
     */
    private static JdbcConfiguration testJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, 1521)
                .withDefault(JdbcConfiguration.USER, SCHEMA_USER)
                .withDefault(JdbcConfiguration.PASSWORD, SCHEMA_PASS)
                .withDefault(JdbcConfiguration.DATABASE, DATABASE)
                .build();
    }

    /**
     * Returns a JDBC configuration for database admin user (NOT the XStream user).
     */
    private static JdbcConfiguration adminJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties("database.admin."))
                .withDefault(JdbcConfiguration.HOSTNAME, HOST)
                .withDefault(JdbcConfiguration.PORT, 1521)
                .withDefault(JdbcConfiguration.USER, "sys as sysdba")
                .withDefault(JdbcConfiguration.PASSWORD, "top_secret")
                .withDefault(JdbcConfiguration.DATABASE, DATABASE)
                .build();
    }

    public static Configuration.Builder testConfig() {
        JdbcConfiguration jdbcConfiguration = testJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value)
        );

        return builder;
    }

    private static Configuration.Builder adminConfig() {
        JdbcConfiguration jdbcConfiguration = adminJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(OracleConnectorConfig.DATABASE_CONFIG_PREFIX + field, value)
        );

        return builder;
    }

    public static OracleConnection testConnection() {
        Configuration config = testConfig().build();
        Configuration jdbcConfig = config.subset("database.", true);

        OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, new OracleConnectionFactory());
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String pdbName = new OracleConnectorConfig(config).getPdbName();

        if (pdbName != null) {
            jdbcConnection.setSessionToPdb(pdbName);
        }

        return jdbcConnection;
    }

    public static OracleConnection adminConnection() {
        Configuration config = adminConfig().build();
        Configuration jdbcConfig = config.subset("database.", true);

        OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, new OracleConnectionFactory());
        try {
            jdbcConnection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String pdbName = new OracleConnectorConfig(config).getPdbName();

        if (pdbName != null) {
            jdbcConnection.setSessionToPdb(pdbName);
        }

        return jdbcConnection;
    }

    public static void dropTable(OracleConnection connection, String table) {
        try {
            connection.execute("drop table " + table);
        }
        catch (SQLException e) {
            if (!e.getMessage().contains("table or view does not exist")) {
                throw new RuntimeException(e);
            }
        }
    }

    public static int defaultMessageConsumerPollTimeout() {
        return 120;
    }
}
