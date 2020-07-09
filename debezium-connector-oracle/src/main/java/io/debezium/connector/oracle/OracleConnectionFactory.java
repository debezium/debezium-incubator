/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleConnectionFactory implements ConnectionFactory {

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        final String driverType = config.getString(OracleConnectorConfig.DRIVER_TYPE);
        final String user = config.getUser();
        final String password = config.getPassword();
        final String hostName = config.getHostname();
        int port = config.getPort();
        final String database = config.getDatabase();
        final String url = "jdbc:oracle:" + driverType + ":@" + hostName + ":" + port + "/" + database;

        return DriverManager.getConnection(url, user, password);
    }
}
