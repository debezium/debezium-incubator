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

import static io.debezium.connector.oracle.OracleConnectorConfig.CONNECTOR_ADAPTER;

public class OracleConnectionFactory implements ConnectionFactory {

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        String driverType = "oci";
        String user = config.getUser();
        String password = config.getPassword();
        String hostName = config.getHostname();
        int port = config.getPort();
        String database = config.getDatabase();

        OracleConnectorConfig oracleConnectorConfig = new OracleConnectorConfig(config);
        String adapterString = config.getString("connection.adapter");
        adapterString =  adapterString == null ?  config.getString(OracleConnectorConfig.CONNECTOR_ADAPTER) : adapterString;
        OracleConnectorConfig.ConnectorAdapter adapter = OracleConnectorConfig.ConnectorAdapter.parse(adapterString);
        if (adapter == OracleConnectorConfig.ConnectorAdapter.LOG_MINER) {
            driverType = "thin";
        }

        return DriverManager.getConnection(
              "jdbc:oracle:" + driverType + ":@" + hostName + ":" + port + "/" + database, user, password
        );
    }
}
