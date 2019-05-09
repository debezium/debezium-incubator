/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;
import static io.debezium.connector.oracle.OracleConnectorConfig.CONNECTOR_ADAPTER;

public class OracleConnectionFactory implements ConnectionFactory {

    private String driverType;

    public OracleConnectionFactory(Configuration config) {
        OracleConnectorConfig.ConnectorAdapter adapter  =
                OracleConnectorConfig.ConnectorAdapter.parse(config.getString(CONNECTOR_ADAPTER));
        if (adapter == OracleConnectorConfig.ConnectorAdapter.XSTREAM){
            driverType = "oci";
        } else {
            driverType = "thin";
        }
    }

    @Override
    public Connection connect(JdbcConfiguration config) throws SQLException {
        String hostName = config.getHostname();
        int port = config.getPort();
        String database = config.getDatabase();
        String user = config.getUser();
        String password = config.getPassword();

        return DriverManager.getConnection(
              "jdbc:oracle:" + driverType + ":@" + hostName + ":" + port + "/" + database, user, password
        );
    }
}
