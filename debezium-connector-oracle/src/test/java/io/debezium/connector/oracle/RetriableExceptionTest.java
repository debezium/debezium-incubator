/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.sql.SQLRecoverableException;

import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.pipeline.DataChangeEvent;

import oracle.jdbc.driver.OracleSQLException;
import oracle.net.ns.NetException;

/**
 * Unit tests for the Oracle connector's {@link io.debezium.pipeline.ErrorHandler}.
 *
 * @author Chris Cranford
 */
public class RetriableExceptionTest {

    private static OracleErrorHandler errorHandler;

    @BeforeClass
    public static void beforeClass() {
        Configuration config = TestHelper.defaultConfig().build();
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        ChangeEventQueue<?> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .build();

        errorHandler = new OracleErrorHandler(connectorConfig.getLogicalName(), queue);
    }

    @Test
    public void testConnectionProblemIsRetryable() {
        assertThat(errorHandler.isRetriable(new Exception("problem", new IOException("connection")))).isTrue();
        assertThat(errorHandler.isRetriable(new SQLRecoverableException("connection", new OracleSQLException("cause")))).isTrue();
        assertThat(errorHandler.isRetriable(createOracleException("ORA-03135"))).isTrue();
        assertThat(errorHandler.isRetriable(createOracleException("ORA-12543"))).isTrue();
        assertThat(errorHandler.isRetriable(createOracleException("ORA-00604"))).isTrue();
        assertThat(errorHandler.isRetriable(createOracleException("ORA-01089"))).isTrue();
        assertThat(errorHandler.isRetriable(createOracleException("ORA-00604"))).isTrue();
        assertThat(errorHandler.isRetriable(createOracleException("ORA-01333"))).isTrue();
        assertThat(errorHandler.isRetriable(new Exception("NO MORE DATA TO READ FROM SOCKET problem", new OracleSQLException("cause")))).isTrue();
        assertThat(errorHandler.isRetriable(new Exception("fail", new Exception("fail", new NetException(25))))).isTrue();
    }

    @Test
    public void testConnectionProblemIsNotRetryable() {
        assertThat(errorHandler.isRetriable(new Throwable())).isFalse();
        assertThat(errorHandler.isRetriable(new Exception("ORA-99999 problem"))).isFalse();
        assertThat(errorHandler.isRetriable(new Exception("12543 problem"))).isFalse();
    }

    private static Throwable createOracleException(String code) {
        return new Exception(code + " problem", new OracleSQLException("failure"));
    }
}
