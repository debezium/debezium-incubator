/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import java.util.function.Supplier;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.spi.OffsetContext.Loader;
import io.debezium.relational.RelationalConnectorTask;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext.PreviousContext;
import io.debezium.util.SchemaNameAdjuster;

public class OracleConnectorTask extends RelationalConnectorTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnectorTask.class);
    private static final String CONTEXT_NAME = "oracle-connector-task";

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected StartingContext getStartingContext(Configuration config) {
        OracleConnectorConfig connectorConfig = new OracleConnectorConfig(config);
        OracleTaskContext taskContext = new OracleTaskContext(connectorConfig);
        TopicSelector<TableId> topicSelector = OracleTopicSelector.defaultSelector(connectorConfig);

        Configuration jdbcConfig = config.subset("database.", true);
        OracleConnection jdbcConnection = new OracleConnection(jdbcConfig, new OracleConnectionFactory());

        OracleDatabaseSchema schema = new OracleDatabaseSchema(connectorConfig, SchemaNameAdjuster.create(LOGGER), topicSelector, jdbcConnection);

        return new StartingContext() {

            @Override
            public TopicSelector<TableId> getTopicSelector() {
                return topicSelector;
            }

            @Override
            public Loader getOffsetContextLoader() {
                return new OracleOffsetContext.Loader(connectorConfig.getLogicalName());
            }

            @Override
            public Supplier<PreviousContext> getLoggingContextSupplier() {
                return () -> taskContext.configureLoggingContext(CONTEXT_NAME);
            }

            @Override
            public JdbcConnection getJdbcConnection() {
                return jdbcConnection;
            }

            @Override
            public RelationalDatabaseSchema getDatabaseSchema() {
                return schema;
            }

            @Override
            public RelationalDatabaseConnectorConfig getConnectorConfig() {
                return connectorConfig;
            }

            @Override
            public Class<? extends SourceConnector> getConnectorClass() {
                return OracleConnector.class;
            }

            @Override
            public ChangeEventSourceFactory getChangeEventSourceFactory(ErrorHandler errorHandler,
                    EventDispatcher<TableId> dispatcher, Clock clock) {
                return new OracleChangeEventSourceFactory(connectorConfig, jdbcConnection, errorHandler, dispatcher, clock, schema);
            }
        };
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return OracleConnectorConfig.ALL_FIELDS;
    }
}
