/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.LegacyV1AbstractSourceInfoStructMaker;
import io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class LegacyV1CassandraSourceInfoStructMaker extends LegacyV1AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public LegacyV1CassandraSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        this.schema = commonSchemaBuilder()
                .name(SchemaNameAdjuster.defaultAdjuster().adjust(Record.SOURCE))
                .field(SourceInfo.CLUSTER_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.COMMITLOG_FILENAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.COMMITLOG_POSITION_KEY, Schema.INT32_SCHEMA)
                .field(SourceInfo.SNAPSHOT_KEY, Schema.BOOLEAN_SCHEMA)
                .field(SourceInfo.KEYSPACE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TIMESTAMP_KEY, CassandraTypeKafkaSchemaBuilders.TIMESTAMP_MICRO_TYPE)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        return super.commonStruct()
                .put(SourceInfo.CLUSTER_KEY, sourceInfo.cluster)
                .put(SourceInfo.COMMITLOG_FILENAME_KEY, sourceInfo.offsetPosition.fileName)
                .put(SourceInfo.COMMITLOG_POSITION_KEY, sourceInfo.offsetPosition.filePosition)
                .put(SourceInfo.SNAPSHOT_KEY, sourceInfo.snapshot)
                .put(SourceInfo.KEYSPACE_NAME_KEY, sourceInfo.keyspaceTable.keyspace)
                .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.keyspaceTable.table)
                .put(SourceInfo.TIMESTAMP_KEY, sourceInfo.tsMicro);
    }

}
