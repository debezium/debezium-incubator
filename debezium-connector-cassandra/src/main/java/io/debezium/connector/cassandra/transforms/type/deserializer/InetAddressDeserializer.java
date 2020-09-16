/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;

import io.debezium.connector.cassandra.transforms.CassandraTypeKafkaSchemaBuilders;
import org.apache.kafka.connect.data.SchemaBuilder;

public class InetAddressDeserializer extends TypeDeserializer {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        InetAddress inetAddress = (InetAddress) super.deserialize(abstractType, bb);
        return inetAddress.toString();
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        return CassandraTypeKafkaSchemaBuilders.STRING_TYPE;
    }
}
