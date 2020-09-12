/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class MapTypeDeserializer extends CollectionTypeDeserializer<MapType<?, ?>> {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Map<?, ?> deserializedMap = (Map<?, ?>) super.deserialize(abstractType, bb);
        Map<Object, Object> convertedDeserializedMap = new HashMap<>();
        MapType<?, ?> mapType = (MapType<?, ?>) abstractType;
        AbstractType<?> keysType = mapType.getKeysType();
        AbstractType<?> valuesType = mapType.getValuesType();
        for (Map.Entry entry : deserializedMap.entrySet()) {
            Object convertedKey = CassandraTypeDeserializer.convertDeserializedValue(keysType, entry.getKey());
            Object convertedValue = CassandraTypeDeserializer.convertDeserializedValue(valuesType, entry.getValue());
            convertedDeserializedMap.put(convertedKey, convertedValue);
        }
        return Values.convertToMap(getSchemaBuilder(abstractType).build(), convertedDeserializedMap);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        MapType<?, ?> mapType = (MapType<?, ?>) abstractType;
        AbstractType<?> keysType = mapType.getKeysType();
        AbstractType<?> valuesType = mapType.getValuesType();
        Schema keySchema = CassandraTypeDeserializer.getSchemaBuilder(keysType).build();
        Schema valuesSchema = CassandraTypeDeserializer.getSchemaBuilder(valuesType).build();
        return SchemaBuilder.map(keySchema, valuesSchema).optional();
    }

    @Override
    public Object deserialize(MapType<?, ?> mapType, ComplexColumnData ccd) {
        List<ByteBuffer> bbList = mapType.serializedValues(ccd.iterator());
        AbstractType<?> keyType = mapType.getKeysType();
        AbstractType<?> valueType = mapType.getValuesType();

        Map<Object, Object> deserializedMap = new HashMap<>();
        int i = 0;
        while (i < bbList.size()) {
            ByteBuffer kbb = bbList.get(i++);
            ByteBuffer vbb = bbList.get(i++);
            deserializedMap.put(super.deserialize(keyType, kbb), super.deserialize(valueType, vbb));
        }

        return Values.convertToMap(getSchemaBuilder(mapType).build(), deserializedMap);
    }
}
