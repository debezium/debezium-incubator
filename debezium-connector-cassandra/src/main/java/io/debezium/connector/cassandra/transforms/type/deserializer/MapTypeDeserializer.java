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
        deserializedMap = convertDeserializedElementsIfNecessary(abstractType, deserializedMap);
        return Values.convertToMap(getSchemaBuilder(abstractType).build(), deserializedMap);
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

    private Map<?, ?> convertDeserializedElementsIfNecessary(AbstractType<?> abstractType, Map<?, ?> deserializedMap) {
        MapType<?, ?> mapType = (MapType<?, ?>) abstractType;
        AbstractType<?> keysType = mapType.getKeysType();
        AbstractType<?> valuesType = mapType.getValuesType();
        TypeDeserializer keysTypeDeserializer = CassandraTypeDeserializer.getTypeDeserializer(keysType);
        TypeDeserializer valuesTypeDeserializer = CassandraTypeDeserializer.getTypeDeserializer(valuesType);
        Map<Object, Object> resultedMap = new HashMap<>();
        for (Map.Entry entry : deserializedMap.entrySet()) {
            Object key = entry.getKey();
            if (CassandraTypeDeserializer.isLogicalTypeDeserializer(keysTypeDeserializer)) {
                key = ((LogicalTypeDeserializer) keysTypeDeserializer).convertDeserializedValue(keysType, key);
            }
            Object value = entry.getValue();
            if (CassandraTypeDeserializer.isLogicalTypeDeserializer(valuesTypeDeserializer)) {
                value = ((LogicalTypeDeserializer) valuesTypeDeserializer).convertDeserializedValue(valuesType, value);
            }
            resultedMap.put(key, value);
        }
        return resultedMap;
    }
}
