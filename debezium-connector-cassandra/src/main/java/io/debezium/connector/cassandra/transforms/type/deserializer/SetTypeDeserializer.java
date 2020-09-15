/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class SetTypeDeserializer extends CollectionTypeDeserializer<SetType<?>> {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Set<?> deserializedSet = (Set<?>) super.deserialize(abstractType, bb);
        List<?> deserializedList = convertDeserializedElementsIfNecessary(abstractType, deserializedSet);
        return Values.convertToList(getSchemaBuilder(abstractType).build(), deserializedList);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        AbstractType<?> elementsType = ((SetType<?>) abstractType).getElementsType();
        Schema innerSchema = CassandraTypeDeserializer.getSchemaBuilder(elementsType).build();
        return SchemaBuilder.array(innerSchema).optional();
    }

    @Override
    public Object deserialize(SetType<?> setType, ComplexColumnData ccd) {
        List<ByteBuffer> bbList = setType.serializedValues(ccd.iterator());
        AbstractType<?> elementsType = setType.getElementsType();
        Set<Object> deserializedSet = new HashSet<>();
        for (ByteBuffer bb : bbList) {
            deserializedSet.add(super.deserialize(elementsType, bb));
        }
        List<Object> deserializedList = new ArrayList<>(deserializedSet);
        return Values.convertToList(getSchemaBuilder(setType).build(), deserializedList);
    }

    public List<Object> convertDeserializedElementsIfNecessary(AbstractType<?> abstractType, Set<?> deserializedSet) {
        AbstractType<?> elementsType = ((SetType<?>) abstractType).getElementsType();
        TypeDeserializer elementsTypeDeserializer = CassandraTypeDeserializer.getTypeDeserializer(elementsType);
        if (CassandraTypeDeserializer.isLogicalTypeDeserializer(elementsTypeDeserializer)) {
            List<Object> convertedDeserializedList = new ArrayList<>();
            for (Object element : deserializedSet) {
                Object convertedValue = ((LogicalTypeDeserializer) elementsTypeDeserializer).convertDeserializedValue(elementsType, element);
                convertedDeserializedList.add(convertedValue);
            }
            return convertedDeserializedList;
        }
        return new ArrayList<>(deserializedSet);
    }
}
