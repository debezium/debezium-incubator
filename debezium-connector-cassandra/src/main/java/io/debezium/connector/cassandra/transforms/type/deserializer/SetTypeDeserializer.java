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

public class SetTypeDeserializer extends TypeDeserializer {

    @Override
    @SuppressWarnings("unchecked")
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        Set<?> deserializedSet = (Set<?>) super.deserialize(abstractType, bb);
        List<?> deserializedList = (new ArrayList<>(deserializedSet));
        return Values.convertToList(getSchemaBuilder(abstractType).build(), deserializedList);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        SetType<?> listType = (SetType<?>) abstractType;
        AbstractType<?> elementsType = listType.getElementsType();
        Schema innerSchema = CassandraTypeDeserializer.getSchemaBuilder(elementsType).build();
        return SchemaBuilder.array(innerSchema);
    }

    @Override
    public Object deserialize(AbstractType<?> abstractType, ComplexColumnData ccd) {
        List<ByteBuffer> bbList = ((SetType) abstractType).serializedValues(ccd.iterator());
        AbstractType innerType = ((SetType) abstractType).getElementsType();
        Set<Object> deserializedSet = new HashSet<>();
        for (int i = 0; i < bbList.size(); i++) {
            ByteBuffer bb = bbList.get(i);
            deserializedSet.add(super.deserialize(innerType, bb));
        }
        List<Object> deserializedList = new ArrayList<>(deserializedSet);
        return Values.convertToList(getSchemaBuilder(abstractType).build(), deserializedList);
    }
}
