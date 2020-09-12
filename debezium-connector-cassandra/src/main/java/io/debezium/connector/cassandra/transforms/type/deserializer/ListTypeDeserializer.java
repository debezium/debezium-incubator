/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class ListTypeDeserializer extends CollectionTypeDeserializer<ListType<?>> {

    @Override
    public Object deserialize(AbstractType<?> abstractType, ByteBuffer bb) {
        List<?> deserializedList = (List<?>) super.deserialize(abstractType, bb);
        List<Object> convertedDeserializedList = new ArrayList<>();
        AbstractType<?> elementsType = ((ListType<?>) abstractType).getElementsType();
        for (Object value : deserializedList) {
            convertedDeserializedList.add(CassandraTypeDeserializer.convertDeserializedValue(elementsType, value));
        }
        return Values.convertToList(getSchemaBuilder(abstractType).build(), convertedDeserializedList);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(AbstractType<?> abstractType) {
        ListType<?> listType = (ListType<?>) abstractType;
        AbstractType<?> elementsType = listType.getElementsType();
        Schema innerSchema = CassandraTypeDeserializer.getSchemaBuilder(elementsType).build();
        return SchemaBuilder.array(innerSchema).optional();
    }

    @Override
    public Object deserialize(ListType<?> listType, ComplexColumnData ccd) {
        List<ByteBuffer> bbList = listType.serializedValues(ccd.iterator());
        AbstractType<?> elementsType = listType.getElementsType();

        List<Object> deserializedList = new ArrayList<>(bbList.size());
        for (ByteBuffer bb : bbList) {
            deserializedList.add(super.deserialize(elementsType, bb));
        }

        return Values.convertToList(getSchemaBuilder(listType).build(), deserializedList);
    }
}
