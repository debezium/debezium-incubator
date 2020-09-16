/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import org.apache.cassandra.db.marshal.AbstractType;

public abstract class LogicalTypeDeserializer extends TypeDeserializer {

    /**
     * Convert the deserialized value from Cassandra to an object that fits kafka schema type
     * @param abstractType the {@link AbstractType} of a column in cassandra
     * @param value the deserialized value of a column in cassandra
     * @return the object converted from deserialized value
     */
    public abstract Object convertDeserializedValue(AbstractType<?> abstractType, Object value);

    /**
     * Check if a TypeDeserializer extends from LogicalTypeDeserializer
     * @param typeDeserializer TypeDeserializer
     * @return true if the TypeDeserializer extends from LogicalTypeDeserializer
     */
    public static boolean isParentOf(TypeDeserializer typeDeserializer) {
        return LogicalTypeDeserializer.class.isAssignableFrom(typeDeserializer.getClass());
    }
}
