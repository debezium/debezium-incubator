/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import org.apache.cassandra.db.marshal.AbstractType;

public abstract class LogicalTypeDeserializer extends TypeDeserializer {
    public abstract Object convertDeserializedValue(AbstractType<?> abstractType, Object value);
}
