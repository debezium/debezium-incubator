/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.data.Envelope;

import java.util.Map;

/**
 * This class describes a change to the data in a single row that results from DML.
 */
public class RowChange {

    private final Envelope.Operation operation;
    private final Map<String, Object> oldValues;
    private final Map<String, Object> newValues;

    public RowChange(Envelope.Operation operation, Map<String, Object> oldValues, Map<String, Object> newValues) {
        this.operation = operation;
        this.oldValues = oldValues;
        this.newValues = newValues;
    }

    public Envelope.Operation getOperation() {
        return operation;
    }

    public Map<String, Object> getOldValues() {
        return oldValues;
    }

    public Map<String, Object> getNewValues() {
        return newValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowChange)) {
            return false;
        }
        RowChange rowChange = (RowChange) o;
        return operation == rowChange.operation
                && oldValues.equals(rowChange.oldValues)
                && newValues.equals(rowChange.newValues);
    }

    @Override
    public int hashCode() {
        int result = operation.hashCode();
        result = 31 * result + oldValues.hashCode();
        result = 31 * result + newValues.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RowChange{"
                + "operation=" + operation
                + ", oldValues=" + oldValues
                + ", newValues=" + newValues
                + '}';
    }
}
