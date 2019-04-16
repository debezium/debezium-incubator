/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

/**
 * This class is a wrapper class holds a ColumnValue. and an indicator if the column was processed by a parser listener.
 * The "processed" is true means a listener has parsed a value
 * The "processed" flag helps to filter the resulting collection of new and old values.
 *
 */
public class ColumnValueHolder {
    private boolean processed;
    private final ColumnValue columnValue;

    public ColumnValueHolder(ColumnValue columnValue) {
        this.columnValue = columnValue;
    }

    public ColumnValue getColumnValue() {
        return columnValue;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

}
