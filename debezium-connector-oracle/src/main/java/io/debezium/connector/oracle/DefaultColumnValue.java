/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

/**
 * This class mimics the API of oracle.streams.DefaultColumnValue implementation of oracle.streams.ColumnValue interface
 *
 */
public class DefaultColumnValue implements ColumnValue {

    private String columnName;
    private Object columnData;
    private int columnType;

    public DefaultColumnValue(String columnName, int columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    @Override
    public int getColumnDataType() {
        return columnType;
    }

    @Override
    public Object getColumnData() {
        return columnData;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public void setColumnData(Object columnData) {
        this.columnData = columnData;
    }

}
