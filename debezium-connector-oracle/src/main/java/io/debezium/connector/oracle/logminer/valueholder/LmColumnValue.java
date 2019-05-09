/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

/**
 * This interface mimics API of oracle.streams.ColumnValue interface
 * it does not cover LOB which should be handled in chunks
 */
public interface LmColumnValue {
    /**
     * @return value of the database record
     * with exception of LOB types
     */
    Object getColumnData();

    /**
     * @return java.sql.Type constant
     */
    int getColumnDataType();

    /**
     * @return column name
     */
    String getColumnName();

    /**
     * This sets the database record value with the exception of LOBs
     * @param columnData data
     */
    void setColumnData(Object columnData);
}
