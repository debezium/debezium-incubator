/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.data.Envelope;

import java.util.List;

/**
 * This class mimics the API of oracle.streams.DefaultRowLCR class (LCR stands for logical change record)
 *
 */
public class DefaultRowLCR implements RowLCR {

    private Envelope.Operation commandType;
    private List<ColumnValue> newColumnValues;
    private List<ColumnValue> oldColumnValues;

    public DefaultRowLCR(Envelope.Operation commandType, List<ColumnValue> newColumnValues, List<ColumnValue> oldColumnValues) {
        this.commandType = commandType;
        this.newColumnValues = newColumnValues;
        this.oldColumnValues = oldColumnValues;
    }

    @Override
    public Envelope.Operation getCommandType() {
        return commandType;
    }

    @Override
    public List<ColumnValue> getOldValues() {
        return oldColumnValues;
    }

    @Override
    public List<ColumnValue> getNewValues() {
        return newColumnValues;
    }

}
