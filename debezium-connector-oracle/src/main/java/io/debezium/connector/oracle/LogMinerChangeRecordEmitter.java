/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

import java.util.Map;

/**
 * Emits change data based on a single {@link RowChange} instance.
 */
public class LogMinerChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final RowChange rowChange;
    private final Table table;

    public LogMinerChangeRecordEmitter(OffsetContext offsetContext, RowChange rowChange, Table table, Clock clock) {
        super(offsetContext, clock);
        this.rowChange = rowChange;
        this.table = table;
    }

    @Override
    protected Envelope.Operation getOperation() {
        return rowChange.getOperation();
    }

    @Override
    protected Object[] getOldColumnValues() {
        return getColumnValues(rowChange.getOldValues());
    }

    @Override
    protected Object[] getNewColumnValues() {
        return getColumnValues(rowChange.getNewValues());
    }

    private Object[] getColumnValues(Map<String, Object> values) {
        Object[] columnValues = new Object[table.columns().size()];
        for (int index = 0; index < table.columns().size(); index++) {
            Column column = table.columns().get(index);
            columnValues[index] = values.get(column.name());
        }
        return columnValues;
    }
}
