/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Emits change record based on a single {@link RowLCR} event.
 */
public class LogMinerChangeRecordEmitter extends RelationalChangeRecordEmitter {

    private final DefaultRowLCR lcr;
    private final Table table;

    public LogMinerChangeRecordEmitter(OffsetContext offset, DefaultRowLCR lcr, Table table, Clock clock) {
        super(offset, clock);
        this.lcr = lcr;
        this.table = table;
    }

    @Override
    protected Operation getOperation() {
        return lcr.getCommandType();
    }

    @Override
    protected Object[] getOldColumnValues() {
        return getColumnValues((ColumnValue[])lcr.getOldValues().toArray());
    }

    @Override
    protected Object[] getNewColumnValues() {
        return getColumnValues((ColumnValue[]) lcr.getNewValues().toArray());
    }

    private Object[] getColumnValues(ColumnValue[] columnValues) {
        Object[] values = new Object[table.columns().size()];

        for (ColumnValue columnValue : columnValues) {
            int index = table.columnWithName(columnValue.getColumnName()).position() - 1;
            values[index] = columnValue.getColumnData();
        }

        return values;
    }
}
