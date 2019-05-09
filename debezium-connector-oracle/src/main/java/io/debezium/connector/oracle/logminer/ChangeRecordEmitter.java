/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.valueholder.LmColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LmRowLCR;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Emits change record based on a single {@link LmRowLCR} event.
 */
public class ChangeRecordEmitter extends BaseChangeRecordEmitter<LmColumnValue> {

    private LmRowLCR lcr;

    public ChangeRecordEmitter(OffsetContext offset, LmRowLCR lcr, Table table, Clock clock) {
        super(offset, table, clock);
        this.lcr = lcr;
    }

    @Override
    protected Operation getOperation() {
        return lcr.getCommandType();
    }

    @Override
    protected Object[] getOldColumnValues() {
        return getColumnValues((LmColumnValue[]) lcr.getOldValues().toArray());
    }

    @Override
    protected Object[] getNewColumnValues() {
        return getColumnValues((LmColumnValue[]) lcr.getNewValues().toArray());
    }

    @Override
    protected String getColumnName(LmColumnValue columnValue) {
        return columnValue.getColumnName();
    }

    @Override
    protected Object getColumnData(LmColumnValue columnValue) {
        return columnValue.getColumnData();
    }
}
