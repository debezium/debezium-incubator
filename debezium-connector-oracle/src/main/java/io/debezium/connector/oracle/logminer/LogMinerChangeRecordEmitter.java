/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.BaseChangeRecordEmitter;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

import java.util.Arrays;
import java.util.List;

/**
 * Emits change record based on a single {@link LogMinerRowLcr} event.
 */
public class LogMinerChangeRecordEmitter extends BaseChangeRecordEmitter<LogMinerColumnValue> {

    private LogMinerRowLcr lcr;
    protected final Table table;


    public LogMinerChangeRecordEmitter(OffsetContext offset, LogMinerRowLcr lcr, Table table, Clock clock) {
        super(offset, table, clock);
        this.lcr = lcr;
        this.table = table;
    }

    @Override
    protected Operation getOperation() {
        return lcr.getCommandType();
    }

    @Override
    protected Object[] getOldColumnValues() {
        List<LogMinerColumnValue> valueList =  lcr.getOldValues();
        LogMinerColumnValue[] result = Arrays.copyOf(valueList.toArray(), valueList.size(), LogMinerColumnValue[].class );
        return getColumnValues(result);
    }

    @Override
    protected Object[] getNewColumnValues() {
        List<LogMinerColumnValue> valueList =  lcr.getNewValues();
        LogMinerColumnValue[] result = Arrays.copyOf(valueList.toArray(), valueList.size(), LogMinerColumnValue[].class );
        return getColumnValues(result);
    }

    @Override
    protected String getColumnName(LogMinerColumnValue columnValue) {
        return columnValue.getColumnName();
    }

    protected Object getColumnData(LogMinerColumnValue columnValue) {
        return columnValue.getColumnData();
    }
}
