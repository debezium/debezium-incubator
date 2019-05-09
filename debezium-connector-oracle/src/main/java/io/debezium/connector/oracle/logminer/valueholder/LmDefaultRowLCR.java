/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

import io.debezium.data.Envelope;

import java.util.List;

/**
 * This class mimics the API of oracle.streams.DefaultRowLCR class (LCR stands for logical change record)
 *
 */
public class LmDefaultRowLCR implements LmRowLCR {

    private Envelope.Operation commandType;
    private List<LmColumnValue> newLmColumnValues;
    private List<LmColumnValue> oldLmColumnValues;

    public LmDefaultRowLCR(Envelope.Operation commandType, List<LmColumnValue> newLmColumnValues, List<LmColumnValue> oldLmColumnValues) {
        this.commandType = commandType;
        this.newLmColumnValues = newLmColumnValues;
        this.oldLmColumnValues = oldLmColumnValues;
    }

    @Override
    public Envelope.Operation getCommandType() {
        return commandType;
    }

    @Override
    public List<LmColumnValue> getOldValues() {
        return oldLmColumnValues;
    }

    @Override
    public List<LmColumnValue> getNewValues() {
        return newLmColumnValues;
    }

    @Override
    public byte[] getPosition() {
        return new byte[0];
    }

    @Override
    public String getTransactionId() {
        return null;
    }

    @Override
    public String getSourceDatabaseName() {
        return null;
    }

    @Override
    public String getObjectOwner() {
        return null;
    }

}
