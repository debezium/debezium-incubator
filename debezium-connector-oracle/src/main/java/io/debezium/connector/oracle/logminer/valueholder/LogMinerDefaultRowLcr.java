/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

import io.debezium.data.Envelope;

import java.sql.Timestamp;
import java.util.List;

/**
 * This class mimics the API of oracle.streams.DefaultRowLCR class (LCR stands for logical change record)
 *
 */
public class LogMinerDefaultRowLcr implements LogMinerRowLcr {

    private Envelope.Operation commandType;
    private List<LogMinerColumnValue> newLmColumnValues;
    private List<LogMinerColumnValue> oldLmColumnValues;
    private String objectOwner;
    private String objectName;
    private Timestamp sourceTime;
    private String transactionId;
    private String databaseName;
    private long actualCommitScn;
    private long actualScn;

    public LogMinerDefaultRowLcr(Envelope.Operation commandType, List<LogMinerColumnValue> newLmColumnValues, List<LogMinerColumnValue> oldLmColumnValues) {
        this.commandType = commandType;
        this.newLmColumnValues = newLmColumnValues;
        this.oldLmColumnValues = oldLmColumnValues;
    }

    @Override
    public Envelope.Operation getCommandType() {
        return commandType;
    }

    @Override
    public List<LogMinerColumnValue> getOldValues() {
        return oldLmColumnValues;
    }

    @Override
    public List<LogMinerColumnValue> getNewValues() {
        return newLmColumnValues;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public String getSourceDatabaseName() {
        return databaseName;
    }

    @Override
    public String getObjectOwner() {
        return objectOwner;
    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    @Override
    public void setObjectName(String name) {
        this.objectName = name;
    }

    @Override
    public void setObjectOwner(String name) {
        this.objectOwner = name;
    }

    @Override
    public void setSourceTime(Timestamp changeTime) {
        this.sourceTime = changeTime;
    }

    @Override
    public Timestamp getSourceTime() {
        return sourceTime;
    }

    @Override
    public void setTransactionId(String id) {
        this.transactionId = id;
    }

    public void setSourceDatabaseName(String name){
        this.databaseName = name;
    }

    @Override
    public long getActualCommitScn() {
        return actualCommitScn;
    }

    @Override
    public void setActualCommitScn(long actualCommitScn) {
        this.actualCommitScn = actualCommitScn;
    }

    @Override
    public long getActualScn() {
        return actualScn;
    }

    @Override
    public void setActualScn(long actualScn) {
        this.actualScn = actualScn;
    }


}

