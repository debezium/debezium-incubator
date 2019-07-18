/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

import io.debezium.data.Envelope;

import java.math.BigDecimal;
import java.util.List;

/**
 * This interface mimics the API of oracle.streams.RowLCR interface
 *
 */
public interface LogMinerRowLcr extends LogMinerLcr {
    /**
     * This getter
     * @return old(current) values of the database record.
     * They represent values in WHERE clauses
     */
    List<LogMinerColumnValue> getOldValues();

    /**
     * this getter
     * @return new values to be applied to the database record
     * Those values are applicable for INSERT and UPDATE statements
     */
    List<LogMinerColumnValue> getNewValues();

    /**
     * this getter
     * @return Envelope.Operation enum
     */
    Envelope.Operation getCommandType();

    /**
     * the commit scn obtained from a Log Miner entry
     * @return it's value
     */
    BigDecimal getActualCommitScn();

    /**
     * sets commit scn obtained from a Log Miner entry
     * @param actualCommitScn the value
     */
    void setActualCommitScn(BigDecimal actualCommitScn);

    /**
     * the scn obtained from a Log Miner entry
     * @return it's value
     */
    BigDecimal getActualScn();

    /**
     * sets scn obtained from a Log Miner entry
     * @param actualScn it's value
     */
    void setActualScn(BigDecimal actualScn);
}
