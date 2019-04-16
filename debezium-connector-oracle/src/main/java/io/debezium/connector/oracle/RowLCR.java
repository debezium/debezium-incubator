/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.data.Envelope;

import java.util.List;

/**
 * This interface mimics the API of oracle.streams.RowLCR interface
 *
 */
public interface RowLCR {
    /**
     * This getter
     * @return old(current) values of the database record.
     * They represent values in WHERE clauses
     */
    List<ColumnValue> getOldValues();

    /**
     * this getter
     * @return new values to be applied to the database record
     * Those values are applicable for INSERT and UPDATE statements
     */
    List<ColumnValue> getNewValues();

    /**
     * this getter
     * @return Envelope.Operation enum
     */
    Envelope.Operation getCommandType();
}
