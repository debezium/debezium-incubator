/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

import java.sql.Timestamp;

/**
 * This class mimics API of the oracle.streams.LCR interface (logical change record)
 */
public interface LogMinerLcr {

/*
     * The position of the LCR identifies its placement in the stream of LCR in a transaction.
     * SCN (system change number) could be converted to a position in the code, based on the Oracle version
     * @return position
     *//*

    byte[] getPosition(); // todo maybe we still need it?
*/

    /**
     * @return transaction ID
     */
    String getTransactionId();

    /**
     * @return schema name
     */
    String getObjectOwner();

    /**
     * @return Envelope.Operation value or String representing DDL
     */
    Object getCommandType();

    /**
     * @return table name
     */
    String getObjectName();

    /**
     * Sets table name
     * @param name table name
     */
    void setObjectName(String name);

    /**
     * Sets schema owner
     * @param name schema owner
     */
    void setObjectOwner(String name);

    /**
     * Sets the time of the database change
     * @param changeTime the time of the change
     */
    void setSourceTime(Timestamp changeTime);

    /**
     * @return database change time of this logical record
     */
    Timestamp getSourceTime();

    /**
     * @param id unique transaction ID
     */
    void setTransactionId(String id);


}
