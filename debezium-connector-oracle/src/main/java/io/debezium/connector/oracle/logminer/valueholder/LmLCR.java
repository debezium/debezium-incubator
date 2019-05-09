/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

/**
 * This class mimics some API of the oracle.streams.LCR interface (logical change record)
 */
public interface LmLCR {

    /**
     * The position of the LCR identifies its placement in the stream of LCR in a transaction.
     * SCN (system change number) could be converted to a position in the code, based on the Oracle version
     * @return position
     */
    byte[] getPosition();

    /**
     * @return transaction ID
     */
    String getTransactionId();

    /**
     * @return source database name
     */
    String getSourceDatabaseName();

    /**
     * @return schema name
     */
    String getObjectOwner();

    /**
     * @return Envelope.Operation value or String representing DDL
     */
    Object getCommandType();

}
