/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.valueholder;

/**
 * This class mimics some API of oracle.streams.DDLLCR interface
 *
 */
public interface LmDDLLCR extends LmLCR {
    /**
     * @return text of the DDL statement
     */
    String getDDLText();

    /**
     * @return string such as "CREATE TABLE", "ALTER TABLE", "DROP TABLE"
     */
    String getCommandType();
}
