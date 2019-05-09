/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.BaseOracleSchemaChangeEventEmitter;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.logminer.valueholder.LmDDLLCR;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;

/**
 * {@link SchemaChangeEventEmitter} implementation based on Oracle LogMiner utility.
 */
public class OracleSchemaChangeEventEmitter extends BaseOracleSchemaChangeEventEmitter {

    public OracleSchemaChangeEventEmitter(OracleOffsetContext offsetContext, TableId tableId, LmDDLLCR ddlLcr) {
        super(offsetContext,
                tableId,
                ddlLcr.getSourceDatabaseName(),
                ddlLcr.getObjectOwner(),
                ddlLcr.getDDLText(),
                ddlLcr.getCommandType());
    }
}
