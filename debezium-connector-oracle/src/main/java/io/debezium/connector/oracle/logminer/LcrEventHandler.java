/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.logminer.valueholder.LmLCR;
import io.debezium.connector.oracle.xstream.OracleStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for Oracle DDL and DML events. Just forwards events to the {@link EventDispatcher}.
 *
 * todo : implement
 */
class LcrEventHandler implements LmLRCCallbackHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleStreamingChangeEventSource.class);

    private final ErrorHandler errorHandler;
    private final EventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final RelationalDatabaseSchema schema;
    private final OracleOffsetContext offsetContext;
    private final boolean tablenameCaseInsensitive;

    public LcrEventHandler(ErrorHandler errorHandler, EventDispatcher<TableId> dispatcher, Clock clock,
                           RelationalDatabaseSchema schema, OracleOffsetContext offsetContext,
                           boolean tablenameCaseInsensitive) {
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.offsetContext = offsetContext;
        this.tablenameCaseInsensitive = tablenameCaseInsensitive;
    }

    @Override
    public void processLCR(LmLCR lcr) throws RuntimeException {
    }

    @Override
    public void processChunk(Object arg0) throws RuntimeException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public LmLCR createLCR() throws RuntimeException {
        throw new UnsupportedOperationException("Should never be called");
    }

    @Override
    public Object createChunk() throws RuntimeException {
        throw new UnsupportedOperationException("Should never be called");
    }
}