/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.connector.oracle.logminer.valueholder.LmLCR;

/**
 * This class is mimicking oracle.streams.XStreamLCRCallbackHandler interface
 * todo: complete
 */
public interface LmLRCCallbackHandler {
    void processLCR(LmLCR var1) throws RuntimeException;
    LmLCR createLCR() throws RuntimeException;
    // todo
    void processChunk(Object var1) throws RuntimeException;
    // todo
    Object createChunk() throws RuntimeException;

}
