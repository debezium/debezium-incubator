/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.logwriter;

/**
 * Strategy that controls how the Oracle LogWriter (LGWR) process is to be flushed.
 */
public interface LogWriterFlushStrategy extends AutoCloseable {
    /**
     * Performs the Oracle LogWriter flush to disk.
     *
     * @param currentScn the value to flushed to the flush table
     */
    void flush(long currentScn);
}
