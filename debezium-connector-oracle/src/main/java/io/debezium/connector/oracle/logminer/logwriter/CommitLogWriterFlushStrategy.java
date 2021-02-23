/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer.logwriter;

import java.sql.SQLException;
import java.util.function.Consumer;

import io.debezium.DebeziumException;
import io.debezium.connector.oracle.OracleConnection;

/**
 * A {@link LogWriterFlushStrategy} that uses a transaction commit to force flushes.
 *
 * @author Chris Cranford
 */
public class CommitLogWriterFlushStrategy implements LogWriterFlushStrategy {

    public static final String LOGMNR_FLUSH_TABLE = "LOG_MINING_FLUSH";

    private static final String CREATE_FLUSH_TABLE = "CREATE TABLE " + LOGMNR_FLUSH_TABLE + "(LAST_SCN NUMBER(19,0))";
    private static final String INSERT_FLUSH_TABLE = "INSERT INTO " + LOGMNR_FLUSH_TABLE + " VALUES (0)";
    private static final String UPDATE_FLUSH_TABLE = "UPDATE " + LOGMNR_FLUSH_TABLE + " SET LAS_SCN = %d";
    private static final String FLUSH_TABLE_NOT_EMPTY = "SELECT '1' AS ONE FROM " + LOGMNR_FLUSH_TABLE;
    private static final String TABLE_EXISTS = "SELECT '1' AS ONE FROM USER_TABLES WHERE TABLE_NAME = '%s'";

    private final OracleConnection connection;
    private final Consumer<OracleConnection> closeHandler;

    /**
     * Create a transaction commit Oracle LogWriter (LGWR) process flush strategy.
     *
     * @param connection the database connection
     * @param closeHandler a handler that is called the strategy is closed.
     */
    public CommitLogWriterFlushStrategy(OracleConnection connection, Consumer<OracleConnection> closeHandler) {
        this.connection = connection;
        this.closeHandler = closeHandler;
        createFlushTable();
    }

    @Override
    public void flush(long currentScn) {
        try {
            connection.prepareCallWithCommit(false, String.format(UPDATE_FLUSH_TABLE, currentScn));
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to flush Oracle LogWriter (LGWR) buffers to disk", e);
        }
    }

    @Override
    public void close() {
        if (closeHandler != null) {
            closeHandler.accept(connection);
        }
    }

    /**
     * Makes sure that the flush table is created in the database and that it at least has 1 row of data
     * so that when flushes occur that the update succeeds without failure.
     */
    private void createFlushTable() {
        try {
            String tableExistsQuery = String.format(TABLE_EXISTS, LOGMNR_FLUSH_TABLE);
            String tableExists = connection.querySingleResult(tableExistsQuery, (rs) -> rs.getString(1));
            if (tableExists == null) {
                connection.prepareCall(false, CREATE_FLUSH_TABLE);
            }
            String recordExists = connection.querySingleResult(FLUSH_TABLE_NOT_EMPTY, (rs) -> rs.getString(1));
            if (recordExists == null) {
                connection.prepareCallWithCommit(false, INSERT_FLUSH_TABLE);
            }
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to create flush table", e);
        }
    }
}
