/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

@NotThreadSafe
public class LogMiner {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSnapshotChangeEventSource.class);

    public static final int COMMIT = 7;
    public static final int ROLLBACK = 36;

    private static final int XIDUSN = 1;
    private static final int XIDSLT = 2;
    private static final int XIDSQN = 3;
    private static final int OPERATION_CODE = 4;
    private static final int SCN = 5;
    private static final int TIMESTAMP = 6;
    private static final int SEG_OWNER = 7;
    private static final int TABLE_NAME = 8;
    private static final int CSF = 9;
    private static final int SQL_REDO = 10;

    private static final int INSERT = 1;
    private static final int DELETE = 2;
    private static final int UPDATE = 3;

    private static final String OPERATION_CODE_FILTER = operationCode(INSERT, DELETE, UPDATE);
    private static final String END_LOGMNR = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";
    private static final String START_LOGMNR = "BEGIN"
            + "  DBMS_LOGMNR.START_LOGMNR("
            + "    startscn => ?,"
            + "    endscn => ?,"
            + "    options => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG"
            + "             + DBMS_LOGMNR.CONTINUOUS_MINE"
            + "             + DBMS_LOGMNR.NO_ROWID_IN_STMT"
            + "             + DBMS_LOGMNR.NO_SQL_DELIMITER); "
            + "END;";
    private static final String GET_LOGMNR_CONTENTS_BASE = "SELECT xidusn,"
            + "  xidslt,"
            + "  xidsqn,"
            + "  operation_code,"
            + "  scn,"
            + "  timestamp,"
            + "  seg_owner,"
            + "  table_name,"
            + "  csf,"
            + "  sql_redo "
            + "FROM v$logmnr_contents "
            + "WHERE ";
    private static final String NLS_SESSION_PARAMETERS = "ALTER SESSION SET "
            + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
            + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
            + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'"
            + "  NLS_NUMERIC_CHARACTERS = '.,'";

    private final OracleConnection jdbcConnection;
    private String queryToContents = null;

    public LogMiner(OracleConnection jdbcConnection) {
        this.jdbcConnection = jdbcConnection;
    }

    private static String segOwner(String schema) {
        return "seg_owner = '" + schema + "'";
    }

    private static String tableName(List<String> tables) {
        StringJoiner tableNames = new StringJoiner(",");
        tables.forEach(table -> tableNames.add("'" + table + "'"));
        return "table_name IN (" + tableNames + ")";
    }

    private static String operationCode(int... operationCodes) {
        StringJoiner result = new StringJoiner(",", "operation_code IN (", ")");
        for (int operationCode : operationCodes) {
            result.add(String.valueOf(operationCode));
        }
        return result.toString();
    }

    /**
     * Sets NLS parameters of user session.
     *
     * @throws SQLException if anything unexpected fails
     */
    public void setNlsSessionParameters() throws SQLException {
        jdbcConnection.executeWithoutCommitting(NLS_SESSION_PARAMETERS);
    }

    /**
     * Starts a LogMiner session.
     *
     * @param startScn start SCN
     * @param endScn end SCN
     * @throws SQLException if anything unexpected fails
     */
    public void start(BigDecimal startScn, BigDecimal endScn) throws SQLException {
        jdbcConnection.prepareUpdate(START_LOGMNR, statement -> {
            statement.setBigDecimal(1, startScn);
            statement.setBigDecimal(2, endScn);
        });
    }

    /**
     * Finishes a LogMiner session.
     *
     * @throws SQLException if anything unexpected fails
     */
    public void end() throws SQLException {
        jdbcConnection.executeWithoutCommitting(END_LOGMNR);
    }

    /**
     * Follows the tables used by LogMiner.
     *
     * @param tableIds table ids to follow
     */
    public void followTables(Set<TableId> tableIds) {
        if (tableIds.isEmpty()) {
            throw new IllegalStateException("Tables are missing");
        }
        queryToContents = GET_LOGMNR_CONTENTS_BASE + buildConditionsForQueryToContents(tableIds);
        LOGGER.trace("Query to LogMiner contents: {}", queryToContents);
    }

    /**
     * Returns LogMiner contents by specified parameters.
     *
     * @param startScn start SCN
     * @param consumer consumer to read LogMiner contents
     * @throws SQLException if anything unexpected fails
     */
    public void getContents(BigDecimal startScn, JdbcConnection.BlockingResultSetConsumer consumer) throws SQLException, InterruptedException {
        if (queryToContents == null) {
            throw new IllegalStateException("Query is not initialized");
        }
        jdbcConnection.prepareQueryWithBlockingConsumer(queryToContents, statement -> statement.setBigDecimal(1, startScn), consumer);
    }

    public String getTransactionId(ResultSet rs) throws SQLException {
        return rs.getString(XIDUSN) + "." + rs.getString(XIDSLT) + "." + rs.getString(XIDSQN);
    }

    public int getOperationCode(ResultSet rs) throws SQLException {
        return rs.getInt(LogMiner.OPERATION_CODE);
    }

    public BigDecimal getScn(ResultSet rs) throws SQLException {
        return rs.getBigDecimal(LogMiner.SCN);
    }

    public Timestamp getTimestamp(ResultSet rs) throws SQLException {
        return rs.getTimestamp(LogMiner.TIMESTAMP);
    }

    public TableId getTableId(String catalogName, ResultSet rs) throws SQLException {
        return new TableId(catalogName, rs.getString(SEG_OWNER), rs.getString(TABLE_NAME));
    }

    public String getSqlRedo(ResultSet rs) throws SQLException {
        int csf = rs.getInt(CSF);
        // 0 - indicates SQL_REDO is contained within the same row
        // 1 - indicates that either SQL_REDO is greater than 4000 bytes in size and is continued in
        // the next row returned by the ResultSet
        if (csf == 0) {
            return rs.getString(SQL_REDO);
        }
        else {
            StringBuilder result = new StringBuilder(rs.getString(SQL_REDO));
            while (csf == 1) {
                rs.next();
                csf = rs.getInt(CSF);
                result.append(rs.getString(SQL_REDO));
            }
            return result.toString();
        }
    }

    String buildConditionsForQueryToContents(Set<TableId> tableIds) {
        Map<String, List<String>> schemasToTables = new HashMap<>();
        tableIds.forEach(tableId -> {
            List<String> tables = schemasToTables.computeIfAbsent(tableId.schema(), schema -> new ArrayList<>());
            tables.add(tableId.table());
        });
        StringJoiner conditions = new StringJoiner(" OR ", OPERATION_CODE_FILTER + " AND (", ")");
        schemasToTables.forEach((schema, tables) -> conditions.add("(" + segOwner(schema) + " AND " + tableName(tables) + ")"));
        return "scn > ? AND ((" + conditions + ") OR " + operationCode(COMMIT, ROLLBACK) + ")";
    }
}
