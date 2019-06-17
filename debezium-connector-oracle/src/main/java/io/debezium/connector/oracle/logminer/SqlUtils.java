/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

/**
 * This utility class contains SQL statements to configure, manage and query Oracle LogMiner
 */
public class SqlUtils {

    static final String BUILD_DICTIONARY = "BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;";
    static final String ONLINE_LOG_FILENAME = "SELECT MEMBER FROM V$LOGFILE";
    static final String CURRENT_SCN = "select CURRENT_SCN from V$DATABASE";
    static final String START_LOGMINER_FOR_ARCHIVE_STATEMENT = "BEGIN sys.dbms_logmnr.start_logmnr(" +
            "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY); END;";
    static final String END_LOGMNR = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;";
    static final String OLDEST_FIRST_CHANGE = "select min(first_change#) from V$LOG";
    static final String OLDEST_ARCHIVED_CHANGE = "select min(first_change#) from v$archived_log";
    static final String LATEST_SCN_FROM_ARCHIVED_LOG = "select max(NEXT_CHANGE#) from v$archived_log";
    static final String ALL_ARCHIVED_LOGS_NAMES_FOR_OFFSET = "select name as file_name, next_change# as next_change from v$archived_log where first_change# between ? and ? and status = 'A' and standby_dest='NO' order by next_change asc";

    /**
     * This returns statement to build log miner view for online redo log files
     * @param startScn mine from
     * @param endScn mine till
     * @return statement
     */
    static String getStartLogMinerStatement(Long startScn, Long endScn) {
        return "BEGIN sys.dbms_logmnr.start_logmnr(" +
                "startScn => '" + startScn + "', " +
                "endScn => '" + endScn + "', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_REDO_LOGS + " +
                "DBMS_LOGMNR.DDL_DICT_TRACKING + " +
                "DBMS_LOGMNR.COMMITTED_DATA_ONLY); " +
                "END;";
    }

    /**
     * This is the query from the log miner view to get changes. Columns of the view are:
     *
     * SCN - The SCN at which a change was made
     * COMMIT_SCN - The SCN at which a change was committed
     * OPERATION - User level SQL operation that made the change. Possible values are:
     *      INSERT = change was caused by an insert statement
     *      UPDATE = change was caused by an update statement
     *      DELETE = change was caused by a delete statement
     *      DDL = change was caused by a DDL statement
     *      START = change was caused by the start of a transaction
     *      COMMIT = change was caused by the commit of a transaction
     *      ROLLBACK = change was caused by a full rollback of a transaction
     *      LOB_WRITE = change was caused by an invocation of DBMS_LOB.WRITE
     *      LOB_TRIM = change was caused by an invocation of DBMS_LOB.TRIM
     *      LOB_ERASE = change was caused by an invocation of DBMS_LOB.ERASE
     *      SELECT_FOR_UPDATE = operation was a SELECT FOR UPDATE statement
     *      SEL_LOB_LOCATOR = operation was a SELECT statement that returns a LOB locator
     *      MISSING_SCN = LogMiner encountered a gap in the redo records. This is most likely because not all redo logs were registered with LogMiner.
     *      INTERNAL = change was caused by internal operations initiated by the database
     *      UNSUPPORTED = change was caused by operations not currently supported by LogMiner (for example, changes made to tables with ADT c
     * USERNAME - Name of the user who executed the transaction
     * SRC_CON_NAME - Contains the pluggable database (PDB) name
     * SQL_REDO Reconstructed SQL statement that is equivalent to the original SQL statement that made the change
     * SEG_TYPE - Segment type. Possible values are:
     *      0 = UNKNOWN
     *      1 = INDEX
     *      2 = TABLE
     *      19 = TABLE PARTITION
     *      20 = INDEX PARTITION
     *      34 = TABLE SUBPARTITION
     *      All other values = UNSUPPORTED
     * TABLE_SPACE
     * STATUS - A value of 0 indicates that the reconstructed SQL statements as shown
     *          in the SQL_REDO and SQL_UNDO columns are valid executable SQL statements
     * OPERATION_CODE - Number of the operation code. Possible values are:
     *      0 = INTERNAL
     *      1 = INSERT
     *      2 = DELETE
     *      3 = UPDATE
     *      5 = DDL
     *      6 = START
     *      7 = COMMIT
     *      9 = SELECT_LOB_LOCATOR
     *      10 = LOB_WRITE
     *      11 = LOB_TRIM
     *      25 = SELECT_FOR_UPDATE
     *      28 = LOB_ERASE
     *      34 = MISSING_SCN
     *      36 = ROLLBACK
     *      255 = UNSUPPORTED
     * TABLE_NAME - Name of the modified table
     * TIMESTAMP - Timestamp when the database change was made
     * COMMIT_TIMESTAMP - Timestamp when the transaction committed
     * XID - Raw representation of the transaction identifier
     * XIDSQN - Transaction ID sequence number of the transaction that generated the change
     *
     * @param schemaName user name
     * @return the query
     */
    public static String queryLogMinerContents(String schemaName)  {
        return "SELECT SCN, COMMIT_SCN, OPERATION, USERNAME, SRC_CON_NAME, SQL_REDO, SEG_TYPE, " +
                        "STATUS, OPERATION_CODE, TABLE_NAME, TIMESTAMP, COMMIT_TIMESTAMP, XID, XIDSQN " +
                        "FROM v$logmnr_contents " +
                        "WHERE " +
                        "username = '"+ schemaName.toUpperCase() +"' " +
//                        " AND OPERATION_CODE in (1,2,3,5,7, 36) " +// 5 - DDL
                        "AND seg_owner = '"+ schemaName.toUpperCase() +"' " +
                        "AND " +
                        "(commit_scn >= ? " +
                " OR scn >= ?)"; // OR scn stands for DDL
        // todo ROW_ID, CSF?
    }

    /**
     * This returns statement to query log miner view, pre-built for archived log files
     * @param schemaName user name
     * @return query
     */
    static String queryLogMinerArchivedContents(String schemaName)  {
        return "SELECT SCN, COMMIT_SCN, OPERATION, USERNAME, SRC_CON_NAME, SQL_REDO, SEG_TYPE, " +
                        "STATUS, OPERATION_CODE, TABLE_NAME, TIMESTAMP, COMMIT_TIMESTAMP, XID, XIDSQN " +
                        "FROM v$logmnr_contents " +
                        "WHERE " +
                        "username = '"+ schemaName.toUpperCase() +"' " +
//                        " AND OPERATION_CODE in (1,2,3,5,7, 36) " +// 5 - DDL
                        "AND seg_owner = '"+ schemaName.toUpperCase() +"'";
    }

    /**
     * After mining archived log files, we should remove them from the analysis.
     * NOTE. It does not physically remove the log file.
     * @param logFileName file ro remove
     * @return statement
     */
    static String getRemoveLogFileFromMiningStatement(String logFileName) {
        return "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE('" + logFileName + "'); END;";
    }

    static String getAddLogFileStatement(String option, String fileName) {
        return "BEGIN sys." +
                "dbms_logmnr.add_logfile(" +
                "LOGFILENAME => '" + fileName + "', " +
                "OPTIONS => " + option + ");" +
                "END;";
    }
}
