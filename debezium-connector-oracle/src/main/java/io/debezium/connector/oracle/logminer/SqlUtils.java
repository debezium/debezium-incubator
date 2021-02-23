/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import java.time.Duration;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.oracle.OracleConnectorConfig.LogMiningStrategy;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.relational.TableId;

/**
 * Utility class that exposes several SQL statements used by LogMiner.
 *
 * todo handle INVALID file member (report somehow and continue to work with valid file), handle adding multiplexed files,
 * todo SELECT name, value FROM v$sysstat WHERE name = 'redo wastage';
 * todo SELECT GROUP#, STATUS, MEMBER FROM V$LOGFILE WHERE STATUS='INVALID'; (drop and recreate? or do it manually?)
 * todo table level supplemental logging
 * todo When you use the SKIP_CORRUPTION option to DBMS_LOGMNR.START_LOGMNR, any corruptions in the redo log files are
 *      skipped during select operations from the V$LOGMNR_CONTENTS view.
 * todo if file is compressed?
 *
 * todo For every corrupt redo record encountered, a row is returned that contains the value CORRUPTED_BLOCKS in the
 *      OPERATION column, 1343 in the STATUS column, and the number of blocks skipped in the INFO column.
 */
public class SqlUtils {

    // ****** RAC specifics *****//
    // https://docs.oracle.com/cd/B28359_01/server.111/b28319/logminer.htm#i1015913
    // https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:18183400346178753
    // We should never read from GV$LOG, GV$LOGFILE, GV$ARCHIVED_LOG, GV$ARCHIVE_DEST_STATUS and GV$LOGMNR_CONTENTS
    // using GV$DATABASE is also misleading
    // Those views are exceptions on RAC system, all corresponding V$ views see entries from all RAC nodes.
    // So reading from GV* will return duplications, do no do it
    // *****************************

    // database system views
    private static final String DATABASE_VIEW = "V$DATABASE";
    private static final String LOG_VIEW = "V$LOG";
    private static final String LOGFILE_VIEW = "V$LOGFILE";
    private static final String ARCHIVED_LOG_VIEW = "V$ARCHIVED_LOG";
    private static final String ARCHIVE_DEST_STATUS_VIEW = "V$ARCHIVE_DEST_STATUS";
    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";
    private static final String ALL_LOG_GROUPS = "ALL_LOG_GROUPS";

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);

    static String redoLogStatusQuery() {
        return String.format("SELECT F.MEMBER, R.STATUS FROM %s F, %s R WHERE F.GROUP# = R.GROUP# ORDER BY 2",
                LOGFILE_VIEW, LOG_VIEW);
    }

    static String switchHistoryQuery() {
        return String.format("SELECT 'TOTAL', COUNT(1) FROM %s WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM %s WHERE STATUS='VALID' AND TYPE='LOCAL')",
                ARCHIVED_LOG_VIEW, ARCHIVE_DEST_STATUS_VIEW);
    }

    static String currentRedoNameQuery() {
        return String.format("SELECT F.MEMBER FROM %s LOG, %s F  WHERE LOG.GROUP#=F.GROUP# AND LOG.STATUS='CURRENT'",
                LOG_VIEW, LOGFILE_VIEW);
    }

    static String databaseSupplementalLoggingAllCheckQuery() {
        return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM %s", DATABASE_VIEW);
    }

    static String databaseSupplementalLoggingMinCheckQuery() {
        return String.format("SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM %s", DATABASE_VIEW);
    }

    static String tableSupplementalLoggingCheckQuery(TableId tableId) {
        return String.format("SELECT 'KEY', LOG_GROUP_TYPE FROM %s WHERE OWNER = '%s' AND TABLE_NAME = '%s'",
                ALL_LOG_GROUPS, tableId.schema(), tableId.table());
    }

    static String currentScnQuery() {
        return String.format("SELECT CURRENT_SCN FROM %s", DATABASE_VIEW);
    }

    static String oldestFirstChangeQuery(Duration archiveLogRetention) {
        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            return String.format("SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM %s " +
                    "UNION SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM %s " +
                    "WHERE FIRST_TIME >= SYSDATE - (%d/24))", LOG_VIEW, ARCHIVED_LOG_VIEW, archiveLogRetention.toHours());
        }
        return String.format("SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM %s " +
                "UNION SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM %s)", LOG_VIEW, ARCHIVED_LOG_VIEW);
    }

    public static String allOnlineLogsQuery() {
        return String.format("SELECT MIN(F.MEMBER) AS FILE_NAME, L.NEXT_CHANGE# AS NEXT_CHANGE, F.GROUP#, " +
                "L.FIRST_CHANGE# AS FIRST_CHANGE " +
                " FROM %s L, %s F " +
                " WHERE F.GROUP# = L.GROUP# AND L.NEXT_CHANGE# > 0 " +
                " GROUP BY F.GROUP#, L.NEXT_CHANGE#, L.FIRST_CHANGE# ORDER BY 3", LOG_VIEW, LOGFILE_VIEW);
    }

    /**
     * Obtain the query to be used to fetch archive logs.
     *
     * @param scn oldest scn to search for
     * @param archiveLogRetention duration archive logs will be mined
     * @return query
     */
    public static String archiveLogsQuery(Long scn, Duration archiveLogRetention) {
        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            return String.format("SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE, FIRST_CHANGE# AS FIRST_CHANGE FROM %s " +
                    " WHERE NAME IS NOT NULL AND FIRST_TIME >= SYSDATE - (%d/24) AND ARCHIVED = 'YES' " +
                    " AND STATUS = 'A' AND NEXT_CHANGE# > %s ORDER BY 2", ARCHIVED_LOG_VIEW, archiveLogRetention.toHours(), scn);
        }
        return String.format("SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE, FIRST_CHANGE# AS FIRST_CHANGE FROM %s " +
                "WHERE NAME IS NOT NULL AND ARCHIVED = 'YES' " +
                "AND STATUS = 'A' AND NEXT_CHANGE# > %s ORDER BY 2", ARCHIVED_LOG_VIEW, scn);
    }

    /**
     * This returns statement to build LogMiner view for online redo log files
     * @param startScn mine from
     * @param endScn mine till
     * @param strategy Log Mining strategy
     * @return statement todo: handle corruption. STATUS (Double) — value of 0 indicates it is executable
     */
    static String startLogMinerStatement(Long startScn, Long endScn, LogMiningStrategy strategy, boolean isContinuousMining) {
        String miningStrategy;
        if (strategy.equals(LogMiningStrategy.CATALOG_IN_REDO)) {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING ";
        }
        else {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG ";
        }
        if (isContinuousMining) {
            miningStrategy += " + DBMS_LOGMNR.CONTINUOUS_MINE ";
        }
        return "BEGIN sys.dbms_logmnr.start_logmnr(" +
                "startScn => '" + startScn + "', " +
                "endScn => '" + endScn + "', " +
                "OPTIONS => " + miningStrategy +
                " + DBMS_LOGMNR.NO_ROWID_IN_STMT);" +
                "END;";
    }

    /**
     * This is the query from the LogMiner view to get changes. Columns of the view we using are:
     * NOTE. Currently we do not capture changes from other schemas
     * SCN - The SCN at which a change was made
     * COMMIT_SCN - The SCN at which a change was committed
     * USERNAME - Name of the user who executed the transaction
     * SQL_REDO Reconstructed SQL statement that is equivalent to the original SQL statement that made the change
     * OPERATION_CODE - Number of the operation code.
     * TABLE_NAME - Name of the modified table
     * TIMESTAMP - Timestamp when the database change was made
     *
     * @param schemaName user name
     * @param logMinerUser log mining session user name
     * @param schema schema
     * @return the query
     */
    static String logMinerContentsQuery(String schemaName, String logMinerUser, OracleDatabaseSchema schema) {
        List<String> whiteListTableNames = schema.tableIds().stream().map(TableId::table).collect(Collectors.toList());

        // todo: add ROW_ID, SESSION#, SERIAL#, RS_ID, and SSN
        return "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME " +
                " FROM " + LOGMNR_CONTENTS_VIEW + " WHERE  OPERATION_CODE in (1,2,3,5) " + // 5 - DDL
                " AND SEG_OWNER = '" + schemaName.toUpperCase() + "' " +
                buildTableInPredicate(whiteListTableNames) +
                " AND SCN >= ? AND SCN < ? " +
                // Capture DDL and MISSING_SCN rows only hwne not performed by SYS, SYSTEM, and LogMiner user
                " OR (OPERATION_CODE IN (5,34) AND USERNAME NOT IN ('SYS','SYSTEM','" + logMinerUser.toUpperCase() + "')) " +
                // Capture all COMMIT and ROLLBACK operations performed by the database
                " OR (OPERATION_CODE IN (7,36))"; // todo username = schemaName?
    }

    static String addLogFileStatement(String option, String fileName) {
        return "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '" + fileName + "', OPTIONS => " + option + ");END;";
    }

    static String deleteLogFileStatement(String fileName) {
        return "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '" + fileName + "');END;";
    }

    static String tableExistsQuery(String tableName) {
        return "SELECT '1' AS ONE FROM USER_TABLES WHERE TABLE_NAME = '" + tableName + "'";
    }

    static String truncateTableStatement(String tableName) {
        return "TRUNCATE TABLE " + tableName;
    }

    /**
     * This method return query which converts given SCN in days and deduct from the current day
     */
    public static String diffInDaysQuery(Long scn) {
        if (scn == null) {
            return null;
        }
        return "select sysdate - CAST(scn_to_timestamp(" + scn + ") as date) from dual";
    }

    /**
     * This method builds table_name IN predicate, filtering out non whitelisted tables from Log Mining.
     * It limits joining condition over 1000 tables, Oracle will throw exception in such predicate.
     * @param tables white listed table names
     * @return IN predicate or empty string if number of whitelisted tables exceeds 1000
     */
    private static String buildTableInPredicate(List<String> tables) {
        if (tables.size() == 0 || tables.size() > 1000) {
            LOGGER.warn(" Cannot apply {} whitelisted tables condition", tables.size());
            return "";
        }

        StringJoiner tableNames = new StringJoiner(",");
        tables.forEach(table -> tableNames.add("'" + table + "'"));
        return " AND table_name IN (" + tableNames + ") ";
    }
}
