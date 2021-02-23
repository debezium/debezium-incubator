/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.junit.SkipTestDependingOnAdapterNameRule;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot;
import io.debezium.connector.oracle.junit.SkipWhenAdapterNameIsNot.AdapterName;
import io.debezium.relational.TableId;

@SkipWhenAdapterNameIsNot(value = AdapterName.LOGMINER)
public class SqlUtilsTest {

    @Rule
    public TestRule skipRule = new SkipTestDependingOnAdapterNameRule();

    @Test
    public void testStatements() {
        String result = SqlUtils.addLogFileStatement("ADD", "FILENAME");
        String expected = "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => 'FILENAME', OPTIONS => ADD);END;";
        assertThat(expected.equals(result)).isTrue();

        OracleDatabaseSchema schema = mock(OracleDatabaseSchema.class);
        TableId table1 = new TableId("catalog", "schema", "table1");
        TableId table2 = new TableId("catalog", "schema", "table2");
        Set<TableId> tables = new HashSet<>();
        Mockito.when(schema.tableIds()).thenReturn(tables);
        result = SqlUtils.logMinerContentsQuery("DATABASE", "SCHEMA", schema);
        expected = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME  " +
                "FROM V$LOGMNR_CONTENTS WHERE  OPERATION_CODE in (1,2,3,5)  AND SEG_OWNER = 'DATABASE'  AND SCN >= ? AND SCN < ?  " +
                "OR (OPERATION_CODE IN (5,34) AND USERNAME NOT IN ('SYS','SYSTEM','SCHEMA')) " +
                " OR (OPERATION_CODE IN (7,36))";
        assertThat(result).isEqualTo(expected);

        tables.add(table1);
        tables.add(table2);
        result = SqlUtils.logMinerContentsQuery("DATABASE", "SCHEMA", schema);
        expected = "SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, USERNAME  " +
                "FROM V$LOGMNR_CONTENTS WHERE  OPERATION_CODE in (1,2,3,5)  " +
                "AND SEG_OWNER = 'DATABASE'  AND table_name IN ('table1','table2')  " +
                "AND SCN >= ? AND SCN < ?  OR (OPERATION_CODE IN (5,34) AND USERNAME NOT IN ('SYS','SYSTEM','SCHEMA')) " +
                " OR (OPERATION_CODE IN (7,36))";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.databaseSupplementalLoggingMinCheckQuery();
        expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.tableSupplementalLoggingCheckQuery(new TableId(null, "s", "t"));
        expected = "SELECT 'KEY', LOG_GROUP_TYPE FROM ALL_LOG_GROUPS WHERE OWNER = 's' AND TABLE_NAME = 't'";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.startLogMinerStatement(10L, 20L, OracleConnectorConfig.LogMiningStrategy.ONLINE_CATALOG, true);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG  + DBMS_LOGMNR.CONTINUOUS_MINE  + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.startLogMinerStatement(10L, 20L, OracleConnectorConfig.LogMiningStrategy.CATALOG_IN_REDO, false);
        expected = "BEGIN sys.dbms_logmnr.start_logmnr(startScn => '10', endScn => '20', " +
                "OPTIONS => DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING  + DBMS_LOGMNR.NO_ROWID_IN_STMT);END;";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.truncateTableStatement("table_name");
        expected = "TRUNCATE TABLE table_name";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.diffInDaysQuery(123L);
        expected = "select sysdate - CAST(scn_to_timestamp(123) as date) from dual";
        assertThat(expected.equals(result)).isTrue();
        result = SqlUtils.diffInDaysQuery(null);
        assertThat(result).isNull();

        result = SqlUtils.redoLogStatusQuery();
        expected = "SELECT F.MEMBER, R.STATUS FROM V$LOGFILE F, V$LOG R WHERE F.GROUP# = R.GROUP# ORDER BY 2";
        assertThat(expected.equals(result)).isTrue();

        result = SqlUtils.switchHistoryQuery();
        expected = "SELECT 'TOTAL', COUNT(1) FROM V$ARCHIVED_LOG WHERE FIRST_TIME > TRUNC(SYSDATE)" +
                " AND DEST_ID IN (SELECT DEST_ID FROM V$ARCHIVE_DEST_STATUS WHERE STATUS='VALID' AND TYPE='LOCAL')";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.currentRedoNameQuery();
        expected = "SELECT F.MEMBER FROM V$LOG LOG, V$LOGFILE F  WHERE LOG.GROUP#=F.GROUP# AND LOG.STATUS='CURRENT'";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.databaseSupplementalLoggingAllCheckQuery();
        expected = "SELECT 'KEY', SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.currentScnQuery();
        expected = "SELECT CURRENT_SCN FROM V$DATABASE";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.oldestFirstChangeQuery(Duration.ofHours(0L));
        expected = "SELECT MIN(FIRST_CHANGE#) FROM (SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$LOG UNION SELECT MIN(FIRST_CHANGE#) AS FIRST_CHANGE# FROM V$ARCHIVED_LOG)";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.allOnlineLogsQuery();
        expected = "SELECT MIN(F.MEMBER) AS FILE_NAME, L.NEXT_CHANGE# AS NEXT_CHANGE, F.GROUP#, L.FIRST_CHANGE# AS FIRST_CHANGE " +
                " FROM V$LOG L, V$LOGFILE F " +
                " WHERE F.GROUP# = L.GROUP# AND L.NEXT_CHANGE# > 0 " +
                " GROUP BY F.GROUP#, L.NEXT_CHANGE#, L.FIRST_CHANGE# ORDER BY 3";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.tableExistsQuery("table_name");
        expected = "SELECT '1' AS ONE FROM USER_TABLES WHERE TABLE_NAME = 'table_name'";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.archiveLogsQuery(10L, Duration.ofHours(0L));
        expected = "SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE, FIRST_CHANGE# AS FIRST_CHANGE FROM V$ARCHIVED_LOG " +
                "WHERE NAME IS NOT NULL AND ARCHIVED = 'YES' " +
                "AND STATUS = 'A' AND NEXT_CHANGE# > 10 ORDER BY 2";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.archiveLogsQuery(10L, Duration.ofHours(1L));
        expected = "SELECT NAME AS FILE_NAME, NEXT_CHANGE# AS NEXT_CHANGE, FIRST_CHANGE# AS FIRST_CHANGE FROM V$ARCHIVED_LOG " +
                " WHERE NAME IS NOT NULL AND FIRST_TIME >= SYSDATE - (1/24) AND ARCHIVED = 'YES' " +
                " AND STATUS = 'A' AND NEXT_CHANGE# > 10 ORDER BY 2";
        assertThat(result).isEqualTo(expected);

        result = SqlUtils.deleteLogFileStatement("file_name");
        expected = "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => 'file_name');END;";
        assertThat(result).isEqualTo(expected);
    }
}
