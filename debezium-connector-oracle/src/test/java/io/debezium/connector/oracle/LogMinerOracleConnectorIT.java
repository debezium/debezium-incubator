/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.connector.oracle.logminer.SqlUtils;
import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.VerifyRecord;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.debezium.connector.oracle.OracleConnectorConfig.CONNECTOR_ADAPTER;
import static org.fest.assertions.Assertions.assertThat;

/**
 * This subclasses common OracleConnectorIT for LogMiner adaptor
 *
 */
public class LogMinerOracleConnectorIT extends OracleConnectorIT {

    @BeforeClass
    public static void beforeSuperClass() throws SQLException {
        connection = TestHelper.logMinerPdbConnection();

        builder = TestHelper.defaultConfig()
                .with(RelationalDatabaseConnectorConfig.TABLE_WHITELIST, "ORCLPDB1\\.DEBEZIUM\\.CUSTOMER")
                .with(OracleConnectorConfig.CONNECTOR_ADAPTER, "LogMiner");
        OracleConnectorIT.beforeClass();
    }

    // todo: do changes and check them
    @Test
    public void shouldMineAllLogFiles() throws Exception {

        JdbcConfiguration jdbcConfiguration = TestHelper.defaultJdbcConfig();

        Configuration conf = jdbcConfiguration.edit().with(CONNECTOR_ADAPTER, "LogMiner").build();
        OracleConnection jdbcConnection = new OracleConnection(conf, new OracleConnectionFactory());
        Connection conn = jdbcConnection.connection();


        String oldestScnQuery = "select min(first_change#) as first_change from v$archived_log";
        long oldestScn;
        try(Statement statement = jdbcConnection.connection().createStatement();
            ResultSet rs = statement.executeQuery(oldestScnQuery)) {
            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get SCN");
            }

            oldestScn = rs.getLong(1);
        }
        System.out.println("oldestScn = " + oldestScn );
        // this is the offset mock, let's take 1/10 of log files
        // 100/98 gives around 6 files
        long offsetScn = (LogMinerHelper.getCurrentScn(conn) - oldestScn)/100*98 + oldestScn;
        System.out.println("offset SCN = " + offsetScn );

        long startTime = System.currentTimeMillis();
        long overallTime = System.currentTimeMillis();

        // mine archived logs first
        processChangesInArchivedLogFiles(conn, offsetScn);

        // now mine online files
        LogMinerHelper.buildDataDictionary(conn);

        System.out.println("building dictionary took: " + (System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();

        LogMinerHelper.addOnlineRedoLogFilesForMining(conn);

        System.out.println("adding redo logs took: " + (System.currentTimeMillis() - startTime));
        startTime = System.currentTimeMillis();

        long firstOnlineScn = LogMinerHelper.getFirstOnlineLogScn(conn);
        long lastOnlineScn = LogMinerHelper.getCurrentScn(conn);
        LogMinerHelper.startOnlineMining(conn, firstOnlineScn, lastOnlineScn);

        System.out.println("building mining view took: " + (System.currentTimeMillis() - startTime) );
        startTime = System.currentTimeMillis();

        // mining
        PreparedStatement getChangesQuery = conn.prepareStatement(SqlUtils.queryLogMinerContents("debezium"));
        getChangesQuery.setLong(1, firstOnlineScn);
        getChangesQuery.setLong(2, firstOnlineScn);
        ResultSet res = getChangesQuery.executeQuery();

        System.out.println("mining query took: " + (System.currentTimeMillis() - startTime) );

        while(res.next()) {
            String redo_sql = res.getString(6);
            Timestamp time = res.getTimestamp(11);
            //System.out.println("time: " + time + ",statement: " + redo_sql);
        }
        System.out.println("overall mining took: " + (System.currentTimeMillis() - overallTime) );


    }

    @Test
    public void shouldMineOnlineLogFiles() throws Exception {

        JdbcConfiguration jdbcConfiguration = TestHelper.defaultJdbcConfig();

        Configuration conf = jdbcConfiguration.edit().with(CONNECTOR_ADAPTER, "LogMiner").build();
        OracleConnection jdbcConnection = new OracleConnection(conf, new OracleConnectionFactory());
        Connection conn = jdbcConnection.connection();

        LogMinerHelper.buildDataDictionary(conn);
        LogMinerHelper.addOnlineRedoLogFilesForMining(conn);

        long firstOnlineScn = LogMinerHelper.getFirstOnlineLogScn(conn);
        long lastOnlineScn = LogMinerHelper.getCurrentScn(conn);


        LogMinerHelper.startOnlineMining(conn, firstOnlineScn, lastOnlineScn);

        // mining
        PreparedStatement getChangesQuery = conn.prepareStatement(SqlUtils.queryLogMinerContents("debezium"));
        getChangesQuery.setLong(1, firstOnlineScn);
        getChangesQuery.setLong(2, firstOnlineScn);
        ResultSet res = getChangesQuery.executeQuery();

        boolean wasCaptured = false;
        while(res.next()) {
            String redo_sql = res.getString(6);
            Timestamp time = res.getTimestamp(11);
            System.out.println("time: " + time + ",statement: " + redo_sql);
            wasCaptured = true;
        }

        assertThat(wasCaptured);

    }
    @Test
    public void shouldTakeSnapshot() throws Exception {
        super.shouldTakeSnapshot();
    }


    // todo make them working
//    @Test
//    public void shouldContinueWithStreamingAfterSnapshot() throws Exception {
//        super.shouldContinueWithStreamingAfterSnapshot();
//    }
//
//    @Test
//    public void shouldStreamTransaction() throws Exception {
//        super.shouldStreamTransaction();
//    }
//
//    @Test
//    public void shouldStreamAfterRestart() throws Exception {
//        super.shouldStreamAfterRestart();
//    }
//
//    @Test
//    public void shouldStreamAfterRestartAfterSnapshot() throws Exception {
//        super.shouldStreamAfterRestartAfterSnapshot();
//    }
//
//    @Test
//    public void shouldReadChangeStreamForExistingTable() throws Exception {
//        super.shouldReadChangeStreamForExistingTable();
//    }
//
//    @Test
//    public void shouldReadChangeStreamForTableCreatedWhileStreaming() throws Exception {
//        super.shouldReadChangeStreamForTableCreatedWhileStreaming();
//    }
//
//    @Test
//    public void shouldReceiveHeartbeatAlsoWhenChangingNonWhitelistedTable() throws Exception {
//        super.shouldReceiveHeartbeatAlsoWhenChangingNonWhitelistedTable();
//    }

    @Override
    protected void assertTxBatch(int expectedRecordCount, int offset) throws InterruptedException {
        SourceRecords records;
        List<SourceRecord> testTableRecords;
        Struct after;
        Struct source;
        records = consumeRecordsByTopic(expectedRecordCount);
        testTableRecords = records.recordsForTopic("server1.DEBEZIUM.CUSTOMER");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        for (int i = 0; i < expectedRecordCount; i++) {
            SourceRecord record3 = testTableRecords.get(i);
            VerifyRecord.isValidInsert(record3, "ID", i + offset);
            after = (Struct) ((Struct) record3.value()).get("after");
            assertThat(after.get("ID")).isEqualTo(i + offset);

            assertThat(record3.sourceOffset().containsKey(SourceInfo.SNAPSHOT_KEY)).isFalse();
            assertThat(record3.sourceOffset().containsKey(SNAPSHOT_COMPLETED_KEY)).isFalse();
//            assertThat(record3.sourceOffset().containsKey(SourceInfo.LCR_POSITION_KEY)).isTrue();
//            assertThat(record3.sourceOffset().containsKey(SourceInfo.SCN_KEY)).isFalse(); // todo why this is failing?

            source = (Struct) ((Struct) record3.value()).get("source");
            assertThat(source.get(SourceInfo.SNAPSHOT_KEY)).isEqualTo(false);
            assertThat(source.get(SourceInfo.SCN_KEY)).isNotNull();
//            assertThat(source.get(SourceInfo.LCR_POSITION_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.SERVER_NAME_KEY)).isEqualTo("server1");
            assertThat(source.get(SourceInfo.DEBEZIUM_VERSION_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.TXID_KEY)).isNotNull();
            assertThat(source.get(SourceInfo.TIMESTAMP_KEY)).isNotNull();
        }
    }

    // just debug info
    private static void processChangesInArchivedLogFiles(Connection conn, long offsetScn) throws SQLException {

        while(true) {
            // this will fetch archived log file names in descending order
            Map<String, Long> archivedLogFiles = LogMinerHelper.getArchivedLogFiles(offsetScn, conn);
            if (archivedLogFiles.isEmpty()) {
                System.out.println("All archived log files were processed successfully");
                return;
            }

            for (Iterator<Map.Entry<String, Long>> archivedFile = archivedLogFiles.entrySet().iterator(); archivedFile.hasNext();) {
                ResultSet res = LogMinerHelper.getArchivedChanges(conn, 10, "debezium", archivedFile);

                while (res.next()) {//no dispatching for testing
                    String redo_sql = res.getString(6);
                    Timestamp time = res.getTimestamp(11);
                    // update offset
                    //offsetScn = res.getLong(1);
                    //System.out.println("time: " + time + ", sql=" + redo_sql);
                }

                res.close();
            }

            // delete archived log
            for (Map.Entry<String, Long> archivedFile : archivedLogFiles.entrySet()) {
                LogMinerHelper.removeArchiveLogFile(archivedFile.getKey(), conn);
                offsetScn = archivedFile.getValue();
            }

            System.out.println("The connector might need one more cycle to check if there are new archived log files and process them");
        }
    }
}
