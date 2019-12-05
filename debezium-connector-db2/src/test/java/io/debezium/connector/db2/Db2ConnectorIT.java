/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotMode;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaAndValueField;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium DB2 connector.
 *
 * @author Jiri Pechanec, Luis Garcés-Erice, Peter Urbanetz
 */
public class Db2ConnectorIT extends AbstractConnectorTest {

    private Db2Connection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        TestHelper.enableDbCdc(connection);
        TestHelper.waitForCDC();
        connection.execute(
                "CREATE TABLE tablea (id int not null, cola varchar(30), primary key (id))",
                "CREATE TABLE tableb (id int not null, colb varchar(30), primary key (id))",
                "INSERT INTO tablea VALUES(1, 'a')");
        TestHelper.enableTableCdc(connection, "TABLEA");
        TestHelper.enableTableCdc(connection, "TABLEB");

        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableTableCdc(connection, "TABLEB");
            TestHelper.disableTableCdc(connection, "TABLEA");
            connection.execute("DROP TABLE tablea", "DROP TABLE tableb");
            connection.close();
        }
    }

    @Test
    public void createAndDelete() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        try {
            Thread.sleep(TestHelper.WAIT_FOR_CDC);
        }
        catch (Exception e) {
        }

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct keyA = (Struct) recordA.key();
            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get("after"), expectedRowA);
            assertNull(valueA.get("before"));

            final Struct keyB = (Struct) recordB.key();
            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        connection.execute("DELETE FROM tableB");

        TestHelper.waitForCDC();

        final SourceRecords deleteRecords = consumeRecordsByTopic(2 * RECORDS_PER_TABLE);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(deleteTableA).isNullOrEmpty();
        Assertions.assertThat(deleteTableB).hasSize(2 * RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord deleteRecord = deleteTableB.get(i * 2);
            final SourceRecord tombstoneRecord = deleteTableB.get(i * 2 + 1);
            final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct deleteKey = (Struct) deleteRecord.key();
            final Struct deleteValue = (Struct) deleteRecord.value();
            assertRecord((Struct) deleteValue.get("before"), expectedDeleteRow);
            assertNull(deleteValue.get("after"));

            final Struct tombstoneKey = (Struct) tombstoneRecord.key();
            final Struct tombstoneValue = (Struct) tombstoneRecord.value();
            assertNull(tombstoneValue);
        }

        stopConnector();
    }

    @Test
    public void deleteWithoutTombstone() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int TABLES = 2;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.execute(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }

        TestHelper.waitForCDC();

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);

        connection.execute("DELETE FROM tableB");

        TestHelper.waitForCDC();

        final SourceRecords deleteRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> deleteTableA = deleteRecords.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> deleteTableB = deleteRecords.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(deleteTableA).isNullOrEmpty();
        Assertions.assertThat(deleteTableB).hasSize(RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord deleteRecord = deleteTableB.get(i);
            final List<SchemaAndValueField> expectedDeleteRow = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct deleteKey = (Struct) deleteRecord.key();
            final Struct deleteValue = (Struct) deleteRecord.value();
            assertRecord((Struct) deleteValue.get("before"), expectedDeleteRow);
            assertNull(deleteValue.get("after"));
        }

        stopConnector();
    }

    @Test
    public void update() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.setAutoCommit(false);
        final String[] tableBInserts = new String[RECORDS_PER_TABLE];
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            tableBInserts[i] = "INSERT INTO tableb VALUES(" + id + ", 'b')";
        }
        connection.execute(tableBInserts);
        connection.setAutoCommit(true);

        connection.execute("UPDATE tableb SET colb='z'");

        TestHelper.waitForCDC();

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE * 2);
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE * 2);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct keyB = (Struct) recordB.key();
            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final SourceRecord recordB = tableB.get(i + RECORDS_PER_TABLE);
            final List<SchemaAndValueField> expectedBefore = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));
            final List<SchemaAndValueField> expectedAfter = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, i + ID_START),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "z"));

            final Struct keyB = (Struct) recordB.key();
            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("before"), expectedBefore);
            assertRecord((Struct) valueB.get("after"), expectedAfter);
        }

        stopConnector();
    }

    // urb not working
    @Test
    public void updatePrimaryKey() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Testing.Print.enable();
        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        connection.execute("INSERT INTO tableb VALUES(1, 'b')");

        TestHelper.waitForCDC();

        consumeRecordsByTopic(1);

        connection.setAutoCommit(false);

        connection.execute(
                "UPDATE tablea SET id=100 WHERE id=1",
                "UPDATE tableb SET id=100 WHERE id=1");

        TestHelper.waitForCDC();

        final SourceRecords records = consumeRecordsByTopic(2);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.DB2INST1.TABLEA");
        final List<SourceRecord> tableB = records.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(tableA).hasSize(1);
        Assertions.assertThat(tableB).hasSize(1);

        final SourceRecord updaterecordA = tableA.get(0);

        final List<SchemaAndValueField> expectedBeforeA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> expectedAfterA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));

        final Struct keyA = (Struct) updaterecordA.key();
        final Struct valueA = (Struct) updaterecordA.value();
        assertRecord((Struct) valueA.get("before"), expectedBeforeA);
        assertRecord((Struct) valueA.get("after"), expectedAfterA);

        final SourceRecord updaterecordB = tableB.get(0);

        final List<SchemaAndValueField> expectedBeforeB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> expectedAfterB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

        final Struct keyB = (Struct) updaterecordB.key();
        final Struct valueB = (Struct) updaterecordB.value();
        assertRecord((Struct) valueB.get("before"), expectedBeforeB);
        assertRecord((Struct) valueB.get("after"), expectedAfterB);

        stopConnector();
    }

    @Test
    // @FixFor("DBZ-1152")
    public void updatePrimaryKeyWithRestartInMiddle() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        start(Db2Connector.class, config, record -> {
            final Struct envelope = (Struct) record.value();
            return envelope != null && "c".equals(envelope.get("op")) && (envelope.getStruct("after").getInt32("ID") == 100);
        });
        assertConnectorIsRunning();
        // Testing.Print.enable();
        // Wait for snapshot completion
        consumeRecordsByTopic(1);
        connection.execute("INSERT INTO tableb VALUES(1, 'b')");

        TestHelper.waitForCDC();

        consumeRecordsByTopic(1);

        connection.setAutoCommit(false);

        connection.execute(
                "INSERT INTO tablea VALUES(11, 'c')",
                "INSERT INTO tableb VALUES(11, 'c')",
                "UPDATE tablea SET id=100 WHERE id=1",
                "UPDATE tableb SET id=100 WHERE id=1"

        );
        try {
            Thread.sleep(TestHelper.WAIT_FOR_CDC);
        }
        catch (Exception e) {

        }
        final SourceRecords records1 = consumeRecordsByTopic(1);
        stopConnector();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();
        final SourceRecords records2 = consumeRecordsByTopic(3);

        final List<SourceRecord> tableA = records1.recordsForTopic("testdb.DB2INST1.TABLEA");
        tableA.addAll(records2.recordsForTopic("testdb.DB2INST1.TABLEA"));
        final List<SourceRecord> tableB = records2.recordsForTopic("testdb.DB2INST1.TABLEB");
        Assertions.assertThat(tableA).hasSize(2);
        Assertions.assertThat(tableB).hasSize(2);

        final SourceRecord insertrecordA = tableA.get(0);
        final SourceRecord updaterecordA = tableA.get(1);

        final List<SchemaAndValueField> insertexpectedAfterA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 11),
                new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "c"));

        final Struct insertkeyA = (Struct) insertrecordA.key();
        final Struct insertvalueA = (Struct) insertrecordA.value();
        assertNull(insertvalueA.get("before"));
        assertRecord((Struct) insertvalueA.get("after"), insertexpectedAfterA);

        final List<SchemaAndValueField> updateexpectedBeforeA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
        final List<SchemaAndValueField> updateexpectedAfterA = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));

        final Struct updatekeyA = (Struct) updaterecordA.key();
        final Struct updatevalueA = (Struct) updaterecordA.value();
        assertRecord((Struct) updatevalueA.get("before"), updateexpectedBeforeA);
        assertRecord((Struct) updatevalueA.get("after"), updateexpectedAfterA);

        final SourceRecord insertrecordB = tableB.get(0);
        final SourceRecord updaterecordB = tableB.get(1);

        final List<SchemaAndValueField> insertexpectedAfterB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 11),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "c"));

        final Struct insertkeyB = (Struct) insertrecordB.key();
        final Struct insertvalueB = (Struct) insertrecordB.value();
        assertNull(insertvalueB.get("before"));
        assertRecord((Struct) insertvalueB.get("after"), insertexpectedAfterB);

        final List<SchemaAndValueField> updateexpectedBeforeB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 1),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        final List<SchemaAndValueField> updateexpectedAfterB = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, 100),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

        final Struct updatekeyB = (Struct) updaterecordB.key();
        final Struct updatevalueB = (Struct) updaterecordB.value();
        assertRecord((Struct) updatevalueB.get("before"), updateexpectedBeforeB);
        assertRecord((Struct) updatevalueB.get("after"), updateexpectedAfterB);

        stopConnector();
    }

    private void restartInTheMiddleOfTx(boolean restartJustAfterSnapshot, boolean afterStreaming) throws Exception {
        final int RECORDS_PER_TABLE = 30;
        final int TABLES = 2;
        final int ID_START = 10;
        final int ID_RESTART = 1000;
        final int HALF_ID = ID_START + RECORDS_PER_TABLE / 2;
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .build();

        if (restartJustAfterSnapshot) {
            start(Db2Connector.class, config);
            assertConnectorIsRunning();

            // Wait for snapshot to be completed
            consumeRecordsByTopic(1);
            stopConnector();
            connection.execute("INSERT INTO tablea VALUES(-1, '-a')");
            TestHelper.waitForCDC();

        }

        start(Db2Connector.class, config, record -> {
            if (!"testdb.DB2INST1.TABLEA.Envelope".equals(record.valueSchema().name())) {
                return false;
            }
            final Struct envelope = (Struct) record.value();
            final Struct after = envelope.getStruct("after");
            final Integer id = after.getInt32("ID");
            final String value = after.getString("COLA");
            return id != null && id == HALF_ID && "a".equals(value);
        });
        assertConnectorIsRunning();

        // Wait for snapshot to be completed or a first streaming message delivered
        consumeRecordsByTopic(1);

        if (afterStreaming) {
            connection.execute("INSERT INTO tablea VALUES(-2, '-a')");
            TestHelper.waitForCDC();
            final SourceRecords records = consumeRecordsByTopic(1);
            final List<SchemaAndValueField> expectedRow = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, -2),
                    new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "-a"));
            assertRecord(((Struct) records.allRecordsInOrder().get(0).value()).getStruct(Envelope.FieldName.AFTER), expectedRow);
        }

        connection.setAutoCommit(false);
        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.executeWithoutCommitting(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
        }
        connection.connection().commit();

        TestHelper.waitForCDC();

        List<SourceRecord> records = consumeRecordsByTopic(RECORDS_PER_TABLE).allRecordsInOrder();

        assertThat(records).hasSize(RECORDS_PER_TABLE);
        SourceRecord lastRecordForOffset = records.get(RECORDS_PER_TABLE - 1);
        Struct value = (Struct) lastRecordForOffset.value();
        final List<SchemaAndValueField> expectedLastRow = Arrays.asList(
                new SchemaAndValueField("ID", Schema.INT32_SCHEMA, HALF_ID - 1),
                new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));
        assertRecord((Struct) value.get("after"), expectedLastRow);

        stopConnector();
        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        TestHelper.waitForCDC();

        SourceRecords sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE);
        records = sourceRecords.allRecordsInOrder();
        assertThat(records).hasSize(RECORDS_PER_TABLE);

        List<SourceRecord> tableA = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEA");
        List<SourceRecord> tableB = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEB");
        for (int i = 0; i < RECORDS_PER_TABLE / 2; i++) {
            final int id = HALF_ID + i;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get("after"), expectedRowA);
            assertNull(valueA.get("before"));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_RESTART + i;
            connection.executeWithoutCommitting(
                    "INSERT INTO tablea VALUES(" + id + ", 'a')");
            connection.executeWithoutCommitting(
                    "INSERT INTO tableb VALUES(" + id + ", 'b')");
            connection.connection().commit();
        }

        TestHelper.waitForCDC();

        sourceRecords = consumeRecordsByTopic(RECORDS_PER_TABLE * TABLES);
        tableA = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEA");
        tableB = sourceRecords.recordsForTopic("testdb.DB2INST1.TABLEB");

        Assertions.assertThat(tableA).hasSize(RECORDS_PER_TABLE);
        Assertions.assertThat(tableB).hasSize(RECORDS_PER_TABLE);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = i + ID_RESTART;
            final SourceRecord recordA = tableA.get(i);
            final SourceRecord recordB = tableB.get(i);
            final List<SchemaAndValueField> expectedRowA = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLA", Schema.OPTIONAL_STRING_SCHEMA, "a"));
            final List<SchemaAndValueField> expectedRowB = Arrays.asList(
                    new SchemaAndValueField("ID", Schema.INT32_SCHEMA, id),
                    new SchemaAndValueField("COLB", Schema.OPTIONAL_STRING_SCHEMA, "b"));

            final Struct valueA = (Struct) recordA.value();
            assertRecord((Struct) valueA.get("after"), expectedRowA);
            assertNull(valueA.get("before"));

            final Struct valueB = (Struct) recordB.value();
            assertRecord((Struct) valueB.get("after"), expectedRowB);
            assertNull(valueB.get("before"));
        }
    }

    @Test
    // @FixFor("DBZ-1128")
    public void restartInTheMiddleOfTxAfterSnapshot() throws Exception {
        restartInTheMiddleOfTx(true, false);
    }

    @Test
    // @FixFor("DBZ-1128")
    public void restartInTheMiddleOfTxAfterCompletedTx() throws Exception {
        restartInTheMiddleOfTx(false, true);
    }

    @Test
    // @FixFor("DBZ-1242")
    public void testEmptySchemaWarningAfterApplyingFilters() throws Exception {
        // This captures all logged messages, allowing us to verify log message was written.
        final LogInterceptor logInterceptor = new LogInterceptor();

        Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TABLE_WHITELIST, "my_products")
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();
        waitForAvailableRecords(100, TimeUnit.MILLISECONDS);

        stopConnector(value -> assertThat(logInterceptor.containsWarnMessage(NO_MONITORED_TABLES_WARNING)).isTrue());
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
