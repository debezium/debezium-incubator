/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig.SnapshotMode;
import io.debezium.connector.db2.util.TestHelper;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Testing;

/**
 * Integration test for the Debezium DB2 connector testing support for mixed case schema and table.
 *
 * @author Luis Garcés-Erice
 */
public class Db2CaseSensitiveObjectIT extends AbstractConnectorTest {

    private Db2Connection connection;

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();
        connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
        connection.execute(
                "CREATE TABLE \"testSchema\".\"tablea\" (id int not null, cola varchar(30), primary key (id))",
                "INSERT INTO \"testSchema\".\"tablea\" VALUES(1, 'a')");
        TestHelper.enableTableCdc(connection, "testSchema", "tablea");
        initializeConnectorTestFramework();
        Testing.Files.delete(TestHelper.DB_HISTORY_PATH);
        Testing.Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            TestHelper.disableDbCdc(connection);
            TestHelper.disableTableCdc(connection, "testSchema", "tablea");
            connection.execute("DROP TABLE \"testSchema\".\"tablea\"");
            connection.execute("DELETE FROM ASNCDC.IBMSNAP_REGISTER");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_COLVERSION");
            connection.execute("DELETE FROM ASNCDC.IBMQREP_TABVERSION");
            connection.close();
        }
    }

    @Test
    @FixFor("DBZ-2796")
    public void testCaseSensitiveSchemaAndTable() throws Exception {
        final int RECORDS_PER_TABLE = 5;
        final int ID_START = 10;
        final Configuration config = TestHelper.defaultConfig()
                .with(Db2ConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(Db2ConnectorConfig.TABLE_INCLUDE_LIST, "testSchema.tablea")
                .build();

        start(Db2Connector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        consumeRecordsByTopic(1);

        TestHelper.enableDbCdc(connection);
        connection.execute("UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = 'testSchema'");
        TestHelper.refreshAndWait(connection);

        for (int i = 0; i < RECORDS_PER_TABLE; i++) {
            final int id = ID_START + i;
            connection.execute(
                    "INSERT INTO \"testSchema\".\"tablea\" VALUES(" + id + ", 'a')");
        }

        TestHelper.refreshAndWait(connection);

        final SourceRecords records = consumeRecordsByTopic(RECORDS_PER_TABLE);
        final List<SourceRecord> tableA = records.recordsForTopic("testdb.testSchema.tablea");
        assertThat(tableA).hasSize(RECORDS_PER_TABLE);

        stopConnector();
    }
}
