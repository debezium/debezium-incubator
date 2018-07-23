/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.connector.oracle.util.TestHelper;
import io.debezium.data.SchemaAndValueField;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.time.MicroDuration;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.Testing;

/**
 * Integration test to verify different Oracle datatypes.
 *
 * @author Jiri Pechanec
 */
public abstract class AbstractOracleDatatypesTest extends AbstractConnectorTest {

    /**
     * Key for schema parameter used to store DECIMAL/NUMERIC columns' precision.
     */
    static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";

    private static final Schema NUMBER_SCHEMA = Decimal.builder(0).optional().parameter(PRECISION_PARAMETER_KEY, "38").build();

    private static final String DDL_STRING = "create table debezium.type_string (" +
            "  id int not null, " +
            "  val_varchar2 varchar2(1000), " +
            "  val_nvarchar2 nvarchar2(1000), " +
            "  val_char char(3), " +
            "  val_nchar nchar(3), " +
            "  primary key (id)" +
            ")";

    private static final String DDL_FP = "create table debezium.type_fp (" +
            "  id int not null, " +
            "  val_bf binary_float, " +
            "  val_bd binary_double, " +
            "  val_f float, " +
            "  val_f_10 float (10), " +
            "  val_num number(10,6), " +
            "  val_dp double precision, " +
            "  val_r real, " +
            "  val_decimal decimal(10, 6), " +
            "  val_numeric numeric(10, 6), " +
            "  val_num_vs number, " +
            "  primary key (id)" +
            ")";

    private static final String DDL_INT = "create table debezium.type_int (" +
            "  id int not null, " +
            "  val_int int, " +
            "  val_integer integer, " +
            "  val_smallint smallint, " +
            "  val_number_38_no_scale number(38), " +
            "  val_number_38_scale_0 number(38, 0), " +
            "  primary key (id)" +
            ")";

    private static final String DDL_TIME = "create table debezium.type_time (" +
            "  id int not null, " +
            "  val_date date, " +
            "  val_ts timestamp, " +
            "  val_tstz timestamp with time zone, " +
            "  val_tsltz timestamp with local time zone, " +
            "  val_int_ytm interval year to month, " +
            "  val_int_dts interval day(3) to second(2), " +
            "  primary key (id)" +
            ")";

    private static final List<SchemaAndValueField> EXPECTED_STRING = Arrays.asList(
            new SchemaAndValueField("VAL_VARCHAR2", Schema.OPTIONAL_STRING_SCHEMA, "v\u010d2"),
            new SchemaAndValueField("VAL_NVARCHAR2", Schema.OPTIONAL_STRING_SCHEMA, "nv\u010d2"),
            new SchemaAndValueField("VAL_CHAR", Schema.OPTIONAL_STRING_SCHEMA, "c  "),
            new SchemaAndValueField("VAL_NCHAR", Schema.OPTIONAL_STRING_SCHEMA, "n\u010d ")
    );

    private static final List<SchemaAndValueField> EXPECTED_FP = Arrays.asList(
            new SchemaAndValueField("VAL_BF", Schema.OPTIONAL_FLOAT32_SCHEMA, 1.1f),
            new SchemaAndValueField("VAL_BD", Schema.OPTIONAL_FLOAT64_SCHEMA, 2.22),
            new SchemaAndValueField("VAL_F", VariableScaleDecimal.builder().optional().build(), VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new SpecialValueDecimal(new BigDecimal("3.33")))),
            new SchemaAndValueField("VAL_F_10", VariableScaleDecimal.builder().optional().build(), VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new SpecialValueDecimal(new BigDecimal("8.888")))),
            new SchemaAndValueField("VAL_NUM", Decimal.builder(6).parameter(PRECISION_PARAMETER_KEY, "10").optional().build(), new BigDecimal("4.4444")),
            new SchemaAndValueField("VAL_DP", VariableScaleDecimal.builder().optional().build(), VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new SpecialValueDecimal(new BigDecimal("5.555")))),
            new SchemaAndValueField("VAL_R", VariableScaleDecimal.builder().optional().build(), VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new SpecialValueDecimal(new BigDecimal("6.66")))),
            new SchemaAndValueField("VAL_DECIMAL", Decimal.builder(6).parameter(PRECISION_PARAMETER_KEY, "10").optional().build(), new BigDecimal("1234.567891")),
            new SchemaAndValueField("VAL_NUMERIC", Decimal.builder(6).parameter(PRECISION_PARAMETER_KEY, "10").optional().build(), new BigDecimal("1234.567891")),
            new SchemaAndValueField("VAL_NUM_VS", VariableScaleDecimal.builder().optional().build(), VariableScaleDecimal.fromLogical(VariableScaleDecimal.builder().optional().build(), new SpecialValueDecimal(new BigDecimal("77.323"))))
    );

    private static final List<SchemaAndValueField> EXPECTED_INT = Arrays.asList(
            new SchemaAndValueField("VAL_INT", NUMBER_SCHEMA, new BigDecimal("1")),
            new SchemaAndValueField("VAL_INTEGER", NUMBER_SCHEMA, new BigDecimal("22")),
            new SchemaAndValueField("VAL_SMALLINT", NUMBER_SCHEMA, new BigDecimal("333")),
            new SchemaAndValueField("VAL_NUMBER_38_NO_SCALE", NUMBER_SCHEMA, new BigDecimal("4444")),
            new SchemaAndValueField("VAL_NUMBER_38_SCALE_0", NUMBER_SCHEMA, new BigDecimal("5555"))
    );

    private static final List<SchemaAndValueField> EXPECTED_TIME = Arrays.asList(
            new SchemaAndValueField("VAL_DATE", Timestamp.builder().optional().build(), 1522108800_000l),
            new SchemaAndValueField("VAL_TS", MicroTimestamp.builder().optional().build(), LocalDateTime.of(2018, 3, 27, 12, 34, 56).toEpochSecond(ZoneOffset.UTC) * 1_000_000 + 7890),
            new SchemaAndValueField("VAL_TSTZ", ZonedTimestamp.builder().optional().build(), "2018-03-27T01:34:56.00789-11:00"),
            new SchemaAndValueField("VAL_INT_YTM", MicroDuration.builder().optional().build(), -110451600_000_000.0),
            new SchemaAndValueField("VAL_INT_DTS", MicroDuration.builder().optional().build(), -93784_560_000.0)
//            new SchemaAndValueField("VAL_TSLTZ", ZonedTimestamp.builder().optional().build(), "2018-03-27T01:34:56.00789-11:00")
    );

    private static final String[] ALL_TABLES = {
            "debezium.type_string",
            "debezium.type_fp",
            "debezium.type_int",
            "debezium.type_time"
    };

    private static final String[] ALL_DDLS = {
            DDL_STRING,
            DDL_FP,
            DDL_INT,
            DDL_TIME
    };

    private static OracleConnection connection;

    @BeforeClass
    public static void dropTables() throws SQLException {
        connection = TestHelper.testConnection();
        for (String table: ALL_TABLES) {
            TestHelper.dropTable(connection, table);
        }
    }

    protected static void createTables() throws SQLException {
        connection.execute(ALL_DDLS);
        for (String table: ALL_TABLES) {
            streamTable(table);
        }
    }

    protected List<String> getAllTables() {
        return Arrays.asList(ALL_TABLES);
    }

    protected abstract boolean insertRecordsDuringTest();

    private static void streamTable(String table) throws SQLException {
        connection.execute("GRANT SELECT ON " + table + " to " +  TestHelper.CONNECTOR_USER);
        connection.execute("ALTER TABLE " + table + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }

    @AfterClass
    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void stringTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertStringTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_STRING");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(testTableRecords.get(0));
        }
        else {
            VerifyRecord.isValidRead(testTableRecords.get(0));
        }

        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_STRING);
    }

    @Test
    public void fpTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertFpTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_FP");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(testTableRecords.get(0));
        }
        else {
            VerifyRecord.isValidRead(testTableRecords.get(0));
        }

        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_FP);
    }

    @Test
    public void intTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertIntTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_INT");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(testTableRecords.get(0));
        }
        else {
            VerifyRecord.isValidRead(testTableRecords.get(0));
        }

        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_INT);
    }

    @Test
    public void timeTypes() throws Exception {
        int expectedRecordCount = 0;

        if (insertRecordsDuringTest()) {
            insertTimeTypes();
        }

        Testing.debug("Inserted");
        expectedRecordCount++;

        final SourceRecords records = consumeRecordsByTopic(expectedRecordCount);

        List<SourceRecord> testTableRecords = records.recordsForTopic("server1.DEBEZIUM.TYPE_TIME");
        assertThat(testTableRecords).hasSize(expectedRecordCount);

        // insert
        if (insertRecordsDuringTest()) {
            VerifyRecord.isValidInsert(testTableRecords.get(0));
        }
        else {
            VerifyRecord.isValidRead(testTableRecords.get(0));
        }

        Struct after = (Struct) ((Struct)testTableRecords.get(0).value()).get("after");
        assertRecord(after, EXPECTED_TIME);
    }

    protected static void insertStringTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_string VALUES (1, 'v\u010d2', 'nv\u010d2', 'c', 'n\u010d')");
        connection.execute("COMMIT");
    }

    protected static void insertFpTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_fp VALUES (1, 1.1, 2.22, 3.33, 8.888, 4.4444, 5.555, 6.66, 1234.567891, 1234.567891, 77.323)");
        connection.execute("COMMIT");
    }

    protected static void insertIntTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_int VALUES (1, 1, 22, 333, 4444, 5555)");
        connection.execute("COMMIT");
    }

    protected static void insertTimeTypes() throws SQLException {
        connection.execute("INSERT INTO debezium.type_time VALUES ("
                + "1"
                + ", TO_DATE('27-MAR-2018', 'dd-MON-yyyy')"
                + ", TO_TIMESTAMP('27-MAR-2018 12:34:56.00789', 'dd-MON-yyyy HH24:MI:SS.FF5')"
                + ", TO_TIMESTAMP_TZ('27-MAR-2018 01:34:56.00789 -11:00', 'dd-MON-yyyy HH24:MI:SS.FF5 TZH:TZM')"
                + ", TO_TIMESTAMP_TZ('27-MAR-2018 01:34:56.00789', 'dd-MON-yyyy HH24:MI:SS.FF5')"
                + ", INTERVAL '-3-6' YEAR TO MONTH"
                + ", INTERVAL '-1 2:3:4.56' DAY TO SECOND"
                + ")");
        connection.execute("COMMIT");
    }

    private void assertRecord(Struct record, List<SchemaAndValueField> expected) {
        expected.forEach(schemaAndValueField -> schemaAndValueField.assertFor(record));
    }
}
