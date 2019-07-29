/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.OracleChangeRecordValueConverter;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
import io.debezium.data.Envelope;
import io.debezium.relational.Tables;
import io.debezium.util.IoUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.fest.assertions.Assertions.assertThat;


/**
 * This is the test suite for Oracle Antlr dmlParser unit testing
 */
public class OracleDmlParserTest {

    private OracleDdlParser ddlParser;
    private OracleDmlParser dmlParser;
    private Tables tables;
    private static final String TABLE_NAME = "TEST";
    private static final String CATALOG_NAME = "ORCLPDB1";
    private static final String SCHEMA_NAME = "DEBEZIUM";
    private static final String FULL_TABLE_NAME = SCHEMA_NAME + "\".\"" + TABLE_NAME;
    private static final String SPATIAL_DATA = "SDO_GEOMETRY(2003, NULL, NULL, SDO_ELEM_INFO_ARRAY(1, 1003, 1), SDO_ORDINATE_ARRAY" +
            "(102604.878, 85772.8286, 101994.879, 85773.6633, 101992.739, 84209.6648, 102602.738, 84208.83, 102604.878, 85772.8286))";
    private static String CLOB_DATA;
    private static byte[] BLOB_DATA; // todo


    @Before
    public void setUp(){
        OracleChangeRecordValueConverter converters = new OracleChangeRecordValueConverter(null);

        ddlParser = new OracleDdlParser(true, CATALOG_NAME, SCHEMA_NAME);
        dmlParser = new OracleDmlParser(true, CATALOG_NAME, SCHEMA_NAME, converters);
        tables = new Tables();

        CLOB_DATA = StringUtils.repeat("clob_", 4000);
        String blobString = "blob_";
        BLOB_DATA = Arrays.copyOf(blobString.getBytes(), 8000); //todo doesn't support blob

    }

    @Test
    public void shouldParseAlias() throws Exception {
        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "update \"" + FULL_TABLE_NAME + "\" a set a.\"col1\" = '9', a.col2 = 'diFFerent', a.col3 = 'anotheR', a.col4 = '123', a.col6 = 5.2, " +
                "a.col8 = TO_TIMESTAMP('2019-05-14 02:28:32.302000'), a.col10 = " + CLOB_DATA + ", a.col11 = null, a.col12 = '1' "+
                "where a.ID = 5 and a.COL1 = 6 and a.\"COL2\" = 'text' " +
                "and a.COL3 = 'text' and a.COL4 IS NULL and a.\"COL5\" IS NULL and a.COL6 IS NULL " +
                "and a.COL8 = TO_TIMESTAMP('2019-05-14 02:28:32.') and a.col11 is null;";

        dmlParser.parse(dml, tables);
        LogMinerRowLcr record = dmlParser.getDmlChange();
        verifyUpdate(record, false);

        dml = "insert into \"" + FULL_TABLE_NAME + "\" a (a.\"ID\",a.\"COL1\",a.\"COL2\",a.\"COL3\",a.\"COL4\",a.\"COL5\",a.\"COL6\",a.\"COL8\"," +
                "a.\"COL9\",a.\"COL10\") values ('5','4','tExt','text',NULL,NULL,NULL,NULL,EMPTY_BLOB(),EMPTY_CLOB());";
        dmlParser.parse(dml, tables);
        record = dmlParser.getDmlChange();
        verifyInsert(record);

    }

    @Test
    public void shouldParseInsertAndDeleteTable() throws Exception {

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "insert into \"" + FULL_TABLE_NAME + "\"(\"ID\",\"COL1\",\"COL2\",\"COL3\",\"COL4\",\"COL5\",\"COL6\",\"COL8\"," +
                "\"COL9\",\"COL10\") values ('5','4','tExt','text',NULL,NULL,NULL,NULL,EMPTY_BLOB(),EMPTY_CLOB());";
        dmlParser.parse(dml, tables);
        LogMinerRowLcr record = dmlParser.getDmlChange();
        verifyInsert(record);

        dml = "delete from \"" + FULL_TABLE_NAME +
                "\" where id = 6 and col1 = 2 and col2 = 'text' and col3 = 'tExt' and col4 is null and col5 is null " +
                " and col6 is null and col8 is null and col9 is null and col10 is null";
        dmlParser.parse(dml, tables);
        record = dmlParser.getDmlChange();
        verifyDelete(record);

    }

    @Test
    public void shouldParseUpdateTable()  throws Exception {

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "update \"" + FULL_TABLE_NAME + "\" set \"col1\" = '9', col2 = 'diFFerent', col3 = 'anotheR', col4 = '123', col6 = '5.2', " +
                "col8 = TO_TIMESTAMP('2019-05-14 02:28:32.302000'), col10='clob_', col12 = '1' " +
                "where ID = 5 and COL1 = 6 and \"COL2\" = 'text' " +
                "and COL3 = 'text' and COL4 IS NULL and \"COL5\" IS NULL and COL6 IS NULL " +
                "and COL8 = TO_TIMESTAMP('2019-05-14 02:28:32') and col11 = " + SPATIAL_DATA + ";";

        dmlParser.parse(dml, tables);
        LogMinerRowLcr record = dmlParser.getDmlChange();
        verifyUpdate(record, true);

    }

    private void verifyUpdate(LogMinerRowLcr record, boolean checkGeometry) {
        // validate
        assertThat(record.getCommandType() == Envelope.Operation.UPDATE);
        List<LogMinerColumnValue> newValues = record.getNewValues();
        assertThat(newValues.size()).isEqualTo(12);
        String concatenatedNames = newValues.stream().map(LogMinerColumnValue::getColumnName).collect(Collectors.joining());
        assertThat("IDCOL1COL2COL3COL4COL5COL6COL8COL9COL10COL11COL12".equals(concatenatedNames));
        for (LogMinerColumnValue newValue : newValues){
            String columnName = newValue.getColumnName();
            switch (columnName){
                case "COL1":
                    assertThat(newValue.getColumnData()).isEqualTo(BigDecimal.valueOf(9,0));
                    break;
                case "COL2":
                    assertThat(newValue.getColumnData()).isEqualTo("diFFerent");
                    break;
                case "COL3":
                    assertThat(newValue.getColumnData()).isEqualTo("anotheR");
                    break;
                case "COL4":
                    assertThat(newValue.getColumnData()).isEqualTo("123");
                    break;
                case "COL6":
                    // todo, which one is expected value format
                   assertThat(newValue.getColumnData()).isEqualTo(5.2F);
//                    assertThat(((Struct)newValue.getColumnData()).get("scale")).isEqualTo(1);
  //                  assertThat(((byte[])((Struct)newValue.getColumnData()).get("value"))[0]).isEqualTo((byte) 52);
                    break;
                case "COL8":
                    assertThat(newValue.getColumnData()).isInstanceOf(Long.class);
                    assertThat(newValue.getColumnData()).isEqualTo(1557826112302000L);
                    break;
                case "COL10":
                    assertThat(newValue.getColumnData()).isInstanceOf(String.class);
                    assertThat(newValue.getColumnData().toString().contains("clob_")).isTrue();
                    break;
                case "COL11":
                    if (checkGeometry) {
                        assertThat(newValue.getColumnData()).isInstanceOf(String.class);
                        assertThat(newValue.getColumnData().toString().contains("SDO_GEOMETRY")).isTrue();
                    } else {
                        assertThat(newValue.getColumnData()).isNull();
                    }
                    break;
                case "COL12":
                    assertThat(newValue.getColumnData()).isInstanceOf(Boolean.class);
                    assertThat(newValue.getColumnData()).isEqualTo(true);
                    break;
            }
        }

        List<LogMinerColumnValue> oldValues = record.getOldValues();
        assertThat(oldValues.size()).isEqualTo(9);
        concatenatedNames = oldValues.stream().map(LogMinerColumnValue::getColumnName).collect(Collectors.joining());
        assertThat("IDCOL1COL2COL3COL4COL6COL8COL11COL12".equals(concatenatedNames));
        for (LogMinerColumnValue oldValue : oldValues){
            String columnName = oldValue.getColumnName();
            switch (columnName){
                case "COL1":
                    assertThat(oldValue.getColumnData()).isEqualTo(BigDecimal.valueOf(6,0));
                    break;
                case "COL2":
                    assertThat(oldValue.getColumnData()).isEqualTo("text");
                    break;
                case "COL3":
                    assertThat(oldValue.getColumnData()).isEqualTo("text");
                    break;
                case "COL4":
                    assertThat(oldValue.getColumnData()).isNull();
                    break;
                case "COL5":
                    assertThat(oldValue.getColumnData()).isNull();
                    break;
                case "COL6":
                    assertThat(oldValue.getColumnData()).isNull();
                    break;
                case "COL8":
                    assertThat(oldValue.getColumnData()).isInstanceOf(Long.class);
                    assertThat(oldValue.getColumnData()).isEqualTo(1557826112000000L);
                    break;
                case "COL11":
                    if (checkGeometry) {
                        assertThat(oldValue.getColumnData()).isInstanceOf(String.class);
                        assertThat(oldValue.getColumnData().toString().contains("SDO_GEOMETRY")).isTrue();
                    } else {
                        assertThat(oldValue.getColumnData()).isNull();
                    }
                    break;
                case "ID":
                    assertThat(oldValue.getColumnData()).isEqualTo(new BigDecimal(5));
                    break;
            }
        }
    }

    private void verifyInsert(LogMinerRowLcr record) {
        List<LogMinerColumnValue> oldValues = record.getOldValues();
        assertThat(oldValues.size()).isEqualTo(0);

        List<LogMinerColumnValue> newValues = record.getNewValues();

        Iterator<LogMinerColumnValue> iterator = newValues.iterator();
        assertThat(iterator.next().getColumnData()).isEqualTo(new BigDecimal(5));
        assertThat(iterator.next().getColumnData()).isEqualTo(BigDecimal.valueOf(4,0));
        assertThat(iterator.next().getColumnData()).isEqualTo("tExt");
        assertThat(iterator.next().getColumnData()).isEqualTo("text");
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
// todo handle LOBS
//        assertThat(iterator.next().getColumnData()).isNull();
//        assertThat(iterator.next().getColumnData()).isNull();


    }
    private void verifyDelete(LogMinerRowLcr record) {
        assertThat(record.getCommandType() == Envelope.Operation.DELETE);
        List<LogMinerColumnValue> newValues = record.getNewValues();
        assertThat(newValues.size()).isEqualTo(0);

        List<LogMinerColumnValue> oldValues = record.getOldValues();
        assertThat(oldValues.size()).isEqualTo(12);
        String concatenatedColumnNames = oldValues.stream().map(LogMinerColumnValue::getColumnName).collect(Collectors.joining());
        assertThat("IDCOL1COL2COL3COL4COL5COL6COL8COL9COL10COL11COL12".equals(concatenatedColumnNames));

        Iterator<LogMinerColumnValue>  iterator = oldValues.iterator();
        assertThat(iterator.next().getColumnData()).isEqualTo(new BigDecimal(6));
        assertThat(iterator.next().getColumnData()).isEqualTo(BigDecimal.valueOf(2,0));
        assertThat(iterator.next().getColumnData()).isEqualTo("text");
        assertThat(iterator.next().getColumnData()).isEqualTo("tExt");
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
    }
}
