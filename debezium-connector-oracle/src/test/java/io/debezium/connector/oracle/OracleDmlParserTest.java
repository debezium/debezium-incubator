/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.data.Envelope;
import io.debezium.relational.Tables;
import io.debezium.util.IoUtil;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
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
    private static final String TABLE_NAME = "DEBEZIUM";

    @Before
    public void setUp(){
        OracleValueConverters converters = new OracleValueConverters(null);
        OracleValuePreConverter preConverter = new OracleValuePreConverter();

        ddlParser = new OracleDdlParser(true, null, null);
        dmlParser = new OracleDmlParser(true, null, null, converters,
                preConverter);
        tables = new Tables();
    }

    @Test
    public void shouldParseInsertAndDeleteTable() throws Exception {

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "insert into \"DEBEZIUM\"(\"ID\",\"COL1\",\"COL2\",\"COL3\",\"COL4\",\"COL5\",\"COL6\",\"COL7\",\"COL8\"," +
                "\"COL9\",\"COL10\") values ('5','4','tExt','text',NULL,NULL,NULL,NULL,NULL,EMPTY_BLOB(),EMPTY_CLOB());";
        dmlParser.parse(dml, tables);
        RowLCR record = dmlParser.getDmlChange();
        List<ColumnValue> oldValues = record.getOldValues();
        assertThat(oldValues.size()).isEqualTo(0);

        List<ColumnValue> newValues = record.getNewValues();

        Iterator<ColumnValue> iterator = newValues.iterator();
        assertThat(iterator.next().getColumnData()).isEqualTo(new BigDecimal(5));
        assertThat(iterator.next().getColumnData()).isEqualTo(BigDecimal.valueOf(400,2));
        assertThat(iterator.next().getColumnData()).isEqualTo("tExt");
        assertThat(iterator.next().getColumnData()).isEqualTo("text");
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
// todo handle LOBS
//        assertThat(iterator.next().getColumnData()).isNull();
//        assertThat(iterator.next().getColumnData()).isNull();


        dml = "delete from " + TABLE_NAME +
                " where id = 6 and col1 = 2 and col2 = 'text' and col3 = 'tExt' and col4 is null and col5 is null " +
                " and col6 is null and col7 is null and col8 is null and col9 is null and col10 is null";
        dmlParser.parse(dml, tables);
        record = dmlParser.getDmlChange();
        assertThat(record.getCommandType() == Envelope.Operation.DELETE);
        newValues = record.getNewValues();
        assertThat(newValues.size()).isEqualTo(0);

        oldValues = record.getOldValues();
        assertThat(oldValues.size()).isEqualTo(11);
        String concatenatedColumnNames = oldValues.stream().map(ColumnValue::getColumnName).collect(Collectors.joining());
        assertThat("IDCOL1COL2COL3COL4COL5COL6COL7COL8COL9COL10".equals(concatenatedColumnNames));

        iterator = oldValues.iterator();
        assertThat(iterator.next().getColumnData()).isEqualTo(new BigDecimal(6));
        assertThat(iterator.next().getColumnData()).isEqualTo(BigDecimal.valueOf(200,2));
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

    @Test
    public void shouldParseUpdateTable()  throws Exception {
        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "update " + TABLE_NAME + " set col1 = 9, col2 = 'diFFerent', col3 = 'anotheR', col4 = '123', col6 = 5.2, " +
                "col7 = '2019-02-28 07:49:26', col8 = '2019-02-28 07:49:26.157000' " +
                "where ID = 5 and COL1 = 6 and COL2 = 'text' " +
                "and COL3 = 'text' and COL4 IS NULL and COL5 IS NULL and COL6 IS NULL " +
                "and COL7 = TIMESTAMP ' 2019-02-27 18:08:52' and COL8 = TIMESTAMP ' 2019-02-27 18:08:52.650000';";
        dmlParser.parse(dml, tables);
        RowLCR record = dmlParser.getDmlChange();

        // validate
        assertThat(record.getCommandType() == Envelope.Operation.UPDATE);
        List<ColumnValue> newValues = record.getNewValues();
        assertThat(newValues.size()).isEqualTo(7);
        String concatenatedNames = newValues.stream().map(ColumnValue::getColumnName).collect(Collectors.joining());
        assertThat("COL1COL2COL3COL4COL6COL7COL8".equals(concatenatedNames));
        for (ColumnValue newValue : newValues){
            String columnName = newValue.getColumnName();
            switch (columnName){
                case "COL1":
                    assertThat(newValue.getColumnData()).isEqualTo(BigDecimal.valueOf(900,2));
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
                    // todo, please review which one is expected value format
//                    assertThat(newValue.getColumnData()).isEqualTo(5.2);
                    assertThat(((Struct)newValue.getColumnData()).get("scale")).isEqualTo(1);
                    assertThat(((byte[])((Struct)newValue.getColumnData()).get("value"))[0]).isEqualTo((byte) 52);
                    break;
                case "COL7":
                    assertThat(newValue.getColumnData()).isInstanceOf(Long.class);
                    assertThat(newValue.getColumnData()).isEqualTo(1551340166000L);
                    break;
                case "COL8":
                    assertThat(newValue.getColumnData()).isInstanceOf(Long.class);
                    assertThat(newValue.getColumnData()).isEqualTo(1551340166157000L);
                    break;
            }
        }

        List<ColumnValue> oldValues = record.getOldValues();
        assertThat(oldValues.size()).isEqualTo(9);
        concatenatedNames = oldValues.stream().map(ColumnValue::getColumnName).collect(Collectors.joining());
        assertThat("IDCOL1COL2COL3COL4COL6COL7COL8".equals(concatenatedNames));
        for (ColumnValue oldValue : oldValues){
            String columnName = oldValue.getColumnName();
            switch (columnName){
                case "COL1":
                    assertThat(oldValue.getColumnData()).isEqualTo(BigDecimal.valueOf(600,2));
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
                case "COL7":
                    assertThat(oldValue.getColumnData()).isInstanceOf(Long.class);
                    assertThat(oldValue.getColumnData()).isEqualTo(1551290932000L);
                    break;
                case "COL8":
                    assertThat(oldValue.getColumnData()).isInstanceOf(Long.class);
                    assertThat(oldValue.getColumnData()).isEqualTo(1551290932650000L);
                    break;
                case "ID":
                    assertThat(oldValue.getColumnData()).isEqualTo(new BigDecimal(5));
                    break;
            }
        }
    }
}
