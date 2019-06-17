/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.oracle.antlr.OracleDdlParser;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
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
    private static final String TABLE_NAME = "TEST";
    private static final String CATALOG_NAME = "ORCLPDB1";
    private static final String SCHEMA_NAME = "DEBEZIUM";
    private static final String FULL_TABLE_NAME = SCHEMA_NAME + "\".\"" + TABLE_NAME;

    @Before
    public void setUp(){
        OracleValueConverters converters = new OracleValueConverters(null);
        OracleValuePreConverter preConverter = new OracleValuePreConverter();

        ddlParser = new OracleDdlParser(true, CATALOG_NAME, SCHEMA_NAME);
        dmlParser = new OracleDmlParser(true, CATALOG_NAME, SCHEMA_NAME, converters,
                preConverter);
        tables = new Tables();
    }

    @Test
    public void shouldParseInsertAndDeleteTable() throws Exception {

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "insert into \"" + FULL_TABLE_NAME + "\"(\"ID\",\"COL1\",\"COL2\",\"COL3\",\"COL4\",\"COL5\",\"COL6\",\"COL8\"," +
                "\"COL9\",\"COL10\") values ('5','4','tExt','text',NULL,NULL,NULL,NULL,EMPTY_BLOB(),EMPTY_CLOB());";
        dmlParser.parse(dml, tables);
        LogMinerRowLcr record = dmlParser.getDmlChange();
        List<LogMinerColumnValue> oldValues = record.getOldValues();
        assertThat(oldValues.size()).isEqualTo(0);

        List<LogMinerColumnValue> newValues = record.getNewValues();

        Iterator<LogMinerColumnValue> iterator = newValues.iterator();
        assertThat(iterator.next().getColumnData()).isEqualTo(new BigDecimal(5));
        assertThat(iterator.next().getColumnData()).isEqualTo(BigDecimal.valueOf(400,2));
        assertThat(iterator.next().getColumnData()).isEqualTo("tExt");
        assertThat(iterator.next().getColumnData()).isEqualTo("text");
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
        assertThat(iterator.next().getColumnData()).isNull();
// todo handle LOBS
//        assertThat(iterator.next().getColumnData()).isNull();
//        assertThat(iterator.next().getColumnData()).isNull();


        dml = "delete from \"" + FULL_TABLE_NAME +
                "\" where id = 6 and col1 = 2 and col2 = 'text' and col3 = 'tExt' and col4 is null and col5 is null " +
                " and col6 is null and col8 is null and col9 is null and col10 is null";
        dmlParser.parse(dml, tables);
        record = dmlParser.getDmlChange();
        assertThat(record.getCommandType() == Envelope.Operation.DELETE);
        newValues = record.getNewValues();
        assertThat(newValues.size()).isEqualTo(0);

        oldValues = record.getOldValues();
        assertThat(oldValues.size()).isEqualTo(10);
        String concatenatedColumnNames = oldValues.stream().map(LogMinerColumnValue::getColumnName).collect(Collectors.joining());
        assertThat("IDCOL1COL2COL3COL4COL5COL6COL8COL9COL10".equals(concatenatedColumnNames));

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
    }

    @Test
    public void shouldParseUpdateTable()  throws Exception {

        String createStatement = IoUtil.read(IoUtil.getResourceAsStream("ddl/create_table.sql", null, getClass(), null, null));
        ddlParser.parse(createStatement, tables);

        String dml = "update \"" + FULL_TABLE_NAME + "\" set \"col1\" = '9', col2 = 'diFFerent', col3 = 'anotheR', col4 = '123', col6 = 5.2, " +
                "col8 = TO_TIMESTAMP('14-MAY-19 02.28.32.302000 AM') " +
                "where ID = 5 and COL1 = 6 and \"COL2\" = 'text' " +
                "and COL3 = 'text' and COL4 IS NULL and \"COL5\" IS NULL and COL6 IS NULL " +
                "and COL8 = TO_TIMESTAMP('14-MAY-19 02.28.32.302000 AM');";

//        String dml = "update \"DEBEZIUM\".\"TEST\" set \"DUMMY\" = '5' where \"DUAL_GKEY\" = '1' and \"DUMMY\" = '6' and ROWID = 'AAAURzAAVAAAG8DAAA';";
        dmlParser.parse(dml, tables);
        LogMinerRowLcr record = dmlParser.getDmlChange();

        // validate
        assertThat(record.getCommandType() == Envelope.Operation.UPDATE);
        List<LogMinerColumnValue> newValues = record.getNewValues();
        assertThat(newValues.size()).isEqualTo(6);
        String concatenatedNames = newValues.stream().map(LogMinerColumnValue::getColumnName).collect(Collectors.joining());
        assertThat("COL1COL2COL3COL4COL6COL8".equals(concatenatedNames));
        for (LogMinerColumnValue newValue : newValues){
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
                    // todo, which one is expected value format
//                    assertThat(newValue.getColumnData()).isEqualTo(5.2);
                    assertThat(((Struct)newValue.getColumnData()).get("scale")).isEqualTo(1);
                    assertThat(((byte[])((Struct)newValue.getColumnData()).get("value"))[0]).isEqualTo((byte) 52);
                    break;
                case "COL8":
                    assertThat(newValue.getColumnData()).isInstanceOf(Long.class);
                    assertThat(newValue.getColumnData()).isEqualTo(1557800912302000L);
                    break;
            }
        }

        List<LogMinerColumnValue> oldValues = record.getOldValues();
        assertThat(oldValues.size()).isEqualTo(8);
        concatenatedNames = oldValues.stream().map(LogMinerColumnValue::getColumnName).collect(Collectors.joining());
        assertThat("IDCOL1COL2COL3COL4COL6COL8".equals(concatenatedNames));
        for (LogMinerColumnValue oldValue : oldValues){
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
                case "COL8":
                    assertThat(oldValue.getColumnData()).isInstanceOf(Long.class);
                    assertThat(oldValue.getColumnData()).isEqualTo(1557800912302000L);
                    break;
                case "ID":
                    assertThat(oldValue.getColumnData()).isEqualTo(new BigDecimal(5));
                    break;
            }
        }
    }
}
