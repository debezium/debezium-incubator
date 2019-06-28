/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.connector.oracle.logminer.valueholder.ColumnValueHolder;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerColumnValue;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcr;
import io.debezium.connector.oracle.logminer.valueholder.LogMinerRowLcrImpl;
import io.debezium.data.Envelope;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.text.ParsingException;

import java.util.List;
import java.util.stream.Collectors;

import static io.debezium.antlr.AntlrDdlParser.getText;

/**
 * This class parses UPDATE statements.
 * For the original query:
 * update debezium set test = '7' where test1 = '6' (let's assume we have 3 records with such value)
 *
 * logMiner with supply:
 *
 * update "debezium" set "TEST" = '7' where "DUMMY" = '1' and "TEST" = '2' and "TEST1" = '6' and "TEST2" = '1'
 * update "debezium" set "TEST" = '7' where "DUMMY" = '2' and "TEST" = '2' and "TEST1" = '6' and "TEST2" = '1'
 * update "debezium" set "TEST" = '7' where "DUMMY" = '3' and "TEST" = '2' and "TEST1" = '6' and "TEST2" = '1'
 *
 */
public class UpdateParserListener extends BaseDmlStringParserListener {

    UpdateParserListener(String catalogName, String schemaName, OracleDmlParser parser) {
        super(catalogName, schemaName, parser);
    }

    @Override
    protected String getKey(Column column, int index) {
        return column.name();
    }

    @Override
    public void enterUpdate_statement(PlSqlParser.Update_statementContext ctx) {
        init(ctx.general_table_ref().dml_table_expression_clause());
        parseRecursively(ctx.where_clause().expression().logical_expression());
        cloneOldToNewColumnValues();
        super.enterUpdate_statement(ctx);
    }

    @Override
    public void enterColumn_based_update_set_clause(PlSqlParser.Column_based_update_set_clauseContext ctx) {
        if (table == null) {
            throw new ParsingException(null, "Trying to parse a statement for a table which does not exist. " +
                    "Statement: " + getText(ctx));
        }
        String columnName = ctx.column_name().getText().toUpperCase();
        String stripedName = ParserListenerUtils.stripeQuotes(columnName);
        String value = ctx.getText().substring(columnName.length() + 1);
        String nullValue = ctx.expression().getStop().getText();
        if ("null".equalsIgnoreCase(nullValue)) {
            value = nullValue;
        }
       Object stripedValue = removeApostrophes(value);

        Column column = table.columnWithName(stripedName);
        Object valueObject = convertValueToSchemaType(column, stripedValue, converter);

        ColumnValueHolder columnValueHolder = newColumnValues.get(stripedName);
        columnValueHolder.setProcessed(true);
        columnValueHolder.getColumnValue().setColumnData(valueObject);

        super.enterColumn_based_update_set_clause(ctx);
    }

    @Override
    public void exitUpdate_statement(PlSqlParser.Update_statementContext ctx) {
        List<LogMinerColumnValue> actualNewValues = newColumnValues.values().stream()
                .filter(ColumnValueHolder::isProcessed).map(ColumnValueHolder::getColumnValue).collect(Collectors.toList());
        List<LogMinerColumnValue> actualOldValues = oldColumnValues.values().stream()
                .filter(ColumnValueHolder::isProcessed).map(ColumnValueHolder::getColumnValue).collect(Collectors.toList());
        LogMinerRowLcr newRecord = new LogMinerRowLcrImpl(Envelope.Operation.UPDATE, actualNewValues, actualOldValues);
        parser.setRowLCR(newRecord);
        super.exitUpdate_statement(ctx);
    }

    //initialize new column values with old column values.
    private void cloneOldToNewColumnValues() {
        for (Column column : table.columns()) {
            final ColumnValueHolder oldColumnValue = oldColumnValues.get(column.name());
            final ColumnValueHolder newColumnValue = newColumnValues.get(column.name());
            newColumnValue.setProcessed(true);
            newColumnValue.getColumnValue().setColumnData(oldColumnValue.getColumnValue().getColumnData());
        }
    }
}
