/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import io.debezium.connector.oracle.ColumnValue;
import io.debezium.connector.oracle.ColumnValueHolder;
import io.debezium.connector.oracle.DefaultRowLCR;
import io.debezium.connector.oracle.RowLCR;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.data.Envelope;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.text.ParsingException;

import java.util.List;
import java.util.stream.Collectors;

import static io.debezium.antlr.AntlrDdlParser.getText;

/**
 * This class parses UPDATE statements.
 * on the original query:
 * update debezium set test = '7' where test1 = '6' (we have 3 records with such value)
 *
 * logMiner with supply:
 *
 * update "debezium" set "TEST" = '7' where "DUMMY" = '1' and "TEST" = '6' and "TEST1" = '1' and "TEST2" = '1'
 * update "debezium" set "TEST" = '7' where "DUMMY" = '2' and "TEST" = '6' and "TEST1" = '1' and "TEST2" = '1'
 * update "debezium" set "TEST" = '7' where "DUMMY" = '3' and "TEST" = '6' and "TEST1" = '1' and "TEST2" = '1'
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
        super.enterUpdate_statement(ctx);
    }

    @Override
    public void enterColumn_based_update_set_clause(PlSqlParser.Column_based_update_set_clauseContext ctx) {
        if (table == null) {
            throw new ParsingException(null, "Trying to parse a statement for a table which does not exist. " +
                    "Statement: " + getText(ctx));
        }
        String columnName = ctx.column_name().getText().toUpperCase();
        String value = ctx.expression().getStop().getText();
        value = removeApostrophes(value);

        Column column = table.columnWithName(columnName);
        Object valueObject = convertValueToSchemaType(column, value, converters, preConverter);

        ColumnValueHolder columnValueHolder = newColumnValues.get(columnName);
        columnValueHolder.setProcessed(true);
        columnValueHolder.getColumnValue().setColumnData(valueObject);

        super.enterColumn_based_update_set_clause(ctx);
    }

    @Override
    public void exitUpdate_statement(PlSqlParser.Update_statementContext ctx) {
        List<ColumnValue> actualNewValues = newColumnValues.values().stream()
                .filter(ColumnValueHolder::isProcessed).map(ColumnValueHolder::getColumnValue).collect(Collectors.toList());
        List<ColumnValue> actualOldValues = oldColumnValues.values().stream()
                .filter(ColumnValueHolder::isProcessed).map(ColumnValueHolder::getColumnValue).collect(Collectors.toList());
        RowLCR newRecord = new DefaultRowLCR(Envelope.Operation.UPDATE, actualNewValues, actualOldValues);
        parser.setRowLCR(newRecord);// todo, what is the way to emit it?
        super.exitUpdate_statement(ctx);
    }
}
