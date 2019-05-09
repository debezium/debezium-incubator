/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import io.debezium.connector.oracle.logminer.valueholder.ColumnValueHolder;
import io.debezium.connector.oracle.antlr.OracleDmlParser;
import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;

/**
 * This class parses recursively logical expression tree for DELETE and UPDATE statements
 */
abstract class BaseDmlStringParserListener extends BaseDmlParserListener<String> {

    BaseDmlStringParserListener(String catalogName, String schemaName, OracleDmlParser parser) {
        super(catalogName, schemaName, parser);
    }

    /**
     * Logical expressions are trees and (column name, value) pairs are nested in this tree.
     * This methods extracts those pairs and store them in List<columnValue> oldValues
     * This method is used by VALUES parsers of update and delete statements.
     *
     * @param logicalExpression expression tree
     */
    void parseRecursively(PlSqlParser.Logical_expressionContext logicalExpression)  {

        int count = logicalExpression.logical_expression().size();
        if (count == 0){

            String name = logicalExpression.getStart().getText().toUpperCase();

            Column column = table.columnWithName(name);
            String value = logicalExpression.getStop().getText();
            value = removeApostrophes(value);
            Object valueObject = convertValueToSchemaType(column, value, converters, preConverter);

            ColumnValueHolder columnValueHolder = oldColumnValues.get(name);
            columnValueHolder.setProcessed(true);
            columnValueHolder.getColumnValue().setColumnData(valueObject);

        }
        for (int i = 0; i<count; i++) {
            parseRecursively(logicalExpression.logical_expression(i));
        }
    }

}
