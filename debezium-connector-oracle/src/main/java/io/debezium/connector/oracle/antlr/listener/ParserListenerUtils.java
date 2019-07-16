/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.antlr.listener;

import io.debezium.ddl.parser.oracle.generated.PlSqlParser;

/**
 * This class contains a few methods for parser listeners
 */
class ParserListenerUtils {

    private ParserListenerUtils() {}

    /**
     * Obtains the table name
     * @param tableview_name table view context
     * @return table name
     */
    static String getTableName(final PlSqlParser.Tableview_nameContext tableview_name) {
        if (tableview_name.id_expression() != null) {
            return stripeQuotes(tableview_name.id_expression().getText());
        } else {
            return stripeQuotes(tableview_name.identifier().id_expression().getText());
        }
    }

    public static String getColumnName(final PlSqlParser.Column_nameContext ctx) {
        return stripeQuotes(ctx.identifier().id_expression().getText());
    }

    static String stripeQuotes(String text){
        if (text != null && text.indexOf("\"") == 0 && text.lastIndexOf("\"") == text.length()-1){
            return text.substring(1, text.length() -1);
        }
        return text;
    }

    // todo make it better
    static String stripeAlias(String text, String alias){
        int index = text.indexOf(alias + ".");
        if (text != null && index >= 0){
            return text.substring(alias.length() + 1);
        }
        return text;
    }
}
