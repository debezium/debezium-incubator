/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2;

/**
 * By default, names in Db2 are upper case. Unless quoted, 
 * any name in a SQL statement will interpreted as the equivalent upper case string.
 * 
 * @author Luis Garcés-Erice
 */

public class Db2ObjectNameQuoter {

    /**
     * This function quotes a table or schema name in Db2 if the name contains 
     * at least one lower case character.
     *
     * @param name The name of the object.
     * @return The name of object between quotes if it is not all upper-case.
     */
    public static String quoteNameIfNecessary(String name) {
        String quotedNameIfNecessary = name;
        for (Character c : name.toCharArray()) {
            if (Character.isLowerCase(c)) {
                quotedNameIfNecessary = "\"" + quotedNameIfNecessary + "\"";
                break;
            }
        }
        return quotedNameIfNecessary;
    }

}
