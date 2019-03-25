/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.fest.assertions.Assertions.assertThat;

public class LogMinerTest {

    private static final String CATALOG_NAME = "test_catalog";
    private static final String SCHEMA_NAME = "TEST_SCHEMA";
    private static final String OTHER_SCHEMA_NAME = "OTHER_TEST_SCHEMA";
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final TableId TABLE_ID = new TableId(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME);
    private static final TableId OTHER_TABLE_ID = new TableId(CATALOG_NAME, OTHER_SCHEMA_NAME, TABLE_NAME);
    private static final JdbcConnection.ConnectionFactory CONNECTION_FACTORY = config -> null;

    @Test
    public void testBuildConditionsForQueryToContents() {
        OracleConnection jdbcConnection = new OracleConnection(Configuration.empty(), CONNECTION_FACTORY);
        LogMiner logMiner = new LogMiner(jdbcConnection);
        Set<TableId> tableIds = new HashSet<>();
        tableIds.add(TABLE_ID);
        tableIds.add(OTHER_TABLE_ID);
        String conditions = logMiner.buildConditionsForQueryToContents(tableIds);
        String expected = "scn > ? AND ("
                + "("
                + "operation_code IN (1,2,3) AND ("
                + "(seg_owner = 'TEST_SCHEMA' AND table_name IN ('TEST_TABLE'))"
                + " OR "
                + "(seg_owner = 'OTHER_TEST_SCHEMA' AND table_name IN ('TEST_TABLE'))"
                + ")"
                + ") OR operation_code IN (7,36)"
                + ")";
        assertThat(conditions).isEqualTo(expected);
    }

    @Test(expected = IllegalStateException.class)
    public void testFollowTablesWhenTableIdsAreEmpty() {
        OracleConnection jdbcConnection = new OracleConnection(Configuration.empty(), CONNECTION_FACTORY);
        LogMiner logMiner = new LogMiner(jdbcConnection);
        logMiner.followTables(Collections.emptySet());
    }
}
