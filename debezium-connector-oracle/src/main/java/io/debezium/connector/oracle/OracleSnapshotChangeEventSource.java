/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.connector.oracle.logminer.LogMinerHelper;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.HistorizedRelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link StreamingChangeEventSource} for Oracle.
 *
 * @author Gunnar Morling
 */
public class OracleSnapshotChangeEventSource extends HistorizedRelationalSnapshotChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSnapshotChangeEventSource.class);

    private final OracleConnectorConfig connectorConfig;
    private final OracleConnection jdbcConnection;
    private final Clock clock;

    public OracleSnapshotChangeEventSource(OracleConnectorConfig connectorConfig, OracleOffsetContext previousOffset, OracleConnection jdbcConnection, OracleDatabaseSchema schema, EventDispatcher<TableId> dispatcher, Clock clock, SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, previousOffset, jdbcConnection, schema, dispatcher, clock, snapshotProgressListener);

        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnection;
        this.clock = clock;
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;
        boolean skipSnapsotLock = false;

        // found a previous offset and the earlier snapshot has completed
        if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            snapshotSchema = false;
            snapshotData = false;
        }
        else {
            snapshotData = connectorConfig.getSnapshotMode().includeData();
            skipSnapsotLock = connectorConfig.skipSnapshotLock();
        }

        return new SnapshottingTask(snapshotSchema, snapshotData, skipSnapsotLock);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext context) throws Exception {
        if (connectorConfig.getPdbName() != null) {
            jdbcConnection.setSessionToPdb(connectorConfig.getPdbName());
        }

        return new OracleSnapshotContext(
                connectorConfig.getPdbName() != null ? connectorConfig.getPdbName() : connectorConfig.getDatabaseName()
        );
    }

    @Override
    protected Set<TableId> getAllTableIds(SnapshotContext ctx) throws Exception {
        return jdbcConnection.getAllTableIds(ctx.catalogName, connectorConfig.getSchemaName(), false);
        // this very slow approach(commented out), it took 30 minutes on an instance with 600 tables
        //return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"} );
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws SQLException, InterruptedException {
        ((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("dbz_schema_snapshot");

        try (Statement statement = jdbcConnection.connection().createStatement()) {
            for (TableId tableId : snapshotContext.capturedTables) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while locking table " + tableId);
                }

                LOGGER.debug("Locking table {}", tableId);

                statement.execute("LOCK TABLE " + tableId.schema() + "." + tableId.table() + " IN EXCLUSIVE MODE");
            }
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(SnapshotContext snapshotContext) throws SQLException {
        jdbcConnection.connection().rollback(((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
    }

    @Override
    protected void determineSnapshotOffset(SnapshotContext ctx) throws Exception {
        Optional<Long> latestTableDdlScn = Optional.empty();
        try {
            latestTableDdlScn = getLatestTableDdlScn(ctx);
        }
        catch (SQLException e) {
            // this may happen if the DDL is older than the retention policy and no corresponding SCN could be found
            LOGGER.warn("No snapshot found based on specified time");
        }
        long currentScn;

        // we must use an SCN for taking the snapshot that represents a later timestamp than the latest DDL change than
        // any of the captured tables; this will not be a problem in practice, but during testing it may happen that the
        // SCN of "now" represents the same timestamp as a newly created table that should be captured; in that case
        // we'd get a ORA-01466 when running the flashback query for doing the snapshot
        do {
            currentScn = LogMinerHelper.getCurrentScn(jdbcConnection.connection());
        }
        while(areSameTimestamp(latestTableDdlScn.orElse(null), currentScn));

        ctx.offset = OracleOffsetContext.create()
                .logicalName(connectorConfig)
                .scn(currentScn)
                .build();
    }

    /**
     * Whether the two SCNs represent the same timestamp or not (resolution is only 3 seconds).
     */
    private boolean areSameTimestamp(Long scn1, long scn2) throws SQLException {
        if (scn1 == null) {
            return false;
        }

        try(Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT 1 FROM DUAL WHERE SCN_TO_TIMESTAMP(" + scn1 + ") = SCN_TO_TIMESTAMP(" + scn2 + ")" )) {

            return rs.next();
        }
    }

    /**
     * Returns the SCN of the latest DDL change to the captured tables. The result will be empty if there's no table to
     * capture as per the configuration.
     */
    private Optional<Long> getLatestTableDdlScn(SnapshotContext ctx) throws SQLException {
        if (ctx.capturedTables.isEmpty()) {
            return Optional.empty();
        }

        StringBuilder lastDdlScnQuery = new StringBuilder("SELECT MAX(TIMESTAMP_TO_SCN(last_ddl_time))")
                .append(" FROM all_objects")
                .append(" WHERE");

        for(TableId table : ctx.capturedTables) {
            lastDdlScnQuery.append(" (owner = '" + table.schema() + "' AND object_name = '" + table.table() + "') OR");
        }

        String query = lastDdlScnQuery.substring(0, lastDdlScnQuery.length() - 3);
        try(Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery(query)) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get latest table DDL SCN");
            }

            return Optional.of(rs.getLong(1));
        }
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, SnapshotContext snapshotContext) throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
            .map(TableId::schema)
            .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            // todo: slow, don't use metadata for Oracle
//            jdbcConnection.readSchema(
//                    snapshotContext.tables,
//                    snapshotContext.catalogName,
//                    schema,
//                    connectorConfig.getTableFilters().dataCollectionFilter(),
//                    null,
//                    false
//            );
            // todo: faster, but still slow
            jdbcConnection.readSchemaForCapturedTables(
                    snapshotContext.tables,
                    snapshotContext.catalogName,
                    schema,
                    connectorConfig.getColumnFilter(),
                    false,
                    snapshotContext.capturedTables
            );
        }
    }

    @Override
    protected String enhanceOverriddenSelect(SnapshotContext snapshotContext, String overriddenSelect, TableId tableId){
        String columnString = buildSelectColumns(connectorConfig.getConfig().getString(connectorConfig.COLUMN_BLACKLIST), snapshotContext.tables.forTable(tableId));
        overriddenSelect = overriddenSelect.replaceFirst("\\*", columnString);
        long snapshotOffset = (Long) snapshotContext.offset.getOffset().get("scn");
        String token = connectorConfig.getTokenToReplaceInSnapshotPredicate();
        if (token != null){
            return overriddenSelect.replaceAll(token, " AS OF SCN " + snapshotOffset);
        }
        return overriddenSelect;
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(SnapshotContext snapshotContext, Table table) throws SQLException {
        try (Statement statement = jdbcConnection.connection().createStatement();
                ResultSet rs = statement.executeQuery("select dbms_metadata.get_ddl( 'TABLE', '" + table.id().table() + "', '" +  table.id().schema() + "' ) from dual")) {

            if (!rs.next()) {
                throw new IllegalStateException("Couldn't get metadata");
            }

            Object res = rs.getObject(1);
            String ddl = ((Clob) res).getSubString(1, (int) ((Clob) res).length());

            return new SchemaChangeEvent(snapshotContext.offset.getPartition(), snapshotContext.offset.getOffset(), snapshotContext.catalogName,
                    table.id().schema(), ddl, table, SchemaChangeEventType.CREATE, true);
        }
    }

    @Override
    protected String getSnapshotSelect(SnapshotContext snapshotContext, TableId tableId) {
        String columnString = buildSelectColumns(connectorConfig.getConfig().getString(connectorConfig.COLUMN_BLACKLIST), snapshotContext.tables.forTable(tableId));

        long snapshotOffset = (Long) snapshotContext.offset.getOffset().get("scn");
        return "SELECT " + columnString + " FROM " + tableId.schema() + "." + tableId.table() + " AS OF SCN " + snapshotOffset;
    }

    /**
     * This is to build "whitelisted" column list
     * @param blackListColumnStr comma separated columns blacklist
     * @param table the table
     * @return column list for select
     */
    public static String buildSelectColumns(String blackListColumnStr, Table table) {
        String columnsToSelect = "*";
        if (blackListColumnStr != null && blackListColumnStr.trim().length() > 0
                && blackListColumnStr.toUpperCase().contains(table.id().table())) {
            String allTableColumns = table.retrieveColumnNames().stream()
                    .map(columnName -> {
                        StringBuilder sb = new StringBuilder();
                        if (!columnName.contains(table.id().table())){
                            sb.append(table.id().table()).append(".").append(columnName);
                        } else {
                            sb.append(columnName);
                        }
                        return sb.toString();
                    }).collect(Collectors.joining(","));
            // todo this is an unnecessary code, fix unit test, then remove it
            String catalog = table.id().catalog();
            List<String> blackList = new ArrayList<>(Arrays.asList(blackListColumnStr.trim().toUpperCase().replaceAll(catalog + ".", "").split(",")));
            List<String> allColumns = new ArrayList<>(Arrays.asList(allTableColumns.toUpperCase().split(",")));
            allColumns.removeAll(blackList);
            columnsToSelect = String.join(",", allColumns);
        }
        return columnsToSelect;
    }

    @Override
    protected ChangeRecordEmitter getChangeRecordEmitter(SnapshotContext snapshotContext, TableId tableId, Object[] row) {
        // TODO can this be done in a better way than doing it as a side-effect here?
        ((OracleOffsetContext) snapshotContext.offset).setSourceTime(Instant.ofEpochMilli(clock.currentTimeInMillis()));
        ((OracleOffsetContext) snapshotContext.offset).setTableId(tableId);
        return new SnapshotChangeRecordEmitter(snapshotContext.offset, row, clock);
    }

    @Override
    protected void complete(SnapshotContext snapshotContext) {
        if (connectorConfig.getPdbName() != null) {
            jdbcConnection.resetSessionToCdb();
        }
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class OracleSnapshotContext extends SnapshotContext {

        private Savepoint preSchemaSnapshotSavepoint;

        public OracleSnapshotContext(String catalogName) throws SQLException {
            super(catalogName);
        }
    }
}
