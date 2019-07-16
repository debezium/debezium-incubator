/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import oracle.jdbc.OracleTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OracleConnection extends JdbcConnection {

    private final static Logger LOGGER = LoggerFactory.getLogger(OracleConnection.class);

    /**
     * Returned by column metadata in Oracle if no scale is set;
     */
    private static final int ORACLE_UNSET_SCALE = -127;

    public OracleConnection(Configuration config, ConnectionFactory connectionFactory) {
        super(config, connectionFactory);
    }

    public void setSessionToPdb(String pdbName) {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=" + pdbName);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    public void resetSessionToCdb() {
        Statement statement = null;

        try {
            statement = connection().createStatement();
            statement.execute("alter session set container=cdb$root");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Couldn't close statement", e);
                }
            }
        }
    }

    @Override
    public Set<TableId> readTableNames(String databaseCatalog, String schemaNamePattern, String tableNamePattern,
            String[] tableTypes) throws SQLException {

        Set<TableId> tableIds = super.readTableNames(null, schemaNamePattern, tableNamePattern, tableTypes);

        return tableIds.stream()
                .map(t -> new TableId(databaseCatalog, t.schema(), t.table()))
                .collect(Collectors.toSet());
    }

    protected Set<TableId> getAllTableIds(String catalogName, String schemaNamePattern, boolean isView) throws SQLException {

        String query = "select table_name, owner from all_tables t where owner like '%" + schemaNamePattern.toUpperCase() + "%'" +
                " and exists (select 1 from ALL_SDO_INDEX_INFO i where t.table_name = i.table_name)";
        if (isView){
            query = "select view_name, owner from all_views where owner like '%" + schemaNamePattern.toUpperCase() + "%'";
        }
        Set<TableId> tableIds = new HashSet<>();

        try (PreparedStatement statement = connection().prepareStatement(query); ResultSet result = statement.executeQuery();) {
            while (result.next()) {
                String tableName = result.getString(1);
                final String schemaName = result.getString(2);
                TableId tableId = new TableId(catalogName, schemaName, tableName);
                tableIds.add(tableId);
            }
        } finally {
            LOGGER.trace("TableIds are: {}", tableIds);
        }
        return tableIds;
    }

    @Override // todo replace metadata with a query
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, ColumnNameFilter columnFilter) throws SQLException {
        return super.readTableColumn(columnMetadata, tableId, columnFilter);
    }

    // todo replace metadata with something like this
    private ResultSet getTableColumnsInfo(String schemaNamePattern, String tableName) throws SQLException{
        String columnQuery = "select column_name, DATA_TYPE, data_length, data_precision, data_scale, default_length, density, char_length from " +
                "all_tab_columns where owner like '" + schemaNamePattern + "' AND table_name='"+tableName+"'";

        PreparedStatement statement = connection().prepareStatement(columnQuery);
        return statement.executeQuery();
    }

    @Override
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern, TableFilter tableFilter,
            ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc) throws SQLException {

        super.readSchema(tables, null, schemaNamePattern, null, columnFilter, removeTablesNotFoundInJdbc);

        Set<TableId> tableIds = tables.tableIds().stream().filter(x -> schemaNamePattern.equals(x.schema())).collect(Collectors.toSet());

        for (TableId tableId : tableIds) {
            // super.readSchema() populates ids without the catalog; hence we apply the filtering only
            // here and if a table is included, overwrite it with a new id including the catalog
            TableId tableIdWithCatalog = new TableId(databaseCatalog, tableId.schema(), tableId.table());

            if (tableFilter.isIncluded(tableIdWithCatalog)) {
                TableEditor editor = tables.editTable(tableId);
                editor.tableId(tableIdWithCatalog);

                List<String> columnNames = new ArrayList<>(editor.columnNames());
                for (String columnName : columnNames) {
                    Column column = editor.columnWithName(columnName);
                    if (column.jdbcType() == Types.TIMESTAMP) {
                        editor.addColumn(
                                column.edit()
                                    .length(column.scale().orElse(Column.UNSET_INT_VALUE))
                                    .scale(null)
                                    .create()
                                );
                    }
                    // NUMBER columns without scale value have it set to -127 instead of null;
                    // let's rectify that
                    else if (column.jdbcType() == OracleTypes.NUMBER) {
                        column.scale()
                            .filter(s -> s == ORACLE_UNSET_SCALE)
                            .ifPresent(s -> {
                                editor.addColumn(
                                        column.edit()
                                            .scale(null)
                                            .create()
                                        );
                            });
                    }
                }
                tables.overwriteTable(editor.create());
            }

            tables.removeTable(tableId);
        }
    }
}
