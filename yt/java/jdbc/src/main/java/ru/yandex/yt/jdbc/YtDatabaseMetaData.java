package ru.yandex.yt.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.lang.StringUtils;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.request.ColumnFilter;
import ru.yandex.yt.ytclient.proxy.request.ListNode;
import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

class YtDatabaseMetaData extends AbstractWrapper implements DatabaseMetaData {

    private static final String TYPE_TABLE = "TABLE";
    private static final List<String> TABLE_COLUMNS = Arrays.asList("key", "type");

    private final TableSchema emptySchema;
    private final YtConnection connection;
    private final YtClient client;
    private final String home;
    private final boolean scanRecursive;

    YtDatabaseMetaData(YtConnection connection) {
        this.emptySchema = new TableSchema.Builder().setUniqueKeys(false).build();
        this.connection = Objects.requireNonNull(connection);

        final YtClientWrapper wrapper = this.connection.getWrapper();
        this.client = wrapper.getClient();

        final YtClientProperties properties = wrapper.getProperties();
        this.home = properties.getHome();
        this.scanRecursive = properties.isScanRecursive();
    }

    private boolean isMatchedAny(String filter) {
        return StringUtils.isEmpty(filter) || filter.equals("%");
    }

    private boolean isMatchedDefault(String filter) {
        return isMatchedAny(filter);
    }

    private ResultSet emptyResultSet() {
        return new YtResultSet(new YtStatement(this.connection), emptySchema, Collections.emptyList());
    }

    private ResultSet wrappedResultSet(List<YTreeMapNode> list) {
        if (list == null || list.isEmpty()) {
            return emptyResultSet();
        } else {
            final YTreeMapNode firstRow = list.get(0);
            final TableSchema.Builder tableBuilder = new TableSchema.Builder();
            tableBuilder.setUniqueKeys(false);
            firstRow.asMap().forEach((key, value) ->
                    tableBuilder.add(new ColumnSchema(key, YtTypes.nodeToColumnType(value))));
            return new YtResultSet(new YtStatement(this.connection), tableBuilder.build(), list);
        }
    }

    private String getName(Map<String, YTreeNode> column) {
        return column.get("name").stringValue();
    }

    private int getSqlType(Map<String, YTreeNode> column) {
        return YtTypes.ytTypeToSqlType(column.get("type").stringValue());
    }

    private boolean isPrimaryKey(Map<String, YTreeNode> column) {
        final YTreeNode required = column.get("required");
        return required != null && required.boolValue();
    }

    private CompletableFuture<List<YTreeMapNode>> primaryColumns(String table, RowFill rowFill) {
        return columns(table, this::isPrimaryKey, rowFill);
    }

    private CompletableFuture<List<YTreeMapNode>> columns(String table, Predicate<Map<String, YTreeNode>> filter,
                                                          RowFill rowFill) {
        return client.getNode(table + "/@schema").thenApply(node -> {
            final Optional<YTreeNode> uniqueKeys = node.getAttribute("unique_keys");
            if (!uniqueKeys.isPresent() || !uniqueKeys.get().boolValue()) {
                return null;
            }
            return list(result -> {
                int rownum = 0;
                for (YTreeNode columnNode : node.listNode()) {
                    final Map<String, YTreeNode> column = columnNode.asMap();
                    if (filter.test(column)) {
                        result.beginMap();
                        rowFill.fill(column, result, rownum++);
                        result.endMap();
                    }
                }
            });
        });
    }

    private CompletableFuture<Void> tables(String root, Predicate<String> tableFilter, Set<String> paths,
                                           Set<String> tables) {
        final String path = StringUtils.isEmpty(root) ? home : root;
        if (StringUtils.isEmpty(path)) {
            return null; // ---
        }
        final ListNode request = new ListNode(path).setAttributes(new ColumnFilter().setColumns(TABLE_COLUMNS));
        return client.listNode(request).thenApplyAsync(nodes -> {
            final Collection<CompletableFuture<Void>> futures = new ArrayList<>();
            for (YTreeNode item : nodes.asList()) {
                final String tableName = item.getAttributeOrThrow("key").stringValue();
                final String fullPath = path + "/" + tableName;
                final String type = item.getAttributeOrThrow("type").stringValue();
                switch (type) {
                    case "table":
                        if (tableFilter.test(tableName)) {
                            tables.add(fullPath);
                        }
                        break;
                    case "map_node":
                        if (scanRecursive && paths.add(fullPath)) {
                            futures.add(tables(fullPath, tableFilter, paths, tables));
                        }
                        break;
                    default:
                        // do nothing
                }
            }
            futures.stream().filter(Objects::nonNull).forEach(CompletableFuture::join);
            return null;
        });
    }

    private Collection<String> listTables(Predicate<String> filter) {
        final Set<String> tables = new ConcurrentSkipListSet<>();
        Optional.ofNullable(tables(null, filter, ConcurrentHashMap.newKeySet(), tables))
                .ifPresent(CompletableFuture::join);
        return tables;
    }


    @SuppressWarnings("unchecked")
    private List<YTreeMapNode> list(Consumer<YTreeBuilder> listConsumer) {
        final YTreeBuilder result = YTree.listBuilder();
        listConsumer.accept(result);
        return (List) result.buildList().asList();
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getUrl();
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.getWrapper().getProperties().getUsername();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "YT";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return "19.4";
    }

    @Override
    public String getDriverName() throws SQLException {
        return YtDriver.class.getName();
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return "1.0";
    }

    @Override
    public int getDriverMajorVersion() {
        return 1;
    }

    @Override
    public int getDriverMinorVersion() {
        return 0;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "limit";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "is_null,transform,int64,uint64,double,timestamp_floor_year,timestamp_floor_month," +
                "timestamp_floor_week,timestamp_floor_day,timestamp_floor_hour,cardinality";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "is_null,transform,is_substr,is_prefix,lower,regex_full_match,regex_partial_match," +
                "regex_replace_first,regex_replace_all,regex_extract,regex_escape,string";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "if,try_get_int64,get_int64,try_get_uint64,get_uint64,try_get_double,get_double," +
                "try_get_boolean,get_boolean,try_get_string,get_string,try_get_any,get_any,farm_hash,boolean"; //
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "is_null,transform";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "[]";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "Schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "Catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return "/";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 1 << 22;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 1 << 22;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {

        if (!isMatchedDefault(catalog) || !isMatchedDefault(schemaPattern)) {
            return emptyResultSet();
        }

        if (types != null && types.length > 0) {
            boolean accept = false;
            for (String type : types) {
                if (TYPE_TABLE.equals(type)) {
                    accept = true;
                    break;
                }
            }
            if (!accept) {
                return emptyResultSet();
            }
        }

        final Predicate<String> filter;
        if (isMatchedAny(tableNamePattern)) {
            filter = table -> true;
        } else {
            filter = tableNamePattern::equals;
        }

        final Collection<String> tables = listTables(filter);
        return wrappedResultSet(list(result -> {
            for (String table : tables) {
                result.beginMap();
                result.key("TABLE_CAT").value("");
                result.key("TABLE_SCHEM").value("");
                result.key("TABLE_NAME").value(table);
                result.key("TABLE_TYPE").value(TYPE_TABLE);
                result.key("TYPE_CAT").value((String) null);
                result.key("TYPE_SCHEM").value((String) null);
                result.key("TYPE_NAME").value((String) null);
                result.key("SELF_REFERENCING_COL_NAME").value((String) null);
                result.key("REF_GENERATION").value((String) null);
                result.endMap();
            }
        }));
    }


    @Override
    public ResultSet getSchemas() throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return wrappedResultSet(list(result -> {
            result.beginMap();
            result.key("TABLE_TYPE").value(TYPE_TABLE);
            result.endMap();
        }));
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        if (!isMatchedDefault(catalog) || !isMatchedDefault(schemaPattern)) {
            return emptyResultSet();
        }

        final Predicate<Map<String, YTreeNode>> filter;
        if (isMatchedAny(columnNamePattern)) {
            filter = column -> true;
        } else {
            filter = column -> {
                final String name = column.get("name").stringValue();
                return columnNamePattern.equals(name);
            };
        }

        final Collection<String> tables;
        if (isMatchedAny(tableNamePattern)) {
            tables = listTables(table -> true);
        } else {
            tables = Collections.singletonList(tableNamePattern);
        }

        final List<YTreeMapNode> rows = tables.stream()
                .map(table ->
                        columns(table, filter, (column, result, rownum) -> {
                            final boolean primaryKey = isPrimaryKey(column);
                            result.key("TABLE_CAT").value("");
                            result.key("TABLE_SCHEM").value("");
                            result.key("TABLE_NAME").value(table);
                            result.key("COLUMN_NAME").value(getName(column));
                            result.key("DATA_TYPE").value(getSqlType(column));
                            result.key("TYPE_NAME").value((String) null);
                            result.key("COLUMN_SIZE").value(0);
                            result.key("BUFFER_LENGTH").value(0);
                            result.key("DECIMAL_DIGITS").value(0);
                            result.key("NUM_PREC_RADIX").value(10);
                            result.key("NULLABLE").value(primaryKey ? columnNoNulls : columnNullable);
                            result.key("REMARKS").value((String) null);
                            result.key("COLUMN_DEF").value((String) null);
                            result.key("SQL_DATA_TYPE").value(0);
                            result.key("SQL_DATETIME_SUB").value(0);
                            result.key("CHAR_OCTET_LENGTH").value(0);
                            result.key("ORDINAL_POSITION").value(rownum + 1);
                            result.key("IS_NULLABLE").value(primaryKey ? "NO" : "YES");
                            result.key("SCOPE_CATALOG").value((String) null);
                            result.key("SCOPE_SCHEMA").value((String) null);
                            result.key("SCOPE_TABLE").value((String) null);
                            result.key("SOURCE_DATA_TYPE").value(0);
                            result.key("IS_AUTOINCREMENT").value("NO");
                            result.key("IS_GENERATEDCOLUMN").value("NO");
                        }))
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return wrappedResultSet(rows);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        if (!isMatchedDefault(catalog) || !isMatchedDefault(schema)) {
            return emptyResultSet();
        }

        return wrappedResultSet(primaryColumns(table, (column, result, rownum) -> {
            result.key("SCOPE").value(bestRowTemporary);
            result.key("COLUMN_NAME").value(getName(column));
            result.key("DATA_TYPE").value(getSqlType(column));
            result.key("TYPE_NAME").value((String) null);
            result.key("COLUMN_SIZE").value(0);
            result.key("BUFFER_LENGTH").value(0);
            result.key("DECIMAL_DIGITS").value(0);
            result.key("PSEUDO_COLUMN").value(bestRowNotPseudo);
        }).join());
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        if (!isMatchedDefault(catalog) || !isMatchedDefault(schema)) {
            return emptyResultSet();
        }

        return wrappedResultSet(primaryColumns(table, (column, result, rownum) -> {
            result.key("TABLE_CAT").value("");
            result.key("TABLE_SCHEM").value("");
            result.key("TABLE_NAME").value(table);
            result.key("COLUMN_NAME").value(getName(column));
            result.key("KEY_SEQ").value(0);
            result.key("PK_NAME").value((String) null);
        }).join());
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return wrappedResultSet(list(result -> {
            for (ColumnValueType type : YtTypes.columnTypes()) {
                result.beginMap();
                result.key("TYPE_NAME").value(type.name());
                result.key("DATA_TYPE").value(YtTypes.columnTypeToSqlType(type));
                result.key("PRECISION").value(0);
                result.key("LITERAL_PREFIX").value((String) null);
                result.key("LITERAL_SUFFIX").value((String) null);
                result.key("CREATE_PARAMS").value((String) null);
                result.key("NULLABLE").value(type == ColumnValueType.ANY || type == ColumnValueType.STRING ?
                        typeNullable : typeNoNulls);
                result.key("CASE_SENSITIVE").value(true);
                result.key("SEARCHABLE").value(typePredBasic);
                result.key("UNSIGNED_ATTRIBUTE").value(type == ColumnValueType.UINT64);
                result.key("FIXED_PREC_SCALE").value(false);
                result.key("AUTO_INCREMENT").value(false);
                result.key("LOCAL_TYPE_NAME").value((String) null);
                result.key("MINIMUM_SCALE").value(0);
                result.key("MAXIMUM_SCALE").value(0);
                result.key("SQL_DATA_TYPE").value(0);
                result.key("SQL_DATETIME_SUB").value(0);
                result.key("NUM_PREC_RADIX").value(10);
                result.endMap();
            }

        }));
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        return emptyResultSet();
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        final int[] types = YtTypes.sqlTypes();
        for (int sType : types) {
            if (sType == type) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return emptyResultSet();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return 19;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return DatabaseMetaData.sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        return emptyResultSet();
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    //

    interface RowFill {
        void fill(Map<String, YTreeNode> column, YTreeBuilder result, int rownum);
    }

}
