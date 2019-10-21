package ru.yandex.yt.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Objects;

import ru.yandex.yt.ytclient.tables.ColumnSchema;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class YtResultSetMetaData extends AbstractWrapper implements ResultSetMetaData {

    private final TableSchema schema;

    YtResultSetMetaData(TableSchema schema) {
        this.schema = Objects.requireNonNull(schema);
    }

    private ColumnSchema getColumn(int column) {
        return schema.getColumnSchema(column - 1);
    }

    private ColumnValueType getType(int column) {
        return getColumn(column).getType();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return schema.getColumnsCount();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        if (getColumn(column).getSortOrder() != null) {
            return ResultSetMetaData.columnNoNulls;
        } else {
            return ResultSetMetaData.columnNullable;
        }
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getType(column) != ColumnValueType.UINT64;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 80;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return ""; // Не можем определить
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return YtTypes.columnTypeToSqlType(getType(column));
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getType(column).getName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return YtTypes.columnTypeToClass(getType(column)).getName();
    }

}
