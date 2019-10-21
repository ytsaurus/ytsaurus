package ru.yandex.yt.jdbc;

import java.sql.Types;

import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.lang.number.UnsignedLong;
import ru.yandex.yt.ytclient.tables.ColumnValueType;

class YtTypes {

    private YtTypes() {
        //
    }

    static ColumnValueType[] columnTypes() {
        return new ColumnValueType[]{ColumnValueType.INT64, ColumnValueType.UINT64,
                ColumnValueType.DOUBLE, ColumnValueType.STRING, ColumnValueType.ANY, ColumnValueType.BOOLEAN};
    }

    static int[] sqlTypes() {
        return new int[]{Types.BIGINT, Types.DOUBLE, Types.VARCHAR, Types.VARBINARY, Types.BOOLEAN};
    }

    static int columnTypeToSqlType(ColumnValueType type) {
        switch (type) {
            case INT64:
            case UINT64:
                return Types.BIGINT;
            case DOUBLE:
                return Types.DOUBLE;
            case STRING:
                return Types.VARCHAR;
            case ANY:
                return Types.VARBINARY;
            case BOOLEAN:
                return Types.BOOLEAN;
            default:
                throw new UnsupportedOperationException("YT type " + type + " is not supported");
        }
    }

    static int ytTypeToSqlType(String type) {
        switch (type) {
            case "int64":
            case "int32":
            case "int16":
            case "int8":
            case "unt64":
            case "uint32":
            case "uint16":
            case "uint8":
                return Types.BIGINT;
            case "double":
                return Types.DOUBLE;
            case "string":
            case "utf8":
                return Types.VARCHAR;
            case "any":
                return Types.VARBINARY;
            case "boolean":
                return Types.BOOLEAN;
            default:
                throw new UnsupportedOperationException("YT type " + type + " is not supported");
        }
    }

    static Class<?> columnTypeToClass(ColumnValueType type) {
        switch (type) {
            case INT64:
                return Long.class;
            case UINT64:
                return UnsignedLong.class;
            case DOUBLE:
                return Double.class;
            case STRING:
                return String.class;
            case ANY:
                return byte[].class;
            case BOOLEAN:
                return Boolean.class;
            default:
                throw new UnsupportedOperationException("YT type " + type + " is not supported");
        }
    }

    static ColumnValueType nodeToColumnType(YTreeNode value) {
        if (value.isStringNode()) {
            return ColumnValueType.STRING;
        } else if (value.isIntegerNode()) {
            return ColumnValueType.INT64;
        } else if (value.isDoubleNode()) {
            return ColumnValueType.DOUBLE;
        } else if (value.isBooleanNode()) {
            return ColumnValueType.BOOLEAN;
        } else {
            return ColumnValueType.ANY;
        }
    }

}
