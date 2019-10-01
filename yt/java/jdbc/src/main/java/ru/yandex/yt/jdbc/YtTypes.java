package ru.yandex.yt.jdbc;

import java.sql.Types;

import ru.yandex.misc.lang.number.UnsignedLong;
import ru.yandex.yt.ytclient.tables.ColumnValueType;

class YtTypes {

    private YtTypes() {
        //
    }

    static int[] sqlTypes() {
        return new int[]{Types.BIGINT, Types.DOUBLE, Types.VARCHAR, Types.VARBINARY, Types.BOOLEAN};
    }

    static int toSqlType(ColumnValueType type) {
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

    static Class<?> toClass(ColumnValueType type) {
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
}
