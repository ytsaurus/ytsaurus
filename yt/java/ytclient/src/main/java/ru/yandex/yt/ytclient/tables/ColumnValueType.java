package ru.yandex.yt.ytclient.tables;

import java.util.HashMap;
import java.util.Map;

public enum ColumnValueType {
    MIN(0x00, "min"),
    THE_BOTTOM(0x01, "the_bottom"),
    NULL(0x02, "null"),
    INT64(0x03, "int64"),
    UINT64(0x04, "uint64"),
    DOUBLE(0x05, "double"),
    BOOLEAN(0x06, "boolean"),
    STRING(0x10, "string"),
    ANY(0x11, "any"),
    MAX(0xef, "max");

    private final int value;
    private final String name;

    ColumnValueType(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    public boolean isValueType() {
        switch (this) {
            case INT64:
            case UINT64:
            case DOUBLE:
            case BOOLEAN:
                return true;
            default:
                return false;
        }
    }

    public boolean isStringLikeType() {
        switch (this) {
            case STRING:
            case ANY:
                return true;
            default:
                return false;
        }
    }

    public static ColumnValueType fromValue(int value) {
        ColumnValueType type = mapFromValue.get(value);
        if (type == null) {
            throw new IllegalArgumentException(
                    "Unsupported value type 0x" + Integer.toHexString(value));
        }
        return type;
    }

    public static ColumnValueType fromName(String name) {
        ColumnValueType type = mapFromName.get(name);
        if (type == null) {
            throw new IllegalArgumentException("Unsupported value type " + name);
        }
        return type;
    }

    private static final Map<Integer, ColumnValueType> mapFromValue = new HashMap<>();
    private static final Map<String, ColumnValueType> mapFromName = new HashMap<>();

    static {
        for (ColumnValueType type : values()) {
            mapFromValue.put(type.getValue(), type);
            mapFromName.put(type.getName(), type);
        }
    }
}
