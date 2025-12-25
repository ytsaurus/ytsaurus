package tech.ytsaurus.core.tables;

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
    COMPOSITE(0x12, "composite"),
    MAX(0xef, "max");

    private static final Map<String, ColumnValueType> MAP_FROM_NAME = new HashMap<>();
    // Size of COLUMN_VALUE_TYPES array.
    private static final int VALUES_SIZE = 256;
    /**
     * NOTE: An array is used here as a performance optimization for heavy read workloads.
     * Benchmarks show that array access is approximately 4x faster than HashMap lookup.
     * <p>
     * At the Wire Protocol level, the value is represented as a single unsigned byte (0x00–0xFF).
     * The actual maximum allowed value is 0xEF (see {@link tech.ytsaurus.core.tables.ColumnValueType#MAX}),
     * which makes it safe to use a fixed-size array (256 elements) instead of a HashMap.
     */
    private static final ColumnValueType[] COLUMN_VALUE_TYPES = new ColumnValueType[VALUES_SIZE];

    static {
        for (ColumnValueType type : values()) {
            MAP_FROM_NAME.put(type.getName(), type);
            COLUMN_VALUE_TYPES[type.value] = type;
        }
    }

    private final int value;
    private final String name;

    ColumnValueType(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public static ColumnValueType fromValue(int value) {
        ColumnValueType type = value < VALUES_SIZE && value >= 0 ? COLUMN_VALUE_TYPES[value] : null;
        if (type == null) {
            throw new IllegalArgumentException(
                    "Unsupported value type 0x" + Integer.toHexString(value));
        }
        return type;
    }

    public static ColumnValueType fromName(String name) {
        ColumnValueType type = MAP_FROM_NAME.get(name);
        if (type == null) {
            throw new IllegalArgumentException("Unsupported value type " + name);
        }
        return type;
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
            case COMPOSITE:
                return true;
            default:
                return false;
        }
    }
}
