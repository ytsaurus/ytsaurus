package tech.ytsaurus.client.rows;

public class WireProtocol {
    public static final int WIRE_FORMAT_VERSION = 1;
    public static final int MAX_KEY_COLUMN_COUNT = 32;
    public static final int MAX_VALUES_PER_ROW = 1024;
    public static final int MAX_STRING_VALUE_LENGTH = 16 * 1024 * 1024;
    public static final int MAX_ANY_VALUE_LENGTH = 16 * 1024 * 1024;
    public static final int MAX_COMPOSITE_VALUE_LENGTH = 16 * 1024 * 1024;
    public static final int SERIALIZATION_ALIGNMENT = 8;
    public static final byte AGGREGATE_FLAG = 1;

    private WireProtocol() {
    }

    public static int alignTail(int size) {
        return -size & (SERIALIZATION_ALIGNMENT - 1);
    }

    public static int alignUp(int size) {
        return size + alignTail(size);
    }

    public static int validateRowKeyCount(long keyCount) {
        if (keyCount < 0 || keyCount > MAX_KEY_COLUMN_COUNT) {
            throw new IllegalStateException("Unsupported number of keys " + keyCount + " in a row");
        }
        return (int) keyCount;
    }

    public static int validateRowValueCount(long valueCount) {
        if (valueCount < 0 || valueCount > MAX_VALUES_PER_ROW) {
            throw new IllegalStateException("Unsupported number of values " + valueCount + " in a row");
        }
        return (int) valueCount;
    }

    public static int validateRowCount(long rowCount) {
        if (rowCount < 0) {
            throw new IllegalStateException("Unsupported number of rows " + rowCount + " in a rowset");
        }
        return (int) rowCount;
    }

    public static short validateColumnId(int id) {
        if (id < 0 || id > 0xffff) {
            throw new IllegalStateException("Invalid column id " + id);
        }
        return (short) id;
    }
}
