package tech.ytsaurus.yson;

public final class YsonTags {
    public static final byte ENTITY = '#';
    public static final byte COMMA = ',';
    public static final byte BEGIN_LIST = '[';
    public static final byte ITEM_SEPARATOR = ';';
    public static final byte END_LIST = ']';

    public static final byte BEGIN_MAP = '{';
    public static final byte KEY_VALUE_SEPARATOR = '=';
    public static final byte END_MAP = '}';

    public static final byte BEGIN_ATTRIBUTES = '<';
    public static final byte END_ATTRIBUTES = '>';

    public static final byte BINARY_END = 0;
    public static final byte BINARY_STRING = 1;
    public static final byte BINARY_INT = 2;
    public static final byte BINARY_DOUBLE = 3;
    public static final byte BINARY_FALSE = 4;
    public static final byte BINARY_TRUE = 5;
    public static final byte BINARY_UINT = 6;

    private YsonTags() {
        // static class
    }
}
