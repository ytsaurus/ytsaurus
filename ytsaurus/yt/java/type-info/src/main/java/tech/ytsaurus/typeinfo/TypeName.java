package tech.ytsaurus.typeinfo;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Enumeration with all type names that are included in the Common Type System.
 * <p>
 * Each type (except Extension) has wire name that is used in yson/json representation of a type.
 * <p>
 * {@link #Extension} is special type name that doesn't have wire name
 * and reserved for extension types not included into Common Type System.
 */
public enum TypeName {
    Bool("bool"),

    Int8("int8"),
    Int16("int16"),
    Int32("int32"),
    Int64("int64"),
    Uint8("uint8"),
    Uint16("uint16"),
    Uint32("uint32"),
    Uint64("uint64"),

    Float("float"),
    Double("double"),

    String("string"),
    Utf8("utf8"),

    Date("date"),
    Datetime("datetime"),
    Timestamp("timestamp"),
    TzDate("tz_date"),
    TzDatetime("tz_datetime"),
    TzTimestamp("tz_timestamp"),
    Interval("interval"),

    Decimal("decimal"),
    Json("json"),
    Yson("yson"),
    Uuid("uuid"),

    Void("void"),
    Null("null"),

    Optional("optional"),
    List("list"),
    Dict("dict"),
    Struct("struct"),
    Tuple("tuple"),
    Variant("variant"),
    Tagged("tagged"),

    Extension(null);

    static final Map<ByteBuffer, TypeName> BYTE_BUFFER_MAP = new HashMap<>();
    static final Map<String, TypeName> STRING_MAP = new HashMap<>();

    final @Nullable String wireName;
    final @Nullable byte[] wireNameBytes;

    TypeName(@Nullable String wireName) {
        this.wireName = wireName;
        this.wireNameBytes = wireName == null ?
                null :
                wireName.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Get wire name.
     */
    public String getWireName() {
        if (wireName == null) {
            throw new IllegalArgumentException(java.lang.String.format("%s doesn't have wire name", this));
        }
        return wireName;
    }

    /**
     * Decode type name from wire name.
     */
    public static TypeName fromWireName(String value) {
        TypeName result = STRING_MAP.get(value);
        if (result == null) {
            throw new IllegalArgumentException("Unknown type \"" + value + "\"");
        }
        return result;
    }

    /**
     * Decode type name from wire name.
     */
    public static TypeName fromWireName(byte[] value) {
        return fromWireName(value, 0, value.length);
    }

    /**
     * Decode type name from wire name.
     */
    public static TypeName fromWireName(byte[] value, int offset, int size) {
        ByteBuffer wireName = ByteBuffer.wrap(value, offset, size);
        TypeName result = BYTE_BUFFER_MAP.get(wireName);
        if (result == null) {
            throw new IllegalArgumentException("Unknown TypeName" + wireName);
        }
        return result;
    }

    static {
        for (TypeName value : TypeName.values()) {
            if (value.wireName == null) {
                continue;
            }
            STRING_MAP.put(value.wireName, value);
            ByteBuffer byteBuffer = ByteBuffer.wrap(value.wireName.getBytes(StandardCharsets.UTF_8));
            BYTE_BUFFER_MAP.put(byteBuffer, value);
        }
    }
}
