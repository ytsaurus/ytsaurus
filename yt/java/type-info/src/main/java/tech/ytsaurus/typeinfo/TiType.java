package tech.ytsaurus.typeinfo;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.yson.YsonConsumer;

/**
 * Description of the type.
 * <p>
 * This is a base class for all other type describing classes
 * and it provides static factory methods for creating all kinds of types.
 */
public abstract class TiType {
    protected final TypeName typeName;

    TiType(TypeName typeName) {
        this.typeName = typeName;
    }

    //
    // PRIMITIVE TYPE CONSTRUCTORS
    //

    public static TiType bool() {
        return NonParametrizedType.BOOL_INSTANCE;
    }

    public static TiType int8() {
        return NonParametrizedType.INT8_INSTANCE;
    }

    public static TiType int16() {
        return NonParametrizedType.INT16_INSTANCE;
    }

    public static TiType int32() {
        return NonParametrizedType.INT32_INSTANCE;
    }

    public static TiType int64() {
        return NonParametrizedType.INT64_INSTANCE;
    }

    public static TiType uint8() {
        return NonParametrizedType.UINT8_INSTANCE;
    }

    public static TiType uint16() {
        return NonParametrizedType.UINT16_INSTANCE;
    }

    public static TiType uint32() {
        return NonParametrizedType.UINT32_INSTANCE;
    }

    public static TiType uint64() {
        return NonParametrizedType.UINT64_INSTANCE;
    }

    public static TiType floatType() {
        return NonParametrizedType.FLOAT_INSTANCE;
    }

    public static TiType doubleType() {
        return NonParametrizedType.DOUBLE_INSTANCE;
    }

    public static TiType string() {
        return NonParametrizedType.STRING_INSTANCE;
    }

    public static TiType utf8() {
        return NonParametrizedType.UTF8_INSTANCE;
    }

    public static TiType date() {
        return NonParametrizedType.DATE_INSTANCE;
    }

    public static TiType datetime() {
        return  NonParametrizedType.DATETIME_INSTANCE;
    }

    public static TiType timestamp() {
        return NonParametrizedType.TIMESTAMP_INSTANCE;
    }

    public static TiType tzDate() {
        return NonParametrizedType.TZ_DATE_INSTANCE;
    }

    public static TiType tzDatetime() {
        return NonParametrizedType.TZ_DATETIME_INSTANCE;
    }

    public static TiType tzTimestamp() {
        return NonParametrizedType.TZ_TIMESTAMP_INSTANCE;
    }

    public static TiType interval() {
        return NonParametrizedType.INTERVAL_INSTANCE;
    }

    public static TiType json() {
        return NonParametrizedType.JSON_INSTANCE;
    }

    public static TiType yson() {
        return NonParametrizedType.YSON_INSTANCE;
    }

    public static TiType uuid() {
        return NonParametrizedType.UUID_INSTANCE;
    }

    public static TiType voidType() {
        return NonParametrizedType.VOID_INSTANCE;
    }

    public static TiType nullType() {
        return NonParametrizedType.NULL_INSTANCE;
    }

    public static TiType decimal(int precision, int scale) {
        return new DecimalType(precision, scale);
    }

    //
    // CONTAINER TYPE CONSTRUCTORS
    //

    public static OptionalType optional(TiType item) {
        return new OptionalType(item);
    }

    public static ListType list(TiType item) {
        return new ListType(item);
    }

    public static StructType.Builder structBuilder() {
        return StructType.builder();
    }

    public static StructType struct(StructType.Member... members) {
        return new StructType(members);
    }

    public static StructType struct(List<StructType.Member> members) {
        return new StructType(members);
    }

    public static StructType.Member member(String name, TiType type) {
        return new StructType.Member(name, type);
    }

    public static TupleType tuple(TiType... elements) {
        return new TupleType(elements);
    }

    public static TupleType tuple(List<TiType> elements) {
        return new TupleType(elements);
    }

    public static VariantType.Builder variantOverStructBuilder() {
        return VariantType.overStructBuilder();
    }

    public static VariantType variantOverStruct(List<StructType.Member> members) {
        return new VariantType(new StructType(members));
    }

    public static VariantType variantOverTuple(TiType... elements) {
        return new VariantType(new TupleType(elements));
    }

    public static VariantType variantOverTuple(List<TiType> elements) {
        return new VariantType(new TupleType(elements));
    }

    public static DictType dict(TiType key, TiType value) {
        return new DictType(key, value);
    }

    public static TaggedType tagged(TiType item, String tag) {
        return new TaggedType(item, tag);
    }

    //
    // CHECK IF TYPE IS PRIMITIVE
    //

    public boolean isBool() {
        return typeName == TypeName.Bool;
    }

    public boolean isInt8() {
        return typeName == TypeName.Int8;
    }

    public boolean isInt16() {
        return typeName == TypeName.Int16;
    }

    public boolean isInt32() {
        return typeName == TypeName.Int32;
    }

    public boolean isInt64() {
        return typeName == TypeName.Int64;
    }

    public boolean isUint8() {
        return typeName == TypeName.Uint8;
    }

    public boolean isUint16() {
        return typeName == TypeName.Uint16;
    }

    public boolean isUint32() {
        return typeName == TypeName.Uint32;
    }

    public boolean isUint64() {
        return typeName == TypeName.Uint64;
    }

    public boolean isFloat() {
        return typeName == TypeName.Float;
    }

    public boolean isDouble() {
        return typeName == TypeName.Double;
    }

    public boolean isString() {
        return typeName == TypeName.String;
    }
    public boolean isUtf8() {
        return typeName == TypeName.Utf8;
    }

    public boolean isDate() {
        return typeName == TypeName.Date;
    }

    public boolean isDatetime() {
        return typeName == TypeName.Datetime;
    }

    public boolean isTimestamp() {
        return typeName == TypeName.Timestamp;
    }

    public boolean isTzDate() {
        return typeName == TypeName.TzDate;
    }

    public boolean isTzDatetime() {
        return typeName == TypeName.TzDatetime;
    }

    public boolean isTzTimestamp() {
        return typeName == TypeName.TzTimestamp;
    }

    public boolean isInterval() {
        return typeName == TypeName.Interval;
    }

    public boolean isDecimal() {
        return typeName == TypeName.Decimal;
    }

    public boolean isJson() {
        return typeName == TypeName.Json;
    }

    public boolean isYson() {
        return typeName == TypeName.Yson;
    }

    public boolean isUuid() {
        return typeName == TypeName.Uuid;
    }

    public boolean isVoid() {
        return typeName == TypeName.Void;
    }
    public boolean isNull() {
        return typeName == TypeName.Null;
    }

    //
    // CHECK IF TYPE IS CONTAINER TYPE
    //

    public boolean isOptional() {
        return typeName == TypeName.Optional;
    }

    public boolean isList() {
        return typeName == TypeName.List;
    }

    public boolean isDict() {
        return typeName == TypeName.Dict;
    }

    public boolean isStruct() {
        return typeName == TypeName.Struct;
    }

    public boolean isTuple() {
        return typeName == TypeName.Tuple;
    }

    public boolean isVariant() {
        return typeName == TypeName.Variant;
    }

    public boolean isTagged() {
        return typeName == TypeName.Tagged;
    }

    //
    // CAST
    //

    public DecimalType asDecimal() {
        return (DecimalType) this;
    }

    public OptionalType asOptional() {
        return (OptionalType) this;
    }

    public ListType asList() {
        return (ListType) this;
    }

    public DictType asDict() {
        return (DictType) this;
    }

    public StructType asStruct() {
        return (StructType) this;
    }

    public TupleType asTuple() {
        return (TupleType) this;
    }

    public VariantType asVariant() {
        return (VariantType) this;
    }

    public TaggedType asTaggedType() {
        return (TaggedType) this;
    }

    //
    // OTHER METHODS
    //

    public TypeName getTypeName() {
        return typeName;
    }

    public boolean isNullable() {
        return isNull() || isVoid() || isOptional();
    }

    public abstract String toString();
    public abstract boolean equals(@Nullable Object other);
    public abstract int hashCode();

    public abstract void serializeTo(YsonConsumer ysonConsumer);
}

class NonParametrizedType extends TiType {
    static final NonParametrizedType BOOL_INSTANCE = new NonParametrizedType(TypeName.Bool);

    static final NonParametrizedType INT8_INSTANCE = new NonParametrizedType(TypeName.Int8);
    static final NonParametrizedType INT16_INSTANCE = new NonParametrizedType(TypeName.Int16);
    static final NonParametrizedType INT32_INSTANCE = new NonParametrizedType(TypeName.Int32);
    static final NonParametrizedType INT64_INSTANCE = new NonParametrizedType(TypeName.Int64);

    static final NonParametrizedType UINT8_INSTANCE = new NonParametrizedType(TypeName.Uint8);
    static final NonParametrizedType UINT16_INSTANCE = new NonParametrizedType(TypeName.Uint16);
    static final NonParametrizedType UINT32_INSTANCE = new NonParametrizedType(TypeName.Uint32);
    static final NonParametrizedType UINT64_INSTANCE = new NonParametrizedType(TypeName.Uint64);

    static final NonParametrizedType FLOAT_INSTANCE = new NonParametrizedType(TypeName.Float);
    static final NonParametrizedType DOUBLE_INSTANCE = new NonParametrizedType(TypeName.Double);

    static final NonParametrizedType STRING_INSTANCE = new NonParametrizedType(TypeName.String);
    static final NonParametrizedType UTF8_INSTANCE = new NonParametrizedType(TypeName.Utf8);

    static final NonParametrizedType DATE_INSTANCE = new NonParametrizedType(TypeName.Date);
    static final NonParametrizedType DATETIME_INSTANCE = new NonParametrizedType(TypeName.Datetime);
    static final NonParametrizedType TIMESTAMP_INSTANCE = new NonParametrizedType(TypeName.Timestamp);
    static final NonParametrizedType INTERVAL_INSTANCE = new NonParametrizedType(TypeName.Interval);

    static final NonParametrizedType TZ_DATE_INSTANCE = new NonParametrizedType(TypeName.TzDate);
    static final NonParametrizedType TZ_DATETIME_INSTANCE = new NonParametrizedType(TypeName.TzDatetime);
    static final NonParametrizedType TZ_TIMESTAMP_INSTANCE = new NonParametrizedType(TypeName.TzTimestamp);

    static final NonParametrizedType UUID_INSTANCE = new NonParametrizedType(TypeName.Uuid);
    static final NonParametrizedType JSON_INSTANCE = new NonParametrizedType(TypeName.Json);
    static final NonParametrizedType YSON_INSTANCE = new NonParametrizedType(TypeName.Yson);
    static final NonParametrizedType NULL_INSTANCE = new NonParametrizedType(TypeName.Null);
    static final NonParametrizedType VOID_INSTANCE = new NonParametrizedType(TypeName.Void);

    NonParametrizedType(TypeName typeName) {
        super(typeName);
    }

    @Override
    public String toString() {
        return getTypeName().toString();
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NonParametrizedType that = (NonParametrizedType) o;
        return getTypeName() == that.getTypeName();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(typeName);
    }

    @Override
    public void serializeTo(YsonConsumer ysonConsumer) {
        assert typeName.wireNameBytes != null;
        ysonConsumer.onString(typeName.wireNameBytes, 0, typeName.wireNameBytes.length);
    }
}
