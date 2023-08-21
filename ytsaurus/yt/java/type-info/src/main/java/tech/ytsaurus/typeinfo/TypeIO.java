package tech.ytsaurus.typeinfo;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.yson.YsonTextWriter;

public class TypeIO {
    private TypeIO() {
    }

    public static TiType parseYson(String input) {
        return parseYson(input.getBytes(StandardCharsets.UTF_8));
    }

    public static TiType parseYson(byte[] input) {
        YsonParser parser = new YsonParser(input);
        TypeYsonConsumer consumer = new TypeYsonConsumer();
        parser.parseNode(consumer);
        return consumer.getType();
    }

    public static TiType parseYson(Consumer<YsonConsumer> f) {
        TypeYsonConsumer ysonConsumer = new TypeYsonConsumer();
        f.accept(ysonConsumer);
        return ysonConsumer.getType();
    }

    public static void serializeToYson(TiType type, YsonConsumer consumer) {
        type.serializeTo(consumer);
    }

    public static String serializeToTextYson(TiType type) {
        StringBuilder sb = new StringBuilder();
        try (YsonTextWriter writer = new YsonTextWriter(sb)) {
            serializeToYson(type, writer);
        }
        return sb.toString();
    }
}

class KeyNames {
    static final byte[] TYPE_NAME = "type_name".getBytes(StandardCharsets.UTF_8);
    static final byte[] ITEM = "item".getBytes(StandardCharsets.UTF_8);
    static final byte[] TAG = "tag".getBytes(StandardCharsets.UTF_8);
    static final byte[] PRECISION = "precision".getBytes(StandardCharsets.UTF_8);
    static final byte[] SCALE = "scale".getBytes(StandardCharsets.UTF_8);
    static final byte[] MEMBERS = "members".getBytes(StandardCharsets.UTF_8);
    static final byte[] ELEMENTS = "elements".getBytes(StandardCharsets.UTF_8);
    static final byte[] TYPE = "type".getBytes(StandardCharsets.UTF_8);
    static final byte[] NAME = "name".getBytes(StandardCharsets.UTF_8);
    static final byte[] KEY = "key".getBytes(StandardCharsets.UTF_8);
    static final byte[] VALUE = "value".getBytes(StandardCharsets.UTF_8);

    private KeyNames() {
    }
}

class TypeYsonConsumer implements YsonConsumer {
    @Nullable
    TiType type;

    Deque<YsonConsumer> consumerStack = new ArrayDeque<>();

    TypeYsonConsumer() {
        pushConsumer(new TypeParser((type) -> this.type = type));
    }

    void pushConsumer(YsonConsumer consumer) {
        consumerStack.push(consumer);
    }

    void popConsumer() {
        consumerStack.pop();
    }

    YsonConsumer peekConsumer() {
        if (consumerStack.isEmpty()) {
            throw new IllegalStateException();
        }
        YsonConsumer top = consumerStack.peek();
        assert top != null;
        return top;
    }

    public TiType getType() {
        if (type == null) {
            throw new IllegalStateException("Internal error: type is not expected to be null");
        }
        return type;
    }

    @Override
    public void onEntity() {
        peekConsumer().onEntity();
    }

    @Override
    public void onInteger(long value) {
        peekConsumer().onInteger(value);
    }

    @Override
    public void onUnsignedInteger(long value) {
        peekConsumer().onUnsignedInteger(value);
    }

    @Override
    public void onBoolean(boolean value) {
        peekConsumer().onBoolean(value);
    }

    @Override
    public void onDouble(double value) {
        peekConsumer().onDouble(value);
    }

    @Override
    public void onString(@Nonnull byte[] value, int length, int offset) {
        peekConsumer().onString(value, length, offset);
    }

    @Override
    public void onBeginList() {
        peekConsumer().onBeginList();
    }

    @Override
    public void onListItem() {
        peekConsumer().onListItem();
    }

    @Override
    public void onEndList() {
        peekConsumer().onEndList();
    }

    @Override
    public void onBeginAttributes() {
        peekConsumer().onBeginAttributes();
    }

    @Override
    public void onEndAttributes() {
        peekConsumer().onEndAttributes();
    }

    @Override
    public void onBeginMap() {
        peekConsumer().onBeginMap();
    }

    @Override
    public void onEndMap() {
        peekConsumer().onEndMap();
    }

    @Override
    public void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
        peekConsumer().onKeyedItem(value, offset, length);
    }

    static boolean checkKey(byte[] lhs, byte[] rhs, int rhsOffset, int rhsLength) {
        return Arrays.equals(lhs, Arrays.copyOfRange(rhs, rhsOffset, rhsOffset + rhsLength));
    }

    static RuntimeException newMissingKeyError(String keyName, @Nullable TypeName typeName) {
        String message = String.format("Missing required key \"%s\"", keyName);
        if (typeName != null) {
            message += String.format(" for type \"%s\"", typeName);
        }
        return new TypeInfoException(message);
    }

    @Nullable String decodeUtf8(byte[] bytes, int offset, int length) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        CharBuffer decoded;
        try {
            decoded = decoder.decode(ByteBuffer.wrap(bytes, offset, length));
        } catch (CharacterCodingException e) {
            return null;
        }
        return decoded.toString();
    }

    class StringParser extends BaseYsonConsumer {
        final Consumer<String> onComplete;
        final String keyName;

        StringParser(String keyName, Consumer<String> onComplete) {
            super("\"" + keyName + "\" must contain a string");
            this.onComplete = onComplete;
            this.keyName = keyName;
        }

        @Override
        public void onString(byte[] value, int offset, int length) {
            String decoded = decodeUtf8(value, offset, length);
            if (decoded == null) {
                throw new TypeInfoException("\"%s\" must contain valid utf8 string");
            }
            onComplete.accept(decoded);
            popConsumer();
        }
    }

    class IntegerParser extends BaseYsonConsumer {
        final Consumer<Long> onComplete;

        IntegerParser(String keyName, Consumer<Long> onComplete) {
            super("\"" + keyName + "\" must contain a string");
            this.onComplete = onComplete;
        }

        @Override
        public void onInteger(long value) {
            onComplete.accept(value);
            popConsumer();
        }
    }

    class ListParser<T> extends BaseYsonConsumer {
        final Supplier<YsonConsumer> parserSupplier;
        final Consumer<List<T>> onComplete;
        final List<T> list = new ArrayList<>();

        ListParser(
                String keyName,
                Function<Consumer<T>, YsonConsumer> parserCreator,
                Consumer<List<T>> onComplete) {
            super("\"" + keyName + "\" must contain a list");
            this.parserSupplier = () -> parserCreator.apply(list::add);
            this.onComplete = onComplete;
        }

        @Override
        public void onBeginList() {
        }

        @Override
        public void onEndList() {
            onComplete.accept(list);
            popConsumer();
        }

        @Override
        public void onListItem() {
            pushConsumer(parserSupplier.get());
        }
    }

    class MemberParser extends BaseYsonConsumer {
        private final Consumer<StructType.Member> onComplete;

        @Nullable String name;
        @Nullable
        TiType type;

        MemberParser(Consumer<StructType.Member> onComplete) {
            super("\"members\" must contain a list of maps");
            this.onComplete = onComplete;
        }

        @Override
        public void onBeginMap() {
        }

        @Override
        public void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
            if (checkKey(KeyNames.TYPE, value, offset, length)) {
                pushConsumer(new TypeParser(v -> this.type = v));
            } else if (checkKey(KeyNames.NAME, value, offset, length)) {
                pushConsumer(new StringParser("name", v -> this.name = v));
            } else {
                pushConsumer(new SkippingParser());
            }
        }

        @Override
        public void onEndMap() {
            StructType.Member member = new StructType.Member(checkedGetName(), checkedGetType());
            onComplete.accept(member);
            popConsumer();
        }

        TiType checkedGetType() {
            if (type == null) {
                throw newMissingKeyError("type", null);
            }
            return type;
        }

        String checkedGetName() {
            if (name == null) {
                throw newMissingKeyError("name", null);
            }
            return name;
        }
    }

    class ElementParser extends BaseYsonConsumer {
        final Consumer<TiType> onComplete;

        @Nullable
        TiType type;

        ElementParser(Consumer<TiType> onComplete) {
            super("\"elements\" must contain a list of maps");
            this.onComplete = onComplete;
        }

        @Override
        public void onBeginMap() {
        }

        @Override
        public void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
            if (checkKey(KeyNames.TYPE, value, offset, length)) {
                pushConsumer(new TypeParser(v -> this.type = v));
            } else {
                pushConsumer(new SkippingParser());
            }
        }

        @Override
        public void onEndMap() {
            onComplete.accept(checkedGetType());
            popConsumer();
        }

        TiType checkedGetType() {
            if (type == null) {
                throw newMissingKeyError("type", null);
            }
            return type;
        }
    }

    class SkippingParser implements YsonConsumer {
        int nesting = 0;

        void check() {
            if (nesting == 0) {
                popConsumer();
            }
        }

        @Override
        public void onEntity() {
            check();
        }

        @Override
        public void onInteger(long value) {
            check();
        }

        @Override
        public void onUnsignedInteger(long value) {
            check();
        }

        @Override
        public void onBoolean(boolean value) {
            check();
        }

        @Override
        public void onDouble(double value) {
            check();
        }

        @Override
        public void onString(byte[] value, int offset, int length) {
            check();
        }

        @Override
        public void onBeginList() {
            nesting++;
        }

        @Override
        public void onListItem() {
        }

        @Override
        public void onEndList() {
            nesting--;
            check();
        }

        @Override
        public void onBeginAttributes() {
            nesting++;
        }

        @Override
        public void onEndAttributes() {
            nesting--;
        }

        @Override
        public void onBeginMap() {
            nesting++;
        }

        @Override
        public void onEndMap() {
            nesting--;
            check();
        }

        @Override
        public void onKeyedItem(byte[] value, int offset, int length) {
        }
    }

    class TypeParser extends BaseYsonConsumer {
        private final Consumer<TiType> onComplete;

        @Nullable private String typeName;
        @Nullable private String tag;
        @Nullable private TiType item;

        @Nullable private Long precision;
        @Nullable private Long scale;

        @Nullable private List<StructType.Member> members;
        @Nullable private List<TiType> elements;
        @Nullable private TiType key;
        @Nullable private TiType value;

        TypeParser(Consumer<TiType> onComplete) {
            super("type must be either a string or a map");
            this.onComplete = onComplete;
        }

        void complete(TiType type) {
            onComplete.accept(type);
            popConsumer();
        }

        @Override
        public void onBeginMap() {
        }

        @Override
        public void onKeyedItem(byte[] value, int offset, int length) {
            if (checkKey(KeyNames.TYPE_NAME, value, offset, length)) {
                pushConsumer(new StringParser("type_name", v -> this.typeName = v));
            } else if (checkKey(KeyNames.ITEM, value, offset, length)) {
                pushConsumer(new TypeParser(type -> this.item = type));
            } else if (checkKey(KeyNames.TAG, value, offset, length)) {
                pushConsumer(new StringParser("tag", v -> this.tag = v));
            } else if (checkKey(KeyNames.PRECISION, value, offset, length)) {
                pushConsumer(new IntegerParser("precision", v -> this.precision = v));
            } else if (checkKey(KeyNames.SCALE, value, offset, length)) {
                pushConsumer(new IntegerParser("scale", v -> this.scale = v));
            } else if (checkKey(KeyNames.MEMBERS, value, offset, length)) {
                pushConsumer(new ListParser<>(
                        "members",
                        MemberParser::new,
                        v -> this.members = v));
            } else if (checkKey(KeyNames.ELEMENTS, value, offset, length)) {
                pushConsumer(new ListParser<>(
                        "elements",
                        ElementParser::new,
                        v -> this.elements = v));
            } else if (checkKey(KeyNames.KEY, value, offset, length)) {
                pushConsumer(new TypeParser(v -> this.key = v));
            } else if (checkKey(KeyNames.VALUE, value, offset, length)) {
                pushConsumer(new TypeParser(v -> this.value = v));
            } else {
                pushConsumer(new SkippingParser());
            }
        }

        @Override
        public void onString(byte[] value, int offset, int length) {
            typeName = decodeUtf8(value, offset, length);
            complete(buildType());
        }

        @Override
        public void onEndMap() {
            complete(buildType());
        }

        TypeName checkedParseTypeName() {
            if (typeName == null) {
                throw newMissingKeyError("type_name", null);
            }
            return TypeName.fromWireName(typeName);
        }

        int checkedGetPrecision() {
            if (precision == null) {
                throw newMissingKeyError("precision", checkedParseTypeName());
            }
            return precision.intValue();
        }

        int checkedGetScale() {
            if (scale == null) {
                throw newMissingKeyError("scale", checkedParseTypeName());
            }
            return scale.intValue();
        }

        TiType checkedGetItem() {
            if (item == null) {
                throw newMissingKeyError("item", checkedParseTypeName());
            }
            return item;
        }

        String checkedGetTag() {
            if (tag == null) {
                throw newMissingKeyError("tag", checkedParseTypeName());
            }
            return tag;
        }

        List<StructType.Member> checkedGetMembers() {
            if (members == null) {
                throw newMissingKeyError("members", checkedParseTypeName());
            }
            return members;
        }

        TiType buildType() {
            TypeName checkedTypeName = checkedParseTypeName();

            switch (checkedTypeName) {
                case Bool:
                    return TiType.bool();
                case Int8:
                    return TiType.int8();
                case Int16:
                    return TiType.int16();
                case Int32:
                    return TiType.int32();
                case Int64:
                    return TiType.int64();
                case Uint8:
                    return TiType.uint8();
                case Uint16:
                    return TiType.uint16();
                case Uint32:
                    return TiType.uint32();
                case Uint64:
                    return TiType.uint64();
                case Float:
                    return TiType.floatType();
                case Double:
                    return TiType.doubleType();
                case String:
                    return TiType.string();
                case Utf8:
                    return TiType.utf8();
                case Date:
                    return TiType.date();
                case Datetime:
                    return TiType.datetime();
                case Timestamp:
                    return TiType.timestamp();
                case TzDate:
                    return TiType.tzDate();
                case TzDatetime:
                    return TiType.tzDatetime();
                case TzTimestamp:
                    return TiType.tzTimestamp();
                case Interval:
                    return TiType.interval();
                case Json:
                    return TiType.json();
                case Yson:
                    return TiType.yson();
                case Uuid:
                    return TiType.uuid();
                case Void:
                    return TiType.voidType();
                case Null:
                    return TiType.nullType();
                case Optional:
                    return TiType.optional(checkedGetItem());
                case List:
                    return TiType.list(checkedGetItem());
                case Decimal:
                    return TiType.decimal(checkedGetPrecision(), checkedGetScale());
                case Struct:
                    return new StructType(checkedGetMembers());
                case Tuple:
                    return TiType.tuple(checkedGetElements());
                case Dict:
                    return TiType.dict(checkedGetKey(), checkedGetValue());
                case Variant:
                    if (members != null && elements != null) {
                        throw new TypeInfoException(
                                String.format("Both keys \"members\" and \"elements\" are specified for type \"%s\"",
                                        checkedParseTypeName()));
                    } else if (members != null) {
                        return TiType.variantOverStruct(members);
                    } else if (elements != null) {
                        return TiType.variantOverTuple(elements);
                    } else {
                        throw new TypeInfoException(
                                String.format("Missing both keys \"members\" and \"elements\" for type \"%s\"",
                                        checkedParseTypeName()));
                    }
                case Tagged:
                    return TiType.tagged(checkedGetItem(), checkedGetTag());
                default:
                    throw new IllegalStateException();
            }
        }

        private TiType checkedGetValue() {
            if (value == null) {
                throw newMissingKeyError("value", checkedParseTypeName());
            }
            return value;
        }

        private TiType checkedGetKey() {
            if (key == null) {
                throw newMissingKeyError("key", checkedParseTypeName());
            }
            return key;
        }

        private List<TiType> checkedGetElements() {
            if (elements == null) {
                throw newMissingKeyError("elements", checkedParseTypeName());
            }
            return elements;
        }
    }
}

class BaseYsonConsumer implements YsonConsumer {
    final String errorMessage;
    BaseYsonConsumer(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public void onEntity() {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onInteger(long value) {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onUnsignedInteger(long value) {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onBoolean(boolean value) {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onDouble(double value) {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onString(byte[] value, int offset, int length) {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onBeginList() {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onListItem() {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onEndList() {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onBeginAttributes() {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onEndAttributes() {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onBeginMap() {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onEndMap() {
        throw new TypeInfoException(errorMessage);
    }

    @Override
    public void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
        throw new TypeInfoException(errorMessage);
    }
}
