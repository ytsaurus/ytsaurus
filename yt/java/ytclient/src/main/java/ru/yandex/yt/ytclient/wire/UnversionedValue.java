package ru.yandex.yt.ytclient.wire;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import com.google.protobuf.Message;

import ru.yandex.inside.yt.kosher.impl.ytree.YTreeProtoUtils;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeConsumer;
import ru.yandex.inside.yt.kosher.ytree.YTreeBooleanNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeDoubleNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeEntityNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeIntegerNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeStringNode;
import ru.yandex.misc.lang.number.UnsignedLong;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.ytree.YTreeConvertible;

/**
 * Соответствует TUnversionedValue в C++
 */
public class UnversionedValue implements YTreeConvertible {
    private final int id;
    private final ColumnValueType type;
    private final boolean aggregate;
    private final Object value;

    public UnversionedValue(int id, ColumnValueType type, boolean aggregate, Object value) {
        switch (Objects.requireNonNull(type)) {
            case INT64:
            case UINT64:
                if (!(value instanceof Long)) {
                    throw illegalValue(type, value);
                }
                break;
            case DOUBLE:
                if (!(value instanceof Double)) {
                    throw illegalValue(type, value);
                }
                break;
            case BOOLEAN:
                if (!(value instanceof Boolean)) {
                    throw illegalValue(type, value);
                }
                break;
            case STRING:
            case ANY:
                if (!(value instanceof byte[])) {
                    throw illegalValue(type, value);
                }
                break;
            default:
                if (value != null) {
                    throw illegalValue(type, value);
                }
                break;
        }
        this.id = id;
        this.type = type;
        this.aggregate = aggregate;
        this.value = value;
    }

    private static IllegalArgumentException illegalValue(ColumnValueType type, Object value) {
        return new IllegalArgumentException("Illegal value " + value + "(" + value.getClass() + ")" + " for type " + type);
    }

    /**
     * Конвертирует value в правильный для type тип данных
     */
    public static Object convertValueTo(Object value, ColumnValueType type) {
        switch (type) {
            case INT64:
                if (value == null || value instanceof Long) {
                    return value;
                } else if (value instanceof YTreeIntegerNode) {
                    return ((YTreeIntegerNode) value).longValue();
                } else if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
                    return ((Number) value).longValue();
                } else if (value instanceof YTreeEntityNode) {
                    return null;
                }
                break;
            case UINT64:
                if (value == null || value instanceof Long) {
                    return value;
                } else if (value instanceof YTreeIntegerNode) {
                    return ((YTreeIntegerNode) value).longValue();
                } else if (value instanceof Integer) {
                    return Integer.toUnsignedLong((Integer) value);
                } else if (value instanceof Short) {
                    return Short.toUnsignedLong((Short) value);
                } else if (value instanceof Byte) {
                    return Byte.toUnsignedLong((Byte) value);
                } else if (value instanceof YTreeEntityNode) {
                    return null;
                }
                break;
            case DOUBLE:
                if (value == null || value instanceof Double) {
                    return value;
                } else if (value instanceof YTreeDoubleNode) {
                    return ((YTreeDoubleNode) value).doubleValue();
                } else if (value instanceof Float) {
                    return ((Float) value).doubleValue();
                } else if (value instanceof YTreeEntityNode) {
                    return null;
                }
                break;
            case BOOLEAN:
                if (value == null || value instanceof Boolean) {
                    return value;
                } else if (value instanceof YTreeBooleanNode) {
                    return ((YTreeNode) value).boolValue();
                } else if (value instanceof YTreeEntityNode) {
                    return null;
                }
                break;
            case STRING:
                if (value == null || value instanceof byte[]) {
                    return value;
                } else if (value instanceof YTreeStringNode) {
                    return ((YTreeStringNode) value).bytesValue();
                } else if (value instanceof String) {
                    return ((String) value).getBytes(StandardCharsets.UTF_8);
                } else if (value instanceof YTreeEntityNode) {
                    return null;
                }
                break;
            case ANY:
                if (value == null || value instanceof byte[]) {
                    return value;
                }

                YTreeNode node;
                if (value instanceof Message) {
                    node = YTreeProtoUtils.marshal((Message) value);
                } else if (value instanceof YTreeNode) {
                    node = (YTreeNode) value;
                } else {
                    node = YTree.builder().value(value).build();
                }
                return node.toBinary();
            default:
                throw new IllegalArgumentException("Unexpected type " + type);
        }
        throw illegalValue(type, value);
    }

    /**
     * Конвертирует сырое 64-х битное число для сериализации в правильный для type тип данных
     */
    public static Object convertRawBitsTo(long bits, ColumnValueType type) {
        switch (type) {
            case INT64:
            case UINT64:
                return bits;
            case DOUBLE:
                return Double.longBitsToDouble(bits);
            case BOOLEAN:
                // bool is byte-sized in C++, have to be careful about random garbage
                return (bits & 0xff) != 0;
            default:
                throw new IllegalArgumentException(type + " cannot be represented as raw bits");
        }
    }

    public int getId() {
        return id;
    }

    public ColumnValueType getType() {
        return type;
    }

    public boolean isAggregate() {
        return aggregate;
    }

    public Object getValue() {
        return value;
    }

    public int getLength() {
        switch (type) {
            case STRING:
            case ANY:
                return ((byte[]) value).length;
            default:
                return 0;
        }
    }

    public long longValue() {
        switch (type) {
            case INT64:
            case UINT64:
                return (Long) value;
            default:
                throw new IllegalArgumentException(type + " is not an integer value");
        }
    }

    public double doubleValue() {
        switch (type) {
            case DOUBLE:
                return (Double) value;
            default:
                throw new IllegalArgumentException(type + " is not a double value");
        }
    }

    public boolean booleanValue() {
        switch (type) {
            case BOOLEAN:
                return (Boolean) value;
            default:
                throw new IllegalArgumentException(type + " is not a boolean value");
        }
    }

    public byte[] bytesValue() {
        switch (type) {
            case STRING:
            case ANY:
                return (byte[]) value;
            default:
                throw new IllegalArgumentException(type + " is not a string-like value");
        }
    }

    public String stringValue() {
        return new String(bytesValue(), StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UnversionedValue)) {
            return false;
        }

        UnversionedValue other = (UnversionedValue) o;

        if (id != other.id) {
            return false;
        }
        if (aggregate != other.aggregate) {
            return false;
        }
        if (type != other.type) {
            return false;
        }
        switch (type) {
            case INT64:
            case UINT64:
            case DOUBLE:
            case BOOLEAN:
                return value.equals(other.value);
            case STRING:
            case ANY:
                return Arrays.equals((byte[]) value, (byte[]) other.value);
            default:
                return true;
        }
    }

    private static int valueHashCode(Object value) {
        if (value != null) {
            if (value instanceof byte[]) {
                return Arrays.hashCode((byte[]) value);
            }
            return value.hashCode();
        }
        return 0;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + type.hashCode();
        result = 31 * result + (aggregate ? 1 : 0);
        result = 31 * result + valueHashCode(value);
        return result;
    }

    private static String valueString(Object value) {
        if (value instanceof byte[]) {
            return Arrays.toString((byte[]) value);
        }
        return String.valueOf(value);
    }

    @Override
    public String toString() {
        return "UnversionedValue{" +
                "id=" + id +
                ", type=" + type +
                ", aggregate=" + aggregate +
                ", value=" + valueString(value) +
                '}';
    }

    /**
     * Пишет значение в consumer
     */
    public void writeTo(YTreeConsumer consumer) {
        switch (type) {
            case NULL:
                consumer.onEntity();
                break;
            case INT64:
                consumer.onInteger(longValue());
                break;
            case UINT64:
                consumer.onUnsignedInteger(UnsignedLong.valueOf(longValue()));
                break;
            case DOUBLE:
                consumer.onDouble(doubleValue());
                break;
            case BOOLEAN:
                consumer.onBoolean(booleanValue());
                break;
            case STRING:
                consumer.onBytes(bytesValue());
                break;
            case ANY: {
                try {
                    YTreeBinarySerializer.deserialize(new ByteArrayInputStream(bytesValue()), consumer);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                break;
            }
            default:
                throw new IllegalArgumentException("Cannot convert " + type + " to YTree");
        }
    }

    /**
     * Конвертирует значение в соответствующий YTreeNode
     */
    @Override
    public YTreeNode toYTree() {
        YTreeBuilder builder = YTree.builder();
        writeTo(builder);
        return builder.build();
    }

    /**
     * Конвертирует значение в сырое 64-х битное число для сериализации
     */
    public long toRawBits() {
        switch (type) {
            case INT64:
            case UINT64:
                return (Long) value;
            case DOUBLE:
                return Double.doubleToRawLongBits((Double) value);
            case BOOLEAN:
                return ((Boolean) value) ? 1L : 0L;
            default:
                throw new IllegalArgumentException(type + " cannot be represented as raw bits");
        }
    }
}
