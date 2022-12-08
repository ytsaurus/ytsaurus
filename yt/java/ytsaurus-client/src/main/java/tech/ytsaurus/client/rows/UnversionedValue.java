package tech.ytsaurus.client.rows;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.yson.YsonConsumer;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeBooleanNode;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeDoubleNode;
import tech.ytsaurus.ysontree.YTreeEntityNode;
import tech.ytsaurus.ysontree.YTreeIntegerNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeStringNode;


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
            case COMPOSITE:
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
        return new IllegalArgumentException(
                "Illegal value " + value + "(" + value.getClass() + ")" + " for type " + type
        );
    }

    /**
     * Конвертирует value в правильный для type тип данных
     */
    public static Object convertValueTo(
            Object value, ColumnValueType type, SerializationResolver serializationResolver) {
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

                return serializationResolver.toTree(value).toBinary();
            default:
                throw new IllegalArgumentException("Unexpected type " + type);
        }
        throw illegalValue(type, value);
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
            case COMPOSITE:
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
        if (type != ColumnValueType.DOUBLE) {
            throw new IllegalArgumentException(type + " is not a double value");
        }
        return (Double) value;
    }

    public boolean booleanValue() {
        if (type != ColumnValueType.BOOLEAN) {
            throw new IllegalArgumentException(type + " is not a boolean value");
        }
        return (Boolean) value;
    }

    public byte[] bytesValue() {
        switch (type) {
            case STRING:
            case COMPOSITE:
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
    public void writeTo(YsonConsumer consumer) {
        switch (type) {
            case NULL:
                consumer.onEntity();
                break;
            case INT64:
                consumer.onInteger(longValue());
                break;
            case UINT64:
                consumer.onUnsignedInteger(longValue());
                break;
            case DOUBLE:
                consumer.onDouble(doubleValue());
                break;
            case BOOLEAN:
                consumer.onBoolean(booleanValue());
                break;
            case STRING: {
                byte[] bytes = bytesValue();
                consumer.onString(bytes, 0, bytes.length);
                break;
            }
            case COMPOSITE:
            case ANY: {
                YTreeBinarySerializer.deserialize(new ByteArrayInputStream(bytesValue()), consumer);
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
