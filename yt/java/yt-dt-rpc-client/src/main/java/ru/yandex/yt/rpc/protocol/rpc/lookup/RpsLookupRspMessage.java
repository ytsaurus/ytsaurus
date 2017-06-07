package ru.yandex.yt.rpc.protocol.rpc.lookup;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Logger;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.schema.TableSchema;

/**
 * @author valri
 */
abstract class RpsLookupRspMessage {
    TableSchema schema;
    static final int DEFAULT_VALUE_SIZE = 8;
    private static final int STRING_FOLD = 8;

    protected Object parseKey(ByteBuffer buffer, ValueType type, int length) {
        switch (type) {
            case ANY:
            case STRING: {
                byte[] stringBytes = new byte[length];
                buffer.get(stringBytes);
                if (length % STRING_FOLD != 0) {
                    int parity = STRING_FOLD - (length % STRING_FOLD);
                    buffer.position(buffer.position() + parity);
                }
                if (type == ValueType.ANY) {
                    return stringBytes;
                } else {
                    return new String(stringBytes);
                }
            }

            case INT_64:
            case UINT_64: {
                return buffer.getLong();
            }

            case BOOLEAN: {
                Long value = buffer.getLong();
                return value == 1L;
            }

            case DOUBLE: {
                return (double) buffer.getLong();
            }

            case NULL:
                return null;
            case MIN:
            case THE_BOTTOM:
            case MAX:
            default: {
                String message = String.format("Failed to determine rowValue type for object. Type `%s`", type);
                getLogger().error(message);
                throw new RuntimeException(message);
            }
        }
    }

    Pair<String, Object> parseValue(ByteBuffer buffer) {
        if (!enoughCapacity(buffer, DEFAULT_VALUE_SIZE)) {
            throw new RuntimeException("Not enough bytes to parse value. Stop reading.");
        }
        final short id = buffer.getShort();
        final short typeId = (short)(buffer.get() & 0xff);
        final ValueType type = ValueType.fromType(typeId);
        final boolean aggreate = (short) (buffer.get() & 0xff) != 0; // not used
        final int length = buffer.getInt();
        if (type == null) {
            throw new RuntimeException(String.format("Unknown typeId %d", (int)typeId));
        }
        if (!schema.idToName.containsKey(id)) {
            parseKey(buffer, type, length);
            getLogger().warn("Failed to get type {} from response. Column {} skipped", type, id);
            return null;
        }
        return new ImmutablePair<>(schema.idToName.get(id), parseKey(buffer, type, length));
    }

    boolean enoughCapacity(ByteBuffer buffer, int size) {
        return buffer.remaining() >= size;
    }

    protected abstract Logger getLogger();
}
