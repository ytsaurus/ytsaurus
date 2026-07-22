package tech.ytsaurus.flow.row.codec;

import com.google.protobuf.MessageLite;
import tech.ytsaurus.flow.utils.ProtoUtils;

/**
 * {@link ByteArrayCodec} that (de)serializes Protocol Buffer messages as raw byte blobs via
 * {@link MessageLite#toByteArray()} and {@link ProtoUtils#parseBytes(byte[], Class)}.
 *
 * @param <T> protobuf message type
 */
public final class ProtobufByteArrayCodec<T extends MessageLite> implements ByteArrayCodec<T> {

    private final Class<T> messageClass;

    /**
     * Creates a codec for the given protobuf message class.
     *
     * @param messageClass runtime class of the protobuf message type {@code T}
     */
    public ProtobufByteArrayCodec(Class<T> messageClass) {
        this.messageClass = messageClass;
    }

    /**
     * Parses a protobuf message from its serialized byte representation.
     *
     * @param bytes the serialized protobuf bytes
     * @return the parsed message
     */
    @Override
    public T decode(byte[] bytes) {
        return ProtoUtils.parseBytes(bytes, messageClass);
    }

    /**
     * Serializes a protobuf message into its byte representation.
     *
     * @param value the protobuf message
     * @return the serialized bytes
     */
    @Override
    public byte[] encode(T value) {
        return value.toByteArray();
    }
}
