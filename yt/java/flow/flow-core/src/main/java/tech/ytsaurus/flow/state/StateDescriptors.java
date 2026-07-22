package tech.ytsaurus.flow.state;

import java.util.function.Supplier;

import com.google.protobuf.MessageLite;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;
import tech.ytsaurus.flow.row.codec.DefaultYsonCodec;
import tech.ytsaurus.flow.row.codec.IdentityByteArrayCodec;
import tech.ytsaurus.flow.row.codec.ProtobufByteArrayCodec;
import tech.ytsaurus.flow.row.codec.YsonByteArrayCodec;
import tech.ytsaurus.flow.utils.ProtoUtils;

/**
 * Factory methods for creating built-in state descriptors.
 */
public class StateDescriptors {

    StateDescriptors() {
    }

    /**
     * Creates a descriptor for externally managed state.
     *
     * @param name state name
     * @return external state descriptor
     */
    public static ExternalStateDescriptor external(String name) {
        return new ExternalStateDescriptor(name);
    }

    /**
     * Creates a descriptor for read-only external state joined from another computation
     * (must be declared under {@code external_state_joiners} of the computation spec).
     *
     * <p>The resulting accessor reads the upstream computation's state via {@code get()} /
     * {@code getOrDefault()} but throws {@link UnsupportedOperationException} on {@code set()} /
     * {@code clear()}: a joiner is not a write-owner of the state table.
     *
     * @param name joined external state name
     * @return joined external state descriptor
     */
    public static JoinedExternalStateDescriptor externalReadOnly(String name) {
        return new JoinedExternalStateDescriptor(name);
    }

    /**
     * Creates a descriptor for raw binary internal state (identity serialization).
     *
     * <p>{@code getOrDefault()} returns an empty byte array when no value is stored.
     *
     * @param name state name
     * @return internal state descriptor over raw bytes
     */
    public static InternalStateDescriptor<byte[]> raw(String name) {
        return new InternalStateDescriptor<>(
                name,
                byte[].class,
                IdentityByteArrayCodec.INSTANCE,
                () -> new byte[0]
        );
    }

    /**
     * Creates a descriptor for protobuf internal state.
     *
     * <p>{@code getOrDefault()} returns a freshly-built default protobuf message
     * (equivalent to {@code stateClass.newBuilder().build()}) when no value is stored.
     *
     * @param name       state name
     * @param stateClass state class
     * @param <T>        state type
     * @return internal state descriptor that serializes and deserializes protobuf
     */
    public static <T extends MessageLite> InternalStateDescriptor<T> protobuf(
            String name,
            Class<T> stateClass
    ) {
        return new InternalStateDescriptor<>(
                name,
                stateClass,
                new ProtobufByteArrayCodec<>(stateClass),
                protobufDefault(stateClass)
        );
    }

    /**
     * Creates a descriptor for internal state serialized as binary YSON.
     *
     * <p>The state is (de)serialized as binary YSON, supporting every type that
     * {@code tech.ytsaurus.core.rows.YTreeSerializerFactory} handles (primitives, strings, enums,
     * arrays, collections, {@code YTreeNode}, ...) as well as {@code @Entity}-annotated classes.
     *
     * <p>The descriptor has no default-value supplier: {@link StateAccessor#getOrDefault()} on the
     * resulting accessor throws {@link UnsupportedOperationException} when no value is stored.
     *
     * @param name       state name
     * @param stateClass state class
     * @param <T>        state type
     * @return internal state descriptor that serializes and deserializes the state as binary YSON
     */
    public static <T> InternalStateDescriptor<T> yson(
            String name,
            Class<T> stateClass
    ) {
        return customWithoutDefault(
                name, stateClass, new YsonByteArrayCodec<>(stateClass, DefaultYsonCodec.INSTANCE));
    }

    /**
     * Creates an internal state descriptor with a caller-provided {@link ByteArrayCodec}
     * and <strong>no</strong> default-value supplier.
     *
     * <p>{@link StateAccessor#getOrDefault()} on the resulting accessor throws
     * {@link UnsupportedOperationException}; callers that need a canonical default
     * must use {@link #custom(String, Class, ByteArrayCodec, Supplier)} instead.
     *
     * @param name       state name
     * @param stateClass state class
     * @param codec      byte codec for {@code T}
     * @param <T>        state type
     * @return custom internal state descriptor with no default value
     * @throws UnsupportedOperationException from {@link StateAccessor#getOrDefault()} when
     *                                       no value is stored under the given key
     */
    public static <T> InternalStateDescriptor<T> customWithoutDefault(
            String name,
            Class<T> stateClass,
            ByteArrayCodec<T> codec
    ) {
        return new InternalStateDescriptor<>(name, stateClass, codec);
    }

    /**
     * Creates an internal state descriptor with a caller-provided {@link ByteArrayCodec} and a
     * default-value supplier used by {@link StateAccessor#getOrDefault()} when no value
     * is stored.
     *
     * @param name                 state name
     * @param stateClass           state class
     * @param codec                byte codec for {@code T}
     * @param defaultValueSupplier supplier of the canonical default value
     * @param <T>                  state type
     * @return custom internal state descriptor
     */
    public static <T> InternalStateDescriptor<T> custom(
            String name,
            Class<T> stateClass,
            ByteArrayCodec<T> codec,
            Supplier<T> defaultValueSupplier
    ) {
        return new InternalStateDescriptor<>(name, stateClass, codec, defaultValueSupplier);
    }

    @SuppressWarnings("unchecked")
    private static <T extends MessageLite> Supplier<T> protobufDefault(Class<T> stateClass) {
        return () -> (T) ProtoUtils.<MessageLite.Builder>newBuilder(stateClass).build();
    }
}
