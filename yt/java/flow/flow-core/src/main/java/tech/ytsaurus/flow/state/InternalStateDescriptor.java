package tech.ytsaurus.flow.state;

import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.Keyed;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;

/**
 * Describes a Flow internal-state cell: its name, value class, byte-array codec and optional
 * default-value supplier.
 *
 * <p>Instances are created via the factory methods of {@link StateDescriptors}:
 * <pre>{@code
 * private static final InternalStateDescriptor<TWordCountState> COUNTER =
 *         StateDescriptors.protobuf("word-state", TWordCountState.class);
 * }</pre>
 *
 * @param <T> state value type.
 */
public final class InternalStateDescriptor<T> extends StateDescriptor<T> {
    private final String name;
    private final Class<T> stateClass;
    private final ByteArrayCodec<T> codec;
    private final @Nullable Supplier<T> defaultValueSupplier;
    private final boolean readOnly;

    InternalStateDescriptor(
            String name,
            Class<T> stateClass,
            ByteArrayCodec<T> codec,
            @Nullable Supplier<T> defaultValueSupplier
    ) {
        this(name, stateClass, codec, defaultValueSupplier, /*readOnly*/ false);
    }

    InternalStateDescriptor(
            String name,
            Class<T> stateClass,
            ByteArrayCodec<T> codec
    ) {
        this(name, stateClass, codec, null);
    }

    private InternalStateDescriptor(
            String name,
            Class<T> stateClass,
            ByteArrayCodec<T> codec,
            @Nullable Supplier<T> defaultValueSupplier,
            boolean readOnly
    ) {
        this.name = name;
        this.stateClass = stateClass;
        this.codec = codec;
        this.defaultValueSupplier = defaultValueSupplier;
        this.readOnly = readOnly;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<T> getStateClass() {
        return stateClass;
    }

    /**
     * Returns the codec between values of this state and their bytes.
     *
     * @return the value codec
     */
    ByteArrayCodec<T> getCodec() {
        return codec;
    }

    /**
     * Returns the default value backing {@link StateAccessor#getOrDefault()} for this internal
     * state, produced by the default-value supplier configured at construction.
     *
     * @return the default value.
     * @throws UnsupportedOperationException if no default-value supplier was configured.
     */
    T defaultValue() {
        if (defaultValueSupplier == null) {
            throw new UnsupportedOperationException(
                    "Default value construction is not supported for state '" + name + "'");
        }
        return defaultValueSupplier.get();
    }

    /**
     * Returns a descriptor of the same state cell whose accessors reject writes: {@code set()} and
     * {@code clear()} throw {@link UnsupportedOperationException}, and {@code getOrDefault()}
     * returns the default without attaching it to the key. Declaring it once here spares every
     * access site a {@link StateAccessor#readOnly()} call; the two do the same thing at different
     * levels.
     *
     * <p>Read-only is a property of the accessor, not of the state: the state a writable accessor
     * has already read in this request stays tracked whichever descriptor reads it next.
     *
     * @return read-only descriptor of the same state, or this one when it is read-only already.
     */
    public InternalStateDescriptor<T> readOnly() {
        if (readOnly) {
            return this;
        }
        return new InternalStateDescriptor<>(name, stateClass, codec, defaultValueSupplier, /*readOnly*/ true);
    }

    @Override
    InternalStateAccessor<T> create(Keyed key, StateBackend backend) {
        var accessor = new InternalStateAccessor<>(
                key.getKey(),
                this,
                backend.getOrCreateInternalStateHolder(name)
        );
        return readOnly ? new ReadOnlyInternalStateAccessor<>(accessor) : accessor;
    }
}
