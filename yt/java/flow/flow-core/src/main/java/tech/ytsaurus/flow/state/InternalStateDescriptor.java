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

    InternalStateDescriptor(
            String name,
            Class<T> stateClass,
            ByteArrayCodec<T> codec,
            @Nullable Supplier<T> defaultValueSupplier
    ) {
        this.name = name;
        this.stateClass = stateClass;
        this.codec = codec;
        this.defaultValueSupplier = defaultValueSupplier;
    }

    InternalStateDescriptor(
            String name,
            Class<T> stateClass,
            ByteArrayCodec<T> codec
    ) {
        this(name, stateClass, codec, null);
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
     * Returns the byte-array codec used to (de)serialize values of this state.
     *
     * @return the configured codec
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

    @Override
    InternalStateAccessor<T> create(Keyed key, StateBackend backend) {
        return new InternalStateAccessor<>(
                key.getKey(),
                this,
                backend.getOrCreateInternalStateHolder(name)
        );
    }
}
