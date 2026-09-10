package tech.ytsaurus.flow.state;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.row.codec.InternalStateValueCodec;

/**
 * {@link StateAccessor} for the Flow internal state.
 *
 * <p>The value it returns is live: it is decoded once per key and request, every accessor for
 * that key hands out the same object, and the changes made to it in place are written back at
 * the end of the request without a {@link #set} call. Nothing is written when the value encodes
 * to the bytes it arrived with, the default of {@link #getOrDefault} included.
 * {@link #readOnly()} gives the untracked view.
 *
 * @param <T> state value type.
 */
public class InternalStateAccessor<T> implements StateAccessor<T> {
    final Payload key;
    final InternalStateDescriptor<T> descriptor;
    final ByteStringCodec<T> codec;
    final StatesHolder statesHolder;

    InternalStateAccessor(
            Payload key,
            InternalStateDescriptor<T> descriptor,
            StatesHolder statesHolder
    ) {
        this(key, descriptor, statesHolder, CodecRegistry.getInstance().getInternalStateValueCodec());
    }

    InternalStateAccessor(
            Payload key,
            InternalStateDescriptor<T> descriptor,
            StatesHolder statesHolder,
            InternalStateValueCodec wireCodec
    ) {
        this(key, descriptor, statesHolder, new ValueCodec<>(descriptor.getCodec(), wireCodec));
    }

    InternalStateAccessor(
            Payload key,
            InternalStateDescriptor<T> descriptor,
            StatesHolder statesHolder,
            ByteStringCodec<T> codec
    ) {
        this.key = key;
        this.descriptor = descriptor;
        this.codec = codec;
        this.statesHolder = statesHolder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public @Nullable T get() {
        State state = statesHolder.get(key.getRow());
        if (state == null || state.isReset()) {
            return null;
        }
        return state.getMutableValue(codec);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The default is attached to the key, not written: it becomes the state value only once
     * the computation changes it.
     */
    @Override
    public T getOrDefault(T defaultValue) {
        T value = get();
        return value != null ? value : attach(defaultValue);
    }

    /**
     * {@inheritDoc}
     *
     * <p>The default is attached to the key, not written: it becomes the state value only once
     * the computation changes it.
     */
    @Override
    public T getOrDefault() {
        T value = get();
        return value != null ? value : attach(descriptor.defaultValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(T value) {
        statesHolder.set(key.getRow(), new State(value, codec));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        statesHolder.set(key.getRow(), State.RESET);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<T> getStateClass() {
        return descriptor.getStateClass();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StateAccessor<T> readOnly() {
        return new ReadOnlyInternalStateAccessor<>(this);
    }

    /**
     * Puts {@code value} under the key as an unmodified state and encodes it right away, so that
     * the bytes serve as the baseline the end-of-request sweep compares against: an untouched
     * default produces no write, one changed in place does.
     */
    private T attach(T value) {
        State state = new State(value, codec);
        state.getBytes();
        statesHolder.load(key.getRow(), state);
        return value;
    }

    /**
     * Value ⇄ wire bytes: the descriptor codec followed by the wire codec. {@link State} memoizes one value
     * and one codec, so that {@link StatesHolder#collectModifiedStates} can re-encode a mutable value with no
     * accessor in reach; hence a single self-contained value-to-wire codec.
     */
    private static final class ValueCodec<T> implements ByteStringCodec<T> {
        private final ByteArrayCodec<T> codec;
        private final InternalStateValueCodec wireCodec;

        ValueCodec(ByteArrayCodec<T> codec, InternalStateValueCodec wireCodec) {
            this.codec = codec;
            this.wireCodec = wireCodec;
        }

        @Override
        public ByteString encode(T value) {
            return wireCodec.encode(codec.encode(value));
        }

        @Override
        public T decode(ByteString bytes) {
            return codec.decode(wireCodec.decode(bytes));
        }
    }
}
