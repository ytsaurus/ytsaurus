package tech.ytsaurus.flow.testutils;

import java.util.Optional;

import tech.ytsaurus.flow.state.StateAccessor;

/**
 * Read-only {@link StateAccessor} wrapper returned by {@link TestDoProcessResponse}: it delegates the
 * read operations to the underlying accessor and rejects {@link #set} / {@link #clear}, since a
 * processed response is an immutable snapshot.
 *
 * @param <T> state value type.
 */
class ReadOnlyStateAccessor<T> implements StateAccessor<T> {
    private final StateAccessor<T> delegate;

    ReadOnlyStateAccessor(StateAccessor<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<T> get() {
        return delegate.get();
    }

    @Override
    public T getOrDefault(T defaultValue) {
        return delegate.getOrDefault(defaultValue);
    }

    @Override
    public T getOrDefault() {
        return delegate.getOrDefault();
    }

    @Override
    public Class<T> getStateClass() {
        return delegate.getStateClass();
    }

    @Override
    public void set(T value) {
        throw new UnsupportedOperationException("State is read-only on a processed response");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("State is read-only on a processed response");
    }
}
