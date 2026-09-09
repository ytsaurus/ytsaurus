package tech.ytsaurus.flow.state;

import org.jspecify.annotations.Nullable;

/**
 * Read-only view returned by {@link StateAccessor#readOnly()}: reads go to the underlying
 * accessor, {@link #set} and {@link #clear} throw.
 *
 * @param <T> state value type.
 */
final class ReadOnlyStateAccessor<T> implements StateAccessor<T> {
    private final StateAccessor<T> delegate;

    ReadOnlyStateAccessor(StateAccessor<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public @Nullable T get() {
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
        throw new UnsupportedOperationException("State accessor is read-only");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("State accessor is read-only");
    }

    @Override
    public StateAccessor<T> readOnly() {
        return this;
    }
}
