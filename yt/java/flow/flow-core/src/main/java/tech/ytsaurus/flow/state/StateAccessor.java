package tech.ytsaurus.flow.state;

import org.jspecify.annotations.Nullable;

/**
 * Typed accessor for a single keyed value in a {@link StatesHolder}.
 *
 * <p>
 * Instances are obtained from
 * {@link tech.ytsaurus.flow.context.StatefulContext#getState StatefulContext.getState(...)}
 * via a {@link StateDescriptor}.
 * </p>
 * Custom accessors are produced by custom descriptors; user code should not instantiate them directly.
 *
 * @param <T> state value type.
 */
public interface StateAccessor<T> {

    /**
     * Get state value.
     *
     * @return State value, or {@code null} if there is no corresponding value for the key.
     */
    @Nullable
    T get();

    /**
     * Get state value or user-provided default value if none is present.
     *
     * @param defaultValue Default value.
     * @return State value or default value.
     */
    default T getOrDefault(T defaultValue) {
        T value = get();
        return value != null ? value : defaultValue;
    }

    /**
     * Get state value or create default value if default value may be constructed for type T.
     *
     * @return State value or default value.
     */
    default T getOrDefault() {
        throw new UnsupportedOperationException("Default value construction is not supported for given type");
    }

    /**
     * Set state value under the current key.
     *
     * @param value State value.
     */
    void set(T value);

    /**
     * Removes the value stored under the current key.
     */
    void clear();

    /**
     * Returns the runtime class of the state value.
     *
     * @return State class.
     */
    Class<T> getStateClass();

    /**
     * Returns a read-only view of this accessor: reads behave as on this accessor, {@link #set}
     * and {@link #clear} throw {@link UnsupportedOperationException}.
     *
     * <p>Use it wherever a computation only inspects a state. The value of an internal state
     * read through {@link #get} is re-encoded at the end of the batch, to find out whether it
     * was changed in place; a value read through this view is not tracked, so that encoding
     * does not happen and nothing is written back.
     *
     * @return Read-only view of this accessor.
     */
    default StateAccessor<T> readOnly() {
        return new ReadOnlyStateAccessor<>(this);
    }
}
