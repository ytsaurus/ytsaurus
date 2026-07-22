package tech.ytsaurus.flow.state;

import java.util.Optional;

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
     * @return Optional state value. Optional.empty() means that there is no corresponding value for the key.
     */
    Optional<T> get();

    /**
     * Get state value or user-provided default value if none is present.
     *
     * @param defaultValue Default value.
     * @return State value or default value.
     */
    default T getOrDefault(T defaultValue) {
        return get().orElse(defaultValue);
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
}
