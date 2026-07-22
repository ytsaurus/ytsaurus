package tech.ytsaurus.flow.state;

import tech.ytsaurus.flow.row.Keyed;

/**
 * Manages the creation of state accessors for keyed entities based on state descriptors.
 */
public interface StateManager {

    /**
     * Creates a state accessor for the given state descriptor and keyed entity.
     *
     * @param <T>        The type of the state value.
     * @param <A>        The type of the state accessor.
     * @param descriptor The state descriptor that defines the state.
     * @param entity     The keyed entity for which the state accessor is created.
     * @return A state accessor for the given descriptor and entity.
     */
    <T> StateAccessor<T> create(
            StateDescriptor<T> descriptor,
            Keyed entity
    );
}
