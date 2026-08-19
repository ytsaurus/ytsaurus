package tech.ytsaurus.flow.state;


/**
 * Interface for managing state holders in a flow computation.
 */
public interface StateBackend {
    /**
     * Returns existing or creates a new internal state holder for the given state name.
     *
     * @param stateName name of the state.
     * @return internal state holder.
     * @throws IllegalArgumentException if the state name is not configured in the computation spec.
     */
    StatesHolder<InternalState> getOrCreateInternalStateHolder(String stateName);

    /**
     * Returns the external state holder configured for the given state name.
     *
     * @param stateName name of the external state.
     * @return external state holder.
     * @throws NullPointerException if the external state is not configured.
     */
    StatesHolder<ExternalState> getExternalStateHolder(String stateName);

    /**
     * Returns the read-only joined external state holder configured for the given state name.
     * Joined state is read from another computation's state table and is never written back.
     *
     * @param stateName name of the joined external state.
     * @return joined external state holder.
     * @throws IllegalArgumentException if the joined external state is not configured.
     */
    StatesHolder<ExternalState> getJoinedExternalStateHolder(String stateName);
}
