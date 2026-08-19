package tech.ytsaurus.flow.state;

import tech.ytsaurus.flow.row.Keyed;

public class DefaultStateManager implements StateManager {

    private final StateBackend backend;

    public DefaultStateManager(StateBackend backend) {
        this.backend = backend;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> StateAccessor<T> create(
            StateDescriptor<T> descriptor,
            Keyed entity
    ) {
        return descriptor.create(entity, backend);
    }
}
