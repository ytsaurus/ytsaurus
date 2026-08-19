package tech.ytsaurus.flow.testutils;

import java.util.Map;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.state.DefaultStateManager;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateBackend;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * In-memory {@link StateBackend} backing the test harness's typed state reads and writes.
 */
class SnapshotStateBackend implements StateBackend {
    private final Map<String, StatesHolder<InternalState>> internalStates;
    private final Map<String, StatesHolder<ExternalState>> externalStates;
    private final Map<String, TableSchema> externalStateSchemas;
    private final @Nullable TableSchema keySchema;

    SnapshotStateBackend(
            Map<String, StatesHolder<InternalState>> internalStates,
            Map<String, StatesHolder<ExternalState>> externalStates,
            Map<String, TableSchema> externalStateSchemas,
            @Nullable TableSchema keySchema
    ) {
        this.internalStates = internalStates;
        this.externalStates = externalStates;
        this.externalStateSchemas = externalStateSchemas;
        this.keySchema = keySchema;
    }

    /**
     * Builds a {@link StateAccessor} for {@code descriptor} bound to {@code key} over this backend's
     * holders.
     *
     * @param <T> state value type.
     */
    <T> StateAccessor<T> accessor(StateDescriptor<T> descriptor, Payload key) {
        return new DefaultStateManager(this).create(descriptor, () -> key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatesHolder<InternalState> getOrCreateInternalStateHolder(String stateName) {
        return internalStates.computeIfAbsent(stateName, name -> new StatesHolder<>(name, keySchema, null));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatesHolder<ExternalState> getExternalStateHolder(String stateName) {
        return externalStates.computeIfAbsent(
                stateName, name -> new StatesHolder<>(name, keySchema, externalStateSchemas.get(name)));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Joined state is served from the same external holders.
     */
    @Override
    public StatesHolder<ExternalState> getJoinedExternalStateHolder(String stateName) {
        return getExternalStateHolder(stateName);
    }
}
