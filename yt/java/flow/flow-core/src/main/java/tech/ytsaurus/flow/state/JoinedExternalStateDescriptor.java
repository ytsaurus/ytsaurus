package tech.ytsaurus.flow.state;

import tech.ytsaurus.flow.row.Keyed;
import tech.ytsaurus.flow.row.Payload;

/**
 * {@link StateDescriptor} for read-only external state joined from another computation
 * (must be declared under {@code external_state_joiners} of the computation spec).
 *
 * <p>The resulting {@link ReadOnlyExternalStateAccessor} exposes {@code get()} / {@code getOrDefault()}
 * but throws on {@code set()} / {@code clear()}: a joiner is not a write-owner of the state table.
 */
public final class JoinedExternalStateDescriptor extends StateDescriptor<Payload> {
    private final String name;

    JoinedExternalStateDescriptor(String name) {
        ExternalStateDescriptor.validateExternalStateName(name);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Class<Payload> getStateClass() {
        return Payload.class;
    }

    @Override
    ReadOnlyExternalStateAccessor create(Keyed key, StateBackend backend) {
        return new ReadOnlyExternalStateAccessor(key.getKey(), backend.getJoinedExternalStateHolder(name));
    }
}
