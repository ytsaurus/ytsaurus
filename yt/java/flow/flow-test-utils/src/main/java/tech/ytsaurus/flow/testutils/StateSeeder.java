package tech.ytsaurus.flow.testutils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.State;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * Serializes a single seeded state mutation ({@code set} or {@code clear}) through a real
 * {@link StateAccessor} over a throwaway backend, and captures the resulting raw state. Reusing the
 * runtime accessor path guarantees the seed is encoded exactly as the computation would encode it.
 */
final class StateSeeder {

    private StateSeeder() {
    }

    /**
     * The raw state produced by one seed mutation, tagged with the holder it belongs to.
     */
    record CapturedSeed(Kind kind, State<?> state) {
        enum Kind { INTERNAL, EXTERNAL }
    }

    /**
     * Applies {@code op} to a fresh accessor for {@code descriptor} at {@code key} and returns the
     * resulting raw state.
     *
     * @param <T> state value type.
     * @throws IllegalStateException if the mutation produced neither an internal nor external state.
     */
    static <T> CapturedSeed capture(
            StateDescriptor<T> descriptor,
            Payload key,
            Consumer<StateAccessor<T>> op
    ) {
        var internalHolders = new HashMap<String, StatesHolder<InternalState>>();
        var externalHolders = new HashMap<String, StatesHolder<ExternalState>>();
        var backend = new SnapshotStateBackend(internalHolders, externalHolders, Map.of(), null);

        StateAccessor<T> accessor = backend.accessor(descriptor, key);
        op.accept(accessor);

        // The descriptor creates its holder in exactly one of the maps; probe both to learn which.
        String name = descriptor.getName();
        var internalHolder = internalHolders.get(name);
        if (internalHolder != null) {
            return new CapturedSeed(CapturedSeed.Kind.INTERNAL, internalHolder.get(key.getRow()));
        }
        var externalHolder = externalHolders.get(name);
        if (externalHolder != null) {
            return new CapturedSeed(CapturedSeed.Kind.EXTERNAL, externalHolder.get(key.getRow()));
        }
        // Unreachable: creating the accessor always creates its holder in one of the maps above.
        throw new IllegalStateException("State mutation produced no state for '" + name + "'");
    }
}
