package tech.ytsaurus.flow.testutils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.state.ProtoExternalStateDescriptor;
import tech.ytsaurus.flow.state.State;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptor;
import tech.ytsaurus.flow.state.StateFormat;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.utils.ProtoUtils;

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
    record CapturedSeed(Kind kind, State state) {
        enum Kind { INTERNAL, EXTERNAL }
    }

    /**
     * Applies {@code op} to a fresh accessor for {@code descriptor} at {@code key} and returns the
     * resulting raw state.
     *
     * @param <T> state value type.
     * @param stateSchema the state's schema as declared on the harness; {@code null} for an
     *                    internal state or an undeclared external one.
     * @throws IllegalStateException if the mutation produced neither an internal nor external state.
     */
    static <T> CapturedSeed capture(
            StateDescriptor<T> descriptor,
            Payload key,
            Consumer<StateAccessor<T>> op,
            @Nullable TableSchema stateSchema
    ) {
        var internalHolders = new HashMap<String, StatesHolder>();
        var externalHolders = new HashMap<String, StatesHolder>();
        var stateSchemas = stateSchema == null
                ? Map.<String, TableSchema>of()
                : Map.of(descriptor.getName(), stateSchema);
        if (descriptor instanceof ProtoExternalStateDescriptor<?> protoDescriptor) {
            // A proto-format state arrives in a proto-format holder, whatever schema the harness
            // declares under that name.
            String name = descriptor.getName();
            externalHolders.put(name, new StatesHolder(name, null, null, StateFormat.PROTO, protoTypeOf(protoDescriptor)));
        }
        var backend = new SnapshotStateBackend(internalHolders, externalHolders, stateSchemas, null);

        StateAccessor<T> accessor = backend.accessor(descriptor, key);
        op.accept(accessor);

        // The descriptor creates its holder in exactly one of the maps; probe both to learn which.
        String name = descriptor.getName();
        var internalHolder = internalHolders.get(name);
        if (internalHolder != null) {
            // The seed is exactly what the accessor modified; the sweep also picks up a seed
            // made by mutating the value in place.
            var seeded = internalHolder.collectModifiedStates().get(key.getRow());
            return new CapturedSeed(CapturedSeed.Kind.INTERNAL, seeded);
        }
        var externalHolder = externalHolders.get(name);
        if (externalHolder != null) {
            return new CapturedSeed(CapturedSeed.Kind.EXTERNAL, externalHolder.get(key.getRow()));
        }
        // Unreachable: creating the accessor always creates its holder in one of the maps above.
        throw new IllegalStateException("State mutation produced no state for '" + name + "'");
    }

    /**
     * Fully qualified proto message name of {@code descriptor}'s state, as the worker sends it.
     */
    static String protoTypeOf(ProtoExternalStateDescriptor<?> descriptor) {
        return ProtoUtils.<Message.Builder>newBuilder(descriptor.getStateClass())
                .getDescriptorForType().getFullName();
    }
}
