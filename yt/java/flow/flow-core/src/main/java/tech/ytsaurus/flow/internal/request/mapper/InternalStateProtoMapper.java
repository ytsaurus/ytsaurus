package tech.ytsaurus.flow.internal.request.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.codec.InternalStateValueCodec;
import tech.ytsaurus.flow.row.codec.KeyCodec;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.rpc.TStateItem;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * Bidirectional mapper between protobuf {@link TState} and a {@link StatesHolder} of
 * {@link InternalState} entries.
 */
public class InternalStateProtoMapper {

    private final @Nullable TableSchema keySchema;
    private final KeyCodec keyCodec;
    private final InternalStateValueCodec valueCodec;

    /**
     * Creates a mapper with the supplied schema and codecs.
     *
     * @param keySchema  table schema describing state entry keys
     * @param keyCodec   codec used to (de)serialize state entry keys
     * @param valueCodec codec used to (de)serialize opaque state entry value blobs
     */
    public InternalStateProtoMapper(
            @Nullable TableSchema keySchema,
            KeyCodec keyCodec,
            InternalStateValueCodec valueCodec
    ) {
        this.keySchema = keySchema;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    /**
     * Converts a list of protobuf states to a map of {@link StatesHolder} instances.
     *
     * @param protoStates the protobuf states
     * @return map of state name to states holder
     */
    public ConcurrentHashMap<String, StatesHolder<InternalState>> fromProto(List<TState> protoStates) {
        var states = new ConcurrentHashMap<String, StatesHolder<InternalState>>();
        for (var protoState : protoStates) {
            var stateHolder = states.computeIfAbsent(
                    protoState.getName(), name -> new StatesHolder<>(name, keySchema, null)
            );
            for (var stateItem : protoState.getStateItemsList()) {
                var key = keyCodec.decode(stateItem.getKey());
                if (stateItem.getReset()) {
                    // Reset items carry no value; load the canonical RESET so the round-trip
                    // preserves equality with InternalState.RESET.
                    stateHolder.load(key, InternalState.RESET);
                    continue;
                }
                stateHolder.load(key, new InternalState(valueCodec.decode(stateItem.getState())));
            }
        }
        return states;
    }

    /**
     * Converts a {@link StatesHolder} of internal states to a protobuf {@link TState}.
     *
     * @param statesHolder the states holder
     * @return the protobuf state
     */
    public TState toProto(StatesHolder<InternalState> statesHolder) {
        TState.Builder stateBuilder = TState.newBuilder();
        stateBuilder.setName(statesHolder.getName());
        var modifiedStates = statesHolder.getModifiedStates();
        var stateItems = new ArrayList<TStateItem>(modifiedStates.size());
        for (var entry : modifiedStates.entrySet()) {
            var key = entry.getKey();
            InternalState state = entry.getValue();
            var stateEntryBuilder = TStateItem.newBuilder()
                    .setKey(keyCodec.encode(key))
                    .setReset(state.isReset());
            if (!state.isReset()) {
                stateEntryBuilder.setState(valueCodec.encode(
                        Objects.requireNonNull(state.getValue(), "Non-reset state must have value")
                ));
            }
            stateItems.add(stateEntryBuilder.build());
        }
        stateBuilder.addAllStateItems(stateItems);
        return stateBuilder.build();
    }
}
