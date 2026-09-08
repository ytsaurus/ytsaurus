package tech.ytsaurus.flow.internal.request.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.codec.KeyCodec;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.rpc.TStateItem;
import tech.ytsaurus.flow.state.State;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * Bidirectional mapper between protobuf {@link TState} and a {@link StatesHolder}. State values
 * are carried as undecoded wire bytes, so no value codec is involved; the subclasses add what is
 * specific to their state kind.
 */
public abstract class StateProtoMapper {

    protected final @Nullable TableSchema keySchema;
    private final KeyCodec keyCodec;

    /**
     * @param keySchema table schema describing state entry keys
     * @param keyCodec  codec used to (de)serialize state entry keys
     */
    protected StateProtoMapper(
            @Nullable TableSchema keySchema,
            KeyCodec keyCodec
    ) {
        this.keySchema = keySchema;
        this.keyCodec = keyCodec;
    }

    /**
     * Converts a list of protobuf states to a map of {@link StatesHolder} instances.
     *
     * @param protoStates the protobuf states
     * @param jobId       job identifier for error messages
     * @param requestId   request identifier for error messages
     * @return map of state name to states holder
     */
    public ConcurrentHashMap<String, StatesHolder> fromProto(
            List<TState> protoStates,
            GUID jobId,
            GUID requestId
    ) {
        var states = new ConcurrentHashMap<String, StatesHolder>();
        for (var protoState : protoStates) {
            var stateHolder = states.computeIfAbsent(protoState.getName(), name -> createHolder(protoState));
            for (var stateItem : protoState.getStateItemsList()) {
                UnversionedRow key = keyCodec.decode(stateItem.getKey());
                if (stateItem.getReset()) {
                    stateHolder.load(key, State.RESET);
                    continue;
                }
                validateItem(protoState, stateHolder, jobId, requestId);
                stateHolder.load(key, new State(stateItem.getState()));
            }
        }
        return states;
    }

    /**
     * Converts a {@link StatesHolder} to a protobuf {@link TState} carrying its modified states.
     *
     * @param statesHolder the states holder
     * @return the protobuf state
     */
    public TState toProto(StatesHolder statesHolder) {
        TState.Builder stateBuilder = TState.newBuilder();
        stateBuilder.setName(statesHolder.getName());
        describeState(stateBuilder, statesHolder);
        var modifiedStates = statesHolder.getModifiedStates();
        var stateItems = new ArrayList<TStateItem>(modifiedStates.size());
        for (var entry : modifiedStates.entrySet()) {
            State state = entry.getValue();
            var stateItemBuilder = TStateItem.newBuilder()
                    .setKey(keyCodec.encode(entry.getKey()))
                    .setReset(state.isReset());
            if (!state.isReset()) {
                stateItemBuilder.setState(
                        Objects.requireNonNull(state.getBytes(), "Non-reset state must have bytes")
                );
            }
            stateItems.add(stateItemBuilder.build());
        }
        stateBuilder.addAllStateItems(stateItems);
        return stateBuilder.build();
    }

    /**
     * Creates the holder for {@code protoState}.
     */
    protected abstract StatesHolder createHolder(TState protoState);

    /**
     * Checks that a non-reset item of {@code protoState} may be loaded into {@code stateHolder}.
     */
    protected void validateItem(TState protoState, StatesHolder stateHolder, GUID jobId, GUID requestId) {
    }

    /**
     * Describes {@code statesHolder} on {@code stateBuilder} beyond its name — the schema, the
     * format — before the items are added.
     */
    protected void describeState(TState.Builder stateBuilder, StatesHolder statesHolder) {
    }
}
