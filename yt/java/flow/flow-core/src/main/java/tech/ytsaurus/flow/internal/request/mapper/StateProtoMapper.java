package tech.ytsaurus.flow.internal.request.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.google.protobuf.ByteString;
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
                    stateHolder.loadReset(key);
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
     * @param statesHolder   the states holder
     * @param modifiedStates the states to send back, as collected from {@code statesHolder}
     *                       by {@link StatesHolder#collectModifiedStates}
     * @return the protobuf state
     */
    public TState toProto(StatesHolder statesHolder, Map<UnversionedRow, State> modifiedStates) {
        TState.Builder stateBuilder = TState.newBuilder();
        stateBuilder.setName(statesHolder.getName());
        describeState(stateBuilder, statesHolder);
        var stateItems = new ArrayList<TStateItem>(modifiedStates.size());
        boolean emptyPayloadAllowed = allowsEmptyPayload(statesHolder);
        for (var entry : modifiedStates.entrySet()) {
            stateItems.add(toProtoItem(entry.getKey(), entry.getValue(), emptyPayloadAllowed));
        }
        stateBuilder.addAllStateItems(stateItems);
        return stateBuilder.build();
    }

    private TStateItem toProtoItem(UnversionedRow key, State state, boolean emptyPayloadAllowed) {
        var stateItemBuilder = TStateItem.newBuilder().setKey(keyCodec.encode(key));
        if (state.isReset()) {
            return stateItemBuilder.setReset(true).build();
        }
        ByteString bytes = Objects.requireNonNull(state.getBytes(), "Non-reset state must have bytes");
        if (bytes.isEmpty() && !emptyPayloadAllowed) {
            // A value that encodes to no bytes is no value, and a reset is the only way the wire
            // spells that: the worker rejects a non-reset item with an empty payload.
            return stateItemBuilder.setReset(true).build();
        }
        return stateItemBuilder.setReset(false).setState(bytes).build();
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

    /**
     * Tells whether a value of {@code statesHolder} may go out as a non-reset item with an empty
     * payload.
     */
    protected boolean allowsEmptyPayload(StatesHolder statesHolder) {
        return false;
    }
}
