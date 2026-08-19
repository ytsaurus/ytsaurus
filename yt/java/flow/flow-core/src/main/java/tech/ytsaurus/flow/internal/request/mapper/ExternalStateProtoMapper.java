package tech.ytsaurus.flow.internal.request.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.ByteStringCodec;
import tech.ytsaurus.flow.row.codec.KeyCodec;
import tech.ytsaurus.flow.row.codec.PayloadCodec;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.rpc.TStateItem;
import tech.ytsaurus.flow.state.ExternalState;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.utils.YsonUtils;

/**
 * Bidirectional mapper between protobuf {@link TState} and a {@link StatesHolder} of
 * {@link ExternalState} entries.
 */
public class ExternalStateProtoMapper {

    private final @Nullable TableSchema keySchema;
    private final KeyCodec keyCodec;
    private final PayloadCodec valueCodec;

    /**
     * Creates a mapper with the supplied schema and codecs.
     *
     * @param keySchema  table schema describing state entry keys
     * @param keyCodec   codec used to (de)serialize state entry keys
     * @param valueCodec factory of codecs used to (de)serialize state entry payload values
     */
    public ExternalStateProtoMapper(
            @Nullable TableSchema keySchema,
            KeyCodec keyCodec,
            PayloadCodec valueCodec
    ) {
        this.keySchema = keySchema;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    /**
     * Converts a list of protobuf states to a map of {@link StatesHolder} instances.
     *
     * @param protoStates the protobuf states
     * @param jobId       job identifier for error messages
     * @param requestId   request identifier for error messages
     * @return map of state name to states holder
     */
    public ConcurrentHashMap<String, StatesHolder<ExternalState>> fromProto(
            List<TState> protoStates,
            GUID jobId,
            GUID requestId
    ) {
        var states = new ConcurrentHashMap<String, StatesHolder<ExternalState>>();
        for (var protoState : protoStates) {
            // The schema may be empty for a reset-only state (see companion_service.proto). Defer the
            // schema requirement and codec binding until a non-reset item that actually needs them.
            TableSchema stateSchema = protoState.getSchema().isEmpty()
                    ? null
                    : TableSchema.fromYTree(YsonUtils.yTreeFromProto(protoState.getSchema()));
            var stateHolder = states.computeIfAbsent(
                    protoState.getName(), name -> new StatesHolder<>(name, keySchema, stateSchema)
            );
            // Bound lazily to the per-state schema on the first non-reset item: the schema is the
            // same for all items inside a single TState message.
            ByteStringCodec<Payload> boundValueCodec = null;
            for (var stateItem : protoState.getStateItemsList()) {
                UnversionedRow key = keyCodec.decode(stateItem.getKey());
                if (stateItem.getReset()) {
                    stateHolder.load(key, ExternalState.RESET);
                    continue;
                }
                if (stateSchema == null) {
                    throw new IllegalArgumentException(
                            "External state with a non-reset item must have a schema "
                                    + "(StateName: %s, JobId: %s, RequestId: %s)"
                                    .formatted(protoState.getName(), jobId, requestId)
                    );
                }
                if (boundValueCodec == null) {
                    boundValueCodec = valueCodec.codecFor(stateSchema);
                }
                stateHolder.load(
                        key,
                        new ExternalState(boundValueCodec.decode(stateItem.getState()))
                );
            }
        }
        return states;
    }

    /**
     * Converts a {@link StatesHolder} of external states to a protobuf {@link TState}.
     *
     * @param statesHolder the states holder
     * @return the protobuf state
     */
    public TState toProto(StatesHolder<ExternalState> statesHolder) {
        TState.Builder stateBuilder = TState.newBuilder();
        stateBuilder.setName(statesHolder.getName());
        var stateSchema = Objects.requireNonNull(
                statesHolder.getStateSchema(), "External state must have a schema"
        );
        stateBuilder.setSchema(YsonUtils.protoFromYTree(stateSchema.toYTree()));
        // Bind the payload codec to the per-state schema once.
        ByteStringCodec<Payload> boundValueCodec = valueCodec.codecFor(stateSchema);
        var modifiedStates = statesHolder.getModifiedStates();
        var stateItems = new ArrayList<TStateItem>(modifiedStates.size());
        for (var entry : modifiedStates.entrySet()) {
            var key = entry.getKey();
            ExternalState state = entry.getValue();
            var stateItemBuilder = TStateItem.newBuilder()
                    .setKey(keyCodec.encode(key))
                    .setReset(state.isReset());
            if (!state.isReset()) {
                Objects.requireNonNull(state.getValue(), "Non-reset state must have value");
                stateItemBuilder.setState(boundValueCodec.encode(state.getValue()));
            }
            stateItems.add(stateItemBuilder.build());
        }
        stateBuilder.addAllStateItems(stateItems);
        return stateBuilder.build();
    }
}
