package tech.ytsaurus.flow.internal.request.mapper;

import java.util.Objects;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.codec.KeyCodec;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.state.StateFormat;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.utils.YsonUtils;

/**
 * {@link StateProtoMapper} for external states: a row-format state carries its schema on the
 * wire, a proto-format one its format and proto type.
 */
public class ExternalStateProtoMapper extends StateProtoMapper {

    public ExternalStateProtoMapper(@Nullable TableSchema keySchema, KeyCodec keyCodec) {
        super(keySchema, keyCodec);
    }

    @Override
    protected StatesHolder createHolder(TState protoState) {
        // The schema may be empty for a reset-only state (see companion_service.proto). Defer the
        // schema requirement until a non-reset item that actually needs it.
        TableSchema stateSchema = protoState.getSchema().isEmpty()
                ? null
                : TableSchema.fromYTree(YsonUtils.yTreeFromProto(protoState.getSchema()));
        return new StatesHolder(
                protoState.getName(),
                keySchema,
                stateSchema,
                StateFormat.fromWireValue(protoState.getFormat()),
                protoState.getProtoType());
    }

    @Override
    protected void validateItem(TState protoState, StatesHolder stateHolder, GUID jobId, GUID requestId) {
        if (stateHolder.getFormat() != StateFormat.PROTO && stateHolder.getStateSchema() == null) {
            throw new IllegalArgumentException(
                    "External state with a non-reset item must have a schema "
                            + "(StateName: %s, JobId: %s, RequestId: %s)"
                            .formatted(protoState.getName(), jobId, requestId)
            );
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>An all-default proto message serializes to zero bytes, a legal payload.
     */
    @Override
    protected void describeState(TState.Builder stateBuilder, StatesHolder statesHolder) {
        if (statesHolder.getFormat() == StateFormat.PROTO) {
            stateBuilder.setFormat(StateFormat.PROTO.getWireValue());
            String protoType = statesHolder.getProtoType();
            if (protoType != null && !protoType.isEmpty()) {
                stateBuilder.setProtoType(protoType);
            }
            return;
        }
        var stateSchema = Objects.requireNonNull(
                statesHolder.getStateSchema(), "External state must have a schema"
        );
        stateBuilder.setSchema(YsonUtils.protoFromYTree(stateSchema.toYTree()));
    }
}
