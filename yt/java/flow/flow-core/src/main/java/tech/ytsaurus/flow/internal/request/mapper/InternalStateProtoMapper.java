package tech.ytsaurus.flow.internal.request.mapper;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.codec.KeyCodec;
import tech.ytsaurus.flow.rpc.TState;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * {@link StateProtoMapper} for internal states: no schema or format travels on the wire.
 */
public class InternalStateProtoMapper extends StateProtoMapper {

    public InternalStateProtoMapper(@Nullable TableSchema keySchema, KeyCodec keyCodec) {
        super(keySchema, keyCodec);
    }

    @Override
    protected StatesHolder createHolder(TState protoState) {
        return new StatesHolder(protoState.getName(), keySchema);
    }
}
