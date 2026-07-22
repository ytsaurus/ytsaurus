package tech.ytsaurus.flow.internal.request.mapper;

import com.google.protobuf.ByteString;
import tech.ytsaurus.flow.row.NewTimer;
import tech.ytsaurus.flow.rpc.TNewTimer;

/**
 * Mapper between protobuf {@link TNewTimer} and {@link NewTimer}.
 * <p>
 * This mapper is stateless — it does not require any external context.
 * </p>
 */
public class NewTimerProtoMapper implements ProtoMapper<TNewTimer, NewTimer> {

    @Override
    public NewTimer fromProto(TNewTimer protoTimer) {
        String streamId = protoTimer.hasStreamId() ? protoTimer.getStreamId().toStringUtf8() : null;
        return new NewTimer(
                protoTimer.getTriggerTimestamp(),
                protoTimer.getEventTimestamp(),
                streamId
        );
    }

    @Override
    public TNewTimer toProto(NewTimer timer) {
        var timerBuilder = TNewTimer.newBuilder();
        timerBuilder.setTriggerTimestamp(timer.getTriggerTimestamp());
        if (timer.getEventTimestamp() > 0) {
            timerBuilder.setEventTimestamp(timer.getEventTimestamp());
        }
        if (timer.getStreamId() != null) {
            timerBuilder.setStreamId(ByteString.copyFromUtf8(timer.getStreamId()));
        }
        return timerBuilder.build();
    }
}
