package tech.ytsaurus.flow.internal.request.mapper;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.row.codec.KeyCodec;

/**
 * Mapper that converts a protobuf {@link NYT.NFlow.NProto.Timer.TTimer} into a domain
 * {@link Timer}.
 */
public class TimerProtoMapper {

    private final TableSchema keySchema;
    private final KeyCodec keyCodec;

    /**
     * Creates a mapper with the supplied schema and key codec.
     *
     * @param keySchema table schema describing the timer key
     * @param keyCodec  codec used to decode the timer key from its wire representation
     */
    public TimerProtoMapper(TableSchema keySchema, KeyCodec keyCodec) {
        this.keySchema = keySchema;
        this.keyCodec = keyCodec;
    }

    /**
     * Converts a protobuf timer to a domain {@link Timer}.
     *
     * @param protoTimer the protobuf timer
     * @return the domain timer
     */
    public Timer fromProto(NYT.NFlow.NProto.Timer.TTimer protoTimer) {
        return new Timer(
                protoTimer.getMessageId(),
                protoTimer.getEventTimestamp(),
                protoTimer.getSystemTimestamp(),
                protoTimer.getStreamId().toStringUtf8(),
                /*StreamSpecId*/ -1L,
                protoTimer.getTriggerTimestamp(),
                new Payload(
                        keyCodec.decode(protoTimer.getKey()),
                        keySchema
                )
        );
    }
}
