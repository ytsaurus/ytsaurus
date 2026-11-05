package tech.ytsaurus.flow.internal.request.mapper;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.Visit;
import tech.ytsaurus.flow.row.codec.KeyCodec;

/**
 * Mapper that converts a protobuf {@link NYT.NFlow.NProto.Visit.TVisit} into a domain {@link Visit}.
 */
public class VisitProtoMapper {

    private final TableSchema keySchema;
    private final KeyCodec keyCodec;

    public VisitProtoMapper(TableSchema keySchema, KeyCodec keyCodec) {
        this.keySchema = keySchema;
        this.keyCodec = keyCodec;
    }

    public Visit fromProto(NYT.NFlow.NProto.Visit.TVisit protoVisit) {
        return new Visit(
                protoVisit.getMessageId(),
                protoVisit.getEventTimestamp(),
                protoVisit.getSystemTimestamp(),
                protoVisit.getStreamId().toStringUtf8(),
                new Payload(
                        keyCodec.decode(protoVisit.getKey()),
                        keySchema
                )
        );
    }
}
