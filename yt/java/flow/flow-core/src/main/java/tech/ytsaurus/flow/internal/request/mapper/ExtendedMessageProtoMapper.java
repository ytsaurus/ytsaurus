package tech.ytsaurus.flow.internal.request.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.KeyCodec;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.stream.StreamSpecs;


/**
 * Bidirectional mapper between protobuf {@link TReqProcessBatch.TExtendedMessage} and the domain
 * {@link ExtendedMessage}.
 */
public class ExtendedMessageProtoMapper {

    private static final Logger log = LoggerFactory.getLogger(ExtendedMessageProtoMapper.class);

    private final TableSchema keySchema;
    private final StreamSpecs streamSpecs;
    private final KeyCodec keyCodec;

    /**
     * Creates a mapper with the supplied schema, stream specs and key codec.
     *
     * @param keySchema   table schema describing the message key
     * @param streamSpecs registered streams used to (de)serialize message payloads
     * @param keyCodec    codec used to (de)serialize message keys
     */
    public ExtendedMessageProtoMapper(TableSchema keySchema, StreamSpecs streamSpecs, KeyCodec keyCodec) {
        this.keySchema = keySchema;
        this.streamSpecs = streamSpecs;
        this.keyCodec = keyCodec;
    }

    /**
     * Converts a protobuf extended message to a domain {@link ExtendedMessage}.
     *
     * @param protoMessage the protobuf extended message
     * @return the domain extended message
     */
    public ExtendedMessage fromProto(TReqProcessBatch.TExtendedMessage protoMessage) {
        var streamId = streamSpecs.getStreamId(protoMessage.getMessage().getStreamSpecId());
        var stream = streamSpecs.getStream(streamId);
        if (stream == null) {
            throw new IllegalArgumentException(
                    "Unknown streamId: %s (streamSpecId: %d)"
                            .formatted(streamId, protoMessage.getMessage().getStreamSpecId())
            );
        }
        Payload key = new Payload(
                keyCodec.decode(protoMessage.getKey()),
                keySchema
        );
        if (log.isTraceEnabled()) {
            log.trace("Deserialized key: {}", key);
        }

        var payload = stream.getCodec().decode(protoMessage.getMessage().getPayload());
        if (log.isTraceEnabled()) {
            log.trace("Deserialized payload: {}", payload);
        }
        return ExtendedMessage.builder()
                .setMessageId(protoMessage.getMessage().getMessageId())
                .setEventTimestamp(protoMessage.getMessage().getEventTimestamp())
                .setSystemTimestamp(protoMessage.getMessage().getSystemTimestamp())
                .setStreamId(streamId)
                .setStreamSpecId(protoMessage.getMessage().getStreamSpecId())
                .setPayload(payload)
                .setKey(key)
                .build();
    }
}
