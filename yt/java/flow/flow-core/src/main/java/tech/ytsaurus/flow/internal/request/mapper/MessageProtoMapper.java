package tech.ytsaurus.flow.internal.request.mapper;

import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.stream.StreamSpecs;

/**
 * Mapper between protobuf {@link NYT.NFlow.NProto.Message.TMessage} and {@link Message}.
 * <p>
 * Requires {@link StreamSpecs} to resolve stream IDs and delegate payload serialization
 * to the appropriate {@link tech.ytsaurus.flow.stream.FlowStream}.
 * </p>
 */
public class MessageProtoMapper implements ProtoMapper<NYT.NFlow.NProto.Message.TMessage, Message> {

    /**
     * For protobuf mandatory attribute only.
     * The real message id will be assigned at Worker side.
     */
    private static final String FAKE_MESSAGE_ID = "1";
    private static final long UNSET_TIMESTAMP = 0L;

    private final StreamSpecs streamSpecs;

    public MessageProtoMapper(StreamSpecs streamSpecs) {
        this.streamSpecs = streamSpecs;
    }

    @Override
    public Message fromProto(NYT.NFlow.NProto.Message.TMessage protoMessage) {
        var streamId = streamSpecs.getStreamId(protoMessage.getStreamSpecId());
        var stream = streamSpecs.getStream(streamId);
        if (stream == null) {
            throw new IllegalArgumentException(
                    "Unknown streamId: %s (streamSpecId: %d)"
                            .formatted(streamId, protoMessage.getStreamSpecId())
            );
        }
        var payload = stream.getCodec().decode(protoMessage.getPayload());
        return Message.builder()
                .setMessageId(protoMessage.getMessageId())
                .setEventTimestamp(protoMessage.getEventTimestamp())
                .setSystemTimestamp(protoMessage.getSystemTimestamp())
                .setStreamId(streamId)
                .setStreamSpecId(protoMessage.getStreamSpecId())
                .setPayload(payload)
                .build();
    }

    @Override
    public NYT.NFlow.NProto.Message.TMessage toProto(Message message) {
        var messageBuilder = NYT.NFlow.NProto.Message.TMessage.newBuilder();
        var stream = streamSpecs.getStream(message.getStreamId());
        if (stream == null) {
            throw new IllegalArgumentException(
                    "Unknown streamId: %s".formatted(message.getStreamId())
            );
        }
        messageBuilder.setStreamSpecId(streamSpecs.getStreamSpecId(message.getStreamId()));
        messageBuilder.setPayload(stream.getCodec().encode(message.getPayload()));
        messageBuilder.setMessageId(FAKE_MESSAGE_ID);
        if (message.getSystemTimestamp() > 0) {
            messageBuilder.setSystemTimestamp(message.getSystemTimestamp());
        } else {
            messageBuilder.setSystemTimestamp(UNSET_TIMESTAMP);
        }
        if (message.getEventTimestamp() > 0) {
            messageBuilder.setEventTimestamp(message.getEventTimestamp());
        } else {
            messageBuilder.setEventTimestamp(UNSET_TIMESTAMP);
        }
        return messageBuilder.build();
    }
}
