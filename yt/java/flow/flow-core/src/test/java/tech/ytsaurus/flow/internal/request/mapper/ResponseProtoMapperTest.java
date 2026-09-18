package tech.ytsaurus.flow.internal.request.mapper;

import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.computation.AddMessageOptions;
import tech.ytsaurus.flow.computation.MessageIdSuffix;
import tech.ytsaurus.flow.computation.TransformResult;
import tech.ytsaurus.flow.request.ResponseContext;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.rpc.TMessageIdSuffix;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ResponseProtoMapperTest {
    private static final String STREAM_ID = "output";
    private static final StreamSpecs STREAM_SPECS = new StreamSpecs(
            StreamIdsMapping.builder().addMapping(STREAM_ID, 0L).build(),
            List.of(FlowStreams.raw(STREAM_ID, Payload.EMPTY_SCHEMA))
    );

    private final ResponseProtoMapper mapper = new ResponseProtoMapper();

    @Test
    void defaultSuffixesAreOmitted() {
        var result = new TransformResult(List.of("parent"));
        result.addMessage(message());

        var proto = mapper.toProto(response(result), STREAM_SPECS);

        assertEquals(0, proto.getOutput(0).getMessageIdSuffixesCount());
    }

    @Test
    void mixedSuffixesRoundTrip() {
        var result = mixedResult();

        var proto = mapper.toProto(response(result), STREAM_SPECS);

        var suffixes = proto.getOutput(0).getMessageIdSuffixesList();
        assertEquals(3, suffixes.size());
        assertEquals(TMessageIdSuffix.EMode.MIS_SEQUENCE_NUMBER, suffixes.get(0).getMode());
        assertEquals(TMessageIdSuffix.EMode.MIS_PAYLOAD_HASH, suffixes.get(1).getMode());
        assertEquals(TMessageIdSuffix.EMode.MIS_USER_DEFINED, suffixes.get(2).getMode());
        assertEquals(ByteString.copyFromUtf8("ключ"), suffixes.get(2).getUserDefined());

        var roundTrip = mapper.fromProto(
                proto,
                STREAM_SPECS,
                Payload.EMPTY_SCHEMA,
                GUID.create(),
                GUID.create()
        );
        var actual = roundTrip.getTransformResults().get(0).getMessageIdSuffixes();
        assertEquals(MessageIdSuffix.Mode.SEQUENCE_NUMBER, actual.get(0).getMode());
        assertEquals(MessageIdSuffix.Mode.PAYLOAD_HASH, actual.get(1).getMode());
        assertEquals(MessageIdSuffix.Mode.USER_DEFINED, actual.get(2).getMode());
        assertEquals("ключ", actual.get(2).getValue());
    }

    @Test
    void malformedSuffixCountIsRejected() {
        var proto = mapper.toProto(response(mixedResult()), STREAM_SPECS);
        var malformedGroup = proto.getOutput(0).toBuilder()
                .removeMessageIdSuffixes(2)
                .build();
        var malformed = proto.toBuilder()
                .setOutput(0, malformedGroup)
                .build();

        assertThrows(
                IllegalArgumentException.class,
                () -> mapper.fromProto(
                        malformed,
                        STREAM_SPECS,
                        Payload.EMPTY_SCHEMA,
                        GUID.create(),
                        GUID.create()
                )
        );
    }

    private static TransformResult mixedResult() {
        var result = new TransformResult(List.of("parent"));
        result.addMessage(message());
        result.addMessage(
                message(),
                AddMessageOptions.builder().setMessageIdSuffix(MessageIdSuffix.payloadHash()).build()
        );
        result.addMessage(
                message(),
                AddMessageOptions.builder().setMessageIdSuffix(MessageIdSuffix.userDefined("ключ")).build()
        );
        return result;
    }

    private static Message message() {
        return Message.builder()
                .setStreamId(STREAM_ID)
                .setPayload(Payload.EMPTY)
                .build();
    }

    private static ResponseContext response(TransformResult result) {
        return new ResponseContext(GUID.create(), GUID.create(), List.of(result), Map.of(), Map.of());
    }
}
