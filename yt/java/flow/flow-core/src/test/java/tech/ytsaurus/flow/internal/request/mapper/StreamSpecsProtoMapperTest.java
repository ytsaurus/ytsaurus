package tech.ytsaurus.flow.internal.request.mapper;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.persistence.Entity;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.rpc.TStream;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.stream.FlowStreamsContext;
import tech.ytsaurus.flow.utils.YsonUtils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class StreamSpecsProtoMapperTest {
    private static final TableSchema REGISTERED = TableSchema.builder()
            .addValue("value", ColumnValueType.STRING)
            .build();
    // A native source adds system columns that the application does not register.
    private static final TableSchema NATIVE = TableSchema.builder()
            .addValue("$row_index", ColumnValueType.INT64)
            .addValue("value", ColumnValueType.STRING)
            .build();

    private final FlowStream<Payload> raw = FlowStreams.raw("raw", REGISTERED);
    private final FlowStream<Word> typed = FlowStreams.typed("typed", Word.class);
    private final StreamSpecsProtoMapper mapper = new StreamSpecsProtoMapper(
            new FlowStreamsContext(Map.of("raw", raw, "typed", typed)));

    private static TStream stream(String streamId, long streamSpecId, TableSchema schema) {
        return TStream.newBuilder()
                .setStreamId(streamId)
                .setStreamSpecId(streamSpecId)
                .setSchema(YsonUtils.protoFromYTree(schema.toYTree()))
                .build();
    }

    @Test
    void overridesDecodeRegisteredRawStreamsWithNativeSchemas() {
        var specs = mapper.fromProtoOverrides(List.of(stream("raw", 7, NATIVE)));

        var stream = Objects.requireNonNull(specs.getStream("raw"));
        assertEquals(Payload.class, stream.getMessageClass());
        assertEquals(NATIVE.getColumns(), stream.getSchema().getColumns());
        assertEquals(7L, specs.getStreamSpecId("raw"));
        var row = new PayloadBuilder(NATIVE).set("$row_index", 42L).set("value", new byte[]{1, 2}).finish();
        var decoded = (Payload) stream.getCodec().decode(FlowStreams.raw("raw", NATIVE).getCodec().encode(row));
        assertArrayEquals(new byte[]{1, 2}, decoded.get("value", byte[].class));
    }

    @Test
    void overridesKeepRegisteredTypedStreams() {
        var specs = mapper.fromProtoOverrides(List.of(stream("typed", 1, NATIVE)));

        assertSame(typed, specs.getStream("typed"));
    }

    @Test
    void jobStreamsKeepRegisteredStreams() {
        var specs = mapper.fromProto(List.of(stream("raw", 1, NATIVE), stream("typed", 2, NATIVE)));

        assertSame(raw, specs.getStream("raw"));
        assertSame(typed, specs.getStream("typed"));
    }

    @Test
    void unknownStreamsBecomeRawStreamsWithTheirSchemas() {
        for (var specs : List.of(
                mapper.fromProto(List.of(stream("unknown", 1, NATIVE))),
                mapper.fromProtoOverrides(List.of(stream("unknown", 1, NATIVE))))) {
            var stream = Objects.requireNonNull(specs.getStream("unknown"));
            assertEquals(Payload.class, stream.getMessageClass());
            assertEquals(NATIVE.getColumns(), stream.getSchema().getColumns());
        }
    }

    @Entity
    @FlowMessage(streamIds = {"typed"})
    private static class Word {
        private String word;
    }
}
