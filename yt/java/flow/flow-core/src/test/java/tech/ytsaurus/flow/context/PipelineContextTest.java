package tech.ytsaurus.flow.context;

import java.util.List;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.function.BatchFunction;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PipelineContextTest {

    @Test
    public void testYTree() {
        var expected = YTreeTextSerializer.deserialize("""
                {
                    computations={
                        "computation_id_1"={
                            computation_id="computation_id_1";
                            computation_type="Source";
                        };
                        "computation_id_2"={
                            computation_id="computation_id_2";
                            computation_type="Source";
                        };
                        "computation_id_3"={
                            computation_id="computation_id_3";
                            computation_type="Transform";
                        };
                    };
                }""".stripIndent());
        var context = new PipelineContext();
        var sourceComputation = SourceComputation.builder()
                .setComputationId("computation_id_1")
                .setProcessFunction((BatchFunction) (messages, output, ctx) -> {
                    // Batch passthrough.
                    messages.forEach(output::addMessage);
                })
                .build();
        context.registerComputation(sourceComputation);
        var rowSourceComputation = SourceComputation.builder()
                .setComputationId("computation_id_2")
                .setProcessFunction((RowFunction) (message, output, ctx) -> {
                    // Passthrough.
                    output.addMessage(message);
                })
                .build();
        context.registerComputation(rowSourceComputation);
        var transformComputation = Computation.builder()
                .setComputationId("computation_id_3")
                .setProcessFunction((RowFunction) (message, output, ctx) -> {
                    // Passthrough.
                    output.addMessage(message);
                })
                .build();
        context.registerComputation(transformComputation);
        var expectedComputationsMap = YTreeTextSerializer.stableSerialize(expected);
        var actualComputationsMap = YTreeTextSerializer.stableSerialize(new PipelineContextSnapshot(context).toYTree());
        assertEquals(expectedComputationsMap, actualComputationsMap);
    }

    @Test
    public void nullProcessFunctionIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> SourceComputation.builder()
                .setComputationId("source")
                .build());
        assertThrows(IllegalArgumentException.class, () -> Computation.builder()
                .setComputationId("transform")
                .build());
    }

    @Test
    public void registerStreamsRegistersEveryStream() {
        var context = new PipelineContext();
        var schema = TableSchema.builder()
                .addValue("value", TiType.string())
                .build();

        context.registerStreams(List.of(
                FlowStreams.raw("stream_id_1", schema),
                FlowStreams.raw("stream_id_2", schema)
        ));

        assertEquals(2, context.getStreams().size());
        assertEquals("stream_id_1", context.getStreams().get("stream_id_1").getStreamId());
        assertEquals("stream_id_2", context.getStreams().get("stream_id_2").getStreamId());
    }

    @Test
    public void registerStreamsRejectsDuplicateId() {
        var context = new PipelineContext();
        var schema = TableSchema.builder()
                .addValue("value", TiType.string())
                .build();
        context.registerStream(FlowStreams.raw("stream_id_1", schema));

        assertThrows(IllegalArgumentException.class, () -> context.registerStreams(List.of(
                FlowStreams.raw("stream_id_1", schema)
        )));
    }

    @Test
    public void snapshotDoesNotObserveLaterContextChanges() {
        var context = new PipelineContext();
        var schema = TableSchema.builder()
                .addValue("value", TiType.string())
                .build();
        context.registerComputation(Computation.builder()
                .setComputationId("computation_id_1")
                .setProcessFunction((RowFunction) (message, output, ctx) -> output.addMessage(message))
                .build());
        context.registerStream(FlowStreams.raw("stream_id_1", schema));

        var snapshot = new PipelineContextSnapshot(context);

        context.registerComputation(Computation.builder()
                .setComputationId("computation_id_2")
                .setProcessFunction((RowFunction) (message, output, ctx) -> output.addMessage(message))
                .build());
        context.registerStream(FlowStreams.raw("stream_id_2", schema));

        assertNull(snapshot.getComputation("computation_id_2"));
        assertNull(snapshot.getStreamContext().getStream("stream_id_2"));
    }
}
