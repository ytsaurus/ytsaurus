package tech.ytsaurus.flow.testutils;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;

public class ComputationTestUtils {

    private ComputationTestUtils() {
    }

    public static Computation passthroughComputation(String computationId, @Nullable String outputStreamId) {
        return Computation.builder()
                .setComputationId(computationId)
                .setProcessFunction(new RowFunction() {
                    @Override
                    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
                        // Simple pass-through for testing.
                        output.addMessage(Message.builder()
                                .setStreamId(outputStreamId != null ? outputStreamId : message.getStreamId())
                                .setPayload(message.getPayload())
                                .build()
                        );
                    }
                })
                .build();
    }

    public static Computation passthroughComputation(String computationId) {
        return passthroughComputation(computationId, null);
    }

    public static SourceComputation passthroughSourceComputation(
            String computationId,
            @Nullable String outputStreamId
    ) {
        return SourceComputation.builder()
                .setComputationId(computationId)
                .setProcessFunction(new RowFunction() {
                    @Override
                    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
                        // Simple pass-through for testing.
                        output.addMessage(Message.builder()
                                .setStreamId(outputStreamId != null ? outputStreamId : message.getStreamId())
                                .setPayload(message.getPayload())
                                .build()
                        );
                    }
                })
                .build();
    }

    public static SourceComputation passthroughSourceComputation(String computationId) {
        return passthroughSourceComputation(computationId, null);
    }

}
