package tech.ytsaurus.flow.context;

import java.util.Map;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreamsContext;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Immutable snapshot of a {@link PipelineContext}.
 *
 * <p>A snapshot is produced by passing a {@link PipelineContext} to
 * {@link #PipelineContextSnapshot(PipelineContext)} and contains a defensive copy of all
 * computations and streams that were registered at the moment of the call. The snapshot
 * is safe to publish to multiple threads (including gRPC worker threads) without any
 * synchronization: all fields are {@code final}, all internal collections are
 * unmodifiable, and no mutation API is exposed.
 *
 * <p>Read-side runtime components should depend on this type rather than the mutable {@link PipelineContext}.
 */
public final class PipelineContextSnapshot implements YTreeConvertible {

    private final Map<String, Computation> computations;
    private final FlowStreamsContext streamsContext;

    /**
     * Creates an immutable snapshot from the current state of the given {@link PipelineContext}.
     *
     * <p>The snapshot contains defensive copies of the registered computations and streams;
     * subsequent modifications to the source context do not affect the snapshot. The
     * snapshot is safe to publish to multiple threads.
     *
     * @param context the pipeline context to snapshot
     */
    public PipelineContextSnapshot(PipelineContext context) {
        this(context.getComputations(), context.getStreams());
    }

    private PipelineContextSnapshot(
            Map<String, Computation> computations,
            Map<String, FlowStream<?>> streams
    ) {
        this.computations = Map.copyOf(computations);
        this.streamsContext = new FlowStreamsContext(streams);
    }

    /**
     * Returns a computation registered in the pipeline at snapshot time.
     *
     * @param computationId the computation identifier
     * @return the computation, or {@code null} if no such computation exists in this snapshot
     */
    public @Nullable Computation getComputation(String computationId) {
        return computations.get(computationId);
    }

    /**
     * Returns the immutable streams context associated with this snapshot.
     *
     * @return the streams context
     */
    public FlowStreamsContext getStreamContext() {
        return streamsContext;
    }

    /**
     * Converts this snapshot to a YTree representation.
     *
     * @return the YTree representation
     */
    @Override
    public YTreeNode toYTree() {
        var builder = YTree.builder().beginMap()
                .key("computations")
                .beginMap();
        computations.forEach((key, value) -> builder.key(key).value(value.toYTree()));
        return builder.endMap().endMap().build();
    }
}
