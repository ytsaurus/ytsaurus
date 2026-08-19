package tech.ytsaurus.flow.spring;

import java.util.List;

import tech.ytsaurus.flow.stream.FlowStream;

/**
 * Provider interface for Flow streams in Spring Boot applications.
 * <p>
 * Implement this interface and register it as a Spring bean to declare the streams of a Flow
 * pipeline imperatively in one place. The provided streams are registered with the pipeline
 * context and served via the gRPC companion server.
 * <p>
 * Computations are registered separately, by annotating their process functions with
 * {@link FlowComputation} / {@link FlowSourceComputation}. Streams may also be declared as
 * individual {@link FlowStream} beans instead of implementing this interface.
 * <p>
 * Example usage:
 * <pre>
 * &#64;Configuration
 * public class MyFlowConfig implements ComputationProvider {
 *
 *     &#64;Override
 *     public List&lt;FlowStream&lt;?&gt;&gt; getStreams() {
 *         return List.of(FlowStreams.typed("words", Word.class));
 *     }
 * }
 * </pre>
 *
 * @see FlowAutoConfiguration
 * @see FlowComputation
 * @see FlowStream
 */
public interface ComputationProvider {

    /**
     * Returns the list of streams to register with the Flow pipeline.
     * <p>
     * Each stream must have a unique stream ID that matches
     * the corresponding stream defined in the YT Flow pipeline spec.
     *
     * @return List of streams to register; must not be null.
     */
    List<FlowStream<?>> getStreams();

}
