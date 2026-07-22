package tech.ytsaurus.flow.spring;

import java.util.List;

import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.stream.FlowStream;

/**
 * Provider interface for Flow computations in Spring Boot applications.
 * <p>
 * Implement this interface and register it as a Spring bean to enable
 * automatic Flow pipeline configuration. The provided computations will
 * be registered with the pipeline context and served via the gRPC companion server.
 * <p>
 * Example usage:
 * <pre>
 * &#64;Configuration
 * public class MyFlowConfig implements ComputationProvider {
 *
 *     &#64;Autowired
 *     private MyProcessFunction myProcessFunction;
 *
 *     &#64;Override
 *     public List&lt;Computation&gt; getComputations() {
 *         var computation = Computation.builder()
 *                 .setComputationId("my_computation")
 *                 .setProcessFunction(myProcessFunction)
 *                 .build();
 *         return List.of(computation);
 *     }
 * }
 * </pre>
 *
 * @see FlowAutoConfiguration
 * @see Computation
 */
public interface ComputationProvider {

    /**
     * Returns the list of computations to register with the Flow pipeline.
     * <p>
     * Each computation must have a unique computation ID that matches
     * the corresponding computation defined in the YT Flow pipeline spec.
     *
     * @return List of computations to register; must not be null.
     */
    List<Computation> getComputations();

    /**
     * Returns the list of streams to register with the Flow pipeline.
     * <p>
     * Each stream must have a unique stream ID that matches
     * the corresponding stream defined in the YT Flow pipeline spec.
     *
     * @return List of streams to register; must not be null.
     */
    default List<FlowStream<?>> getStreams() {
        return List.of();
    }

}
