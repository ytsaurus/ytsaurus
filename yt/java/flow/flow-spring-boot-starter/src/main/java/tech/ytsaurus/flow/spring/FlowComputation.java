package tech.ytsaurus.flow.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.stereotype.Component;

/**
 * Marks a {@link tech.ytsaurus.flow.function.ProcessFunction} (a
 * {@link tech.ytsaurus.flow.function.RowFunction} or
 * {@link tech.ytsaurus.flow.function.BatchFunction}) as a transform computation of a Flow pipeline.
 * <p>
 * This annotation is meta-annotated with {@link Component}, so the annotated class becomes a Spring
 * bean on component scan without any additional stereotype. At startup the autoconfiguration
 * collects every annotated bean, wraps it in a {@link tech.ytsaurus.flow.computation.Computation}
 * with the supplied id and registers it with the pipeline context.
 * <p>
 * Example usage:
 * <pre>
 * &#64;FlowComputation(id = "my_computation")
 * public class MyProcessFunction implements RowFunction {
 *     &#64;Override
 *     public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
 *         // ...
 *     }
 * }
 * </pre>
 *
 * @see FlowSourceComputation
 * @see tech.ytsaurus.flow.computation.Computation
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface FlowComputation {

    /**
     * The computation id from the pipeline static spec.
     * <p>
     * Must be unique across all computations and must match the corresponding computation defined
     * in the YT Flow pipeline spec.
     *
     * @return the computation id.
     */
    String id();
}
