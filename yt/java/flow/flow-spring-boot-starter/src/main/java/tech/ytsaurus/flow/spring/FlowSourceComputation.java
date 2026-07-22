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
 * {@link tech.ytsaurus.flow.function.BatchFunction}) as a source computation of a Flow pipeline.
 * <p>
 * This annotation behaves exactly like {@link FlowComputation} but produces a
 * {@link tech.ytsaurus.flow.computation.SourceComputation} instead of a
 * {@link tech.ytsaurus.flow.computation.Computation}, i.e. the computation type reported to the
 * worker is {@code Source} rather than {@code Transform}. Like {@link FlowComputation}, it is
 * meta-annotated with {@link Component}, so the annotated class becomes a Spring bean on
 * component scan.
 * <p>
 * Example usage:
 * <pre>
 * &#64;FlowSourceComputation(id = "my_source")
 * public class MyReaderFunction implements RowFunction {
 *     &#64;Override
 *     public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
 *         // ...
 *     }
 * }
 * </pre>
 *
 * @see FlowComputation
 * @see tech.ytsaurus.flow.computation.SourceComputation
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface FlowSourceComputation {

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
