package tech.ytsaurus.flow.spring;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.MetricsContext;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.function.ProcessFunction;
import tech.ytsaurus.flow.stream.FlowStream;

/**
 * Collects Flow computations and streams from a Spring application context and assembles a
 * {@link PipelineContext}.
 * <p>
 * Computations are gathered from every bean annotated with {@link FlowComputation} or
 * {@link FlowSourceComputation}. Streams are gathered from every {@link ComputationProvider} bean
 * (via {@link ComputationProvider#getStreams()}) and from every {@link FlowStream} bean declared in
 * the context.
 * <p>
 * {@link ComputationProvider} and {@link FlowStream} beans are supplied as {@link ObjectProvider}s
 * (the idiomatic way to inject all beans of a type), while the annotated computations are discovered
 * through the {@link ListableBeanFactory} — there is no bean type to inject for them, since the
 * computation id and source/transform kind live in the annotation attributes.
 * <p>
 * Duplicate computation or stream ids across any of these sources are rejected by
 * {@link PipelineContext} with an {@link IllegalArgumentException}.
 *
 * @see FlowAutoConfiguration
 * @see FlowComputation
 */
public final class FlowComponents {

    private FlowComponents() {
    }

    /**
     * Builds a {@link PipelineContext} populated with all computations and streams discovered in the
     * application context.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans.
     * @param flowStreams          provider of all {@link FlowStream} beans.
     * @param beanFactory          the bean factory used to discover annotated computations.
     * @return the populated pipeline context.
     */
    public static PipelineContext buildPipelineContext(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory
    ) {
        var context = new PipelineContext(collectComputations(beanFactory));
        context.registerStreams(collectStreams(computationProviders, flowStreams));
        return context;
    }

    /**
     * Builds a {@link PipelineContext} populated with all computations and streams discovered in the
     * application context, binding the supplied {@link MetricsContext}.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans.
     * @param flowStreams          provider of all {@link FlowStream} beans.
     * @param beanFactory          the bean factory used to discover annotated computations.
     * @param metricsContext       the metrics context to bind.
     * @return the populated pipeline context.
     */
    public static PipelineContext buildPipelineContext(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory,
            MetricsContext metricsContext
    ) {
        var context = new PipelineContext(collectComputations(beanFactory), metricsContext);
        context.registerStreams(collectStreams(computationProviders, flowStreams));
        return context;
    }

    /**
     * Collects all computations declared in the application context via {@link FlowComputation} and
     * {@link FlowSourceComputation} annotated beans.
     *
     * @param beanFactory the bean factory used to discover annotated computations.
     * @return the list of computations.
     */
    public static List<Computation> collectComputations(ListableBeanFactory beanFactory) {
        List<Computation> computations = new ArrayList<>();
        beanFactory.getBeansWithAnnotation(FlowComputation.class).forEach((beanName, bean) -> {
            FlowComputation annotation = beanFactory.findAnnotationOnBean(beanName, FlowComputation.class);
            computations.add(Computation.builder()
                    .setComputationId(annotation.id())
                    .setProcessFunction(requireProcessFunction(beanName, bean, FlowComputation.class))
                    .build());
        });
        beanFactory.getBeansWithAnnotation(FlowSourceComputation.class).forEach((beanName, bean) -> {
            FlowSourceComputation annotation =
                    beanFactory.findAnnotationOnBean(beanName, FlowSourceComputation.class);
            computations.add(SourceComputation.builder()
                    .setComputationId(annotation.id())
                    .setProcessFunction(requireProcessFunction(beanName, bean, FlowSourceComputation.class))
                    .build());
        });
        return computations;
    }

    /**
     * Collects all streams declared in the application context, from both
     * {@link ComputationProvider} beans and {@link FlowStream} beans.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans.
     * @param flowStreams          provider of all {@link FlowStream} beans.
     * @return the merged list of streams.
     */
    public static List<FlowStream<?>> collectStreams(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams
    ) {
        List<FlowStream<?>> streams = new ArrayList<>();
        computationProviders.forEach(provider -> streams.addAll(provider.getStreams()));
        flowStreams.forEach(streams::add);
        return streams;
    }

    private static ProcessFunction<?> requireProcessFunction(
            String beanName,
            Object bean,
            Class<? extends Annotation> annotationType
    ) {
        if (!(bean instanceof ProcessFunction<?> processFunction)) {
            throw new IllegalStateException(
                    "Bean '" + beanName + "' annotated with @" + annotationType.getSimpleName()
                            + " must implement RowFunction or BatchFunction, but was "
                            + bean.getClass().getName());
        }
        return processFunction;
    }
}
