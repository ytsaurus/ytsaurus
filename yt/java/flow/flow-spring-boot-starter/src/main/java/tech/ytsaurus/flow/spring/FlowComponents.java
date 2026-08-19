package tech.ytsaurus.flow.spring;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ClassUtils;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.MetricsContext;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.function.ProcessFunction;
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreamAnnotations;

/**
 * Collects Flow computations and streams from a Spring application context into a
 * {@link PipelineContext}.
 * <p>
 * Computations come from {@link FlowComputation}/{@link FlowSourceComputation} annotated beans;
 * streams from {@link ComputationProvider} beans, {@link FlowStream} beans, and {@link FlowMessage}
 * POJOs found by classpath scan. Duplicate ids are rejected by {@link PipelineContext}.
 *
 * @see FlowAutoConfiguration
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
     * @param beanFactory          the bean factory used to discover annotated computations and to scan
     *                             for {@link FlowMessage}-annotated message POJOs.
     * @param scanPackages         additional packages to scan for {@link FlowMessage} POJOs, on top
     *                             of the Spring Boot auto-configuration packages.
     * @return the populated pipeline context.
     */
    public static PipelineContext buildPipelineContext(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory,
            List<String> scanPackages
    ) {
        var context = new PipelineContext(collectComputations(beanFactory));
        context.registerStreams(collectStreams(computationProviders, flowStreams, beanFactory, scanPackages));
        return context;
    }

    /**
     * Builds a {@link PipelineContext} populated with all computations and streams discovered in the
     * application context, binding the supplied {@link MetricsContext}.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans.
     * @param flowStreams          provider of all {@link FlowStream} beans.
     * @param beanFactory          the bean factory used to discover annotated computations and to scan
     *                             for {@link FlowMessage}-annotated message POJOs.
     * @param scanPackages         additional packages to scan for {@link FlowMessage} POJOs, on top
     *                             of the Spring Boot auto-configuration packages.
     * @param metricsContext       the metrics context to bind.
     * @return the populated pipeline context.
     */
    public static PipelineContext buildPipelineContext(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory,
            List<String> scanPackages,
            MetricsContext metricsContext
    ) {
        var context = new PipelineContext(collectComputations(beanFactory), metricsContext);
        context.registerStreams(collectStreams(computationProviders, flowStreams, beanFactory, scanPackages));
        return context;
    }

    /**
     * Builds a {@link PipelineContext} holding only the streams of the pipeline, for the runner.
     * <p>
     * The runner needs the stream schemas to enrich the spec but never invokes user code, so the
     * {@link FlowComputation}/{@link FlowSourceComputation} beans are deliberately not looked up.
     * Looking them up would instantiate them and everything they depend on — a pipeline whose
     * process functions hold caches, clients or connection pools would build all of it just to
     * submit a spec, and a launch would start failing whenever those dependencies are unreachable.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans.
     * @param flowStreams          provider of all {@link FlowStream} beans.
     * @param beanFactory          the bean factory used to scan for {@link FlowMessage} POJOs.
     * @param scanPackages         additional packages to scan for {@link FlowMessage} POJOs, on top
     *                             of the Spring Boot auto-configuration packages.
     * @return a pipeline context holding the declared streams and no computations.
     */
    public static PipelineContext buildRunnerPipelineContext(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory,
            List<String> scanPackages
    ) {
        var context = new PipelineContext();
        context.registerStreams(collectStreams(computationProviders, flowStreams, beanFactory, scanPackages));
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
     * Collects all streams declared in the application context, from {@link ComputationProvider}
     * beans, {@link FlowStream} beans, and {@link FlowMessage}-annotated message POJOs discovered by
     * classpath scan.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans.
     * @param flowStreams          provider of all {@link FlowStream} beans.
     * @param beanFactory          the bean factory used to scan for {@link FlowMessage} POJOs.
     * @param scanPackages         additional packages to scan for {@link FlowMessage} POJOs, on top
     *                             of the Spring Boot auto-configuration packages.
     * @return the merged list of streams.
     */
    public static List<FlowStream<?>> collectStreams(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory,
            List<String> scanPackages
    ) {
        List<FlowStream<?>> streams = new ArrayList<>();
        computationProviders.forEach(provider -> streams.addAll(provider.getStreams()));
        flowStreams.forEach(streams::add);
        streams.addAll(collectAnnotatedStreams(beanFactory, scanPackages));
        return streams;
    }

    /**
     * Discovers {@link FlowMessage}-annotated POJOs by classpath scan and builds their typed streams.
     * <p>
     * Scans {@code scanPackages} together with the Spring Boot auto-configuration packages (when
     * available), de-duplicating by class name; returns nothing when neither yields a package.
     *
     * @param beanFactory  the bean factory carrying the auto-configuration packages.
     * @param scanPackages additional packages to scan on top of the auto-configuration packages.
     * @return the typed streams derived from every discovered {@link FlowMessage} POJO.
     */
    public static List<FlowStream<?>> collectAnnotatedStreams(
            ListableBeanFactory beanFactory,
            List<String> scanPackages
    ) {
        Set<String> basePackages = new LinkedHashSet<>();
        for (String scanPackage : scanPackages) {
            if (scanPackage != null && !scanPackage.isBlank()) {
                basePackages.add(scanPackage.trim());
            }
        }
        if (AutoConfigurationPackages.has(beanFactory)) {
            basePackages.addAll(AutoConfigurationPackages.get(beanFactory));
        }
        if (basePackages.isEmpty()) {
            return List.of();
        }

        var scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(FlowMessage.class));

        Set<String> seenClasses = new LinkedHashSet<>();
        List<Class<?>> messageClasses = new ArrayList<>();
        for (String basePackage : basePackages) {
            for (BeanDefinition candidate : scanner.findCandidateComponents(basePackage)) {
                String className = candidate.getBeanClassName();
                if (className != null && seenClasses.add(className)) {
                    messageClasses.add(ClassUtils.resolveClassName(className, null));
                }
            }
        }
        return FlowStreamAnnotations.fromAnnotatedClasses(messageClasses);
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
