package tech.ytsaurus.flow.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.execution.CompanionExecution;
import tech.ytsaurus.flow.execution.CompanionExecutionSpec;
import tech.ytsaurus.flow.execution.GrpcServerExecution;
import tech.ytsaurus.flow.service.CompanionService;
import tech.ytsaurus.flow.stream.FlowStream;

/**
 * Spring Boot autoconfiguration for a Flow pipeline.
 * <p>
 * This configuration is automatically applied when at least one Flow component is present in the
 * application context (see {@link OnFlowComponentsCondition}) and the required Flow classes are on
 * the classpath.
 * <p>
 * The {@link PipelineContext} is created in both modes; what surrounds it depends on
 * {@code YT_FLOW_MODE}, so one {@code @SpringBootApplication} main serves both roles:
 * <ul>
 *     <li><b>{@code Worker}</b> — the companion beans: the {@link CompanionExecutionConfig} read
 *     from the environment, the gRPC server that answers computation requests from YT Flow workers,
 *     and its {@link FlowCompanionLifecycle}.</li>
 *     <li><b>unset</b> — the runner: {@link FlowRunnerBootstrap}, which submits the pipeline and
 *     exits. Neither the gRPC server nor the monitoring HTTP server is started; their configuration
 *     comes from the worker and does not exist here.</li>
 * </ul>
 * <p>
 * To use this autoconfiguration, annotate your process functions with
 * {@link FlowComputation} / {@link FlowSourceComputation}:
 * <pre>
 * &#64;FlowComputation(id = "my_computation")
 * public class MyProcessFunction implements RowFunction {
 *     &#64;Override
 *     public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
 *         // ...
 *     }
 * }
 * </pre>
 * <p>
 * Streams are declared either as {@link FlowStream} beans or via a {@link ComputationProvider}
 * bean (see {@link ComputationProvider#getStreams()}).
 * <p>
 * Configuration properties can be set in application.yml:
 * <pre>
 * flow:
 *   server:
 *     port: 8080  # Optional dev override; production values come from YT_FLOW_COMPANION_CONFIG
 * </pre>
 *
 * @see ComputationProvider
 * @see FlowComputation
 * @see FlowSourceComputation
 * @see FlowCompanionLifecycle
 * @see FlowProperties
 */
@AutoConfiguration
@EnableConfigurationProperties(FlowProperties.class)
@ConditionalOnClass({PipelineContext.class, CompanionService.class})
@Conditional(OnFlowComponentsCondition.class)
public class FlowAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(FlowAutoConfiguration.class);

    /**
     * Creates the PipelineContext bean with registered computations collected from
     * {@link FlowComputation} / {@link FlowSourceComputation} annotated beans, and streams collected
     * from {@link ComputationProvider} beans and {@link FlowStream} beans.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans
     * @param flowStreams          provider of all {@link FlowStream} beans
     * @param beanFactory          the bean factory used to discover annotated computations
     * @param properties           the Flow properties supplying the {@code @FlowMessage} scan packages
     * @return the configured PipelineContext
     */
    @Bean
    @ConditionalOnMissingBean
    public PipelineContext pipelineContext(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory,
            FlowProperties properties
    ) {
        return FlowComponents.buildPipelineContext(
                computationProviders, flowStreams, beanFactory, properties.getEntityScanPackages());
    }

    /**
     * Creates the runner that launches the pipeline and exits, in runner mode only.
     * <p>
     * It deliberately does not take the {@link PipelineContext} bean: that one collects the
     * computations, and building them is pointless work for a launch — and a launch that fails
     * whenever a process function's dependencies are unreachable. The bootstrap collects the
     * streams itself, and only when it actually launches.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans
     * @param flowStreams          provider of all {@link FlowStream} beans
     * @param beanFactory          the bean factory used to scan for {@code @FlowMessage} POJOs
     * @param properties           the Flow properties supplying the {@code @FlowMessage} scan packages
     * @param applicationContext   the context to close before the JVM exits
     * @return the runner bootstrap.
     */
    @Bean
    @ConditionalOnMissingBean
    @Conditional(OnFlowRunnerModeCondition.class)
    public FlowRunnerBootstrap flowRunnerBootstrap(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory,
            FlowProperties properties,
            ConfigurableApplicationContext applicationContext
    ) {
        return new FlowRunnerBootstrap(
                computationProviders, flowStreams, beanFactory, properties, applicationContext);
    }

    /**
     * Creates the {@link CompanionExecutionConfig} bean from the environment.
     * {@code flow.server.port}, when set, overrides the port field (dev only).
     */
    @Bean
    @ConditionalOnMissingBean
    @Conditional(OnFlowWorkerModeCondition.class)
    public CompanionExecutionConfig companionExecutionConfig(FlowProperties properties) {
        CompanionExecutionConfig config = CompanionExecutionConfig.fromEnvironment();
        Integer portOverride = properties.getServer().getPort();
        if (portOverride != null) {
            log.info("Overriding companion port from flow.server.port property: {}", portOverride);
            config = config.withPort(portOverride);
        }
        return config;
    }

    /**
     * Creates a {@link CompanionExecution} bean if one is not already present in the application context.
     * This bean is responsible for executing gRPC server operations within the flow pipeline context.
     *
     * @param pipelineContext the pipeline context to be used by the gRPC server execution.
     * @param config          the companion execution configuration to be used by the gRPC server execution.
     * @return a new instance of {@link GrpcServerExecution}
     */
    @Bean
    @ConditionalOnMissingBean
    @Conditional(OnFlowWorkerModeCondition.class)
    public CompanionExecution grpcServerExecution(
            PipelineContext pipelineContext,
            CompanionExecutionConfig config
    ) {
        return new GrpcServerExecution(new CompanionExecutionSpec(pipelineContext).setConfig(config));
    }

    /**
     * Creates the FlowCompanionLifecycle bean that manages the gRPC server lifecycle.
     * <p>
     * This bean implements {@link org.springframework.context.SmartLifecycle} to properly
     * integrate with Spring's application lifecycle, ensuring graceful startup and shutdown.
     *
     * @param companionExecution Flow companion execution.
     * @return the FlowCompanionLifecycle instance.
     */
    @Bean
    @ConditionalOnMissingBean
    @Conditional(OnFlowWorkerModeCondition.class)
    public FlowCompanionLifecycle flowCompanionLifecycle(CompanionExecution companionExecution) {
        return new FlowCompanionLifecycle(companionExecution);
    }
}
