package tech.ytsaurus.flow.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
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
 * Spring Boot autoconfiguration for the Flow companion server.
 * <p>
 * This configuration is automatically applied when a {@link ComputationProvider} bean
 * is present in the application context and the required Flow classes are on the classpath.
 * It creates the necessary beans to start a gRPC companion server that handles
 * computation requests from YT Flow workers.
 * <p>
 * To use this autoconfiguration, either annotate your process functions with
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
 * or implement the {@link ComputationProvider} interface and register it as a Spring bean:
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
 * <p>
 * Both mechanisms can be combined; their computations and streams are merged.
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
     * Creates the PipelineContext bean with registered computations and streams collected from both
     * {@link ComputationProvider} beans and {@link FlowComputation} / {@link FlowSourceComputation}
     * annotated beans.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans
     * @param flowStreams          provider of all {@link FlowStream} beans
     * @param beanFactory          the bean factory used to discover annotated computations
     * @return the configured PipelineContext
     */
    @Bean
    @ConditionalOnMissingBean
    public PipelineContext pipelineContext(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory
    ) {
        return FlowComponents.buildPipelineContext(computationProviders, flowStreams, beanFactory);
    }

    /**
     * Creates the {@link CompanionExecutionConfig} bean from the environment.
     * {@code flow.server.port}, when set, overrides the port field (dev only).
     */
    @Bean
    @ConditionalOnMissingBean
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
    public FlowCompanionLifecycle flowCompanionLifecycle(CompanionExecution companionExecution) {
        return new FlowCompanionLifecycle(companionExecution);
    }
}
