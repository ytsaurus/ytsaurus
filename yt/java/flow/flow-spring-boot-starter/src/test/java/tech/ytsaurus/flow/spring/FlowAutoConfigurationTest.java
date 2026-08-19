package tech.ytsaurus.flow.spring;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.execution.CompanionExecution;
import tech.ytsaurus.flow.execution.CompanionExecutionSpec;
import tech.ytsaurus.flow.execution.GrpcServerExecution;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.testutils.CompanionConfigFixtures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlowAutoConfiguration}.
 */
class FlowAutoConfigurationTest {

    // The JVM of a test cannot set YT_FLOW_MODE, so the mode is selected by the property.
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withPropertyValues(FlowRunModeResolver.RUN_MODE_PROPERTY + "=Worker")
            .withConfiguration(AutoConfigurations.of(FlowAutoConfiguration.class));

    private final ApplicationContextRunner runnerModeContextRunner = new ApplicationContextRunner()
            .withPropertyValues(FlowRunModeResolver.RUN_MODE_PROPERTY + "=" + FlowRunModeResolver.RUNNER_MODE)
            .withConfiguration(AutoConfigurations.of(FlowAutoConfiguration.class));

    @Test
    void autoConfigurationNotAppliedWithoutComputationProvider() {
        contextRunner.run(context -> {
            assertFalse(context.containsBean("flowCompanionLifecycle"));
            assertFalse(context.containsBean("pipelineContext"));
        });
    }

    @Test
    void runnerModeStartsNoCompanionServer() {
        runnerModeContextRunner
                .withUserConfiguration(TestComputationProviderConfig.class)
                .run(context -> {
                    assertFalse(context.containsBean("flowCompanionLifecycle"));
                    assertFalse(context.containsBean("grpcServerExecution"));
                    assertTrue(context.containsBean("flowRunnerBootstrap"));
                    // The launch needs the pipeline: its streams enrich the spec.
                    assertTrue(context.containsBean("pipelineContext"));
                    assertNotNull(context.getBean(PipelineContext.class));
                });
    }

    @Test
    void runnerModeRegistersTheSamePipelineAsTheCompanion() {
        // The pipeline context is mode-independent so that a test, which runs with no YT_FLOW_MODE,
        // sees the computations it declared.
        runnerModeContextRunner
                .withUserConfiguration(RecordingComputationConfig.class)
                .run(context -> {
                    var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
                    assertNotNull(snapshot.getComputation("recording"));
                });
    }

    @Test
    void workerModeStartsNoRunner() {
        contextRunner
                .withUserConfiguration(RecordingComputationConfig.class, CompanionConfig.class)
                .run(context -> {
                    assertFalse(context.containsBean("flowRunnerBootstrap"));
                    var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
                    assertNotNull(snapshot.getComputation("recording"));
                });
    }

    @Test
    void controllerModeFailsStartupInsteadOfIdling() {
        // Neither the runner nor the companion serves Controller; without this, keep-alive would
        // leave an empty process running forever. The non-Spring FlowApplication throws too.
        new ApplicationContextRunner()
                .withPropertyValues(FlowRunModeResolver.RUN_MODE_PROPERTY + "=Controller")
                .withConfiguration(AutoConfigurations.of(FlowAutoConfiguration.class))
                .withUserConfiguration(TestComputationProviderConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasStackTraceContaining("Controller mode is not supported");
                });
    }

    @Test
    void autoConfigurationAppliedWithComputationProvider() {
        contextRunner
                .withUserConfiguration(TestComputationProviderConfig.class)
                .run(context -> {
                    assertTrue(context.containsBean("flowCompanionLifecycle"));
                    assertTrue(context.containsBean("pipelineContext"));
                    assertTrue(context.containsBean("companionExecutionConfig"));
                    assertNotNull(context.getBean(FlowCompanionLifecycle.class));
                    assertNotNull(context.getBean(PipelineContext.class));
                    assertNotNull(context.getBean(FlowProperties.class));
                });
    }

    @Test
    void customPipelineContextBeanTakesPrecedence() {
        contextRunner
                .withUserConfiguration(
                        TestComputationProviderConfig.class,
                        CustomPipelineContextConfig.class
                )
                .run(context -> {
                    assertTrue(context.containsBean("pipelineContext"));
                    PipelineContext pipelineContext = context.getBean(PipelineContext.class);
                    assertNotNull(pipelineContext);
                });
    }

    @Test
    void customFlowCompanionLifecycleBeanTakesPrecedence() {
        contextRunner
                .withUserConfiguration(
                        TestComputationProviderConfig.class,
                        CustomFlowCompanionLifecycleConfig.class
                )
                .run(context -> {
                    assertTrue(context.containsBean("flowCompanionLifecycle"));
                    FlowCompanionLifecycle lifecycle = context.getBean(FlowCompanionLifecycle.class);
                    assertNotNull(lifecycle);
                });
    }

    @Configuration
    static class TestComputationProviderConfig {
        @Bean
        ComputationProvider computationProvider() {
            return Collections::emptyList;
        }

        @Bean
        CompanionExecutionConfig companionExecutionConfig() {
            return CompanionConfigFixtures.defaults();
        }
    }

    @Configuration
    static class CustomPipelineContextConfig {
        @Bean
        PipelineContext pipelineContext() {
            return new PipelineContext();
        }
    }

    @Configuration
    static class CustomGrpcExecutionConfig {
        @Bean
        CompanionExecution grpcServerExecution(
                PipelineContext pipelineContext,
                CompanionExecutionConfig config) {
            return new GrpcServerExecution(new CompanionExecutionSpec(pipelineContext).setConfig(config));
        }
    }

    @Configuration
    static class CustomFlowCompanionLifecycleConfig {
        @Bean
        FlowCompanionLifecycle flowCompanionLifecycle(CompanionExecution companionExecution) {
            return new FlowCompanionLifecycle(companionExecution);
        }
    }

    @Configuration
    static class RecordingComputationConfig {
        @Bean
        RecordingComputation recordingComputation() {
            return new RecordingComputation();
        }
    }

    @Configuration
    static class CompanionConfig {
        @Bean
        CompanionExecutionConfig companionExecutionConfig() {
            return CompanionConfigFixtures.defaults();
        }
    }

    @FlowComputation(id = "recording")
    static class RecordingComputation implements RowFunction {
        @Override
        public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext context) {
        }
    }
}
