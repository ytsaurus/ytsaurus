package tech.ytsaurus.flow.spring;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.execution.CompanionExecution;
import tech.ytsaurus.flow.execution.CompanionExecutionSpec;
import tech.ytsaurus.flow.execution.GrpcServerExecution;
import tech.ytsaurus.flow.testutils.CompanionConfigFixtures;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlowAutoConfiguration}.
 */
class FlowAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(FlowAutoConfiguration.class));

    @Test
    void autoConfigurationNotAppliedWithoutComputationProvider() {
        contextRunner.run(context -> {
            assertFalse(context.containsBean("flowCompanionLifecycle"));
            assertFalse(context.containsBean("pipelineContext"));
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
}
