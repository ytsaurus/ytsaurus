package tech.ytsaurus.flow.spring;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.ComputationType;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.testutils.CompanionConfigFixtures;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the annotation-based computation registration of {@link FlowAutoConfiguration}
 * (see {@link FlowComputation}, {@link FlowSourceComputation} and {@link OnFlowComponentsCondition}).
 */
class FlowAnnotationConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(FlowAutoConfiguration.class));

    @Test
    void autoConfigurationAppliedWithAnnotatedComputations() {
        contextRunner
                .withUserConfiguration(ExecConfig.class, AnnotatedMapper.class, AnnotatedReader.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasBean("flowCompanionLifecycle");
                    assertThat(context).hasBean("pipelineContext");

                    var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
                    Computation mapper = snapshot.getComputation("annotated_mapper");
                    Computation reader = snapshot.getComputation("annotated_reader");
                    assertThat(mapper).isNotNull();
                    assertThat(mapper.getComputationType()).isEqualTo(ComputationType.Transform);
                    assertThat(reader).isNotNull();
                    assertThat(reader.getComputationType()).isEqualTo(ComputationType.Source);
                });
    }

    @Test
    void streamBeansAreRegistered() {
        contextRunner
                .withUserConfiguration(ExecConfig.class, AnnotatedMapper.class, StreamConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
                    assertThat(snapshot.getStreamContext().getStream("bean_stream")).isNotNull();
                });
    }

    @Test
    void streamsFromComputationProviderAreRegistered() {
        contextRunner
                .withUserConfiguration(ExecConfig.class, AnnotatedMapper.class, StreamProviderConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
                    assertThat(snapshot.getStreamContext().getStream("provider_stream")).isNotNull();
                });
    }

    @Test
    void annotatedBeanNotImplementingProcessFunctionFailsStartup() {
        contextRunner
                .withUserConfiguration(ExecConfig.class, NotAProcessFunction.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(IllegalStateException.class);
                });
    }

    @FlowComputation(id = "annotated_mapper")
    static class AnnotatedMapper implements RowFunction {
        @Override
        public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        }
    }

    @FlowSourceComputation(id = "annotated_reader")
    static class AnnotatedReader implements RowFunction {
        @Override
        public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        }
    }

    @FlowComputation(id = "not_a_function")
    static class NotAProcessFunction {
    }

    @Configuration
    static class ExecConfig {
        @Bean
        CompanionExecutionConfig companionExecutionConfig() {
            return CompanionConfigFixtures.defaults();
        }
    }

    @Configuration
    static class StreamConfig {
        @Bean
        FlowStream<?> beanStream() {
            FlowStream<?> stream = Mockito.mock(FlowStream.class);
            Mockito.when(stream.getStreamId()).thenReturn("bean_stream");
            return stream;
        }
    }

    @Configuration
    static class StreamProviderConfig {
        @Bean
        ComputationProvider streamProvider() {
            FlowStream<?> stream = Mockito.mock(FlowStream.class);
            Mockito.when(stream.getStreamId()).thenReturn("provider_stream");
            return () -> List.of(stream);
        }
    }
}
