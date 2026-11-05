package tech.ytsaurus.flow.spring;

import java.util.List;

import javax.persistence.Entity;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.AutoConfigurationPackages;
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
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.testutils.CompanionConfigFixtures;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the annotation-based computation registration of {@link FlowAutoConfiguration}
 * (see {@link FlowComputation}, {@link FlowSourceComputation} and {@link OnFlowComponentsCondition}).
 */
class FlowAnnotationConfigurationTest {

    // The JVM of a test cannot set YT_FLOW_MODE, so the companion mode is selected by the property.
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withPropertyValues(FlowRunModeResolver.RUN_MODE_PROPERTY + "=Worker")
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
    void annotatedStreamPojoIsScannedAndRegistered() {
        contextRunner
                .withPropertyValues("flow.entity-scan-packages=tech.ytsaurus.flow.spring")
                .withUserConfiguration(ExecConfig.class, AnnotatedMapper.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
                    var stream = snapshot.getStreamContext().getStream("scanned_stream");
                    assertThat(stream).isNotNull();
                    assertThat(stream.getMessageClass()).isEqualTo(ScannedMessage.class);
                });
    }

    @Test
    void annotatedStreamPojoIsScannedFromAutoConfigurationPackagesFallback() {
        // No property set: registering AutoConfigurationPackages exercises the default fallback branch.
        contextRunner
                .withInitializer(ctx -> AutoConfigurationPackages.register(
                        (BeanDefinitionRegistry) ctx.getBeanFactory(), "tech.ytsaurus.flow.spring"))
                .withUserConfiguration(ExecConfig.class, AnnotatedMapper.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
                    assertThat(snapshot.getStreamContext().getStream("scanned_stream")).isNotNull();
                });
    }

    @Test
    void multiIdAnnotatedStreamPojoRegistersEveryId() {
        contextRunner
                .withPropertyValues("flow.entity-scan-packages=tech.ytsaurus.flow.spring")
                .withUserConfiguration(ExecConfig.class, AnnotatedMapper.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
                    assertThat(snapshot.getStreamContext().getStream("multi_a")).isNotNull();
                    assertThat(snapshot.getStreamContext().getStream("multi_b")).isNotNull();
                });
    }

    @Test
    void duplicateIdBetweenScannedPojoAndStreamBeanFailsStartup() {
        contextRunner
                .withPropertyValues("flow.entity-scan-packages=tech.ytsaurus.flow.spring")
                .withUserConfiguration(ExecConfig.class, AnnotatedMapper.class, DuplicateStreamConfig.class)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(IllegalArgumentException.class);
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

    // The scan is package-wide: stream ids must stay unique across the test classes of this package.
    @Entity
    @FlowMessage(streamIds = {"scanned_stream"})
    static class ScannedMessage {
        private String word;
        private long count;
    }

    @Entity
    @FlowMessage(streamIds = {"multi_a", "multi_b"})
    static class MultiIdMessage {
        private String word;
    }

    @Entity
    @FlowMessage(streamIds = {"dup_stream"})
    static class DuplicateMessage {
        private String word;
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

    @Configuration
    static class DuplicateStreamConfig {
        @Bean
        FlowStream<?> duplicateBeanStream() {
            FlowStream<?> stream = Mockito.mock(FlowStream.class);
            Mockito.when(stream.getStreamId()).thenReturn("dup_stream");
            return stream;
        }
    }
}
