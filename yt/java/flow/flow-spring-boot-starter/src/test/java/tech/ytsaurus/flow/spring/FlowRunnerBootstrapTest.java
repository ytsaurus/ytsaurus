package tech.ytsaurus.flow.spring;

import java.util.List;
import java.util.Map;

import javax.persistence.Entity;

import com.beust.jcommander.ParameterException;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.ResolvableType;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the runner-mode wiring of {@link FlowAutoConfiguration} (see {@link FlowRunnerBootstrap}).
 */
class FlowRunnerBootstrapTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withPropertyValues(FlowRunModeResolver.RUN_MODE_PROPERTY + "=" + FlowRunModeResolver.RUNNER_MODE)
            .withConfiguration(AutoConfigurations.of(FlowAutoConfiguration.class));

    @Test
    void bootstrapIsRegisteredWithTheDeclaredStreams() {
        contextRunner
                .withUserConfiguration(StreamConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(FlowRunnerBootstrap.class);

                    var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
                    assertThat(snapshot.getStreams()).containsKey("words");
                });
    }

    @Test
    void noBootstrapWithoutFlowComponents() {
        contextRunner.run(context -> assertThat(context).doesNotHaveBean(FlowRunnerBootstrap.class));
    }

    @Test
    void testCreatedContextLaunchesNothing() throws Exception {
        // Running under JUnit, the real detection must skip the launch whatever the arguments say.
        contextRunner
                .withUserConfiguration(StreamConfig.class)
                .run(context -> {
                    var bootstrap = context.getBean(FlowRunnerBootstrap.class);
                    assertThat(bootstrap.startedByTestFramework()).isTrue();
                    bootstrap.run(new DefaultApplicationArguments());
                    bootstrap.run(new DefaultApplicationArguments("--spring.profiles.active=test"));
                    bootstrap.run(new DefaultApplicationArguments("--config", "p.yson"));
                });
    }

    @Test
    void productionLaunchWithoutConfigFailsInsteadOfExitingSuccessfully() {
        // Outside a test even an empty command line is parsed and rejected for the missing --config.
        contextRunner
                .withUserConfiguration(StreamConfig.class)
                .run(context -> {
                    var bootstrap = productionBootstrap(context);
                    assertThatThrownBy(() -> bootstrap.run(new DefaultApplicationArguments()))
                            .isInstanceOf(ParameterException.class);
                    assertThatThrownBy(() -> bootstrap.run(
                            new DefaultApplicationArguments("--flow-bin", "/bin/true")))
                            .isInstanceOf(ParameterException.class);
                });
    }

    @Test
    void productionLaunchPassesTheArgumentsAndStreamsOn() throws Exception {
        contextRunner
                .withUserConfiguration(StreamConfig.class)
                .run(context -> {
                    var bootstrap = productionBootstrap(context);
                    bootstrap.run(new DefaultApplicationArguments("--config", "p.yson", "--flow-bin", "fs"));

                    assertThat(bootstrap.launchedArgs).containsExactly("--config", "p.yson", "--flow-bin", "fs");
                    assertThat(bootstrap.launchedStreams).containsKey("words");
                    assertThat(bootstrap.exitCode).isEqualTo(7);
                });
    }

    @Test
    void disabledRunnerLaunchesNothingEvenOutsideATest() throws Exception {
        // The escape hatch for undetected test frameworks.
        contextRunner
                .withUserConfiguration(StreamConfig.class)
                .withPropertyValues(FlowProperties.RUNNER_ENABLED_PROPERTY + "=false")
                .run(context -> {
                    var bootstrap = productionBootstrap(context);
                    bootstrap.run(new DefaultApplicationArguments("--config", "p.yson", "--flow-bin", "fs"));
                    assertThat(bootstrap.launchedArgs).isNull();
                });
    }

    @Test
    void bootstrapRunsAfterTheApplicationsOwnRunners() {
        // It ends the JVM, so it must sort after the runners of the application.
        contextRunner
                .withUserConfiguration(StreamConfig.class)
                .run(context -> assertThat(context.getBean(FlowRunnerBootstrap.class).getOrder())
                        .isEqualTo(Ordered.LOWEST_PRECEDENCE));
    }

    /** Wired like the bean, but launching and exiting are recorded instead of performed. */
    private static RecordingBootstrap productionBootstrap(AssertableApplicationContext context) {
        ConfigurableApplicationContext source = context.getSourceApplicationContext(
                ConfigurableApplicationContext.class);
        ObjectProvider<FlowStream<?>> flowStreams = source.getBeanProvider(
                ResolvableType.forClass(FlowStream.class));
        return new RecordingBootstrap(
                source.getBeanProvider(ComputationProvider.class),
                flowStreams,
                source.getBeanFactory(),
                source.getBean(FlowProperties.class),
                source);
    }

    private static class RecordingBootstrap extends FlowRunnerBootstrap {
        String @Nullable [] launchedArgs;
        @Nullable Map<String, FlowStream<?>> launchedStreams;
        @Nullable Integer exitCode;

        RecordingBootstrap(
                ObjectProvider<ComputationProvider> computationProviders,
                ObjectProvider<FlowStream<?>> flowStreams,
                ListableBeanFactory beanFactory,
                FlowProperties properties,
                ConfigurableApplicationContext applicationContext
        ) {
            super(computationProviders, flowStreams, beanFactory, properties, applicationContext);
        }

        @Override
        boolean startedByTestFramework() {
            return false;
        }

        @Override
        int launch(String[] args, Map<String, FlowStream<?>> streams) throws Exception {
            if (!List.of(args).contains("--config")) {
                // Mirror the parser contract the real launch enforces.
                return super.launch(args, streams);
            }
            this.launchedArgs = args;
            this.launchedStreams = streams;
            return 7;
        }

        @Override
        void exit(int code) {
            this.exitCode = code;
        }
    }

    @Configuration
    static class StreamConfig {
        @Bean
        ComputationProvider computationProvider() {
            return () -> List.of(FlowStreams.typed("words", Word.class));
        }
    }

    // The scan is package-wide: stream ids must stay unique across the test classes of this package.
    @Entity
    @FlowMessage(streamIds = {"words"})
    static class Word {
        private String word;
    }
}
