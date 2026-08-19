package tech.ytsaurus.flow.spring;

import java.util.List;
import java.util.Map;

import javax.persistence.Entity;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ResolvableType;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a real runner-mode start does to the application's beans.
 *
 * <p>Uses a full {@link SpringApplicationBuilder} rather than an {@code ApplicationContextRunner},
 * which instantiates every singleton eagerly and would hide exactly what is under test here. The JVM
 * of a test has no {@code YT_FLOW_MODE}, which is runner mode.
 */
class FlowRunnerLaunchPipelineTest {

    /** Records whether the container ever built the process function or its dependency. */
    static boolean computationConstructed;
    static boolean heavyDependencyConstructed;

    private @Nullable ConfigurableApplicationContext context;

    @BeforeEach
    void resetFlags() {
        computationConstructed = false;
        heavyDependencyConstructed = false;
    }

    @AfterEach
    void closeContext() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void testContextKeepsVanillaSpringSemantics() {
        context = start();

        // The runner-mode Spring defaults must not leak into a test-created context: no lazy
        // initialization, no forced non-web context. (This context is created under JUnit, which
        // the environment post-processor detects.)
        assertThat(context.getEnvironment().getProperty("spring.main.lazy-initialization")).isNull();
        assertThat(context.getEnvironment().getProperty("spring.main.web-application-type")).isNull();
        assertThat(context.getBean(FlowRunnerBootstrap.class)).isNotNull();
    }

    @Test
    void theLaunchCollectsStreamsWithoutBuildingComputations() {
        context = start();
        var bootstrap = context.getBean(FlowRunnerBootstrap.class);
        // In a test context Spring builds singletons eagerly; the launch-time invariant is that
        // collecting the pipeline for a launch triggers no construction of its own.
        computationConstructed = false;
        heavyDependencyConstructed = false;

        // Exactly what run() feeds to the launch.
        PipelineContext launchPipeline = bootstrap.buildLaunchPipeline();

        var snapshot = new PipelineContextSnapshot(launchPipeline);
        // Both ways of declaring a stream reach the launch: the ComputationProvider bean and the
        // @FlowMessage classpath scan, which needs the auto-configuration packages of a real
        // @SpringBootApplication.
        assertThat(snapshot.getStreams()).containsKey("launch-words");
        assertThat(snapshot.getStreams()).containsKey("launch-annotated-words");
        assertThat(snapshot.getComputation("mapper")).isNull();
        // Submitting a spec must not build the process functions: a pipeline whose functions hold
        // caches or clients would load all of it on every launch, and fail the launch when those
        // dependencies are unreachable.
        assertThat(computationConstructed).isFalse();
        assertThat(heavyDependencyConstructed).isFalse();
    }

    @Test
    void theLaunchExitCodeReachesExit() throws Exception {
        context = start();
        var bootstrap = new LaunchRecordingBootstrap(context);

        bootstrap.run(new DefaultApplicationArguments("--config", "p.yson", "--flow-bin", "fs"));

        assertThat(bootstrap.exitCode).isEqualTo(11);
    }

    @Test
    void theLaunchSeesEveryStreamTheCompanionSees() {
        context = start();
        var bootstrap = context.getBean(FlowRunnerBootstrap.class);

        var launched = new PipelineContextSnapshot(bootstrap.buildLaunchPipeline());
        var companion = new PipelineContextSnapshot(context.getBean(PipelineContext.class));

        // The two collect streams the same way and must not drift apart: the spec is enriched from
        // the first, and the companion serves messages against the second.
        assertThat(launched.getStreams().keySet()).isEqualTo(companion.getStreams().keySet());
    }

    @Test
    void theCompanionPipelineStillCarriesTheComputations() {
        context = start();

        // The PipelineContext bean is unchanged — the companion, and any test autowiring it, still
        // sees the computations.
        var snapshot = new PipelineContextSnapshot(context.getBean(PipelineContext.class));
        assertThat(snapshot.getComputation("mapper")).isNotNull();
        assertThat(computationConstructed).isTrue();
    }

    private ConfigurableApplicationContext start() {
        return new SpringApplicationBuilder(App.class)
                .web(WebApplicationType.NONE)
                .run();
    }

    /** Forces the production branch and records what the launch would have received. */
    private static final class LaunchRecordingBootstrap extends FlowRunnerBootstrap {
        @Nullable Integer exitCode;

        LaunchRecordingBootstrap(ConfigurableApplicationContext context) {
            super(
                    context.getBeanProvider(ComputationProvider.class),
                    context.getBeanProvider(ResolvableType.forClass(FlowStream.class)),
                    context.getBeanFactory(),
                    context.getBean(FlowProperties.class),
                    context);
        }

        @Override
        boolean startedByTestFramework() {
            return false;
        }

        @Override
        int launch(String[] args, Map<String, FlowStream<?>> streams) {
            return 11;
        }

        @Override
        void exit(int code) {
            this.exitCode = code;
        }
    }

    @SpringBootApplication
    static class App {
        @Bean
        ComputationProvider computationProvider() {
            return () -> List.of(FlowStreams.typed("launch-words", Word.class));
        }

        @Bean
        HeavyDependency heavyDependency() {
            return new HeavyDependency();
        }

        @Bean
        MapperFunction mapperFunction(HeavyDependency heavyDependency) {
            return new MapperFunction(heavyDependency);
        }
    }

    /** Stands in for a cache, client or connection pool a process function depends on. */
    static class HeavyDependency {
        HeavyDependency() {
            heavyDependencyConstructed = true;
        }
    }

    @FlowComputation(id = "mapper")
    static class MapperFunction implements RowFunction {
        MapperFunction(HeavyDependency heavyDependency) {
            computationConstructed = true;
        }

        @Override
        public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext context) {
        }
    }

    /** Declared through the provider; deliberately not scannable, unlike {@link AnnotatedWord}. */
    @Entity
    static class Word {
        private String word;
    }

    /**
     * Declared by annotation and found by the package-wide scan. The stream id has to be unique
     * across this package: every {@code @FlowMessage} POJO here is a candidate for that scan.
     */
    @Entity
    @FlowMessage(streamIds = {"launch-annotated-words"})
    static class AnnotatedWord {
        private String word;
    }
}
