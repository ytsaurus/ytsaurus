package tech.ytsaurus.flow.spring;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.pipeline.SimpleRunnerProgram;
import tech.ytsaurus.flow.stream.FlowStream;

/**
 * Launches the pipeline in runner mode ({@code YT_FLOW_MODE} unset), so one Spring Boot main serves
 * both modes.
 * <p>
 * Enriches the spec from the registered streams, hands the launch to {@code flow_server} and ends
 * the JVM with its exit code — the application never continues past this runner.
 * <p>
 * A test context never launches, since a test starts application runners too. Everywhere else the
 * command line is always parsed, so a launch naming no {@code --config} fails rather than exiting
 * successfully without submitting anything. Parsing is strict: property-style options with dotted
 * keys ({@code --spring.*}, {@code --server.port=...}) are skipped, anything else unknown fails.
 * <p>
 * Only the streams are collected, never the computations: submitting a spec runs no user code, and
 * building the process functions would build everything they depend on on every launch.
 */
public class FlowRunnerBootstrap implements ApplicationRunner, Ordered {

    private static final Logger log = LoggerFactory.getLogger(FlowRunnerBootstrap.class);

    private final ObjectProvider<ComputationProvider> computationProviders;
    private final ObjectProvider<FlowStream<?>> flowStreams;
    private final ListableBeanFactory beanFactory;
    private final FlowProperties properties;
    private final ConfigurableApplicationContext applicationContext;

    /**
     * Takes the ingredients of the pipeline rather than the assembled context, so that nothing is
     * collected unless a launch actually happens.
     *
     * @param computationProviders provider of all {@link ComputationProvider} beans.
     * @param flowStreams          provider of all {@link FlowStream} beans.
     * @param beanFactory          the bean factory used to scan for message POJOs.
     * @param properties           the Flow properties supplying the scan packages.
     * @param applicationContext   the context to close before the JVM exits.
     */
    public FlowRunnerBootstrap(
            ObjectProvider<ComputationProvider> computationProviders,
            ObjectProvider<FlowStream<?>> flowStreams,
            ListableBeanFactory beanFactory,
            FlowProperties properties,
            ConfigurableApplicationContext applicationContext
    ) {
        this.computationProviders = computationProviders;
        this.flowStreams = flowStreams;
        this.beanFactory = beanFactory;
        this.properties = properties;
        this.applicationContext = applicationContext;
    }

    /**
     * Sorts after every runner with an explicit order — not a guarantee of running last, since
     * Spring breaks ties between unordered runners arbitrarily. A runner that must complete before
     * the launch, and the JVM exit that follows it, needs an explicit order.
     */
    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!properties.getRunner().isEnabled()) {
            if (startedByTestFramework()) {
                log.info("The pipeline launch is disabled by {}=false", FlowProperties.RUNNER_ENABLED_PROPERTY);
            } else {
                // Outside a test this exits successfully having submitted nothing — say so.
                log.warn(
                        "The pipeline was NOT submitted: {}=false outside a test context. Remove the "
                                + "property from the application configuration to launch.",
                        FlowProperties.RUNNER_ENABLED_PROPERTY);
            }
            return;
        }
        if (startedByTestFramework()) {
            // A test context must never launch a pipeline or end the JVM.
            log.info("Context created by a test framework, skipping the pipeline launch");
            return;
        }

        var snapshot = new PipelineContextSnapshot(buildLaunchPipeline());
        log.info(
                "Launching the pipeline as the runner; if this context belongs to a test, set {}=false",
                FlowProperties.RUNNER_ENABLED_PROPERTY);

        // Always parsed, so a launch naming no --config fails here.
        int exitCode = launch(args.getSourceArgs(), snapshot.getStreams());
        exit(exitCode);
    }

    /** Whether this context belongs to a test. Overridable so a test can force the launch branch. */
    boolean startedByTestFramework() {
        return TestFrameworkDetector.testFrameworkOnStack();
    }

    /**
     * The pipeline as the launch needs it: the declared streams and nothing else. Built here rather
     * than injected so that a context which never launches collects nothing at all.
     */
    PipelineContext buildLaunchPipeline() {
        return FlowComponents.buildRunnerPipelineContext(
                computationProviders, flowStreams, beanFactory, properties.getEntityScanPackages());
    }

    /** Submits the pipeline. Overridable so a test can drive {@link #run} without a real launch. */
    int launch(String[] args, Map<String, FlowStream<?>> streams) throws Exception {
        return SimpleRunnerProgram.runPipeline(args, streams);
    }

    /** Closes the context and ends the JVM. Overridable so a test can drive {@link #run}. */
    void exit(int exitCode) {
        System.exit(SpringApplication.exit(applicationContext, () -> exitCode));
    }
}
