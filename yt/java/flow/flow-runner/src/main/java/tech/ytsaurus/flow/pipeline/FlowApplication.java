package tech.ytsaurus.flow.pipeline;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.flow.config.FlowRunMode;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.execution.CompanionExecutionSpec;
import tech.ytsaurus.flow.execution.GrpcServerExecution;

/**
 * The entry point of a Flow pipeline binary: one {@code main} that serves the pipeline as a companion
 * or launches it as the runner, selected by {@code YT_FLOW_MODE}.
 *
 * <p>The worker exports {@code YT_FLOW_MODE=Worker} to the companion it spawns, so an unset variable
 * means the process was started by a user to launch the pipeline.
 *
 * <pre>
 * public final class PipelineMain {
 *     public static void main(String[] args) throws Exception {
 *         var context = new PipelineContext();
 *         context.registerComputation(...);
 *         context.registerTypedStreams(Word.class);
 *         FlowApplication.run(args, context);
 *     }
 * }
 * </pre>
 *
 * <p>In runner mode the registered streams supply the schemas of {@code spec.streams}, so they do
 * not have to be written into the pipeline spec by hand. The {@code main_class} of every companion
 * resource is set in the pipeline spec — normally to the same entry-point class.
 */
public final class FlowApplication {

    private static final Logger log = LoggerFactory.getLogger(FlowApplication.class);

    private FlowApplication() {
    }

    /**
     * Runs the pipeline in the mode selected by {@code YT_FLOW_MODE}.
     *
     * <p>In runner mode this terminates the JVM with the exit code of {@code flow_server} and never
     * returns; in companion mode it blocks until the server stops.
     *
     * @param args    command-line arguments from the main method.
     * @param context the pipeline: its computations serve companion requests, its streams enrich the
     *                spec.
     * @throws Exception if the pipeline fails to launch or to serve.
     */
    public static void run(String[] args, PipelineContext context) throws Exception {
        run(args, context, new EnvironmentReader());
    }

    static void run(String[] args, PipelineContext context, EnvironmentReader envReader) throws Exception {
        Optional<FlowRunMode> runMode = FlowRunMode.fromEnvironment(envReader);
        if (runMode.isEmpty()) {
            log.info("Selected runner mode");
            var snapshot = new PipelineContextSnapshot(context);
            System.exit(SimpleRunnerProgram.runPipeline(args, snapshot.getStreams()));
            return;
        }

        if (runMode.get() != FlowRunMode.Worker) {
            throw new IllegalStateException(
                    "Controller mode is not supported yet, got %s".formatted(runMode.get()));
        }

        log.info("Selected companion mode");
        var spec = new CompanionExecutionSpec(context)
                .setConfig(CompanionExecutionConfig.fromEnvironment(envReader));
        new GrpcServerExecution(spec).start();
    }
}
