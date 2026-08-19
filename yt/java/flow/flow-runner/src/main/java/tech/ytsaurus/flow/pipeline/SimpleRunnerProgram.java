package tech.ytsaurus.flow.pipeline;

import java.util.Arrays;
import java.util.Map;

import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.stream.FlowStream;

/** Default pipeline runner implementation. */
public class SimpleRunnerProgram {
    private static final Logger log = LoggerFactory.getLogger(SimpleRunnerProgram.class);

    /**
     * Runs a Flow pipeline.
     * <p>
     * Prefer {@link FlowApplication#run(String[], tech.ytsaurus.flow.context.PipelineContext)}: it
     * selects the runner or the companion by {@code YT_FLOW_MODE} and derives the stream schemas
     * from the registered pipeline. This method always launches the runner and leaves the spec as
     * written.
     * <p>
     * Usage code example:
     * <pre>
     * public class PipelineMain {
     *     public static void main(String[] args) throws Exception {
     *         FlowApplication.run(args, context);
     *     }
     * }
     * </pre>
     *
     * <p>
     * Command line example:
     * <pre>
     * java -cp ./lib com.example.PipelineMain --config ./pipeline.yson --flow-bin ./flow_server
     * </pre>
     * <p>
     * The runner enriches the spec and hands the launch off to flow_server, which sets the spec.
     * <p>
     * This method never returns: it terminates the JVM with the exit code of {@code flow_server}
     * (or {@code 0} after printing usage for {@code --help}). An embedding {@code main} that has to
     * continue afterwards must use {@link #runPipeline(String[], Map)}.
     *
     * @param args Command-line arguments from main method.
     * @throws Exception If an error occurs during pipeline execution or initialization.
     */
    public static void runPipeline(String[] args) throws Exception {
        System.exit(runPipeline(args, Map.of()));
    }

    /**
     * Runs a Flow pipeline, writing the schemas of the registered streams into the spec before the
     * launch.
     * <p>
     * Parsing is strict — an unknown flag such as {@code --conifg} fails the launch — with one
     * exemption: property-style options with a dotted key ({@code --spring.profiles.active=p},
     * {@code --server.port=8081}) are Spring Boot's, mean nothing to the launch, and are skipped.
     * Dotless Spring toggles ({@code --debug}, {@code --trace}) are not accepted on the runner
     * command line; pass them as system properties or environment variables instead.
     *
     * @param args    command-line arguments from the main method.
     * @param streams streams registered by the pipeline, keyed by stream id.
     * @return the exit code of {@code flow_server}, or {@code 0} when only the usage was printed.
     * @throws Exception if an error occurs during pipeline execution or initialization.
     */
    public static int runPipeline(String[] args, Map<String, FlowStream<?>> streams) throws Exception {
        log.info("Starting runner execution");
        var arguments = new FlowCliArguments();
        var commander = JCommander.newBuilder()
                .addObject(arguments)
                .build();
        // Parsing is strict — a typo like --conifg fails here rather than being silently dropped.
        // Only property-style options are exempt: a Spring Boot application receives arbitrary
        // --key.subkey=value arguments on the same command line, and they mean nothing to the
        // launch. A dotted key cannot collide with a runner flag, so typo detection is unaffected.
        commander.parse(dropPropertyStyleOptions(args));
        if (arguments.isHelp()) {
            commander.usage();
            return 0;
        }

        // The runner enriches the spec and hands the launch off to flow_server, which sets the spec.
        log.info("Launching via flow_server (FlowBin: {})", arguments.getFlowBin());
        return new FlowLauncher().launch(
                arguments.getConfigPath(),
                arguments.getFlowBin(),
                streams,
                arguments.getFlowServerFlags());
    }

    private static String[] dropPropertyStyleOptions(String[] args) {
        return Arrays.stream(args)
                .filter(arg -> !isPropertyStyleOption(arg))
                .toArray(String[]::new);
    }

    /**
     * Whether the argument is a Spring Boot property option: {@code --key=value} or {@code --key}
     * where the key contains a dot. Every runner flag is dotless, so the two sets cannot overlap.
     */
    private static boolean isPropertyStyleOption(String arg) {
        if (!arg.startsWith("--")) {
            return false;
        }
        String key = arg.substring(2);
        int eq = key.indexOf('=');
        if (eq >= 0) {
            key = key.substring(0, eq);
        }
        return key.indexOf('.') >= 0;
    }
}
