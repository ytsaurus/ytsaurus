package tech.ytsaurus.flow.pipeline;

import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default pipline runner implementation.
 * Java equivalent of the NYT::NFlow::TSimpleRunnerProgram.
 */
public class SimpleRunnerProgram {
    private static final Logger log = LoggerFactory.getLogger(SimpleRunnerProgram.class);

    /**
     * Runs a Flow pipeline.
     * <p>
     * Usage code example:
     * <pre>
     * public class RunnerMain() {
     *      public static void main(String[] args) throws Exception {
     *          SimpleRunnerProgram.runPipeline(args);
     *      }
     * }
     * </pre>
     *
     * <p>
     * Command line example:
     * <pre>
     * java -cp ./lib RunnerMain --config ./pipeline.yson --flow-bin ./flow_server
     * </pre>
     * <p>
     * The runner enriches the spec and hands the launch off to flow_server, which sets the spec.
     *
     * @param args Command-line arguments from main method.
     * @throws Exception If an error occurs during pipeline execution or initialization.
     */
    public static void runPipeline(String[] args) throws Exception {
        log.info("Starting runner execution");
        var arguments = new FlowCliArguments();
        var commander = JCommander.newBuilder()
                .addObject(arguments)
                .build();
        commander.parse(args);
        if (arguments.isHelp()) {
            commander.usage();
            return;
        }

        // The runner enriches the spec and hands the launch off to flow_server, which sets the spec.
        log.info("Launching via flow_server (FlowBin: {})", arguments.getFlowBin());
        int exitCode = new FlowLauncher().launch(arguments.getConfigPath(), arguments.getFlowBin());
        System.exit(exitCode);
    }
}
