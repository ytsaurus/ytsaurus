package tech.ytsaurus.flow.examples.wordcount;

import tech.ytsaurus.flow.pipeline.SimpleRunnerProgram;

public class RunnerMain {

    private RunnerMain() {
    }

    // [BEGIN main]
    public static void main(String[] args) throws Exception {
        SimpleRunnerProgram.runPipeline(args);
    }
    // [END main]
}
