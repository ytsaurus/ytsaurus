package tech.ytsaurus.flow.tests.types;

import tech.ytsaurus.flow.pipeline.SimpleRunnerProgram;

public class RunnerMain {

    private RunnerMain() {
    }

    public static void main(String[] args) throws Exception {
        SimpleRunnerProgram.runPipeline(args);
    }
}
