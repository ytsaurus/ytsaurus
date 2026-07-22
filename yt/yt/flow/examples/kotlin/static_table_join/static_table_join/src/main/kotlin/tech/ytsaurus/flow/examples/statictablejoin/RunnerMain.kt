package tech.ytsaurus.flow.examples.statictablejoin

import tech.ytsaurus.flow.pipeline.SimpleRunnerProgram

object RunnerMain {

    // [BEGIN main]
    @JvmStatic
    fun main(args: Array<String>) {
        SimpleRunnerProgram.runPipeline(args)
    }
    // [END main]
}
