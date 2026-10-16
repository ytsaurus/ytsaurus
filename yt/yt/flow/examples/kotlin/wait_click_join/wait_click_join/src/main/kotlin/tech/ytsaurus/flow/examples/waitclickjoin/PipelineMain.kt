package tech.ytsaurus.flow.examples.waitclickjoin

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder

@SpringBootApplication
open class PipelineMain {
    companion object {
        // [BEGIN main]
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplicationBuilder(PipelineMain::class.java)
                .run(*args)
        }
        // [END main]
    }
}
