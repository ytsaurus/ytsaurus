package tech.ytsaurus.flow.examples.retryableasyncrequest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder

// [BEGIN main]
@SpringBootApplication
open class PipelineMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplicationBuilder(PipelineMain::class.java)
                .run(*args)
        }
    }
}
// [END main]
