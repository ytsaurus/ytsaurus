package tech.ytsaurus.flow.examples.shuffle

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder

// [BEGIN main]
@SpringBootApplication
open class NodeCompanionMain {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplicationBuilder(NodeCompanionMain::class.java)
                .run(*args)
        }
    }
}
// [END main]
