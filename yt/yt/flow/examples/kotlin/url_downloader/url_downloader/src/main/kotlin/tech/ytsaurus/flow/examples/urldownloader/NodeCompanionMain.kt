package tech.ytsaurus.flow.examples.urldownloader

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder

@SpringBootApplication
open class NodeCompanionMain {
    companion object {
        // [BEGIN main]
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplicationBuilder(NodeCompanionMain::class.java)
                .run(*args)
        }
        // [END main]
    }
}
