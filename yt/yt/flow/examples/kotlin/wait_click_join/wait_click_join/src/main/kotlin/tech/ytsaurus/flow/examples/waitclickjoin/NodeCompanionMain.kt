package tech.ytsaurus.flow.examples.waitclickjoin

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder

@SpringBootApplication
open class NodeCompanionMain {
    companion object {
        private val log: Logger = LoggerFactory.getLogger(NodeCompanionMain::class.java)

        // [BEGIN main]
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplicationBuilder(NodeCompanionMain::class.java)
                .run(*args)
        }
        // [END main]
    }
}
