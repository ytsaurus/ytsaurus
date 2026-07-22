package tech.ytsaurus.flow.examples.wordcount

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder

// [BEGIN word_count_application]
@SpringBootApplication
open class WordCountApplication {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            SpringApplicationBuilder(WordCountApplication::class.java)
                .run(*args)
        }
    }
}
// [END word_count_application]
