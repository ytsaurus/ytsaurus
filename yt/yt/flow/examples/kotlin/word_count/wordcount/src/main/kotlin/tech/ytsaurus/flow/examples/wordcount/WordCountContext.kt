package tech.ytsaurus.flow.examples.wordcount

import org.springframework.context.annotation.Configuration
import tech.ytsaurus.flow.examples.wordcount.model.Word
import tech.ytsaurus.flow.spring.ComputationProvider
import tech.ytsaurus.flow.stream.FlowStream
import tech.ytsaurus.flow.stream.FlowStreams

// [BEGIN stream_context]
@Configuration
class WordCountContext : ComputationProvider {

    override fun getStreams(): List<FlowStream<*>> {
        return listOf(FlowStreams.typed("words", Word::class.java))
    }
}
// [END stream_context]
