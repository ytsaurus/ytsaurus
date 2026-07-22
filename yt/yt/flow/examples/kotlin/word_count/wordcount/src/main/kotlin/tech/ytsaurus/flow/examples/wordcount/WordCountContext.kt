package tech.ytsaurus.flow.examples.wordcount

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Configuration
import tech.ytsaurus.flow.computation.Computation
import tech.ytsaurus.flow.examples.wordcount.model.Word
import tech.ytsaurus.flow.spring.ComputationProvider
import tech.ytsaurus.flow.stream.FlowStream
import tech.ytsaurus.flow.stream.FlowStreams

// [BEGIN word_count_context]
@Configuration
class WordCountContext : ComputationProvider {

    @Autowired
    private lateinit var wordCountMapper: WordCountMapper

    override fun getComputations(): List<Computation> {
        // "reader" is a native passthrough source (declared in the pipeline spec via
        // computation_class_name) and is therefore not registered with the companion.
        val mapper = Computation.builder()
            .setComputationId("mapper")
            .setProcessFunction(wordCountMapper)
            .build()
        return listOf(mapper)
    }

    override fun getStreams(): List<FlowStream<*>> {
        return listOf(FlowStreams.typed("words", Word::class.java))
    }
}
// [END word_count_context]
