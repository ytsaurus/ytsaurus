package tech.ytsaurus.flow.examples.wordcount

import org.springframework.stereotype.Component
import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.examples.wordcount.model.Word
import tech.ytsaurus.flow.examples.wordcount.model.WordCountState
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.state.InternalStateDescriptor
import tech.ytsaurus.flow.state.StateDescriptors

@Component
class WordCountMapper : RowFunction {
    companion object {
        val WORD_STATE: InternalStateDescriptor<WordCountState> =
            StateDescriptors.yson("word-state", WordCountState::class.java)
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val input: Word = message.getPayload()
        val stateAccessor = ctx.getState(WORD_STATE, message)
        val state = stateAccessor.getOrDefault(WordCountState(input.word, 0))
        state.count += 1
        stateAccessor.set(state)
    }
    // [END on_message]
}
