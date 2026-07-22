package tech.ytsaurus.flow.examples.shuffle

import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.state.ExternalStateDescriptor
import tech.ytsaurus.flow.state.StateDescriptors

class EventReducer : RowFunction {
    companion object {
        private val SHUFFLE_STATE: ExternalStateDescriptor =
            StateDescriptors.external("/shuffle-state")
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val externalStateAccessor = ctx.getState(SHUFFLE_STATE, message)
        val state = externalStateAccessor.getOrDefault()
        val stateBuilder = state.toBuilder()
        val count = state.get("count", Long::class.java) ?: 0L
        stateBuilder.set("count", count + 1)
        externalStateAccessor.set(stateBuilder.finish())
    }
    // [END on_message]
}
