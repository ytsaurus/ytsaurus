package tech.ytsaurus.flow.examples.statictablejoin

import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.state.JoinedExternalStateDescriptor
import tech.ytsaurus.flow.state.StateDescriptors

/**
 * Joins each event against the reference state and emits the enriched row.
 */
class Enricher : RowFunction {
    companion object {
        private val REFERENCE_STATE: JoinedExternalStateDescriptor =
            StateDescriptors.externalReadOnly("/reference_state")
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val stateAccessor = ctx.getState(REFERENCE_STATE, message)
        val normalizedName = stateAccessor.orDefault.get("normalized_name", String::class.java) ?: return

        output.addMessage(
            ctx.createMessageBuilder("enriched")
                .set("key", message.get("key", Long::class.java))
                .set("name", normalizedName)
                .finish()
        )
    }
    // [END on_message]
}
