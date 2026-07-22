package tech.ytsaurus.flow.examples.externalstatejoin

import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.state.JoinedExternalStateDescriptor
import tech.ytsaurus.flow.state.StateDescriptors

/**
 * Joins each event against a pre-built dynamic reference table reached through
 * the `/reference` external state. The external state joiner resolves the Cypress
 * symlink behind that path per lookup, so repointing the symlink swaps the
 * reference dataset for keys looked up afterwards without a pipeline restart.
 */
class LookupJoin : RowFunction {
    companion object {
        private val REFERENCE_STATE: JoinedExternalStateDescriptor =
            StateDescriptors.externalReadOnly("/reference")
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val stateAccessor = ctx.getState(REFERENCE_STATE, message)
        val name = stateAccessor.orDefault.get("name", String::class.java) ?: return

        output.addMessage(
            ctx.createMessageBuilder("enriched")
                .set("key", message.get("key", Long::class.java))
                .set("name", name)
                .finish()
        )
    }
    // [END on_message]
}
