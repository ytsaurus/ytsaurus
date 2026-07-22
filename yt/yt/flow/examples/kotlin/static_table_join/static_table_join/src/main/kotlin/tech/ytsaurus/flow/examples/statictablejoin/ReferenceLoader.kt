package tech.ytsaurus.flow.examples.statictablejoin

import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.state.ExternalStateDescriptor
import tech.ytsaurus.flow.state.StateDescriptors

/**
 * Trims and lowercases the reference name, storing it in keyed external state.
 */
class ReferenceLoader : RowFunction {
    companion object {
        private val REFERENCE_STATE: ExternalStateDescriptor =
            StateDescriptors.external("/reference_state")
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val normalizedName = message.get("name", String::class.java)?.trim()?.lowercase() ?: return

        val stateAccessor = ctx.getState(REFERENCE_STATE, message)
        stateAccessor.set(
            stateAccessor.orDefault.toBuilder()
                .set("normalized_name", normalizedName)
                .finish()
        )
    }
    // [END on_message]
}
