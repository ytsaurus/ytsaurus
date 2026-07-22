package tech.ytsaurus.flow.examples.asyncrequest

import org.slf4j.LoggerFactory
import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.state.ExternalStateDescriptor
import tech.ytsaurus.flow.state.StateDescriptors
import java.util.Random

/**
 * Handles both "event" and "response" streams:
 *   - event  → creates a request message and emits it to the "request" stream
 *   - response → accumulates total_length in external state ("state" table)
 */
class StateKeeperFunction : RowFunction {
    companion object {
        private val log = LoggerFactory.getLogger(StateKeeperFunction::class.java)
        private val RANDOM = Random()
        private val STATE: ExternalStateDescriptor = StateDescriptors.external("/state")
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val streamId = message.getStreamId()

        if ("event" == streamId) {
            handleEvent(message, output, ctx)
        } else if ("response" == streamId) {
            handleResponse(message, ctx)
        } else {
            throw IllegalArgumentException("Unknown streamId: $streamId")
        }
    }
    // [END on_message]

    private fun handleEvent(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val key = message.get("key", Long::class.java)
        val data = message.get("data", String::class.java)
        val requestId = RANDOM.nextLong().ushr(1)

        log.debug("Handling event (key={}, requestId={})", key, requestId)

        val builder = ctx.createMessageBuilder("request")
        builder.set("request_id", requestId)
            .set("key", key)
            .set("request", data)

        output.addMessage(builder.finish())
    }

    private fun handleResponse(message: ExtendedMessage, ctx: RuntimeContext) {
        val length = message.get("length", Long::class.java)!!
        val key = message.get("key", Long::class.java)

        log.debug("Handling response (key={}, length={})", key, length)

        val stateAccessor = ctx.getState(STATE, message)
        val current = stateAccessor.getOrDefault()
        val updated = current.toBuilder()

        val totalLength = current.get("total_length", Long::class.java) ?: 0L
        updated.set("total_length", totalLength + length)

        stateAccessor.set(updated.finish())
    }
}
