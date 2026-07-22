package tech.ytsaurus.flow.examples.retryableasyncrequest

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.row.Payload
import tech.ytsaurus.flow.spring.FlowComputation
import tech.ytsaurus.flow.state.ExternalStateAccessor
import tech.ytsaurus.flow.state.ExternalStateDescriptor
import tech.ytsaurus.flow.state.StateDescriptors
import java.util.Random

// [BEGIN registration]
@FlowComputation(id = "state")
// [END registration]

/**
 * Handles both "event" and "response" streams:
 *   - event  -> creates a request message and emits it to the "request" stream
 *   - response -> accumulates total_length in external state ("state" table)
 *
 * Identical logic to the async_request StateKeeper.
 */
open class StateKeeperFunction : RowFunction {
    companion object {
        private val log: Logger = LoggerFactory.getLogger(StateKeeperFunction::class.java)

        private val RANDOM = Random()

        private val STATE: ExternalStateDescriptor =
            StateDescriptors.external("/state")
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val streamId = message.streamId

        when (streamId) {
            "event" -> handleEvent(message, output, ctx)
            "response" -> handleResponse(message, ctx)
            else -> throw IllegalArgumentException("Unknown streamId: $streamId")
        }
    }
    // [END on_message]

    private fun handleEvent(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val key = message.get("key", Long::class.java)
        val data = message.get("data", String::class.java)
        val requestId = RANDOM.nextLong().ushr(1)

        log.debug("Handling event (key={}, requestId={})", key, requestId)

        val requestBuilder = ctx.createMessageBuilder("request")
        requestBuilder.set("request_id", requestId)
        requestBuilder.set("key", key)
        requestBuilder.set("request", data)

        output.addMessage(requestBuilder.finish())
    }

    private fun handleResponse(message: ExtendedMessage, ctx: RuntimeContext) {
        val length = message.get("length", Long::class.java)!!
        val key = message.get("key", Long::class.java)

        log.debug("Handling response (key={}, length={})", key, length)

        val stateAccessor: ExternalStateAccessor = ctx.getState(STATE, message)
        val current: Payload = stateAccessor.getOrDefault()
        val updated = current.toBuilder()

        val totalLength = current.get("total_length", Long::class.java) ?: 0L
        updated.set("total_length", totalLength + length)

        stateAccessor.set(updated.finish())
    }
}
