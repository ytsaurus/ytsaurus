package tech.ytsaurus.flow.examples.retryableasyncrequest

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.examples.retryableasyncrequest.model.RequestState
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.row.Timer
import tech.ytsaurus.flow.spring.FlowComputation
import tech.ytsaurus.flow.state.InternalStateDescriptor
import tech.ytsaurus.flow.state.StateAccessor
import tech.ytsaurus.flow.state.StateDescriptors

// [BEGIN registration]
@FlowComputation(id = "processor")
// [END registration]

/**
 * Processes request messages with retry logic using timers.
 *
 * On a new request message, stores it in internal state and calls tryRequest.
 * On a timer fire, loads the stored state and retries.
 *
 * Success condition: `(requestId + failedAttempts) % 3 == 0`.
 * On failure: increment failedAttempts, save state, schedule a timer 5 seconds in the future.
 * On success: emit response, clear state.
 */
open class RequestProcessorFunction : RowFunction {
    companion object {
        private val log: Logger = LoggerFactory.getLogger(RequestProcessorFunction::class.java)

        private const val RETRY_DELAY_SECONDS = 5L
        private const val MODULO = 3L

        private val REQUEST_STATE: InternalStateDescriptor<RequestState> =
            StateDescriptors.yson("request-state", RequestState::class.java)
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val requestId = message.get("request_id", Long::class.java)!!
        val key = message.get("key", Long::class.java)!!
        val request = message.get("request", String::class.java)

        log.debug("Received request message (requestId={}, key={})", requestId, key)

        val accessor: StateAccessor<RequestState> =
            ctx.getState(REQUEST_STATE, message)

        val state = RequestState(requestId, key, request, 0)
        accessor.set(state)

        tryRequest(state, accessor, output, ctx)
    }
    // [END on_message]

    // [BEGIN on_timer]
    override fun onTimer(timer: Timer, output: OutputCollector, ctx: RuntimeContext) {
        log.debug("Timer fired, retrying request")

        val accessor: StateAccessor<RequestState> =
            ctx.getState(REQUEST_STATE, timer)

        val state = accessor.get()
            .orElseThrow { IllegalStateException("No request state found on timer fire") }

        tryRequest(state, accessor, output, ctx)
    }
    // [END on_timer]

    private fun isSucceed(requestId: Long, failedAttempts: Int): Boolean {
        return (requestId + failedAttempts) % MODULO == 0L
    }

    private fun tryRequest(
        state: RequestState,
        accessor: StateAccessor<RequestState>,
        output: OutputCollector,
        ctx: RuntimeContext,
    ) {
        val requestId = state.requestId
        val failedAttempts = state.failedAttempts

        if (!isSucceed(requestId, failedAttempts)) {
            state.failedAttempts = failedAttempts + 1
            accessor.set(state)
            val nextAttemptTime = System.currentTimeMillis() / 1000L + RETRY_DELAY_SECONDS
            output.addTimer(nextAttemptTime, 0L)
            log.debug(
                "Request failed, scheduling retry (requestId={}, failedAttempts={})",
                requestId,
                state.failedAttempts,
            )
            return
        }

        val length = if (state.request == null) 0L else state.request!!.length.toLong()
        log.debug(
            "Request succeeded (requestId={}, failedAttempts={}, length={})",
            requestId,
            failedAttempts,
            length,
        )

        accessor.clear()

        val responseBuilder = ctx.createMessageBuilder("response")
        responseBuilder.set("request_id", requestId)
        responseBuilder.set("key", state.key)
        responseBuilder.set("length", length)

        output.addMessage(responseBuilder.finish())
    }
}
