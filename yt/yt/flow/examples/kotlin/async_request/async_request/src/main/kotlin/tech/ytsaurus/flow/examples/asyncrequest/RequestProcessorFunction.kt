package tech.ytsaurus.flow.examples.asyncrequest

import org.slf4j.LoggerFactory
import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage

/**
 * Processes request messages: computes the length of the request string
 * and emits a response message back to the "response" stream.
 */
class RequestProcessorFunction : RowFunction {
    companion object {
        private val log = LoggerFactory.getLogger(RequestProcessorFunction::class.java)
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val requestId = message.get("request_id", Long::class.java)
        val key = message.get("key", Long::class.java)
        val request = message.get("request", String::class.java)
        val length = if (request == null) 0L else request.length.toLong()

        log.debug("Processing request (requestId={}, key={}, length={})", requestId, key, length)

        val builder = ctx.createMessageBuilder("response")
        builder.set("request_id", requestId)
            .set("key", key)
            .set("length", length)

        output.addMessage(builder.finish())
    }
    // [END on_message]
}
