package tech.ytsaurus.flow.examples.shuffle

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage

class EventMapper : RowFunction {
    companion object {
        private val log = LoggerFactory.getLogger(EventMapper::class.java)
    }

    private val ysonMapper = ObjectMapper()

    // [BEGIN on_message]
    @Suppress("UNCHECKED_CAST")
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val messageBuilder = ctx.createMessageBuilder("event")
        val data = message.get("data", String::class.java)
        try {
            val map = ysonMapper.readValue(data, Map::class.java) as Map<String, Any?>
            messageBuilder.set("value", map["value"])
            messageBuilder.set("key_a", map["key_a"])
            messageBuilder.set("key_b", map["key_b"])
            messageBuilder.set("key_c", map["key_c"])
            messageBuilder.set("key_d", map["key_d"])
            output.addMessage(messageBuilder.finish())
        } catch (e: JsonProcessingException) {
            log.error("Error parsing json from data filed", e)
            throw RuntimeException(e)
        }
    }
    // [END on_message]
}
