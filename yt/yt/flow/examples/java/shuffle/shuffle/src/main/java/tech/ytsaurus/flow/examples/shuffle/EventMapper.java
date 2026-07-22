package tech.ytsaurus.flow.examples.shuffle;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;

public class EventMapper implements RowFunction {
    private static final Logger log = LoggerFactory.getLogger(EventMapper.class);
    private final ObjectMapper ysonMapper = new ObjectMapper();

    // [BEGIN on_message]
    @SuppressWarnings("unchecked")
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        var messageBuilder = ctx.createMessageBuilder("event");
        String data = message.get("data", String.class);
        try {
            Map<String, Object> map = ysonMapper.readValue(data, Map.class);
            messageBuilder.set("value", map.get("value"));
            messageBuilder.set("key_a", map.get("key_a"));
            messageBuilder.set("key_b", map.get("key_b"));
            messageBuilder.set("key_c", map.get("key_c"));
            messageBuilder.set("key_d", map.get("key_d"));
            output.addMessage(messageBuilder.finish());
        } catch (JsonProcessingException e) {
            log.error("Error parsing json from data filed", e);
            throw new RuntimeException(e);
        }
    }
    // [END on_message]
}
