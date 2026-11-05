package tech.ytsaurus.flow.examples.asyncrequest;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.spring.FlowComputation;
import tech.ytsaurus.flow.state.ExternalStateAccessor;
import tech.ytsaurus.flow.state.ExternalStateDescriptor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * Handles both "event" and "response" streams:
 * <ul>
 *   <li>event  → creates a request message and emits it to the "request" stream</li>
 *   <li>response → accumulates total_length in external state ("state" table)</li>
 * </ul>
 */
// [BEGIN registration]
@FlowComputation(id = "state")
// [END registration]
public class StateKeeperFunction implements RowFunction {
    private static final Logger log = LoggerFactory.getLogger(StateKeeperFunction.class);

    private static final Random RANDOM = new Random();

    private static final ExternalStateDescriptor STATE =
            StateDescriptors.external("/state");

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        String streamId = message.getStreamId();

        if ("event".equals(streamId)) {
            handleEvent(message, output, ctx);
        } else if ("response".equals(streamId)) {
            handleResponse(message, ctx);
        } else {
            throw new IllegalArgumentException("Unknown streamId: " + streamId);
        }
    }
    // [END on_message]

    private void handleEvent(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        long key = message.get("key", Long.class);
        String data = message.get("data", String.class);
        long requestId = RANDOM.nextLong() >>> 1;

        log.debug("Handling event (key={}, requestId={})", key, requestId);

        var builder = ctx.createMessageBuilder("request");
        builder.set("request_id", requestId)
               .set("key", key)
               .set("request", data);

        output.addMessage(builder.finish());
    }

    private void handleResponse(ExtendedMessage message, RuntimeContext ctx) {
        long length = message.get("length", Long.class);
        long key = message.get("key", Long.class);

        log.debug("Handling response (key={}, length={})", key, length);

        ExternalStateAccessor stateAccessor = ctx.getState(STATE, message);
        Payload current = stateAccessor.getOrDefault();
        PayloadBuilder updated = current.toBuilder();

        Long totalLength = current.get("total_length", Long.class);
        if (totalLength == null) {
            totalLength = 0L;
        }
        updated.set("total_length", totalLength + length);

        stateAccessor.set(updated.finish());
    }
}
