package tech.ytsaurus.flow.examples.asyncrequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.spring.FlowComputation;

/**
 * Processes request messages: computes the length of the request string
 * and emits a response message back to the "response" stream.
 */
// [BEGIN registration]
@FlowComputation(id = "processor")
// [END registration]
public class RequestProcessorFunction implements RowFunction {
    private static final Logger log = LoggerFactory.getLogger(RequestProcessorFunction.class);

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        long requestId = message.get("request_id", Long.class);
        long key = message.get("key", Long.class);
        String request = message.get("request", String.class);
        long length = request == null ? 0L : (long) request.length();

        log.debug("Processing request (requestId={}, key={}, length={})", requestId, key, length);

        var builder = ctx.createMessageBuilder("response");
        builder.set("request_id", requestId)
               .set("key", key)
               .set("length", length);

        output.addMessage(builder.finish());
    }
    // [END on_message]
}
