package tech.ytsaurus.flow.examples.retryableasyncrequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.examples.retryableasyncrequest.model.RequestState;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.MessageBuilder;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.state.InternalStateDescriptor;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * Processes request messages with retry logic using timers.
 *
 * <p>On a new request message, stores it in internal state and calls tryRequest.
 * On a timer fire, loads the stored state and retries.
 *
 * <p>Success condition: {@code (requestId + failedAttempts) % 3 == 0}.
 * On failure: increment failedAttempts, save state, schedule a timer 5 seconds in the future.
 * On success: emit response, clear state.
 */
public class RequestProcessorFunction implements RowFunction {
    private static final Logger log = LoggerFactory.getLogger(RequestProcessorFunction.class);

    private static final long RETRY_DELAY_SECONDS = 5L;
    private static final long MODULO = 3L;

    private static final InternalStateDescriptor<RequestState> REQUEST_STATE =
            StateDescriptors.yson("request-state", RequestState.class);

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        long requestId = message.get("request_id", Long.class);
        long key = message.get("key", Long.class);
        String request = message.get("request", String.class);

        log.debug("Received request message (requestId={}, key={})", requestId, key);

        StateAccessor<RequestState> accessor =
                ctx.getState(REQUEST_STATE, message);

        RequestState state = new RequestState(requestId, key, request, 0);
        accessor.set(state);

        tryRequest(state, accessor, output, ctx);
    }
    // [END on_message]

    // [BEGIN on_timer]
    @Override
    public void onTimer(Timer timer, OutputCollector output, RuntimeContext ctx) {
        log.debug("Timer fired, retrying request");

        StateAccessor<RequestState> accessor =
                ctx.getState(REQUEST_STATE, timer);

        RequestState state = accessor.get()
                .orElseThrow(() -> new IllegalStateException("No request state found on timer fire"));

        tryRequest(state, accessor, output, ctx);
    }
    // [END on_timer]

    private boolean isSucceed(long requestId, int failedAttempts) {
        return (requestId + failedAttempts) % MODULO == 0;
    }

    private void tryRequest(
            RequestState state,
            StateAccessor<RequestState> accessor,
            OutputCollector output,
            RuntimeContext ctx
    ) {
        long requestId = state.getRequestId();
        int failedAttempts = state.getFailedAttempts();

        if (!isSucceed(requestId, failedAttempts)) {
            state.setFailedAttempts(failedAttempts + 1);
            accessor.set(state);
            long nextAttemptTime = System.currentTimeMillis() / 1000L + RETRY_DELAY_SECONDS;
            output.addTimer(nextAttemptTime, 0L);
            log.debug("Request failed, scheduling retry (requestId={}, failedAttempts={})",
                    requestId, state.getFailedAttempts());
            return;
        }

        long length = state.getRequest() == null ? 0L : (long) state.getRequest().length();
        log.debug("Request succeeded (requestId={}, failedAttempts={}, length={})",
                requestId, failedAttempts, length);

        accessor.clear();

        MessageBuilder responseBuilder = ctx.createMessageBuilder("response");
        responseBuilder.set("request_id", requestId);
        responseBuilder.set("key", state.getKey());
        responseBuilder.set("length", length);

        output.addMessage(responseBuilder.finish());
    }
}
