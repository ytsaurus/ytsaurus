package tech.ytsaurus.flow.examples.shuffle;

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

// [BEGIN registration]
@FlowComputation(id = "reducer")
// [END registration]
public class EventReducer implements RowFunction {
    private static final ExternalStateDescriptor SHUFFLE_STATE =
            StateDescriptors.external("/shuffle-state");

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        ExternalStateAccessor externalStateAccessor = ctx.getState(SHUFFLE_STATE, message);
        Payload state = externalStateAccessor.getOrDefault();
        PayloadBuilder stateBuilder = state.toBuilder();
        if (state.get("count", Long.class) == null) {
            stateBuilder.set("count", 1L);
        } else {
            stateBuilder.set("count", state.get("count", Long.class) + 1);
        }
        externalStateAccessor.set(stateBuilder.finish());
    }
    // [END on_message]
}
