package tech.ytsaurus.flow.examples.statictablejoin;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.spring.FlowComputation;
import tech.ytsaurus.flow.state.JoinedExternalStateDescriptor;
import tech.ytsaurus.flow.state.ReadOnlyExternalStateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * Joins each event against the reference state and emits the enriched row.
 */
// [BEGIN registration]
@FlowComputation(id = "enricher")
// [END registration]
public class EnricherFunction implements RowFunction {
    private static final JoinedExternalStateDescriptor REFERENCE_STATE =
            StateDescriptors.externalReadOnly("/reference_state");

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        ReadOnlyExternalStateAccessor stateAccessor = ctx.getState(REFERENCE_STATE, message);
        String normalizedName = stateAccessor.getOrDefault().get("normalized_name", String.class);
        if (normalizedName == null) {
            return;
        }

        output.addMessage(ctx.createMessageBuilder("enriched")
                .set("key", message.get("key", Long.class))
                .set("name", normalizedName)
                .finish());
    }
    // [END on_message]
}
