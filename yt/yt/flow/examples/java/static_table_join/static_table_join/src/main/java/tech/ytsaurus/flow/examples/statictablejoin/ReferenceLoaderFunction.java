package tech.ytsaurus.flow.examples.statictablejoin;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.state.ExternalStateAccessor;
import tech.ytsaurus.flow.state.ExternalStateDescriptor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * Trims and lowercases the reference name, storing it in keyed external state.
 */
public class ReferenceLoaderFunction implements RowFunction {
    private static final ExternalStateDescriptor REFERENCE_STATE =
            StateDescriptors.external("/reference_state");

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        String name = message.get("name", String.class);
        String normalizedName = name.strip().toLowerCase();

        ExternalStateAccessor stateAccessor = ctx.getState(REFERENCE_STATE, message);
        stateAccessor.set(stateAccessor.getOrDefault().toBuilder()
                .set("normalized_name", normalizedName)
                .finish());
    }
    // [END on_message]
}
