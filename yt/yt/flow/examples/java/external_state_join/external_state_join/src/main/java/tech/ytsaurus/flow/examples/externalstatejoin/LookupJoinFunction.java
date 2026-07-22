package tech.ytsaurus.flow.examples.externalstatejoin;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.state.JoinedExternalStateDescriptor;
import tech.ytsaurus.flow.state.ReadOnlyExternalStateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;

/**
 * Joins each event against a pre-built dynamic reference table reached through
 * the {@code /reference} external state. The external state joiner resolves the Cypress
 * symlink behind that path per lookup, so repointing the symlink swaps the
 * reference dataset for keys looked up afterwards without a pipeline restart.
 */
public class LookupJoinFunction implements RowFunction {
    private static final JoinedExternalStateDescriptor REFERENCE_STATE =
            StateDescriptors.externalReadOnly("/reference");

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        ReadOnlyExternalStateAccessor stateAccessor = ctx.getState(REFERENCE_STATE, message);
        String name = stateAccessor.getOrDefault().get("name", String.class);
        if (name == null) {
            return;
        }

        output.addMessage(ctx.createMessageBuilder("enriched")
                .set("key", message.get("key", Long.class))
                .set("name", name)
                .finish());
    }
    // [END on_message]
}
