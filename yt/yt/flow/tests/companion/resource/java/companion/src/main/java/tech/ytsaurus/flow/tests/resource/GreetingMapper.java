package tech.ytsaurus.flow.tests.resource;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;

/**
 * Copies the current state of the greeting resource into every output row, so
 * the test can assert on the resource's init parameters, applied dynamic
 * parameters and the serving companion pid.
 */
public class GreetingMapper implements RowFunction {

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        GreetingResource resource = ctx.getResource("greeting_view", GreetingResource.class);
        output.addMessage(ctx.createMessageBuilder("mapped")
                .set("key", message.get("key", String.class))
                .set("greeting", resource.getGreeting())
                .set("suffix", resource.getSuffix())
                .set("dependency_value", resource.getDependencyValue())
                .set("pid", resource.getInitPid())
                .finish());
    }
}
