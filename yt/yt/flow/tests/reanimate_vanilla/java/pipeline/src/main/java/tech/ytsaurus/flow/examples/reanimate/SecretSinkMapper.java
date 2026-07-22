package tech.ytsaurus.flow.examples.reanimate;

import org.springframework.stereotype.Component;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.examples.reanimate.model.SecretState;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.state.InternalStateDescriptor;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;

// Counts processed messages per key and records the secret delivered via vanilla.secret_env. The
// internal state (the pipeline's states table) is the observable output: a growing count proves the
// pipeline keeps processing (e.g. after reanimate) and the secret field proves the secret reached
// the Java companion process.
@Component
public class SecretSinkMapper implements RowFunction {
    private static final InternalStateDescriptor<SecretState> SECRET_STATE =
            StateDescriptors.yson("secret-state", SecretState.class);

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        StateAccessor<SecretState> stateAccessor = ctx.getState(SECRET_STATE, message);
        SecretState state = stateAccessor.getOrDefault(new SecretState(0, ""));
        state.setCount(state.getCount() + 1);
        String secret = System.getenv("YT_MY_SECRET");
        state.setSecret(secret != null ? secret : "");
        stateAccessor.set(state);
    }
}
