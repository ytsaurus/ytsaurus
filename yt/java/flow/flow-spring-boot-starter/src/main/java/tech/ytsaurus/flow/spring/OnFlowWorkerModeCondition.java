package tech.ytsaurus.flow.spring;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import tech.ytsaurus.flow.config.FlowRunMode;

/**
 * Matches when the process serves a pipeline as a companion, i.e. {@code YT_FLOW_MODE=Worker}.
 * <p>
 * The companion beans — the execution config, the gRPC server and its lifecycle — exist only in this
 * mode: their configuration comes from the worker through the environment and is absent anywhere
 * else.
 *
 * @see OnFlowRunnerModeCondition
 * @see FlowAutoConfiguration
 */
public class OnFlowWorkerModeCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return FlowRunModeResolver.resolve(context.getEnvironment())
                .filter(FlowRunMode.Worker::equals)
                .isPresent();
    }
}
