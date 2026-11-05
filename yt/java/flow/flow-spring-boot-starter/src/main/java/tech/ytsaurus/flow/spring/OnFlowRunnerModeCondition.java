package tech.ytsaurus.flow.spring;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when the process launches a pipeline, i.e. {@code YT_FLOW_MODE} is not set.
 * <p>
 * The worker exports {@code YT_FLOW_MODE=Worker} to the companion it spawns, so an unset variable
 * means the application was started by a user to submit the pipeline spec.
 *
 * @see OnFlowWorkerModeCondition
 * @see FlowRunnerBootstrap
 */
public class OnFlowRunnerModeCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return FlowRunModeResolver.isRunnerMode(context.getEnvironment());
    }
}
