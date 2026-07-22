package tech.ytsaurus.flow.spring;

import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;

/**
 * Condition that matches when the application context contains at least one Flow component:
 * a {@link ComputationProvider} bean, or a bean annotated with {@link FlowComputation} or
 * {@link FlowSourceComputation}.
 * <p>
 * This is the activation trigger for the Flow companion autoconfiguration. It supersedes the
 * previous {@code @ConditionalOnBean(ComputationProvider.class)} gate so that purely
 * annotation-based pipelines (without any {@link ComputationProvider} implementation) also start
 * the companion server.
 * <p>
 * It extends {@link AnyNestedCondition} with the {@code REGISTER_BEAN} configuration phase so the
 * member {@link ConditionalOnBean} checks are evaluated after all bean definitions have been
 * registered (the {@code OR} of the nested conditions), which is the reliable phase for
 * bean-presence checks in autoconfiguration.
 *
 * @see FlowAutoConfiguration
 */
public class OnFlowComponentsCondition extends AnyNestedCondition {

    public OnFlowComponentsCondition() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnBean(ComputationProvider.class)
    static class HasComputationProvider {
    }

    @ConditionalOnBean(annotation = FlowComputation.class)
    static class HasFlowComputation {
    }

    @ConditionalOnBean(annotation = FlowSourceComputation.class)
    static class HasFlowSourceComputation {
    }
}
