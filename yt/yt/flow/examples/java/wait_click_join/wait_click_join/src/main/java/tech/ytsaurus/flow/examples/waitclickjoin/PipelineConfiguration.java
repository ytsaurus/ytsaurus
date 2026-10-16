package tech.ytsaurus.flow.examples.waitclickjoin;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.examples.waitclickjoin.model.Action;
import tech.ytsaurus.flow.examples.waitclickjoin.model.Hit;
import tech.ytsaurus.flow.examples.waitclickjoin.model.JoinedAction;
import tech.ytsaurus.flow.spring.ComputationProvider;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;

// [BEGIN pipeline_configuration]
@Configuration
public class PipelineConfiguration implements ComputationProvider {

    @Override
    public List<FlowStream<?>> getStreams() {
        return List.of(
                FlowStreams.typed("hit", Hit.class),
                FlowStreams.typed("action", Action.class),
                FlowStreams.typed("joined_action", JoinedAction.class)
        );
    }
}
// [END pipeline_configuration]
