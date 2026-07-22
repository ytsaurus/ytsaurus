package tech.ytsaurus.flow.examples.reanimate;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.examples.reanimate.model.Word;
import tech.ytsaurus.flow.spring.ComputationProvider;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;

@Configuration
public class SecretContext implements ComputationProvider {

    @Override
    public List<FlowStream<?>> getStreams() {
        return List.of(FlowStreams.typed("words", Word.class));
    }
}
