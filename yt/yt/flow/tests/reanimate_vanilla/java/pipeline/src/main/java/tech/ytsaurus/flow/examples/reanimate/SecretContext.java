package tech.ytsaurus.flow.examples.reanimate;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.examples.reanimate.model.Word;
import tech.ytsaurus.flow.spring.ComputationProvider;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;

@Configuration
public class SecretContext implements ComputationProvider {

    @Autowired
    private SecretSinkMapper secretSinkMapper;

    @Override
    public List<Computation> getComputations() {
        // "reader" is a native passthrough source (declared in the pipeline spec via
        // computation_class_name) and is therefore not registered with the companion.
        Computation mapper = Computation.builder()
                .setComputationId("mapper")
                .setProcessFunction(secretSinkMapper)
                .build();
        return List.of(mapper);
    }

    @Override
    public List<FlowStream<?>> getStreams() {
        return List.of(FlowStreams.typed("words", Word.class));
    }
}
