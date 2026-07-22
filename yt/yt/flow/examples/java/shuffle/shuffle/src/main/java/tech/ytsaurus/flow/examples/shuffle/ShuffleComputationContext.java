package tech.ytsaurus.flow.examples.shuffle;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.spring.ComputationProvider;

// [BEGIN computation_context]
@Configuration
public class ShuffleComputationContext implements ComputationProvider {

    @Override
    public List<Computation> getComputations() {
        return List.of(
                SourceComputation.builder()
                        .setComputationId("reader")
                        .setProcessFunction(new EventMapper())
                        .build(),
                Computation.builder()
                        .setComputationId("reducer")
                        .setProcessFunction(new EventReducer())
                        .build()
        );
    }
}
// [END computation_context]
