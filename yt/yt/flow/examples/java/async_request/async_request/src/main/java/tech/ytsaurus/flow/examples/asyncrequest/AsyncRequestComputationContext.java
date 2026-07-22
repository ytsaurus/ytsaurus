package tech.ytsaurus.flow.examples.asyncrequest;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.spring.ComputationProvider;

// [BEGIN computation_context]
@Configuration
public class AsyncRequestComputationContext implements ComputationProvider {

    @Override
    public List<Computation> getComputations() {
        return List.of(
                Computation.builder()
                        .setComputationId("state")
                        .setProcessFunction(new StateKeeperFunction())
                        .build(),
                Computation.builder()
                        .setComputationId("processor")
                        .setProcessFunction(new RequestProcessorFunction())
                        .build()
        );
    }
}
// [END computation_context]
