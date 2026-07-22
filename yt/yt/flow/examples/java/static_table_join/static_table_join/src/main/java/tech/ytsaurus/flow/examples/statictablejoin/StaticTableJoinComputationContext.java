package tech.ytsaurus.flow.examples.statictablejoin;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.spring.ComputationProvider;

// [BEGIN computation_context]
@Configuration
public class StaticTableJoinComputationContext implements ComputationProvider {

    @Override
    public List<Computation> getComputations() {
        return List.of(
                Computation.builder()
                        .setComputationId("reference_loader")
                        .setProcessFunction(new ReferenceLoaderFunction())
                        .build(),
                Computation.builder()
                        .setComputationId("enricher")
                        .setProcessFunction(new EnricherFunction())
                        .build()
        );
    }
}
// [END computation_context]
