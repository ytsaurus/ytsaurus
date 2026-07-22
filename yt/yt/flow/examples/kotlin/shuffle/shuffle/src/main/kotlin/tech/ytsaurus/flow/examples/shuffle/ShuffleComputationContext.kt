package tech.ytsaurus.flow.examples.shuffle

import org.springframework.context.annotation.Configuration
import tech.ytsaurus.flow.computation.Computation
import tech.ytsaurus.flow.computation.SourceComputation
import tech.ytsaurus.flow.spring.ComputationProvider

// [BEGIN computation_context]
@Configuration
open class ShuffleComputationContext : ComputationProvider {

    override fun getComputations(): List<Computation> {
        return listOf(
            SourceComputation.builder()
                .setComputationId("reader")
                .setProcessFunction(EventMapper())
                .build(),
            Computation.builder()
                .setComputationId("reducer")
                .setProcessFunction(EventReducer())
                .build(),
        )
    }
}
// [END computation_context]
