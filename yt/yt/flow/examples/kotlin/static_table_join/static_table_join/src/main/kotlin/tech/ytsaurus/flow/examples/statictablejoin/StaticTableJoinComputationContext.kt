package tech.ytsaurus.flow.examples.statictablejoin

import org.springframework.context.annotation.Configuration
import tech.ytsaurus.flow.computation.Computation
import tech.ytsaurus.flow.spring.ComputationProvider

// [BEGIN computation_context]
@Configuration
open class StaticTableJoinComputationContext : ComputationProvider {
    override fun getComputations(): List<Computation> {
        return listOf(
            Computation.builder()
                .setComputationId("reference_loader")
                .setProcessFunction(ReferenceLoader())
                .build(),
            Computation.builder()
                .setComputationId("enricher")
                .setProcessFunction(Enricher())
                .build()
        )
    }
}
// [END computation_context]
