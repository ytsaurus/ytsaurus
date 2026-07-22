package tech.ytsaurus.flow.examples.externalstatejoin

import org.springframework.context.annotation.Configuration
import tech.ytsaurus.flow.computation.Computation
import tech.ytsaurus.flow.spring.ComputationProvider

// [BEGIN computation_context]
@Configuration
open class ExternalStateJoinComputationContext : ComputationProvider {
    override fun getComputations(): List<Computation> {
        return listOf(
            Computation.builder()
                .setComputationId("lookup_join")
                .setProcessFunction(LookupJoin())
                .build()
        )
    }
}
// [END computation_context]
