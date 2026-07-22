package tech.ytsaurus.flow.examples.retryableasyncrequest

import org.springframework.context.annotation.Configuration
import tech.ytsaurus.flow.computation.Computation
import tech.ytsaurus.flow.spring.ComputationProvider

// [BEGIN computation_context]
@Configuration
open class RetryableAsyncRequestComputationContext : ComputationProvider {

    override fun getComputations(): List<Computation> {
        return listOf(
            Computation.builder()
                .setComputationId("state")
                .setProcessFunction(StateKeeperFunction())
                .build(),
            Computation.builder()
                .setComputationId("processor")
                .setProcessFunction(RequestProcessorFunction())
                .build(),
        )
    }
}
// [END computation_context]
