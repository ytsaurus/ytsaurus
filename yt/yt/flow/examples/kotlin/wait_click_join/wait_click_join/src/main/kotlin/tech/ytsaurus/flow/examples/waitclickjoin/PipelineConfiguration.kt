package tech.ytsaurus.flow.examples.waitclickjoin

import org.springframework.context.annotation.Configuration
import tech.ytsaurus.flow.computation.Computation
import tech.ytsaurus.flow.examples.waitclickjoin.model.Action
import tech.ytsaurus.flow.examples.waitclickjoin.model.Hit
import tech.ytsaurus.flow.examples.waitclickjoin.model.JoinedAction
import tech.ytsaurus.flow.spring.ComputationProvider
import tech.ytsaurus.flow.stream.FlowStream
import tech.ytsaurus.flow.stream.FlowStreams

// [BEGIN pipeline_configuration]
@Configuration
open class PipelineConfiguration : ComputationProvider {
    override fun getComputations(): List<Computation> {
        val join = Computation.builder()
            .setComputationId("join")
            .setProcessFunction(JoinProcessFunction())
            .build()
        return listOf(join)
    }

    override fun getStreams(): List<FlowStream<*>> {
        return listOf(
            FlowStreams.typed("hit", Hit::class.java),
            FlowStreams.typed("action", Action::class.java),
            FlowStreams.typed("joined_action", JoinedAction::class.java)
        )
    }
}
// [END pipeline_configuration]
