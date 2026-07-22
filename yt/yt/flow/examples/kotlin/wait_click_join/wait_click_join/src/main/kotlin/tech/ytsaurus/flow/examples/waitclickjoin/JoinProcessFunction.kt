package tech.ytsaurus.flow.examples.waitclickjoin

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.examples.waitclickjoin.model.Action
import tech.ytsaurus.flow.examples.waitclickjoin.model.Hit
import tech.ytsaurus.flow.examples.waitclickjoin.model.JoinedAction
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.row.Message
import tech.ytsaurus.flow.row.Timer
import tech.ytsaurus.flow.spring.FlowComputation
import tech.ytsaurus.flow.state.ExternalStateDescriptor
import tech.ytsaurus.flow.state.StateDescriptors
import java.time.Duration

// [BEGIN registration]
@FlowComputation(id = "join")
// [END registration]
open class JoinProcessFunction : RowFunction {
    companion object {
        val JOINED_ACTION_STATE: ExternalStateDescriptor =
            StateDescriptors.external("/join-state")

        private val log: Logger = LoggerFactory.getLogger(JoinProcessFunction::class.java)

        private fun parseDuration(duration: String): Duration {
            return Duration.parse("PT" + duration.uppercase())
        }
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val streamId = message.streamId
        val hitId: String
        val hitTime: Long
        var hit: Hit? = null
        var action: Action? = null

        if ("hit" == streamId) {
            hit = message.getPayload<Hit>()
            hitId = hit.hitId!!
            hitTime = hit.hitTime!!
        } else if ("action" == streamId) {
            action = message.getPayload<Action>()
            hitId = action.hitId!!
            hitTime = action.hitTime!!
        } else {
            throw IllegalArgumentException("Unknown streamId: $streamId")
        }
        log.debug("Process message (Key: {}, HitId: {}, HitTime: {})", message.key, hitId, hitTime)
        val waitTime = parseDuration(
            ctx.computationParameters.get("wait_for_actions")!!.stringValue()
        ).toSeconds()
        // [BEGIN timer_setup]
        val maxTime = hitTime + waitTime
        // All messages out of wait range would be dropped.
        if (message.eventTimestamp >= maxTime) {
            log.warn(
                "Message is out of wait range, skip (HitId: {}, HitTime: {}, MaxTime: {})",
                hitId, hitTime, maxTime
            )
            return
        }
        // Drop late data.
        // [BEGIN late_data_check]
        if (message.eventTimestamp < ctx.epochInputEventWatermark) {
            log.warn(
                "Late message, skip (HitId: {}, EventTimestamp: {}, InputEventWatermark: {})",
                hitId, message.eventTimestamp, ctx.epochInputEventWatermark
            )
            return
        }
        // [END late_data_check]
        // [BEGIN state_update]
        val stateAccessor = ctx.getState(JOINED_ACTION_STATE, message)
        val joinState = stateAccessor.orDefault.toBuilder()

        if ("hit" == streamId) {
            log.debug(
                "Process hit message (HitId: {}, HitPayload: {}, Hit: {})",
                hitId, hit!!.hitPayload, hit
            )
            joinState.set("hit_payload", hit.hitPayload)
        } else {
            log.debug(
                "Process action message (HitId: {}, ActionTime: {}, Action: {})",
                hitId, action!!.actionTime, action
            )
            if (action.isClick() == true) {
                joinState.set("click_time", action.actionTime)
            } else {
                joinState.set("show_time", action.actionTime)
            }
        }
        log.debug("Set state (KeyHitId: {}, State: {})", hitId, joinState)
        stateAccessor.set(joinState.finish())
        // [END state_update]
        log.debug("Add timer for Key (MaxTime: {}, HitTime: {})", maxTime, hitTime)
        output.addTimer(maxTime, hitTime)
        // [END timer_setup]
    }
    // [END on_message]

    // [BEGIN on_timer]
    override fun onTimer(timer: Timer, output: OutputCollector, ctx: RuntimeContext) {
        // Key to pojo conversion is not supported yet.
        val hitId = timer.key.get("hit_id", String::class.java)
        log.debug("Process timer (KeyHitId: {}, Timer: {})", hitId, timer)
        val stateAccessor = ctx.getState(JOINED_ACTION_STATE, timer)
        if (stateAccessor.get().isEmpty) {
            throw IllegalStateException("Empty state for timer: $timer")
        }
        val joinState = stateAccessor.get().orElseThrow()
        log.debug("Process state (KeyHitId: {}, State: {})", hitId, joinState)
        if ("timer" == timer.streamId) {
            // ExternalState to pojo conversion is not supported yet.
            if (joinState.get("show_time", Long::class.java) != null &&
                joinState.get("hit_payload", String::class.java) != null &&
                joinState.get("show_time", Long::class.java) != 0L
            ) {
                log.debug("Click join: {}", joinState)
                val joinedAction = JoinedAction()
                val hitTime = timer.key.get("hit_time", Long::class.java)
                joinedAction.hitId = hitId
                joinedAction.hitTime = hitTime
                joinedAction.showTime = joinState.get("show_time", Long::class.java)
                if (joinState.get("click_time", Long::class.java) != null &&
                    joinState.get("click_time", Long::class.java) != 0L
                ) {
                    joinedAction.isClick = true
                    joinedAction.clickTime = joinState.get("click_time", Long::class.java)
                } else {
                    joinedAction.isClick = false
                    joinedAction.clickTime = 0L
                }
                joinedAction.hitPayload = joinState.get("hit_payload", String::class.java)
                output.addMessage(
                    Message.builder().setStreamId("joined_action").setPayload(joinedAction).build()
                )
            }
            stateAccessor.clear()
        } else {
            throw IllegalArgumentException("Unknown streamId: ${timer.streamId}")
        }
    }
    // [END on_timer]
}
