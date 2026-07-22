package tech.ytsaurus.flow.examples.waitclickjoin;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.examples.waitclickjoin.model.Action;
import tech.ytsaurus.flow.examples.waitclickjoin.model.Hit;
import tech.ytsaurus.flow.examples.waitclickjoin.model.JoinedAction;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.state.ExternalStateAccessor;
import tech.ytsaurus.flow.state.ExternalStateDescriptor;
import tech.ytsaurus.flow.state.StateDescriptors;

public class JoinProcessFunction implements RowFunction {
    static final ExternalStateDescriptor JOINED_ACTION_STATE =
            StateDescriptors.external("/join-state");

    private static final Logger log = LoggerFactory.getLogger(JoinProcessFunction.class);

    private static Duration parseDuration(String duration) {
        return Duration.parse("PT" + duration.toUpperCase());
    }

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        var streamId = message.getStreamId();
        String hitId;
        Long hitTime;
        Hit hit = null;
        Action action = null;

        if ("hit".equals(streamId)) {
            hit = message.getPayload();
            hitId = hit.getHitId();
            hitTime = hit.getHitTime();
        } else if ("action".equals(streamId)) {
            action = message.getPayload();
            hitId = action.getHitId();
            hitTime = action.getHitTime();
        } else {
            throw new IllegalArgumentException("Unknown streamId: %s".formatted(streamId));
        }
        log.debug("Process message (Key: {}, HitId: {}, HitTime: {})", message.getKey(), hitId, hitTime);
        long waitTime = parseDuration(ctx.getComputationParameters().get("wait_for_actions").stringValue()).toSeconds();
        // [BEGIN timer_setup]
        long maxTime = hitTime + waitTime;
        // All messages out of wait range would be dropped.
        if (message.getEventTimestamp() >= maxTime) {
            log.warn("Message is out of wait range, skip (HitId: {}, HitTime: {}, MaxTime: {})",
                    hitId, hitTime, maxTime);
            return;
        }
        // Drop late data.
        // [BEGIN late_data_check]
        if (message.getEventTimestamp() < ctx.getEpochInputEventWatermark()) {
            log.warn("Late message, skip (HitId: {}, EventTimestamp: {}, InputEventWatermark: {})",
                    hitId, message.getEventTimestamp(), ctx.getEpochInputEventWatermark());
            return;
        }
        // [END late_data_check]
        // [BEGIN state_update]
        ExternalStateAccessor stateAccessor = ctx.getState(JOINED_ACTION_STATE, message);
        PayloadBuilder joinState = stateAccessor.getOrDefault().toBuilder();

        if ("hit".equals(streamId)) {
            log.debug("Process hit message (HitId: {}, HitPayload: {}, Hit: {})", hitId, hit.getHitPayload(), hit);
            joinState.set("hit_payload", hit.getHitPayload());
        } else {
            log.debug("Process action message (HitId: {}, ActionTime: {}, Action: {})",
                    hitId, action.getActionTime(), action);
            if (Boolean.TRUE.equals(action.isClick())) {
                joinState.set("click_time", action.getActionTime());
            } else {
                joinState.set("show_time", action.getActionTime());
            }
        }
        log.debug("Set state (KeyHitId: {}, State: {})", hitId, joinState);
        stateAccessor.set(joinState.finish());
        // [END state_update]
        log.debug("Add timer for Key (MaxTime: {}, HitTime: {})", maxTime, hitTime);
        output.addTimer(maxTime, hitTime);
        // [END timer_setup]
    }
    // [END on_message]

    // [BEGIN on_timer]
    @Override
    public void onTimer(Timer timer, OutputCollector output, RuntimeContext ctx) {
        // Key to pojo conversion is not supported yet.
        String hitId = timer.getKey().get("hit_id", String.class);
        log.debug("Process timer (KeyHitId: {}, Timer: {})", hitId, timer);
        ExternalStateAccessor stateAccessor = ctx.getState(JOINED_ACTION_STATE, timer);
        if (stateAccessor.get().isEmpty()) {
            throw new IllegalStateException("Empty state for timer: " + timer);
        }
        Payload joinState = stateAccessor.get().orElseThrow();
        log.debug("Process state (KeyHitId: {}, State: {})", hitId, joinState);
        if ("timer".equals(timer.getStreamId())) {
            // ExternalState to pojo conversion is not supported yet.
            if (joinState.get("show_time", Long.class) != null
                    && joinState.get("hit_payload", String.class) != null
                    && joinState.get("show_time", Long.class) != 0) {
                log.debug("Click join: {}", joinState);
                JoinedAction joinedAction = new JoinedAction();
                long hitTime = timer.getKey().get("hit_time", Long.class);
                joinedAction.setHitId(hitId);
                joinedAction.setHitTime(hitTime);
                joinedAction.setShowTime(joinState.get("show_time", Long.class));
                if (joinState.get("click_time", Long.class) != null && joinState.get("click_time", Long.class) != 0) {
                    joinedAction.setClick(true);
                    joinedAction.setClickTime(joinState.get("click_time", Long.class));
                } else {
                    joinedAction.setClick(false);
                    joinedAction.setClickTime(0L);
                }
                joinedAction.setHitPayload(joinState.get("hit_payload", String.class));
                output.addMessage(Message.builder().setStreamId("joined_action").setPayload(joinedAction).build());
            }
            stateAccessor.clear();
        } else {
            throw new IllegalArgumentException("Unknown streamId: %s".formatted(timer.getStreamId()));
        }
    }
    // [END on_timer]
}
