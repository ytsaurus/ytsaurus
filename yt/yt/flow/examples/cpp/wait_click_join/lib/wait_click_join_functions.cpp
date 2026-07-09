#include "wait_click_join_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("WaitClickJoin");

////////////////////////////////////////////////////////////////////////////////

void TJoinParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("wait_for_actions", &TThis::WaitForActions);
}

void TJoinState::Register(TRegistrar registrar)
{
    registrar.Parameter("show_time", &TThis::ShowTime)
        .Default();
    registrar.Parameter("click_time", &TThis::ClickTime)
        .Default();
    registrar.Parameter("hit_payload", &TThis::HitPayload)
        .Default();
}

void TJoinKey::Register(TRegistrar registrar)
{
    registrar.Parameter("hash", &TThis::Hash);
    registrar.Parameter("hit_id", &TThis::HitId);
    registrar.Parameter("hit_time", &TThis::HitTime);
}

void TActionMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("hit_id", &TThis::HitId)
        .Default();
    registrar.Parameter("hit_time", &TThis::HitTime)
        .Default();
    registrar.Parameter("is_click", &TThis::IsClick)
        .Default();
    registrar.Parameter("action_time", &TThis::ActionTime)
        .Default();
}

void THitMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("hit_id", &TThis::HitId)
        .Default();
    registrar.Parameter("hit_time", &TThis::HitTime)
        .Default();
    registrar.Parameter("hit_payload", &TThis::HitPayload)
        .Default();
}

void TJoinedActionMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("hit_id", &TThis::HitId)
        .Default();
    registrar.Parameter("hit_time", &TThis::HitTime)
        .Default();
    registrar.Parameter("is_click", &TThis::IsClick)
        .Default();
    registrar.Parameter("show_time", &TThis::ShowTime)
        .Default(0);
    registrar.Parameter("click_time", &TThis::ClickTime)
        .Default(0);
    registrar.Parameter("hit_payload", &TThis::HitPayload)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJoinFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    WaitForActions_ = initContext->GetParameters<TJoinParameters>()->WaitForActions;
    initContext->InitClient<TJoinState>(JoinStateClient_, "join_state");
}

TSystemTimestamp TJoinFunction::GetWindowEnd(const TJoinKeyPtr& ysonKey) const
{
    return TSystemTimestamp(ysonKey->HitTime + WaitForActions_.Seconds());
}

// [BEGIN join_process_message]
void TJoinFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto ysonKey = context->ConvertToYsonKey<TJoinKey>(message->Key);
    auto state = JoinStateClient_.GetState(message->Key);

    auto windowEnd = GetWindowEnd(ysonKey);
    auto eventTimestamp = message->GetMeta().EventTimestamp;

    // All messages out of the wait range are dropped.
    if (eventTimestamp >= windowEnd) {
        YT_LOG_WARNING("Message is out of wait range, skip (HitId: %v, WindowEnd: %v)",
            ysonKey->HitId,
            windowEnd);
        return;
    }

    // Drop late data.
    auto inputEventWatermark = context->GetInputEventWatermark();
    if (eventTimestamp < inputEventWatermark) {
        YT_LOG_WARNING("Late message, skip (HitId: %v, InputEventWatermark: %v)",
            ysonKey->HitId,
            inputEventWatermark);
        return;
    }

    if (message->StreamId == "hit") {
        auto hit = context->ConvertToYsonMessage<THitMessage>(message);
        state->HitPayload = hit->HitPayload;
    } else if (message->StreamId == "action") {
        auto action = context->ConvertToYsonMessage<TActionMessage>(message);
        if (action->IsClick) {
            state->ClickTime = action->ActionTime;
        } else {
            state->ShowTime = action->ActionTime;
        }
    } else {
        THROW_ERROR_EXCEPTION("Unknown stream %Qv", message->StreamId);
    }

    // Set a timer at the end of the wait window. With DeduplicateEqualTimestamps (on by
    // default) timers are deduplicated by trigger timestamp; the timer fires once the event
    // watermark reaches windowEnd, i.e. after (almost) all messages within the window.
    output->AddTimer(windowEnd, TSystemTimestamp(ysonKey->HitTime));
}

// [END join_process_message]

// [BEGIN join_process_timer]
void TJoinFunction::ProcessTimer(
    const TInputTimerConstPtr& timer,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto ysonKey = context->ConvertToYsonKey<TJoinKey>(timer->Key);
    auto state = JoinStateClient_.GetState(timer->Key);

    if (timer->StreamId != "timer") {
        THROW_ERROR_EXCEPTION("Unknown stream %Qv", timer->StreamId);
    }

    if (state.IsEmpty()) {
        return;
    }

    if (state->ShowTime && state->HitPayload) {
        auto joinedAction = New<TJoinedActionMessage>();
        joinedAction->HitId = ysonKey->HitId;
        joinedAction->HitTime = ysonKey->HitTime;
        joinedAction->ShowTime = *state->ShowTime;
        if (state->ClickTime) {
            joinedAction->IsClick = true;
            joinedAction->ClickTime = *state->ClickTime;
        } else {
            joinedAction->IsClick = false;
            joinedAction->ClickTime = 0;
        }
        joinedAction->HitPayload = *state->HitPayload;
        output->AddMessage(context->ConvertToMessage(joinedAction));
    }

    state.Clear();
}

// [END join_process_timer]

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
