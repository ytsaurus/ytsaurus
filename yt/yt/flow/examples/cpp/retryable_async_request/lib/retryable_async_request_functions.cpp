#include "retryable_async_request_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/core/logging/log.h>

#include <util/random/random.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("RetryableAsyncRequest");

////////////////////////////////////////////////////////////////////////////////

void TEventMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("data", &TThis::Data)
        .Default();
}

void TRequestMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("request_id", &TThis::RequestId)
        .Default();
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("request", &TThis::Request)
        .Default();
}

void TResponseMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("request_id", &TThis::RequestId)
        .Default();
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("length", &TThis::Length)
        .Default();
}

void TDelayedRequestState::Register(TRegistrar registrar)
{
    registrar.Parameter("failed_attempts", &TThis::FailedAttempts)
        .Default();
    registrar.Parameter("request", &TThis::Request)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

// [BEGIN request_processor]
void TRequestProcessor::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitClient<TDelayedRequestState>(RequestStateClient_, "request_state");
}

void TRequestProcessor::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto request = context->ConvertToYsonMessage<TRequestMessage>(message);
    auto state = RequestStateClient_.GetState(message->Key);
    state->Request = request;
    state->FailedAttempts = 0;
    TryRequest(state, output, context);
}

void TRequestProcessor::ProcessTimer(
    const TInputTimerConstPtr& timer,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = RequestStateClient_.GetState(timer->Key);
    TryRequest(state, output, context);
}

TSystemTimestamp TRequestProcessor::GetNextAttempt(const IRuntimeContextPtr& context) const
{
    return TSystemTimestamp{context->GetCurrentTimestamp().Underlying() + Delay};
}

bool TRequestProcessor::IsRequestSuccessful(ui64 requestId, i64 failedAttempts) const
{
    return (requestId + failedAttempts) % MaxRetries == 0;
}

void TRequestProcessor::TryRequest(
    TStateAccessor<TDelayedRequestState>& state,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context) const
{
    if (!IsRequestSuccessful(state->Request->RequestId, state->FailedAttempts)) {
        state->FailedAttempts += 1;
        output->AddTimer(GetNextAttempt(context));
        YT_TLOG_DEBUG("Failed request")
            .With("RequestId", state->Request->RequestId)
            .With("FailedAttempts", state->FailedAttempts);
        return;
    }

    auto response = New<TResponseMessage>();
    response->RequestId = state->Request->RequestId;
    response->Key = state->Request->Key;
    response->Length = std::ssize(state->Request->Request);
    YT_TLOG_DEBUG("Processed request")
        .With("RequestId", state->Request->RequestId)
        .With("FailedAttempts", state->FailedAttempts);
    state.Clear();
    output->AddMessage(context->ConvertToMessage(response));
}

// [END request_processor]

////////////////////////////////////////////////////////////////////////////////

// [BEGIN state_keeper]
void TStateKeeper::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitExternalStateClient(StateClient_, "/state");
}

void TStateKeeper::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    if (message->StreamId == "event") {
        auto event = context->ConvertToYsonMessage<TEventMessage>(message);
        auto request = New<TRequestMessage>();
        request->RequestId = RandomNumber<ui64>();
        request->Key = event->Key;
        request->Request = event->Data;
        output->AddMessage(context->ConvertToMessage(request));
        YT_TLOG_DEBUG("Send request")
            .With("RequestId", request->RequestId);
    } else if (message->StreamId == "response") {
        auto response = context->ConvertToYsonMessage<TResponseMessage>(message);
        YT_TLOG_DEBUG("Received response")
            .With("RequestId", response->RequestId);
        auto state = StateClient_.GetState(message->Key);
        i64 totalLength = state->GetColumnValue<std::optional<i64>>("total_length").value_or(0);
        totalLength += response->Length;
        TPayloadBuilder builder(state->Schema);
        builder.Set(totalLength, "total_length");
        state->Payload = builder.Finish();
    } else {
        THROW_ERROR_EXCEPTION("Unexpected stream_id")
            << TErrorAttribute("stream_id", message->StreamId);
    }
}

// [END state_keeper]

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
