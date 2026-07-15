#include "async_request_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/core/logging/log.h>

#include <util/random/random.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("AsyncRequest");

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

////////////////////////////////////////////////////////////////////////////////

// [BEGIN request_processor]
void TRequestProcessor::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto request = context->ConvertToYsonMessage<TRequestMessage>(message);
    auto response = New<TResponseMessage>();
    response->RequestId = request->RequestId;
    response->Key = request->Key;
    response->Length = std::ssize(request->Request);
    output->AddMessage(context->ConvertToMessage(response));
    YT_LOG_DEBUG("Processed request (RequestId: %v, FailedAttempts: 0)",
        request->RequestId);
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
        YT_LOG_DEBUG("Send request (RequestId: %v)", request->RequestId);
    } else if (message->StreamId == "response") {
        auto response = context->ConvertToYsonMessage<TResponseMessage>(message);
        YT_LOG_DEBUG("Received response (RequestId: %v)", response->RequestId);
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
