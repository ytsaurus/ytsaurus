#include "key_visitor_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/payload.h>

namespace NYT::NFlow::NKeyVisitorTest {

////////////////////////////////////////////////////////////////////////////////

void TKeyMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("payload", &TThis::Payload)
        .Default();
}

void TVisitMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("payload", &TThis::Payload)
        .Default();
    registrar.Parameter("visit_index", &TThis::VisitIndex)
        .Default(0);
}

void TUserState::Register(TRegistrar registrar)
{
    registrar.Parameter("payload", &TThis::Payload)
        .Default();
    registrar.Parameter("visit_index", &TThis::VisitIndex)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

void TVisitTesterFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitClient<TUserState>(StateClient_, "user_state");
}

void TVisitTesterFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& /*output*/,
    const IRuntimeContextPtr& context)
{
    auto ysonMessage = context->ConvertToYsonMessage<TKeyMessage>(message);
    auto state = StateClient_.GetState(message->Key);
    state->Payload = ysonMessage->Payload;
}

void TVisitTesterFunction::ProcessVisit(
    const TInputVisitConstPtr& visit,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = StateClient_.GetState(visit->Key);
    if (state.IsEmpty()) {
        return;
    }
    auto ysonKey = context->ConvertToYsonKey<TKeyMessage>(visit->Key);

    auto outputMessage = New<TVisitMessage>();
    outputMessage->Key = ysonKey->Key;
    outputMessage->Payload = state->Payload;
    outputMessage->VisitIndex = ++state->VisitIndex;

    output->AddMessage(context->ConvertToMessage(outputMessage));
}

////////////////////////////////////////////////////////////////////////////////

void TExternalVisitTesterFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitExternalStateClient(StateClient_, "/user-state-external");
}

void TExternalVisitTesterFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& /*output*/,
    const IRuntimeContextPtr& context)
{
    auto ysonMessage = context->ConvertToYsonMessage<TKeyMessage>(message);
    auto state = StateClient_.GetState(message->Key);
    const auto visitIndex = state->GetColumnValue<std::optional<i64>>("visit_index").value_or(0);
    TPayloadBuilder builder(state->Schema);
    builder.Set(ysonMessage->Payload, "payload");
    builder.Set(visitIndex, "visit_index");
    state->Payload = builder.Finish();
}

void TExternalVisitTesterFunction::ProcessVisit(
    const TInputVisitConstPtr& visit,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = StateClient_.GetState(visit->Key);
    if (state.IsEmpty()) {
        return;
    }
    const auto payload = state->GetColumnValue<std::optional<std::string>>("payload").value_or(std::string{});
    const auto newVisitIndex = state->GetColumnValue<std::optional<i64>>("visit_index").value_or(0) + 1;
    TPayloadBuilder builder(state->Schema);
    builder.Set(payload, "payload");
    builder.Set(newVisitIndex, "visit_index");
    state->Payload = builder.Finish();

    auto ysonKey = context->ConvertToYsonKey<TKeyMessage>(visit->Key);
    auto outputMessage = New<TVisitMessage>();
    outputMessage->Key = ysonKey->Key;
    outputMessage->Payload = payload;
    outputMessage->VisitIndex = newVisitIndex;
    output->AddMessage(context->ConvertToMessage(outputMessage));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NKeyVisitorTest
