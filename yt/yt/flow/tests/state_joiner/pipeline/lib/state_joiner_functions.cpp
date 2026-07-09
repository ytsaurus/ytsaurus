#include "state_joiner_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NFlow::NStateJoinerTest {

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TUserTotalState::Register(TRegistrar registrar)
{
    registrar.Parameter("total", &TThis::Total)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

void TAccumulatorFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitClient(TotalClient_, "total");
}

void TAccumulatorFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = TotalClient_.GetState(message->Key);
    state->Total += GetColumnValue<i64>(message, "Amount");

    auto builder = context->MakeOutputMessageBuilder();
    builder.Payload().Set<TString>(GetColumnValue<TString>(message, "UserId"), "UserId");
    builder.Payload().Set<ui64>(0, "Bucket");
    output->AddMessage(builder.Finish());
}

////////////////////////////////////////////////////////////////////////////////

void TJoinerFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitClient(TotalJoiner_, "user_total");
}

void TJoinerFunction::Process(
    const IInputContextPtr& input,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    if (!context->GetSpec()->StateJoiners.at("/user_total")->AutoPreload) {
        WaitFor(TotalJoiner_.PreloadKeyStates(input)).ThrowOnError();
    }

    for (const auto& message : input->GetMessages()) {
        auto state = TotalJoiner_.GetState(message);

        auto builder = context->MakeOutputMessageBuilder();
        builder.Payload().Set<TString>(GetColumnValue<TString>(message, "UserId"), "UserId");
        builder.Payload().Set<i64>(state->Total, "Total");
        output->AddMessage(builder.Finish());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStateJoinerTest
