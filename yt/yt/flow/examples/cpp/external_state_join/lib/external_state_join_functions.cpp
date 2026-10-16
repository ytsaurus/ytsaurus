#include "external_state_join_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

void TEventRow::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
}

void TEnrichedRow::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("name", &TThis::Name)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TLookupJoinFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitExternalStateClient(ReferenceJoiner_, "/reference");
}

void TLookupJoinFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto event = context->ConvertToYsonMessage<TEventRow>(message);
    auto state = ReferenceJoiner_.GetState(message->Key);
    if (state.IsEmpty()) {
        return;
    }

    auto enriched = New<TEnrichedRow>();
    enriched->Key = event->Key;
    enriched->Name = state->GetColumnValue<std::string>("name");
    output->AddMessage(context->ConvertToMessage(enriched));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
