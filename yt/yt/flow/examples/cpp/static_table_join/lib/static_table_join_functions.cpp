#include "static_table_join_functions.h"

#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/payload.h>

#include <library/cpp/yt/string/string.h>

#include <util/string/strip.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

void TReferenceRow::Register(TRegistrar registrar)
{
    registrar.Parameter("key", &TThis::Key)
        .Default();
    registrar.Parameter("name", &TThis::Name)
        .Default();
}

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

void TReferenceReader::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto name = GetColumnValue<std::optional<TStringBuf>>(message, "name");
    if (!name) {
        return;
    }

    auto builder = context->MakeOutputMessageBuilder("reference");
    builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
    builder.Payload().Set(*name, "name");
    output->AddMessage(builder.Finish());
}

////////////////////////////////////////////////////////////////////////////////

void TReferenceLoader::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitExternalStateClient(ReferenceStateClient_, "/reference_state");
}

void TReferenceLoader::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& /*output*/,
    const IRuntimeContextPtr& context)
{
    auto reference = context->ConvertToYsonMessage<TReferenceRow>(message);
    auto normalizedName = AsciiStringToLower(StripString(TStringBuf(reference->Name)));

    auto state = ReferenceStateClient_.GetState(message->Key);
    TPayloadBuilder builder(state->Schema);
    builder.Set(normalizedName, "normalized_name");
    state->Payload = builder.Finish();
}

////////////////////////////////////////////////////////////////////////////////

void TEnricher::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitExternalStateClient(ReferenceStateJoiner_, "/reference_state");
}

void TEnricher::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto event = context->ConvertToYsonMessage<TEventRow>(message);
    auto state = ReferenceStateJoiner_.GetState(message->Key);
    if (state.IsEmpty()) {
        return;
    }

    auto enriched = New<TEnrichedRow>();
    enriched->Key = event->Key;
    enriched->Name = state->GetColumnValue<std::string>("normalized_name");
    output->AddMessage(context->ConvertToMessage(enriched));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
