#include "proto_parser_function.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

void TLogRecordMessage::Register(TRegistrar registrar)
{
    registrar.Parameter("level", &TThis::Level)
        .Default();
    registrar.Parameter("text", &TThis::Text)
        .Default();
    registrar.Parameter("seen_at_level", &TThis::SeenAtLevel)
        .Default();
}

void TLevelCountsState::Register(TRegistrar registrar)
{
    registrar.Parameter("record_counts", &TThis::RecordCounts)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TProtoLogParserFunction::DoInit(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitClient(StateClient_, "level_counts");
}

void TProtoLogParserFunction::ProcessProto(
    const TInputMessageConstPtr& message,
    TLogRecordProto&& proto,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = StateClient_.GetState(message->Key);
    auto record = New<TLogRecordMessage>();
    record->Level = proto.level();
    record->Text = proto.text();
    record->SeenAtLevel = ++state->RecordCounts[proto.level()];
    output->AddMessage(context->ConvertToMessage(record));
}

void TProtoLogParserFunction::ProcessUnparsed(
    const TInputMessageConstPtr& /*message*/,
    TError /*error*/,
    const IOutputCollectorPtr& /*output*/,
    const IRuntimeContextPtr& /*context*/)
{ }

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TLogRecordMessage);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TProtoLogParserFunction, TProtoParsingProcessFunctionParameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
