#include "log_parser_process_function.h"

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <algorithm>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

void TLogParserProcessFunction::Init(const IRuntimeInitContextPtr& initContext)
{
    initContext->InitClient(StateClient_, WorstSeverityStateName);
}

void TLogParserProcessFunction::ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    auto state = StateClient_.GetState(message->Key);
    for (const auto& record : ParseLogLine(GetColumnValue<std::string>(message, "line"))) {
        state->WorstSeverity = std::max(state->WorstSeverity, SeverityRank(record.Level));

        auto outputRecord = New<TLogRecordMessage>();
        outputRecord->Level = record.Level;
        outputRecord->Text = record.Text;
        outputRecord->WorstLevelSoFar = SeverityName(state->WorstSeverity);
        output->AddMessage(context->ConvertToMessage(outputRecord));
    }
}

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TLogRecordMessage);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TLogParserProcessFunction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
