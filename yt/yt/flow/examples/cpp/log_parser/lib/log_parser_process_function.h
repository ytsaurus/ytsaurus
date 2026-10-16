#pragma once

#include "log_line_parser.h"

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <string>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

struct TLogRecordMessage
    : public TYsonMessage
{
    std::string Level;
    std::string Text;
    std::string WorstLevelSoFar;

    REGISTER_YSON_STRUCT(TLogRecordMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("level", &TThis::Level)
            .Default();
        registrar.Parameter("text", &TThis::Text)
            .Default();
        registrar.Parameter("worst_level_so_far", &TThis::WorstLevelSoFar)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TWorstSeverityState
    : public NYTree::TYsonStruct
{
    int WorstSeverity = -1;

    REGISTER_YSON_STRUCT(TWorstSeverityState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("worst_severity", &TThis::WorstSeverity)
            .Default(-1);
    }
};

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf WorstSeverityStateName = "worst_severity";

class TLogParserProcessFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TMutableStateKeyClient<TWorstSeverityState> StateClient_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
