#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <util/string/split.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

//! Splits a queue row like "info:started;error:failed" into one table row per record.
class TParseLineFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        const auto line = GetColumnValue<std::string>(message, "line");
        for (const auto& entry : StringSplitter(line).Split(';').SkipEmpty()) {
            const TStringBuf token = entry.Token();
            TStringBuf level;
            TStringBuf text;
            // A record without a level is skipped; its row is still exported once, as no table rows.
            if (!token.TrySplit(':', level, text)) {
                continue;
            }
            auto builder = context->MakeOutputMessageBuilder();
            builder.Payload().Set<std::string>(std::string(level), "level");
            builder.Payload().Set<std::string>(std::string(text), "text");
            output->AddMessage(builder.Finish());
        }
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TParseLineFunction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram(NYT::NFlow::TSimpleSpecBuilder()).Run(argc, argv);
}
