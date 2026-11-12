#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

namespace NYT::NFlow::NFailedMessagesTest {

////////////////////////////////////////////////////////////////////////////////

class TCaptureFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        if (GetColumnValue<i64>(*message, "value") < 0) {
            auto error = TError("Invalid input value")
                .With("event_id", GetColumnValue<i64>(*message, "event_id"));
            output->AddFailedMessage(message, error);
            output->AddFailedMessage(message, TError("Duplicate capture"));
            return;
        }

        output->AddMessage(context->ConvertToOutputMessage(*message, TStreamId("output")));
    }
};

YT_FLOW_DEFINE_PROCESS_FUNCTION(TCaptureFunction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFailedMessagesTest

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
