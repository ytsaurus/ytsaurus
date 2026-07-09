#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <yt/yt/library/program/program_config_mixin.h>

using namespace NYT::NFlow;
using namespace NYT::NYson;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

using NYT::NYTree::ConvertTo;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Main");

////////////////////////////////////////////////////////////////////////////////

class TStateKeeper
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*collector*/) override
    {
        auto state = StateClient_.GetState(message->Key);

        TPayloadBuilder builder(state->Schema);
        i64 key = GetColumnValue<i64>(message, "key");
        auto textKey = GetColumnValue<std::string>(message, "text_key");
        i64 value = GetColumnValue<i64>(message, "value");
        if (value % 2 == 0) {
            YT_LOG_DEBUG("Clearing state (Key: %v, TextKey: %v)", key, textKey);
            state->Clear();
        }
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TStateKeeper);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
