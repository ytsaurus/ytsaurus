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

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        output->AddMessage(ConvertToOutputMessage(message));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

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
        IOutputCollectorPtr) override
    {
        auto state = StateClient_.GetState(message->Key);

        TPayloadBuilder builder(state->Schema);
        i64 key = GetColumnValue<i64>(message, "key");
        i64 value = GetColumnValue<i64>(message, "value");
        i64 secondValue = GetColumnValue<i64>(message, "second_value");
        std::optional<i64> mergedValue = GetColumnValue<std::optional<i64>>(message, "merged.value");
        std::optional<i64> mergedSecondValue = GetColumnValue<std::optional<i64>>(message, "merged.second_value");
        bool hasMerged = GetColumnValue<bool>(message, "merged.ispresent");
        YT_VERIFY(value == key + 1);
        YT_VERIFY(secondValue == key + 2);
        if (key % 10 != 0) {
            YT_VERIFY(mergedValue == key + 3);
            YT_VERIFY(mergedSecondValue == key + 4);
            YT_VERIFY(hasMerged);
        } else {
            YT_VERIFY(mergedValue == std::nullopt);
            YT_VERIFY(mergedSecondValue == std::nullopt);
            YT_VERIFY(!hasMerged);
        }

        i64 prevCount = state->GetColumnValue<std::optional<i64>>("count").value_or(0);
        builder.Set(prevCount + 1, "count");
        YT_LOG_INFO("Setting count to %v while key is %v", prevCount + 1, key);
        state->Payload = builder.Finish();
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
