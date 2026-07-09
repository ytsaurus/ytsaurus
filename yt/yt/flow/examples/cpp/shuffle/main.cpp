#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <library/cpp/json/json_reader.h>

using namespace NYT;
using namespace NYT::NFlow;
using namespace NYT::NYPath;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Main");

////////////////////////////////////////////////////////////////////////////////

// [BEGIN example_shuffle_queue_reader]
class TQueueReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) final
    {
        auto data = GetColumnValue<std::string>(message, "data");
        ::NJson::TJsonValue json;
        ::NJson::ReadJsonTree(data, &json, /*throwOnError*/ true);
        auto builder = MakeOutputMessageBuilder("event");
        builder.Payload().Set<std::string>(json["value"].GetStringSafe(), "value");
        builder.Payload().Set<ui64>(json["key_a"].GetUIntegerSafe(), "key_a");
        builder.Payload().Set<ui64>(json["key_b"].GetUIntegerSafe(), "key_b");
        builder.Payload().Set<ui64>(json["key_c"].GetUIntegerSafe(), "key_c");
        builder.Payload().Set<ui64>(json["key_d"].GetUIntegerSafe(), "key_d");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TQueueReader);

// [END example_shuffle_queue_reader]

////////////////////////////////////////////////////////////////////////////////

// [BEGIN example_shuffle_reducer]

class TReducer
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
        IOutputCollectorPtr /*output*/) override
    {
        auto state = StateClient_.GetState(message->Key);
        i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);

        count += 1;

        TPayloadBuilder builder(state->Schema);
        builder.Set(count, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TReducer);

// [END example_shuffle_reducer]

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NFlow::Initialize(argc, argv);
    return TSimpleRunnerProgram().Run(argc, argv);
}
