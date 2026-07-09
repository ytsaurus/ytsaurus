#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto builder = MakeOutputMessageBuilder();
        builder.Payload().SetValue(GetColumn(message, "data"), "data");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

struct TCycleComputationParameters
    : public virtual NYTree::TYsonStruct
{
    THashMap<TStreamId, TStreamId> PassthroughRules;
    THashMap<TStreamId, TDuration> SleepPerMessage;

    REGISTER_YSON_STRUCT(TCycleComputationParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("passthrough_rules", &TThis::PassthroughRules)
            .Default();
        registrar.Parameter("sleep_per_message", &TThis::SleepPerMessage)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCycleTransformComputation
    : public TTransformComputation
{
private:
    struct TExtendedParameters
        : public TTransformComputation::TParameters
        , public TCycleComputationParameters
    {
        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar /*registrar*/)
        { }
    };

public:
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);

    using TTransformComputation::TTransformComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        TDelayedExecutor::WaitForDuration(GetParameters()->SleepPerMessage.Value(message.StreamId, TDuration::Zero()));
        auto builder = MakeOutputMessageBuilder(GetOrCrash(GetParameters()->PassthroughRules, message.StreamId));
        builder.Payload().SetValue(GetColumn(message, "data"), "data");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TCycleTransformComputation);

////////////////////////////////////////////////////////////////////////////////

class TCycleSwiftMapComputation
    : public TSwiftMapComputation
{
private:
    struct TExtendedParameters
        : public TSwiftMapComputation::TParameters
        , public TCycleComputationParameters
    {
        REGISTER_YSON_STRUCT(TExtendedParameters);

        static void Register(TRegistrar /*registrar*/)
        { }
    };

public:
    YT_FLOW_EXTEND_PARAMETERS(TExtendedParameters);

    using TSwiftMapComputation::TSwiftMapComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        TDelayedExecutor::WaitForDuration(GetParameters()->SleepPerMessage.Value(message.StreamId, TDuration::Zero()));
        auto builder = MakeOutputMessageBuilder(GetOrCrash(GetParameters()->PassthroughRules, message.StreamId));
        builder.Payload().SetValue(GetColumn(message, "data"), "data");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TCycleSwiftMapComputation);

////////////////////////////////////////////////////////////////////////////////

class TReducer
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/state");
    }

    void DoProcessKey(
        IInputContextPtr input,
        IOutputCollectorPtr /*output*/) override
    {
        YT_VERIFY(input->GetTimers().empty());
        if (input->GetMessages().empty()) {
            return;
        }
        auto state = StateClient_.GetState(input->GetMessages()[0]->Key);
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10) * std::ssize(input->GetMessages()));

        i64 count = state->GetColumnValue<std::optional<i64>>("count").value_or(0);

        count += std::ssize(input->GetMessages());

        TPayloadBuilder builder(state->Schema);
        builder.Set(count, "count");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TReducer);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
