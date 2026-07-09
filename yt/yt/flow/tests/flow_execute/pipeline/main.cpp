#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT::NFlow;
using namespace NYT;
using namespace NYT::NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    static inline TStreamId OutputStreamId = TStreamId("data");

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto builder = MakeOutputMessageBuilder(OutputStreamId);
        builder.Payload().SetValue(MakeUnversionedStringValue(GetColumnValue<TStringBuf>(message, "data")), "data");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReader);

////////////////////////////////////////////////////////////////////////////////

struct TCounterState
    : public NYTree::TYsonStruct
{
    i64 Count{};

    REGISTER_YSON_STRUCT(TCounterState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("count", &TThis::Count)
            .Default(0);
    }
};

class TStateWriter
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient<TCounterState>(KeyClient_, "key_state");
        initContext->AsPartition()->InitClient<TCounterState>(PartitionClient_, "partition_state");
        initContext->InitExternalStateClient(ExternalStateClient_, "/external_state");
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr /*output*/) override
    {
        auto state = KeyClient_.GetState(message->Key);
        state->Count += 1;
        PartitionClient_->Count += 1;

        auto externalState = ExternalStateClient_.GetState(message->Key);
        i64 count = externalState->GetColumnValue<std::optional<i64>>("count").value_or(0);
        TPayloadBuilder builder(externalState->Schema);
        builder.Set(count + 1, "count");
        externalState->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TCounterState> KeyClient_;
    TMutableStateClient<TCounterState> PartitionClient_;
    TMutableStateKeyClient<TSimpleExternalState> ExternalStateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TStateWriter);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    return NYT::NFlow::TSimpleRunnerProgram().Run(argc, argv);
}
