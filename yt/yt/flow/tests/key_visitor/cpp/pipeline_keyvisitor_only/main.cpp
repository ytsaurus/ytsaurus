#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT::NFlow;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

struct TKeyMessage
    : public TYsonMessage
{
    std::string Key;

    REGISTER_YSON_STRUCT(TKeyMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TKeyMessage);

////////////////////////////////////////////////////////////////////////////////

struct TVisitMessage
    : public TYsonMessage
{
    std::string Key;
    std::string Payload;
    i64 VisitIndex = 0;

    REGISTER_YSON_STRUCT(TVisitMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("payload", &TThis::Payload)
            .Default();
        registrar.Parameter("visit_index", &TThis::VisitIndex)
            .Default(0);
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TVisitMessage);

////////////////////////////////////////////////////////////////////////////////

// Key-visitor-only computation: no input or source streams, work comes solely
// from the key_visitor sweep over an external state table.
class TKeyVisitorOnlyComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(StateClient_, "/user-state");
    }

    void DoProcessVisit(
        const TVisit& visit,
        IOutputCollectorPtr output) override
    {
        auto state = StateClient_.GetState(visit.Key);
        if (state.IsEmpty()) {
            return;
        }
        const auto payload = state->GetColumnValue<std::optional<std::string>>("payload").value_or(std::string{});
        const auto newVisitIndex = state->GetColumnValue<std::optional<i64>>("visit_index").value_or(0) + 1;
        TPayloadBuilder builder(state->Schema);
        builder.Set(payload, "payload");
        builder.Set(newVisitIndex, "visit_index");
        state->Payload = builder.Finish();

        auto ysonKey = ConvertToYsonKey<TKeyMessage>(visit.Key);
        auto outputMessage = New<TVisitMessage>();
        outputMessage->Key = ysonKey->Key;
        outputMessage->Payload = payload;
        outputMessage->VisitIndex = newVisitIndex;
        output->AddMessage(ConvertToMessage(outputMessage));
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> StateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TKeyVisitorOnlyComputation);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TVisitMessage>("visits");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
