#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT::NFlow;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

// Input messages keyed by `key` carry a payload that the swift computation
// stores in its per-key internal state.
struct TKeyMessage
    : public TYsonMessage
{
    std::string Key;
    std::string Payload;

    REGISTER_YSON_STRUCT(TKeyMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("payload", &TThis::Payload)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TKeyMessage);

////////////////////////////////////////////////////////////////////////////////

// Per-key internal state; the key visitor iterates over its keys.
struct TUserState
    : public NYTree::TYsonStruct
{
    std::string Payload;

    REGISTER_YSON_STRUCT(TUserState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("payload", &TThis::Payload)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

// Swift-map computation with a key-visitor stream: messages store their
// payload into internal state, visits copy it into external state and bump
// `visit_count` there. Visits affect state only and never produce output
// messages, so every row of the external state table is visit-driven.
class TSwiftVisitTesterComputation
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::TSwiftMapComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient<TUserState>(InternalStateClient_, "user_state");
        initContext->InitExternalStateClient(ExternalStateClient_, "/state");
    }

    void DoProcessMessage(
        const TInputMessageConstPtr& message,
        IOutputCollectorPtr /*output*/) override
    {
        auto ysonMessage = ConvertToYsonMessage<TKeyMessage>(message);
        auto state = InternalStateClient_.GetState(message->Key);
        state->Payload = ysonMessage->Payload;

        // Reset visit_count so the test's `visit_count >= 1` assertion proves
        // a visit happened after the last ingested payload, not on an earlier
        // pass.
        auto externalState = ExternalStateClient_.GetState(message->Key);
        TPayloadBuilder builder(externalState->Schema);
        builder.Set(std::string{}, "payload");
        builder.Set(i64(0), "visit_count");
        externalState->Payload = builder.Finish();
    }

    void DoProcessVisit(
        const TVisit& visit,
        IOutputCollectorPtr /*output*/) override
    {
        auto internalState = InternalStateClient_.GetState(visit.Key);
        if (internalState.IsEmpty()) {
            return;
        }

        auto externalState = ExternalStateClient_.GetState(visit.Key);
        i64 visitCount = externalState->GetColumnValue<std::optional<i64>>("visit_count").value_or(0);

        TPayloadBuilder builder(externalState->Schema);
        builder.Set(internalState->Payload, "payload");
        builder.Set(visitCount + 1, "visit_count");
        externalState->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TUserState> InternalStateClient_;
    TMutableStateKeyClient<TSimpleExternalState> ExternalStateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TSwiftVisitTesterComputation);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TKeyMessage>("keys");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
