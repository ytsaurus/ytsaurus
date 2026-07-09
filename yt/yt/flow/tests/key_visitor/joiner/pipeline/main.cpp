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

struct TMirrorState
    : public NYTree::TYsonStruct
{
    std::string Payload;

    REGISTER_YSON_STRUCT(TMirrorState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("payload", &TThis::Payload)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMirrorComputation
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitClient<TMirrorState>(MirroredState_, "/mirror");
        initContext->InitExternalStateClient(SourceState_, "/source");
    }

    void DoProcessVisit(
        const TVisit& visit,
        IOutputCollectorPtr /*output*/) override
    {
        auto src = SourceState_.GetState(visit.Key);
        if (!src.IsInitialized()) {
            return;
        }
        auto mirror = MirroredState_.GetState(visit.Key);
        if (src.IsEmpty()) {
            mirror.Clear();
        } else {
            mirror->Payload = src->GetColumnValue<std::optional<std::string>>("payload").value_or(std::string{});
        }
    }

private:
    TMutableStateKeyClient<TMirrorState> MirroredState_;
    TJoinedStateKeyClient<TSimpleExternalState> SourceState_;
};

YT_FLOW_DEFINE_COMPUTATION(TMirrorComputation);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TKeyMessage>("keys");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
