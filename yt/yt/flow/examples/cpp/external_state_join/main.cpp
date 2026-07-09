#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////
// An event of the realtime stream, carrying only the key to enrich.

struct TEventRow
    : public TYsonMessage
{
    ui64 Key{};

    REGISTER_YSON_STRUCT(TEventRow);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TEventRow);

////////////////////////////////////////////////////////////////////////////////
// Output of the join: the event key enriched with the name looked up in the
// reference table.

struct TEnrichedRow
    : public TYsonMessage
{
    ui64 Key{};
    TString Name;

    REGISTER_YSON_STRUCT(TEnrichedRow);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("name", &TThis::Name)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TEnrichedRow);

////////////////////////////////////////////////////////////////////////////////
// Joins each realtime event against a pre-built dynamic reference table reached
// through a Cypress symlink. The joiner is read-only: it never writes the state,
// it only looks it up by key. Repointing the symlink atomically swaps the whole
// reference dataset under the running pipeline.

class TLookupJoin
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(ReferenceJoiner_, "/reference");
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) override
    {
        auto event = ConvertToYsonMessage<TEventRow>(message);
        auto state = ReferenceJoiner_.GetState(message->Key);
        if (state.IsEmpty()) {
            return;
        }

        auto enriched = New<TEnrichedRow>();
        enriched->Key = event->Key;
        enriched->Name = state->GetColumnValue<TString>("name");
        output->AddMessage(ConvertToMessage(enriched));
    }

private:
    TJoinedStateKeyClient<TSimpleExternalState> ReferenceJoiner_;
};

YT_FLOW_DEFINE_COMPUTATION(TLookupJoin);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TEventRow>("event");
    builder.RegisterStream<TEnrichedRow>("enriched");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
