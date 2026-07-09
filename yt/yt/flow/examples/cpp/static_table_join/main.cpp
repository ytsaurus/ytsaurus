#include <yt/yt/flow/library/cpp/computation/job_state/job_init_context.h>
#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>
#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

#include <library/cpp/yt/string/string.h>

#include <util/string/strip.h>

using namespace NYT;
using namespace NYT::NFlow;

////////////////////////////////////////////////////////////////////////////////
// A row of the static reference table: a key and a human-entered name that
// needs normalizing before it can be used as a join attribute.

struct TReferenceRow
    : public TYsonMessage
{
    ui64 Key{};
    TString Name;

    REGISTER_YSON_STRUCT(TReferenceRow);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("name", &TThis::Name)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TReferenceRow);

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
// Output of the join: the event key enriched with the normalized name.

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
// Streams the static reference table row by row into the "reference" stream.

class TReferenceReader
    : public TSwiftOrderedSourceComputation
{
public:
    using TSwiftOrderedSourceComputation::TSwiftOrderedSourceComputation;

    void DoProcessMessage(const TMessage& message, IOutputCollectorPtr output) override
    {
        auto name = GetColumnValue<std::optional<TStringBuf>>(message, "name");
        if (!name) {
            return;
        }

        auto builder = MakeOutputMessageBuilder("reference");
        builder.Payload().Set(GetColumnValue<ui64>(message, "key"), "key");
        builder.Payload().Set(*name, "name");
        output->AddMessage(builder.Finish());
    }
};

YT_FLOW_DEFINE_COMPUTATION(TReferenceReader);

////////////////////////////////////////////////////////////////////////////////
// Applies the per-row transform (trim + lowercase) and writes the normalized
// name into the shared keyed state table.

class TReferenceLoader
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(ReferenceStateClient_, "/reference_state");
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr /*output*/) override
    {
        auto reference = ConvertToYsonMessage<TReferenceRow>(message);
        auto normalizedName = AsciiStringToLower(StripString(reference->Name));

        auto state = ReferenceStateClient_.GetState(message->Key);
        TPayloadBuilder builder(state->Schema);
        builder.Set(normalizedName, "normalized_name");
        state->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<TSimpleExternalState> ReferenceStateClient_;
};

YT_FLOW_DEFINE_COMPUTATION(TReferenceLoader);

////////////////////////////////////////////////////////////////////////////////
// Joins each realtime event against the shared reference state and emits the
// enriched row.

class TEnricher
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoInit(IJobInitContextPtr initContext) override
    {
        initContext->InitExternalStateClient(ReferenceStateJoiner_, "/reference_state");
    }

    void DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output) override
    {
        auto event = ConvertToYsonMessage<TEventRow>(message);
        auto state = ReferenceStateJoiner_.GetState(message->Key);
        if (state.IsEmpty()) {
            return;
        }

        auto enriched = New<TEnrichedRow>();
        enriched->Key = event->Key;
        enriched->Name = state->GetColumnValue<TString>("normalized_name");
        output->AddMessage(ConvertToMessage(enriched));
    }

private:
    TJoinedStateKeyClient<TSimpleExternalState> ReferenceStateJoiner_;
};

YT_FLOW_DEFINE_COMPUTATION(TEnricher);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TReferenceRow>("reference");
    builder.RegisterStream<TEventRow>("event");
    builder.RegisterStream<TEnrichedRow>("enriched");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
