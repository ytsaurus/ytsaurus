#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/multiplexer/dynamic_table_multiplexer_computation.h>

#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/simple_runner_program.h>

using namespace NYT::NFlow;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

// Input message: (key, payload).
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

// Output message: (key, secondary_key, region, payload).
struct TRowMessage
    : public TYsonMessage
{
    std::string Key;
    i64 SecondaryKey = 0;
    std::string Region;
    std::string Payload;

    REGISTER_YSON_STRUCT(TRowMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key", &TThis::Key)
            .Default();
        registrar.Parameter("secondary_key", &TThis::SecondaryKey)
            .Default(0);
        registrar.Parameter("region", &TThis::Region)
            .Default();
        registrar.Parameter("payload", &TThis::Payload)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TRowMessage);

////////////////////////////////////////////////////////////////////////////////

// User state: payload from the latest input message, forwarded into every output row.
struct TTestUserState
    : public NYTree::TYsonStruct
{
    std::string Payload;

    REGISTER_YSON_STRUCT(TTestUserState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("payload", &TThis::Payload)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestMultiplexerComputation
    : public TDynamicTableMultiplexerComputation<TTestUserState>
{
public:
    using TDynamicTableMultiplexerComputation::TDynamicTableMultiplexerComputation;

    void DoOnInputMessage(
        const TKey& /*key*/,
        const TInputMessageConstPtr& message,
        TStateAccessor<TTestUserState>& userState) override
    {
        auto input = ConvertToYsonMessage<TKeyMessage>(message);
        userState->Payload = input->Payload;
    }

    void DoBuildOutputForRow(
        const TKey& key,
        const TPayload& rowPayload,
        const NTableClient::TTableSchemaPtr& rowSchema,
        TStateAccessor<TTestUserState>& userState,
        IOutputCollectorPtr output) override
    {
        auto inputKey = ConvertToYsonKey<TKeyMessage>(key);

        auto row = New<TRowMessage>();
        row->Key = inputKey->Key;
        row->SecondaryKey = GetColumnValue<i64>(rowPayload, rowSchema, "secondary_key");
        row->Region = GetColumnValue<std::string>(rowPayload, rowSchema, "region");
        row->Payload = userState->Payload;

        output->AddMessage(ConvertToMessage(row));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TTestMultiplexerComputation);

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    NYT::NFlow::Initialize(argc, argv);
    TSimpleSpecBuilder builder;
    builder.RegisterStream<TKeyMessage>("keys");
    builder.RegisterStream<TRowMessage>("rows");
    return NYT::NFlow::TSimpleRunnerProgram(std::move(builder)).Run(argc, argv);
}
