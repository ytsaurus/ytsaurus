#include <yt/yt/flow/examples/cpp/shuffle/lib/shuffle_functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NYT::NFlow::NTesting;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

// The shuffle pipeline declares stream schemas in the spec and has no YSON message structs.
// This test-only message mirrors the "event" stream schema so the runtime context can hand
// TQueueReader an output message builder for it.
struct TEventMessage
    : public TYsonMessage
{
    ui64 KeyA{};
    ui64 KeyB{};
    ui64 KeyC{};
    ui64 KeyD{};
    std::string Value;

    REGISTER_YSON_STRUCT(TEventMessage);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("key_a", &TThis::KeyA)
            .Default();
        registrar.Parameter("key_b", &TThis::KeyB)
            .Default();
        registrar.Parameter("key_c", &TThis::KeyC)
            .Default();
        registrar.Parameter("key_d", &TThis::KeyD)
            .Default();
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TEventMessage);

////////////////////////////////////////////////////////////////////////////////

TEST(TShuffleExampleTest, QueueReaderParsesJsonIntoEvent)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TEventMessage>("event")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TQueueReader>(), context);

    auto inputSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=data;type=string}])")));
    auto message = MakeTestMessage("queue", MakeKey<ui64>(0), inputSchema, [] (TMessageBuilder& builder) {
        builder.Payload().Set(std::string(
            R"({"key_a":1,"key_b":2,"key_c":3,"key_d":4,"value":"hello"})"),
            "data");
    });
    harness.RunEpoch({message});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    const auto& event = harness.GetMessages()[0].Message;
    EXPECT_EQ(GetColumnValue<ui64>(event, "key_a"), 1u);
    EXPECT_EQ(GetColumnValue<ui64>(event, "key_b"), 2u);
    EXPECT_EQ(GetColumnValue<ui64>(event, "key_c"), 3u);
    EXPECT_EQ(GetColumnValue<ui64>(event, "key_d"), 4u);
    EXPECT_EQ(GetColumnValue<std::string>(event, "value"), "hello");
}

TEST(TShuffleExampleTest, ReducerCountsPerKey)
{
    TTestStateEnvironment stateEnv;
    auto stateSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=count;type=int64}])")));
    auto stateManager = stateEnv.RegisterExternalState("/state", stateSchema);
    TProcessFunctionTestHarness harness(stateEnv, New<TReducer>());

    auto emptySchema = New<TTableSchema>();
    auto key = MakeKey<ui64>(1);
    harness.RunEpoch({
        MakeTestMessage("event_a", key, emptySchema),
        MakeTestMessage("event_b", key, emptySchema),
        MakeTestMessage("event_c", MakeKey<ui64>(2), emptySchema),
    });

    TMutableStateKeyClient<TSimpleExternalState> client(stateManager);
    EXPECT_EQ(client.GetState(key)->GetColumnValue<i64>("count"), 2);
    EXPECT_EQ(client.GetState(MakeKey<ui64>(2))->GetColumnValue<i64>("count"), 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
