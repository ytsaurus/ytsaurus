#include <yt/yt/flow/examples/cpp/static_table_join/lib/static_table_join_functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/in_memory_external_state_manager.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NYT::NFlow::NTesting;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

// Register the example's YSON messages so the runtime context can validate and convert them.
YT_FLOW_DEFINE_YSON_MESSAGE(TReferenceRow);
YT_FLOW_DEFINE_YSON_MESSAGE(TEventRow);
YT_FLOW_DEFINE_YSON_MESSAGE(TEnrichedRow);

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr ReferenceStateSchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=normalized_name;type=string}])")));
}

TTableSchemaPtr ReferenceTableSchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=key;type=uint64};{name=name;type=string}])")));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticTableJoinExampleTest, ReferenceReaderEmitsRows)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TReferenceRow>("reference")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TReferenceReader>(), context);

    auto message = MakeTestMessage("reference_table", MakeKey<ui64>(0), ReferenceTableSchema(), [] (TMessageBuilder& builder) {
        builder.Payload().Set(ui64(42), "key");
        builder.Payload().Set(std::string("Alice"), "name");
    });
    harness.RunEpoch({message});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<ui64>(harness.GetMessages()[0].Message, "key"), 42u);
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "name"), "Alice");
}

TEST(TStaticTableJoinExampleTest, ReferenceReaderSkipsRowsWithoutName)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TReferenceRow>("reference")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TReferenceReader>(), context);

    // The "name" column is left unset (null), so the row must be dropped.
    auto message = MakeTestMessage("reference_table", MakeKey<ui64>(0), ReferenceTableSchema(), [] (TMessageBuilder& builder) {
        builder.Payload().Set(ui64(42), "key");
    });
    harness.RunEpoch({message});

    EXPECT_TRUE(harness.GetMessages().empty());
}

TEST(TStaticTableJoinExampleTest, ReferenceLoaderNormalizesName)
{
    TTestStateEnvironment stateEnv;
    auto stateManager = stateEnv.RegisterExternalState("/reference_state", ReferenceStateSchema());
    TProcessFunctionTestHarness harness(stateEnv, New<TReferenceLoader>());

    auto key = MakeKey<ui64>(1);
    auto message = MakeTestMessage("reference", key, GetYsonMessagePayloadSchema<TReferenceRow>(), [] (TMessageBuilder& builder) {
        builder.Payload().Set(ui64(1), "key");
        builder.Payload().Set(std::string("  Alice  "), "name");
    });
    harness.RunEpoch({message});

    TMutableStateKeyClient<TSimpleExternalState> client(stateManager);
    EXPECT_EQ(client.GetState(key)->GetColumnValue<std::string>("normalized_name"), "alice");
}

TEST(TStaticTableJoinExampleTest, EnricherJoinsReferenceState)
{
    TTestStateEnvironment stateEnv;
    auto joiner = stateEnv.RegisterExternalStateJoiner("/reference_state", ReferenceStateSchema());

    // Seed the read-only reference state for the joined key.
    auto key = MakeKey<ui64>(7);
    {
        auto& state = joiner->GetMutableState(key)->Get();
        TPayloadBuilder builder(state.Schema);
        builder.Set(std::string("bob"), "normalized_name");
        state.Payload = builder.Finish();
    }

    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TEnrichedRow>("enriched")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TEnricher>(), context);

    auto message = MakeTestMessage("event", key, GetYsonMessagePayloadSchema<TEventRow>(), [] (TMessageBuilder& builder) {
        builder.Payload().Set(ui64(7), "key");
    });
    harness.RunEpoch({message});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<ui64>(harness.GetMessages()[0].Message, "key"), 7u);
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "name"), "bob");
}

TEST(TStaticTableJoinExampleTest, EnricherDropsEventsWithoutReferenceState)
{
    TTestStateEnvironment stateEnv;
    stateEnv.RegisterExternalStateJoiner("/reference_state", ReferenceStateSchema());

    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TEnrichedRow>("enriched")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TEnricher>(), context);

    // No reference state was seeded for this key, so the event must be dropped.
    auto key = MakeKey<ui64>(99);
    auto message = MakeTestMessage("event", key, GetYsonMessagePayloadSchema<TEventRow>(), [] (TMessageBuilder& builder) {
        builder.Payload().Set(ui64(99), "key");
    });
    harness.RunEpoch({message});

    EXPECT_TRUE(harness.GetMessages().empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
