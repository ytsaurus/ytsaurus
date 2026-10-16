#include <yt/yt/flow/examples/cpp/word_count/lib/word_count_functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

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

// Register the example's YSON message so the runtime context can validate and convert it.
YT_FLOW_DEFINE_YSON_MESSAGE(TWordMessage);

////////////////////////////////////////////////////////////////////////////////

TEST(TWordCountFunctionExampleTest, TextReaderSplitsTextIntoWords)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TWordMessage>("words")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TTextReadFunction>(), context);

    auto inputSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=text;type=string}])")));
    auto message = MakeTestMessage("input", MakeKey<ui64>(0), inputSchema, [] (TMessageBuilder& builder) {
        builder.Payload().Set(std::string("hello world hello"), "text");
    });
    harness.RunEpoch({message});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 3);
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[0].Message, "word"), "hello");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[1].Message, "word"), "world");
    EXPECT_EQ(GetColumnValue<std::string>(harness.GetMessages()[2].Message, "word"), "hello");
}

TEST(TWordCountFunctionExampleTest, WordCounterIncrementsPerKey)
{
    TTestStateEnvironment stateEnv;
    auto stateSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=count;type=int64}])")));
    auto stateManager = stateEnv.RegisterExternalState("/state", stateSchema);
    TProcessFunctionTestHarness harness(stateEnv, New<TWordCountFunction>());

    auto emptySchema = New<TTableSchema>();
    auto key = MakeKey<ui64>(1);
    harness.RunEpoch({
        MakeTestMessage("words", key, emptySchema),
        MakeTestMessage("words", key, emptySchema),
        MakeTestMessage("words", MakeKey<ui64>(2), emptySchema),
    });

    TMutableStateKeyClient<TSimpleExternalState> client(stateManager);
    EXPECT_EQ(client.GetState(key)->GetColumnValue<i64>("count"), 2);
    EXPECT_EQ(client.GetState(MakeKey<ui64>(2))->GetColumnValue<i64>("count"), 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
