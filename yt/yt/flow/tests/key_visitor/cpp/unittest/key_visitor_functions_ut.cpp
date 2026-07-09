#include <yt/yt/flow/tests/key_visitor/cpp/lib/key_visitor_functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>

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
using namespace NYT::NFlow::NKeyVisitorTest;

// The functions deserialize the visited key into TKeyMessage (whose `key` is a string), so the
// key schema is a single string column rather than the default uint64.
YT_FLOW_DEFINE_YSON_MESSAGE(TKeyMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TVisitMessage);

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr StringKeySchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=key;type=string}])")));
}

TTableSchemaPtr KeyMessageSchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=key;type=string};{name=payload;type=string}])")));
}

TInputMessageConstPtr MakeKeyMessage(const TKey& key, const std::string& keyId, const std::string& payload)
{
    return MakeTestMessage("keys", key, KeyMessageSchema(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set<TString>(TString(keyId), "key");
        builder.Payload().Set<TString>(TString(payload), "payload");
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyVisitorFunctionTest, InternalVisitEmitsStoredPayload)
{
    auto keySchema = StringKeySchema();
    TTestStateEnvironment stateEnv(keySchema);
    auto context = TTestRuntimeContextBuilder()
        .SetKeySchema(keySchema)
        .RegisterStream<TVisitMessage>("visits")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TVisitTesterFunction>(), context);

    // The message stores the payload; each visit re-emits it with a monotonically increasing
    // visit_index (messages are dispatched before visits within the epoch).
    auto key = MakeKey(TString("user-1"));
    harness.RunEpoch(
        {MakeKeyMessage(key, "user-1", "hello")},
        {},
        {MakeTestVisit(key, "visit_iter"), MakeTestVisit(key, "visit_iter")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 2);
    EXPECT_EQ(GetColumnValue<TString>(harness.GetMessages()[0].Message, "key"), "user-1");
    EXPECT_EQ(GetColumnValue<TString>(harness.GetMessages()[0].Message, "payload"), "hello");
    EXPECT_EQ(GetColumnValue<i64>(harness.GetMessages()[0].Message, "visit_index"), 1);
    EXPECT_EQ(GetColumnValue<i64>(harness.GetMessages()[1].Message, "visit_index"), 2);
}

TEST(TKeyVisitorFunctionTest, InternalVisitOnEmptyStateIsNoOp)
{
    auto keySchema = StringKeySchema();
    TTestStateEnvironment stateEnv(keySchema);
    auto context = TTestRuntimeContextBuilder()
        .SetKeySchema(keySchema)
        .RegisterStream<TVisitMessage>("visits")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TVisitTesterFunction>(), context);

    // A visit for a key that received no message reads a preloaded-but-empty state — no output.
    harness.RunEpoch({}, {}, {MakeTestVisit(MakeKey(TString("ghost")), "visit_iter")});

    EXPECT_TRUE(harness.GetMessages().empty());
}

TEST(TKeyVisitorFunctionTest, ExternalVisitEmitsStoredPayload)
{
    auto keySchema = StringKeySchema();
    TTestStateEnvironment stateEnv(keySchema);
    auto stateSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=payload;type=string};{name=visit_index;type=int64}])")));
    stateEnv.RegisterExternalState("/user-state-external", stateSchema);

    auto context = TTestRuntimeContextBuilder()
        .SetKeySchema(keySchema)
        .RegisterStream<TVisitMessage>("visits")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TExternalVisitTesterFunction>(), context);

    auto key = MakeKey(TString("user-2"));
    harness.RunEpoch(
        {MakeKeyMessage(key, "user-2", "world")},
        {},
        {MakeTestVisit(key, "visit_iter"), MakeTestVisit(key, "visit_iter")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 2);
    EXPECT_EQ(GetColumnValue<TString>(harness.GetMessages()[0].Message, "key"), "user-2");
    EXPECT_EQ(GetColumnValue<TString>(harness.GetMessages()[0].Message, "payload"), "world");
    EXPECT_EQ(GetColumnValue<i64>(harness.GetMessages()[0].Message, "visit_index"), 1);
    EXPECT_EQ(GetColumnValue<i64>(harness.GetMessages()[1].Message, "visit_index"), 2);
}

TEST(TKeyVisitorFunctionTest, ExternalVisitOnEmptyStateIsNoOp)
{
    auto keySchema = StringKeySchema();
    TTestStateEnvironment stateEnv(keySchema);
    auto stateSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=payload;type=string};{name=visit_index;type=int64}])")));
    stateEnv.RegisterExternalState("/user-state-external", stateSchema);

    auto context = TTestRuntimeContextBuilder()
        .SetKeySchema(keySchema)
        .RegisterStream<TVisitMessage>("visits")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TExternalVisitTesterFunction>(), context);

    // A visit for a key that received no message reads a lazily-created empty state — no output.
    harness.RunEpoch({}, {}, {MakeTestVisit(MakeKey(TString("ghost")), "visit_iter")});

    EXPECT_TRUE(harness.GetMessages().empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
