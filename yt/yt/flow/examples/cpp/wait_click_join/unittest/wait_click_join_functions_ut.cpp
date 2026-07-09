#include <yt/yt/flow/examples/cpp/wait_click_join/lib/wait_click_join_functions.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/recording_output_collector.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
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

// The output message must be registered so the runtime context can validate/convert it.
YT_FLOW_DEFINE_YSON_MESSAGE(TJoinedActionMessage);

////////////////////////////////////////////////////////////////////////////////

// Group-by key schema, matching TJoinKey (hash, hit_id, hit_time).
TTableSchemaPtr JoinKeySchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        "[{name=hash;type=uint64;sort_order=ascending};"
        "{name=hit_id;type=string;sort_order=ascending};"
        "{name=hit_time;type=uint64;sort_order=ascending}]")));
}

TInputMessageConstPtr MakeHit(const TKey& key, const std::string& hitId, ui64 hitTime, const std::string& payload)
{
    return MakeTestMessage("hit", key, GetYsonMessagePayloadSchema<THitMessage>(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(hitId, "hit_id");
        builder.Payload().Set(hitTime, "hit_time");
        builder.Payload().Set(payload, "hit_payload");
    });
}

TInputMessageConstPtr MakeAction(const TKey& key, const std::string& hitId, ui64 hitTime, bool isClick, ui64 actionTime)
{
    return MakeTestMessage("action", key, GetYsonMessagePayloadSchema<TActionMessage>(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(hitId, "hit_id");
        builder.Payload().Set(hitTime, "hit_time");
        builder.Payload().Set(isClick, "is_click");
        builder.Payload().Set(actionTime, "action_time");
    });
}

// Configures the wait window on the environment; the harness Inits the function later.
void ConfigureWaitWindow(TTestStateEnvironment& stateEnv)
{
    auto parameters = New<TJoinParameters>();
    parameters->WaitForActions = TDuration::Seconds(3600);
    stateEnv.SetStaticParameters(parameters);
}

IRuntimeContextPtr MakeContext()
{
    // Declare the input streams so the input watermark mins over them (zero when unset);
    // otherwise the late-data guard drops every message.
    auto spec = New<TComputationSpec>();
    spec->InputStreamIds = {TStreamId("hit"), TStreamId("action")};

    return TTestRuntimeContextBuilder()
        .SetKeySchema(JoinKeySchema())
        .SetSpec(std::move(spec))
        .RegisterStream<TJoinedActionMessage>("joined_action")
        .Build();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWaitClickJoinExampleTest, JoinsHitAndShow)
{
    TTestStateEnvironment stateEnv(JoinKeySchema());
    ConfigureWaitWindow(stateEnv);
    TProcessFunctionTestHarness harness(stateEnv, New<TJoinFunction>(), MakeContext());

    auto key = MakeKey(ui64(42), std::string("hit-1"), ui64(100));
    harness.RunEpoch({
        MakeHit(key, "hit-1", 100, "payload-A"),
        MakeAction(key, "hit-1", 100, /*isClick*/ false, /*actionTime*/ 105),
    });

    // Nothing is emitted until the timer fires in a later epoch.
    EXPECT_TRUE(harness.GetMessages().empty());

    harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(3700), JoinKeySchema(), "timer")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    const auto& message = harness.GetMessages()[0].Message;
    EXPECT_EQ(GetColumnValue<std::string>(message, "hit_id"), "hit-1");
    EXPECT_EQ(GetColumnValue<ui64>(message, "hit_time"), 100u);
    EXPECT_EQ(GetColumnValue<bool>(message, "is_click"), false);
    EXPECT_EQ(GetColumnValue<ui64>(message, "show_time"), 105u);
    EXPECT_EQ(GetColumnValue<ui64>(message, "click_time"), 0u);
    EXPECT_EQ(GetColumnValue<std::string>(message, "hit_payload"), "payload-A");
}

TEST(TWaitClickJoinExampleTest, JoinsHitShowAndClick)
{
    TTestStateEnvironment stateEnv(JoinKeySchema());
    ConfigureWaitWindow(stateEnv);
    TProcessFunctionTestHarness harness(stateEnv, New<TJoinFunction>(), MakeContext());

    auto key = MakeKey(ui64(7), std::string("hit-2"), ui64(200));
    harness.RunEpoch({
        MakeHit(key, "hit-2", 200, "payload-B"),
        MakeAction(key, "hit-2", 200, /*isClick*/ false, /*actionTime*/ 205),
        MakeAction(key, "hit-2", 200, /*isClick*/ true, /*actionTime*/ 209),
    });

    harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(3800), JoinKeySchema(), "timer")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    const auto& message = harness.GetMessages()[0].Message;
    EXPECT_EQ(GetColumnValue<bool>(message, "is_click"), true);
    EXPECT_EQ(GetColumnValue<ui64>(message, "show_time"), 205u);
    EXPECT_EQ(GetColumnValue<ui64>(message, "click_time"), 209u);
    EXPECT_EQ(GetColumnValue<std::string>(message, "hit_payload"), "payload-B");
}

TEST(TWaitClickJoinExampleTest, DoesNotEmitWithoutShow)
{
    TTestStateEnvironment stateEnv(JoinKeySchema());
    ConfigureWaitWindow(stateEnv);
    TProcessFunctionTestHarness harness(stateEnv, New<TJoinFunction>(), MakeContext());

    auto key = MakeKey(ui64(9), std::string("hit-3"), ui64(300));
    harness.RunEpoch({MakeHit(key, "hit-3", 300, "payload-C")});

    harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(3900), JoinKeySchema(), "timer")});

    // A hit without a matching show action produces no joined message.
    EXPECT_TRUE(harness.GetMessages().empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
