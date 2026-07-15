#include <yt/yt/flow/examples/cpp/retryable_async_request/lib/retryable_async_request_functions.h>

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
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NYT::NFlow::NTesting;
using namespace NYT::NFlow::NExample;

////////////////////////////////////////////////////////////////////////////////

// Register the emitted YSON messages so the runtime context can validate and convert them.
YT_FLOW_DEFINE_YSON_MESSAGE(TRequestMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TResponseMessage);

////////////////////////////////////////////////////////////////////////////////

TInputMessageConstPtr MakeRequest(const TKey& key, ui64 requestId, ui64 payloadKey, const std::string& request)
{
    return MakeTestMessage("request", key, GetYsonMessagePayloadSchema<TRequestMessage>(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(requestId, "request_id");
        builder.Payload().Set(payloadKey, "key");
        builder.Payload().Set(request, "request");
    });
}

TInputMessageConstPtr MakeResponse(const TKey& key, ui64 requestId, ui64 payloadKey, i64 length)
{
    return MakeTestMessage("response", key, GetYsonMessagePayloadSchema<TResponseMessage>(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(requestId, "request_id");
        builder.Payload().Set(payloadKey, "key");
        builder.Payload().Set(length, "length");
    });
}

TInputMessageConstPtr MakeEvent(const TKey& key, ui64 payloadKey, const std::string& data)
{
    return MakeTestMessage("event", key, GetYsonMessagePayloadSchema<TEventMessage>(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(payloadKey, "key");
        builder.Payload().Set(data, "data");
    });
}

TTableSchemaPtr TotalLengthStateSchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=total_length;type=int64}])")));
}

////////////////////////////////////////////////////////////////////////////////

// RequestId 1 fails twice ((1+0)%3, (1+1)%3) then succeeds on the third attempt ((1+2)%3 == 0).
TEST(TRetryableAsyncRequestExampleTest, RequestProcessorRetriesUntilSuccess)
{
    constexpr ui64 CurrentTimestamp = 100;
    constexpr ui64 ExpectedRetryTrigger = CurrentTimestamp + TRequestProcessor::Delay;

    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TResponseMessage>("response")
        .SetCurrentTimestamp(TSystemTimestamp(CurrentTimestamp))
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TRequestProcessor>(), context);

    auto key = MakeKey<ui64>(1);

    // First attempt fails and schedules a retry timer.
    harness.RunEpoch({MakeRequest(key, /*requestId*/ 1, /*key*/ 42, "hello")});
    EXPECT_TRUE(harness.GetMessages().empty());
    ASSERT_EQ(std::ssize(harness.GetTimers()), 1);
    EXPECT_EQ(harness.GetTimers()[0].TriggerTimestamp, TSystemTimestamp(ExpectedRetryTrigger));

    // Second attempt (via the timer) fails as well and reschedules.
    harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(ExpectedRetryTrigger))});
    EXPECT_TRUE(harness.GetMessages().empty());
    ASSERT_EQ(std::ssize(harness.GetTimers()), 1);

    // Third attempt succeeds and emits the response.
    harness.RunEpoch({}, {MakeTestTimer(key, TSystemTimestamp(ExpectedRetryTrigger))});
    EXPECT_TRUE(harness.GetTimers().empty());
    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    const auto& message = harness.GetMessages()[0].Message;
    EXPECT_EQ(GetColumnValue<ui64>(message, "request_id"), 1u);
    EXPECT_EQ(GetColumnValue<ui64>(message, "key"), 42u);
    EXPECT_EQ(GetColumnValue<i64>(message, "length"), 5);
}

// RequestId 3 succeeds immediately ((3+0)%3 == 0).
TEST(TRetryableAsyncRequestExampleTest, RequestProcessorSucceedsImmediately)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TResponseMessage>("response")
        .SetCurrentTimestamp(TSystemTimestamp(0))
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TRequestProcessor>(), context);

    auto key = MakeKey<ui64>(3);
    harness.RunEpoch({MakeRequest(key, /*requestId*/ 3, /*key*/ 7, "data")});

    EXPECT_TRUE(harness.GetTimers().empty());
    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    EXPECT_EQ(GetColumnValue<i64>(harness.GetMessages()[0].Message, "length"), 4);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TRetryableAsyncRequestExampleTest, StateKeeperAccumulatesResponseLengths)
{
    TTestStateEnvironment stateEnv;
    auto stateManager = stateEnv.RegisterExternalState("/state", TotalLengthStateSchema());
    TProcessFunctionTestHarness harness(stateEnv, New<TStateKeeper>());

    auto key = MakeKey<ui64>(1);
    harness.RunEpoch({
        MakeResponse(key, /*requestId*/ 10, /*key*/ 1, /*length*/ 5),
        MakeResponse(key, /*requestId*/ 11, /*key*/ 1, /*length*/ 7),
        MakeResponse(MakeKey<ui64>(2), /*requestId*/ 12, /*key*/ 2, /*length*/ 3),
    });

    TMutableStateKeyClient<TSimpleExternalState> client(stateManager);
    EXPECT_EQ(client.GetState(key)->GetColumnValue<i64>("total_length"), 12);
    EXPECT_EQ(client.GetState(MakeKey<ui64>(2))->GetColumnValue<i64>("total_length"), 3);
}

TEST(TRetryableAsyncRequestExampleTest, StateKeeperEmitsRequestForEvent)
{
    TTestStateEnvironment stateEnv;
    stateEnv.RegisterExternalState("/state", TotalLengthStateSchema());
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TRequestMessage>("request")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TStateKeeper>(), context);

    auto key = MakeKey<ui64>(1);
    harness.RunEpoch({MakeEvent(key, /*key*/ 55, "payload")});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    const auto& message = harness.GetMessages()[0].Message;
    EXPECT_EQ(GetColumnValue<ui64>(message, "key"), 55u);
    EXPECT_EQ(GetColumnValue<std::string>(message, "request"), "payload");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
