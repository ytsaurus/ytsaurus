#include <yt/yt/flow/examples/cpp/async_request/lib/async_request_functions.h>

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

// Register the example's YSON messages so the runtime context can validate and convert them.
YT_FLOW_DEFINE_YSON_MESSAGE(TEventMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TRequestMessage);
YT_FLOW_DEFINE_YSON_MESSAGE(TResponseMessage);

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncRequestExampleTest, RequestProcessorEmitsResponseLength)
{
    TTestStateEnvironment stateEnv;
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TResponseMessage>("response")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TRequestProcessor>(), context);

    auto message = MakeTestMessage(
        "request",
        MakeKey<ui64>(42),
        GetYsonMessagePayloadSchema<TRequestMessage>(),
        [] (TMessageBuilder& builder) {
            builder.Payload().Set(ui64(7), "request_id");
            builder.Payload().Set(ui64(1), "key");
            builder.Payload().Set(std::string("hello world"), "request");
        });
    harness.RunEpoch({message});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    const auto& response = harness.GetMessages()[0].Message;
    EXPECT_EQ(GetColumnValue<ui64>(response, "request_id"), 7u);
    EXPECT_EQ(GetColumnValue<ui64>(response, "key"), 1u);
    EXPECT_EQ(GetColumnValue<i64>(response, "length"), std::ssize(std::string("hello world")));
}

TEST(TAsyncRequestExampleTest, StateKeeperEmitsRequestForEvent)
{
    TTestStateEnvironment stateEnv;
    stateEnv.RegisterExternalState(
        "/state",
        ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R"([{name=total_length;type=int64}])"))));
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TRequestMessage>("request")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TStateKeeper>(), context);

    auto message = MakeTestMessage(
        "event",
        MakeKey<ui64>(3),
        GetYsonMessagePayloadSchema<TEventMessage>(),
        [] (TMessageBuilder& builder) {
            builder.Payload().Set(ui64(3), "key");
            builder.Payload().Set(std::string("payload"), "data");
        });
    harness.RunEpoch({message});

    ASSERT_EQ(std::ssize(harness.GetMessages()), 1);
    const auto& request = harness.GetMessages()[0].Message;
    EXPECT_EQ(GetColumnValue<ui64>(request, "key"), 3u);
    EXPECT_EQ(GetColumnValue<std::string>(request, "request"), "payload");
}

TEST(TAsyncRequestExampleTest, StateKeeperAccumulatesResponseLength)
{
    auto stateSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        R"([{name=total_length;type=int64}])")));
    TTestStateEnvironment stateEnv;
    auto stateManager = stateEnv.RegisterExternalState("/state", stateSchema);
    auto context = TTestRuntimeContextBuilder()
        .RegisterStream<TRequestMessage>("request")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TStateKeeper>(), context);

    auto key = MakeKey<ui64>(1);
    auto makeResponse = [&] (i64 length) {
        return MakeTestMessage(
            "response",
            key,
            GetYsonMessagePayloadSchema<TResponseMessage>(),
            [length] (TMessageBuilder& builder) {
                builder.Payload().Set(ui64(0), "request_id");
                builder.Payload().Set(ui64(1), "key");
                builder.Payload().Set(length, "length");
            });
    };
    harness.RunEpoch({makeResponse(10), makeResponse(5)});

    EXPECT_TRUE(harness.GetMessages().empty());

    TMutableStateKeyClient<TSimpleExternalState> client(stateManager);
    EXPECT_EQ(client.GetState(key)->GetColumnValue<i64>("total_length"), 15);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
