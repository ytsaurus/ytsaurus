#include <yt/yt/flow/examples/cpp/file_parsing/lib/file_parsing_functions.h>
#include <yt/yt/flow/examples/cpp/file_parsing/lib/messages.h>
#include <yt/yt/flow/examples/cpp/file_parsing/lib/parse_executor_resource.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>
#include <yt/yt/flow/library/cpp/process_function/testing/process_function_test_harness.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_runtime_context.h>
#include <yt/yt/flow/library/cpp/process_function/testing/test_state_environment.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/system/type_name.h>

namespace NYT::NFlow::NFileParsingExample {
namespace {

using namespace NConcurrency;
using namespace NTesting;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TParseTask);
YT_FLOW_DEFINE_YSON_MESSAGE(TParsedFile);
YT_FLOW_DEFINE_YSON_MESSAGE(TFailedFile);

YT_FLOW_DEFINE_RESOURCE(TParseExecutorResource);

////////////////////////////////////////////////////////////////////////////////

constexpr i64 MaxAttempts = 2;

TParseExecutorResourcePtr CreateResource(int slotCount, TDuration maxQueueWait)
{
    auto context = New<TResourceContext>();
    context->ResourceId = ParseExecutorResourceId;
    context->ResourceSpec = ConvertTo<TResourceSpecPtr>(TYsonString(Format("{resource_class_name=%Qv; parameters={slot_count=%v; max_queue_wait=%v; parse_time_per_size_unit=1}}",
        TypeName<TParseExecutorResource>(),
        slotCount,
        maxQueueWait.MilliSeconds())));
    auto dynamicContext = New<TDynamicResourceContext>();
    dynamicContext->DynamicResourceSpec = New<TDynamicResourceSpec>();

    auto resource = New<TParseExecutorResource>(std::move(context), std::move(dynamicContext));
    WaitFor(resource->Load({}))
        .ThrowOnError();
    return resource;
}

TInputMessageConstPtr MakeRequest(ui64 key, const std::string& url, i64 size, i64 attempt = 0)
{
    return MakeTestMessage("requests", MakeKey(key), GetYsonMessagePayloadSchema<TParseTask>(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(url, "url");
        builder.Payload().Set(size, "size");
        builder.Payload().Set(attempt, "attempt");
        builder.Payload().Set(ui64(1'000), "first_seen_at");
    });
}

TInputTimerConstPtr MakeParseTimer(ui64 key)
{
    return MakeTestTimer(MakeKey(key), TSystemTimestamp(1), DefaultTestKeySchema(), ParseTimerStreamId);
}

//! Runs #TParseFunction on |resource| through whole epochs.
class TParseFunctionTest
    : public ::testing::Test
{
protected:
    TTestStateEnvironment Environment_;
    std::optional<TProcessFunctionTestHarness> Harness_;

    void CreateHarness(const TParseExecutorResourcePtr& resource)
    {
        auto parameters = New<TParseFunctionParameters>();
        parameters->MaxAttempts = MaxAttempts;
        Environment_.SetStaticParameters(parameters);
        Environment_.RegisterStaticResource(ParseExecutorResourceId, resource);
        auto context = TTestRuntimeContextBuilder()
            .RegisterStream<TParsedFile>(ParsedStreamId)
            .RegisterStream<TParseTask>(RetriesStreamId)
            .RegisterStream<TFailedFile>(FailedStreamId)
            .SetCurrentTimestamp(TSystemTimestamp(1))
            .Build();
        Harness_.emplace(Environment_, New<TParseFunction>(), std::move(context));
    }

    //! Runs the requests through their timers to the results and returns the outputs of the last epoch.
    std::vector<TMessage> Parse(std::vector<TInputMessageConstPtr> requests, const std::vector<ui64>& keys)
    {
        Harness_->RunEpoch(std::move(requests));
        EXPECT_EQ(std::ssize(Harness_->GetTimers()), std::ssize(keys));

        std::vector<TInputTimerConstPtr> timers;
        for (auto key : keys) {
            timers.push_back(MakeParseTimer(key));
        }
        Harness_->RunEpoch({}, std::move(timers));
        EXPECT_EQ(std::ssize(Harness_->GetTasks()), std::ssize(keys));

        Harness_->RunEpoch({}, {}, {});
        std::vector<TMessage> messages;
        for (const auto& recorded : Harness_->GetMessages()) {
            messages.push_back(recorded.Message);
        }
        return messages;
    }
};

const TMessage& FindByUrl(const std::vector<TMessage>& messages, const std::string& url)
{
    auto it = std::find_if(messages.begin(), messages.end(), [&] (const TMessage& message) {
        return GetColumnValue<std::string>(message, "url") == url;
    });
    YT_VERIFY(it != messages.end());
    return *it;
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TParseFunctionTest, DuplicateInFlightIsAbsorbed)
{
    CreateHarness(CreateResource(/*slotCount*/ 2, TDuration::Seconds(60)));

    auto messages = Parse({MakeRequest(1, "a", 1), MakeRequest(1, "a", 1)}, {1});
    ASSERT_EQ(std::ssize(messages), 1);
    EXPECT_EQ(messages[0].StreamId, ParsedStreamId);

    // A duplicate after the result is parsed again.
    messages = Parse({MakeRequest(1, "a", 1)}, {1});
    ASSERT_EQ(std::ssize(messages), 1);
    EXPECT_EQ(messages[0].StreamId, ParsedStreamId);
}

TEST_F(TParseFunctionTest, ParsedAndFailedFiles)
{
    CreateHarness(CreateResource(/*slotCount*/ 2, TDuration::Seconds(60)));

    auto messages = Parse(
        {
            MakeRequest(1, "good", 3),
            MakeRequest(2, "negative", -1),
        },
        {1, 2});
    ASSERT_EQ(std::ssize(messages), 2);

    const auto& parsed = FindByUrl(messages, "good");
    EXPECT_EQ(parsed.StreamId, ParsedStreamId);
    EXPECT_EQ(GetColumnValue<i64>(parsed, "row_count"), 3);
    EXPECT_EQ(GetColumnValue<i64>(parsed, "attempt"), 0);
    EXPECT_FALSE(GetColumnValue<std::string>(parsed, "worker").empty());

    // A parse error is not a rejection: the file fails at once.
    const auto& failed = FindByUrl(messages, "negative");
    EXPECT_EQ(failed.StreamId, FailedStreamId);
    EXPECT_EQ(GetColumnValue<i64>(failed, "attempt"), 0);
    EXPECT_THAT(GetColumnValue<std::string>(failed, "error"), ::testing::HasSubstr("negative size"));
}

TEST_F(TParseFunctionTest, RejectedFilesGoToNextAttemptOrFail)
{
    // The blocker holds the only slot, so the executor rejects every task at once.
    auto resource = CreateResource(/*slotCount*/ 1, TDuration::Zero());
    auto blocker = New<TParseTask>();
    blocker->Size = 1'000'000;
    auto blockerResult = resource->Parse(blocker);
    CreateHarness(resource);

    auto messages = Parse({MakeRequest(1, "retried", 5), MakeRequest(2, "last", 5, MaxAttempts - 1)}, {1, 2});
    ASSERT_EQ(std::ssize(messages), 2);

    const auto& retry = FindByUrl(messages, "retried");
    EXPECT_EQ(retry.StreamId, RetriesStreamId);
    EXPECT_EQ(GetColumnValue<i64>(retry, "attempt"), 1);
    EXPECT_EQ(GetColumnValue<i64>(retry, "size"), 5);
    EXPECT_EQ(GetColumnValue<ui64>(retry, "first_seen_at"), 1'000u);

    const auto& failed = FindByUrl(messages, "last");
    EXPECT_EQ(failed.StreamId, FailedStreamId);
    EXPECT_EQ(GetColumnValue<i64>(failed, "attempt"), MaxAttempts - 1);
    EXPECT_THAT(GetColumnValue<std::string>(failed, "error"), ::testing::HasSubstr("rejected in all 2 attempts"));

    blockerResult.Cancel(TError("Test is over"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NFileParsingExample
