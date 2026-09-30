#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <class TSink>
struct TTestSinkEnvironment
{
    TIntrusivePtr<TSink> Sink;
    IStatusProfilerPtr StatusProfiler;
    TOutputMessageConstPtr Message;
};

template <class TSink>
TTestSinkEnvironment<TSink> CreateTestSink(
    bool enableAtMostOnce = false,
    TStringBuf hostParameters = R"(host="127.0.0.1";)")
{
    const TStreamId streamId("input");
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::String),
    });
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    streamSpecs[streamId][TStreamSpecId(1)] = streamSpec;

    auto context = New<TSinkContext>();
    auto atMostOnceStrategy = enableAtMostOnce
        ? "at_most_once_strategy={enabled=%true};"
        : "";
    auto specYson = Format("{sink_class_name=%Qv;input_stream_ids=[input];parameters={%vport=1;table=%Qv;%v};}",
        TypeName<TSink>(),
        hostParameters,
        "output",
        atMostOnceStrategy);
    context->SinkSpec = ConvertTo<TSinkSpecPtr>(TYsonStringBuf(specYson));
    context->StreamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(streamSpecs)),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);
    context->Logger = NLogging::TLogger("ClickHouseSinkInitTest");
    auto statusProfiler = CreateSyncStatusProfiler();
    context->StatusProfiler = statusProfiler;

    auto dynamicContext = New<TDynamicSinkContext>();
    dynamicContext->DynamicSinkSpec = ConvertTo<TDynamicSinkSpecPtr>(TYsonStringBuf("{}"));

    TMessageBuilder builder(streamId, schema);
    builder.SetMessageId(TMessageId("message"));
    builder.SetSystemTimestamp(TSystemTimestamp(1));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1));
    builder.SetEventTimestamp(TSystemTimestamp(1));
    builder.Payload().Set<std::string>("value", "value");
    auto message = New<TOutputMessage>(builder.Finish(), context->StreamSpecStorage);

    return {
        .Sink = New<TSink>(context, dynamicContext),
        .StatusProfiler = std::move(statusProfiler),
        .Message = std::move(message),
    };
}

template <class TSink>
void CheckInitDoesNotConnect(bool enableAtMostOnce = false)
{
    auto environment = CreateTestSink<TSink>(enableAtMostOnce);
    auto stateManager = New<TStateManagerMock>();
    EXPECT_NO_THROW(environment.Sink->Init(stateManager->CreateContext()));
}

void CheckInitializationError(const IStatusProfilerPtr& statusProfiler)
{
    const auto errors = statusProfiler->GetStatus().Errors;
    ASSERT_TRUE(errors.contains("/writing"));
    EXPECT_THAT(
        ToString(errors.at("/writing")),
        ::testing::HasSubstr("Failed to initialize ClickHouse writer"));
}

TEST(TClickHouseSinkInitTest, MissingShardingKeyColumnFailsSinkConstruction)
{
    // The at-most-once sink retries session startup forever, so a spec typo caught only at
    // session startup would never surface as a failure there.
    constexpr TStringBuf BadShardingKey =
        R"(shard_hosts={a=["h1"];b=["h2"]};sharding_key_columns=["missing"];)";

    EXPECT_THROW(
        CreateTestSink<TShardedClickHouseBatchingSink>(/*enableAtMostOnce*/ false, BadShardingKey),
        TErrorException);
    EXPECT_THROW(
        CreateTestSink<TAtLeastOnceClickHouseSink>(/*enableAtMostOnce*/ false, BadShardingKey),
        TErrorException);
    EXPECT_THROW(
        CreateTestSink<TAtMostOnceClickHouseSink>(/*enableAtMostOnce*/ true, BadShardingKey),
        TErrorException);
}

TEST(TClickHouseSinkInitTest, BatchingDoesNotConnect)
{
    CheckInitDoesNotConnect<TClickHouseBatchingSink>();
}

TEST(TClickHouseSinkInitTest, ShardedBatchingDoesNotConnect)
{
    auto environment = CreateTestSink<TShardedClickHouseBatchingSink>(
        /*enableAtMostOnce*/ false,
        R"(shard_hosts={a=[h]};)");
    auto stateManager = New<TStateManagerMock>();
    EXPECT_NO_THROW(environment.Sink->Init(stateManager->CreateContext()));
}

TEST(TClickHouseSinkInitTest, AtLeastOnceDoesNotConnect)
{
    CheckInitDoesNotConnect<TAtLeastOnceClickHouseSink>();
}

TEST(TClickHouseSinkInitTest, AtMostOnceDoesNotConnect)
{
    CheckInitDoesNotConnect<TAtMostOnceClickHouseSink>(/*enableAtMostOnce*/ true);
}

TEST(TClickHouseSinkInitTest, BatchingConnectsBeforeAck)
{
    auto environment = CreateTestSink<TClickHouseBatchingSink>();
    auto stateManager = New<TStateManagerMock>();
    ASSERT_NO_THROW(environment.Sink->Init(stateManager->CreateContext()));

    bool acked = false;
    EXPECT_THROW(
        environment.Sink->Distribute(
            environment.Message,
            TOnDistributedCallback::FromCallback([&] {
                acked = true;
            })),
        std::exception);
    EXPECT_FALSE(acked);
    CheckInitializationError(environment.StatusProfiler);
    EXPECT_NO_THROW(environment.Sink->Commit());
}

TEST(TClickHouseSinkInitTest, AtMostOnceWaitsForFirstConnectionBeforeAck)
{
    auto environment = CreateTestSink<TAtMostOnceClickHouseSink>(/*enableAtMostOnce*/ true);
    auto stateManager = New<TStateManagerMock>();
    ASSERT_NO_THROW(environment.Sink->Init(stateManager->CreateContext()));

    auto acked = std::make_shared<std::atomic<bool>>(false);
    auto actionQueue = New<NConcurrency::TActionQueue>("ClickHouseSinkInitTest");
    auto distributeFuture = BIND([sink = environment.Sink, message = environment.Message, acked] {
        sink->Distribute(
            message,
            TOnDistributedCallback::FromCallback([acked] {
                acked->store(true);
            }));
    })
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    bool observedResult = false;
    for (int iteration = 0; iteration < 3000 && !observedResult; ++iteration) {
        observedResult = acked->load() || environment.StatusProfiler->GetStatus().Errors.contains("/writing");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(observedResult);

    EXPECT_FALSE(acked->load());
    CheckInitializationError(environment.StatusProfiler);

    distributeFuture.Cancel(TError("Stop connection retry test"));
    auto cancellationError = NConcurrency::WaitFor(distributeFuture);
    EXPECT_FALSE(cancellationError.IsOK());
    EXPECT_THAT(ToString(cancellationError), ::testing::HasSubstr("Stop connection retry test"));
    EXPECT_FALSE(acked->load());
    EXPECT_NO_THROW(environment.Sink->Commit());
}

TEST(TClickHouseSinkInitTest, AtMostOnceOrderedFallbackConnectsBeforeAck)
{
    auto environment = CreateTestSink<TAtMostOnceClickHouseSink>();
    auto stateManager = New<TStateManagerMock>();
    ASSERT_NO_THROW(environment.Sink->Init(stateManager->CreateContext()));

    bool acked = false;
    EXPECT_THROW(
        environment.Sink->Distribute(
            environment.Message,
            TOnDistributedCallback::FromCallback([&] {
                acked = true;
            })),
        std::exception);
    EXPECT_FALSE(acked);
    CheckInitializationError(environment.StatusProfiler);
    EXPECT_NO_THROW(environment.Sink->Commit());
    EXPECT_FALSE(acked);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
