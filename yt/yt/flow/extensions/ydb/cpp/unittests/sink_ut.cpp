#include <yt/yt/flow/extensions/ydb/cpp/unittests/lib/sink_test_environment.h>

#include <yt/yt/flow/extensions/ydb/cpp/sink.h>
#include <yt/yt/flow/extensions/ydb/cpp/ydb_topic_driver.h>

#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYdbTesting;

////////////////////////////////////////////////////////////////////////////////

TSinkSpecShape PublicAtLeastOnceFramingShape()
{
    return {
        .SinkClassName = TypeName<TYdbAtLeastOnceFramingSink>(),
        .ResourceClassName = TypeName<TYdbTopicDriver>(),
        .ConnectionParameters = {
            {"endpoint", NYTree::ConvertToNode("localhost:2135")},
            {"database", NYTree::ConvertToNode("/Root")},
            {"topic", NYTree::ConvertToNode("topic")},
        },
        .WriterIdentityType = "ytflow-ydb-two-phase-commit",
    };
}

TDynamicSinkContextPtr MakeBatchingContext(const std::string& name, const NYTree::INodePtr& value)
{
    auto context = New<TDynamicSinkContext>();
    context->DynamicSinkSpec = New<TDynamicSinkSpec>();
    context->DynamicSinkSpec->Parameters->AddChild(name, value);
    return context;
}

size_t WriteThreeMessages(const TSinkTestEnvironment& environment, const ISinkPtr& sink, i64 firstId)
{
    const auto writesBefore = environment.WriteState->Writes.size();
    for (i64 id = firstId; id < firstId + 3; ++id) {
        environment.Distribute(sink, environment.MakeMessage(id, "payload"));
    }
    environment.Sync(sink);
    sink->Commit();
    return environment.WriteState->Writes.size() - writesBefore;
}

class TAtLeastOnceFramingSinkTest
    : public ::testing::Test
{
protected:
    TSinkTestEnvironment Environment{PublicAtLeastOnceFramingShape(), /*framing*/ true, /*maxRowsPerBatch*/ 1000, 10, /*asyncInvoker*/ true};
    ISinkPtr Sink;

    void SetUp() override
    {
        Sink = NConcurrency::WaitFor(BIND([&] {
            auto sink = Environment.CreateSink();
            sink->Init(Environment.StateManager->CreateContext());
            return sink;
        }).AsyncVia(Environment.SuspendableInvoker)
                .Run())
            .ValueOrThrow();
    }

    void TearDown() override
    {
        NConcurrency::WaitFor(BIND([&] {
            Sink.Reset();
        }).AsyncVia(Environment.SuspendableInvoker)
                .Run())
            .ThrowOnError();
    }

    TFuture<size_t> WriteThreeMessagesAsync(i64 firstId)
    {
        return BIND(&WriteThreeMessages, std::cref(Environment), Sink, firstId)
            .AsyncVia(Environment.SuspendableInvoker)
            .Run();
    }

    void CheckQueuedSnapshots(const std::string& name, const NYTree::INodePtr& valueA, const NYTree::INodePtr& valueB)
    {
        EXPECT_EQ(NConcurrency::WaitFor(WriteThreeMessagesAsync(1)).ValueOrThrow(), 1u);

        const auto& invoker = Environment.SuspendableInvoker;
        NConcurrency::WaitFor(invoker->Suspend()).ThrowOnError();
        Sink->Reconfigure(MakeBatchingContext(name, valueA));
        auto writesUnderA = WriteThreeMessagesAsync(4);
        auto contextB = MakeBatchingContext(name, valueB);
        Sink->Reconfigure(contextB);
        EXPECT_THROW(Sink->Reconfigure(MakeBatchingContext(name, NYTree::ConvertToNode("invalid"))), std::exception);
        EXPECT_THROW(Sink->Reconfigure(MakeBatchingContext(name, NYTree::ConvertToNode(0))), std::exception);
        EXPECT_EQ(DynamicPointerCast<TSinkBase>(Sink)->GetDynamicContext(), contextB);
        invoker->Resume();

        EXPECT_EQ(NConcurrency::WaitFor(writesUnderA).ValueOrThrow(), 3u);
        EXPECT_EQ(NConcurrency::WaitFor(WriteThreeMessagesAsync(7)).ValueOrThrow(), 2u);
    }
};

TEST_F(TAtLeastOnceFramingSinkTest, QueuedRowLimitUpdatesApplyTheirOwnSnapshots)
{
    CheckQueuedSnapshots("max_rows_per_batch", NYTree::ConvertToNode(1), NYTree::ConvertToNode(2));
}

TEST_F(TAtLeastOnceFramingSinkTest, QueuedByteLimitUpdatesApplyTheirOwnSnapshots)
{
    const auto messageSize = Environment.MakeMessage(0, "payload")->ByteSize;
    CheckQueuedSnapshots(
        "max_bytes_per_batch",
        NYTree::ConvertToNode(1),
        NYTree::ConvertToNode(2 * messageSize));
}

TEST_F(TAtLeastOnceFramingSinkTest, SyncWaitsForEveryBatchWrite)
{
    Sink->Reconfigure(MakeBatchingContext("max_rows_per_batch", NYTree::ConvertToNode(1)));
    Environment.WriteState->AutoAcknowledge = false;
    Environment.WriteState->NotifyAfterWriteCount = 3;

    auto writes = WriteThreeMessagesAsync(1);
    NConcurrency::WaitFor(Environment.WriteState->WriteCountReached.ToFuture()).ThrowOnError();
    for (int acknowledged = 0; acknowledged < 2; ++acknowledged) {
        Environment.WriteState->AcknowledgeNext();
        Sleep(TDuration::MilliSeconds(200));
        EXPECT_FALSE(writes.IsSet());
    }
    Environment.WriteState->AcknowledgeNext();
    EXPECT_EQ(NConcurrency::WaitFor(writes).ValueOrThrow(), 3u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
