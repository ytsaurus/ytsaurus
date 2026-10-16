#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/common/async_at_most_once_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/delegating_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <tuple>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TAtMostOnceTestSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return 1;
    }
};

class TAtMostOnceTestSink
    : public TAsyncAtMostOnceSinkBase
{
public:
    using TSinkController = TAtMostOnceTestSinkController;
    using TAsyncAtMostOnceSinkBase::TAsyncAtMostOnceSinkBase;

    void SetPromises(std::shared_ptr<std::vector<TPromise<void>>> promises)
    {
        Promises_ = std::move(promises);
    }

private:
    std::shared_ptr<std::vector<TPromise<void>>> Promises_;
    int NextPromiseIndex_ = 0;

    void DoInit(const std::string& /*producerId*/) override
    { }

    std::pair<TFuture<void>, ui64> DoDistribute(const TOutputMessageConstPtr& /*message*/, i64 /*seqNo*/) override
    {
        return {Promises_->at(NextPromiseIndex_++).ToFuture(), 1};
    }
};

YT_FLOW_DEFINE_SINK(TAtMostOnceTestSink);

class TDelegatingTestSink
    : public TDelegatingAsyncSinkBase
{
public:
    using TSinkController = TAtMostOnceTestSinkController;
    using TDelegatingAsyncSinkBase::TDelegatingAsyncSinkBase;
    using TDelivery = std::tuple<std::string, TMessageId, i64>;

    std::string ProducerId;
    std::vector<TDelivery> Deliveries;

private:
    void DoInit(const std::string& producerId) override
    {
        ProducerId = producerId;
    }

    std::pair<TFuture<void>, ui64> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) override
    {
        Deliveries.emplace_back(ProducerId, message->MessageId, seqNo);
        return {OKFuture, 1};
    }
};

YT_FLOW_DEFINE_SINK(TDelegatingTestSink);

class TLifetimeProbe
    : public TRefCounted
{ };

////////////////////////////////////////////////////////////////////////////////

TSinkContextPtr MakeContext(const std::string& sinkClassName)
{
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = New<TTableSchema>();
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    streamSpecs[TStreamId("input")][TStreamSpecId(1)] = streamSpec;
    auto context = New<TSinkContext>();
    context->Logger = NLogging::TLogger("AsyncAtMostOnceSinkTest");
    context->StreamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(streamSpecs)),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);
    context->SinkSpec = New<TSinkSpec>();
    context->SinkSpec->SinkClassName = sinkClassName;
    context->SinkSpec->InputStreamIds = {"input"};
    return context;
}

TDynamicSinkContextPtr MakeDynamicContext()
{
    auto context = New<TDynamicSinkContext>();
    context->DynamicSinkSpec = New<TDynamicSinkSpec>();
    return context;
}

std::shared_ptr<bool> Distribute(const TIntrusivePtr<TSinkBase>& sink, const std::string& id)
{
    const auto& storage = sink->GetContext()->StreamSpecStorage;
    TMessageBuilder builder("input", storage->GetSchema("input"));
    builder.SetMessageId(TMessageId(id));
    builder.SetSystemTimestamp(TSystemTimestamp(1));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1));
    builder.SetEventTimestamp(TSystemTimestamp(1));
    auto acknowledged = std::make_shared<bool>(false);
    sink->Distribute(New<TOutputMessage>(builder.Finish(), storage), TOnDistributedCallback::FromCallback([acknowledged] {
        *acknowledged = true;
    }));
    return acknowledged;
}

void CommitEpoch(const ISinkPtr& sink, const TStateManagerMockPtr& stateManager)
{
    sink->Sync(nullptr);
    stateManager->Sync();
    sink->Commit();
}

auto MakeSink(const TStateManagerMockPtr& stateManager, bool atMostOnce = true)
{
    auto context = MakeContext(TypeName<TDelegatingTestSink>());
    auto parameters = New<TAtMostOnceStrategyParameters>();
    parameters->Enabled = atMostOnce;
    context->SinkSpec->Parameters->AddChild("at_most_once_strategy", NYTree::ConvertToNode(parameters));
    auto sink = New<TDelegatingTestSink>(context, MakeDynamicContext());
    sink->Init(stateManager->CreateContext());
    return sink;
}

void RestartSink(TIntrusivePtr<TDelegatingTestSink>& sink, TStateManagerMockPtr& stateManager, bool atMostOnce = true)
{
    sink.Reset();
    auto persisted = stateManager->GetStorage();
    stateManager = New<TStateManagerMock>();
    stateManager->SetStorage(std::move(persisted));
    sink = MakeSink(stateManager, atMostOnce);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncAtMostOnceSinkTest, RetainsResourcesUntilAllAcceptedFuturesSet)
{
    auto actionQueue = New<TActionQueue>("AsyncAtMostOnceSinkTest");
    auto context = MakeContext(TypeName<TAtMostOnceTestSink>());
    context->SerializedInvoker = actionQueue->GetInvoker();
    auto promises = std::make_shared<std::vector<TPromise<void>>>(
        std::initializer_list<TPromise<void>>{NewPromise<void>(), NewPromise<void>()});
    auto sink = New<TAtMostOnceTestSink>(context, MakeDynamicContext());
    sink->SetPromises(promises);
    auto parameters = New<TAtMostOnceStrategyDynamicParameters>();
    parameters->SuspendDestructionDuration = TDuration::Minutes(1);
    parameters->TotalQueueBytesLimit = NYTree::TSize(1_KB);
    sink->Reconfigure(parameters);
    auto stateManager = New<TStateManagerMock>();
    sink->Init(stateManager->CreateContext());
    Distribute(sink, "1");
    Distribute(sink, "2");
    CommitEpoch(sink, stateManager);

    auto probe = New<TLifetimeProbe>();
    auto weakProbe = MakeWeak(probe);
    sink->SuspendDestructionGuarded({probe});
    probe.Reset();
    sink.Reset();
    EXPECT_FALSE(weakProbe.IsExpired());
    promises->at(0).Set(TError("Expected test failure"));
    EXPECT_FALSE(weakProbe.IsExpired());
    promises->at(1).Set();
    WaitFor(BIND([] {
    })
            .AsyncVia(actionQueue->GetInvoker())
            .Run())
        .ThrowOnError();
    EXPECT_TRUE(weakProbe.IsExpired());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncAtMostOnceSinkTest, Recovery)
{
    auto stateManager = New<TStateManagerMock>();
    auto sink = MakeSink(stateManager);
    EXPECT_TRUE(*Distribute(sink, "1"));
    EXPECT_TRUE(*Distribute(sink, "2"));
    EXPECT_TRUE(sink->Deliveries.empty());
    CommitEpoch(sink, stateManager);
    EXPECT_EQ(sink->Deliveries.size(), 2u);

    // Fail before saving the next epoch's progress.
    Distribute(sink, "3");
    sink->Sync(nullptr);
    RestartSink(sink, stateManager);
    EXPECT_TRUE(*Distribute(sink, "1"));
    EXPECT_TRUE(*Distribute(sink, "2"));
    CommitEpoch(sink, stateManager);
    EXPECT_TRUE(sink->Deliveries.empty());
    Distribute(sink, "3");
    CommitEpoch(sink, stateManager);
    ASSERT_EQ(sink->Deliveries.size(), 1u);
    EXPECT_EQ(std::get<1>(sink->Deliveries.front()), TMessageId("3"));

    // Fail after saving progress but before sending.
    Distribute(sink, "4");
    sink->Sync(nullptr);
    stateManager->Sync();
    RestartSink(sink, stateManager);
    EXPECT_TRUE(*Distribute(sink, "4"));
    CommitEpoch(sink, stateManager);
    EXPECT_TRUE(sink->Deliveries.empty());
    Distribute(sink, "5");
    CommitEpoch(sink, stateManager);
    ASSERT_EQ(sink->Deliveries.size(), 1u);
    EXPECT_EQ(std::get<1>(sink->Deliveries.front()), TMessageId("5"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDelegatingAsyncSinkTest, PreservesOrderedProgressAcrossStrategyChanges)
{
    auto stateManager = New<TStateManagerMock>();
    auto sink = MakeSink(stateManager, /*atMostOnce*/ false);
    auto orderedProducerId = sink->ProducerId;
    auto acknowledged = Distribute(sink, "1");
    EXPECT_FALSE(*acknowledged);
    CommitEpoch(sink, stateManager);
    CommitEpoch(sink, stateManager);
    EXPECT_TRUE(*acknowledged);

    RestartSink(sink, stateManager, /*atMostOnce*/ true);
    EXPECT_NE(sink->ProducerId, orderedProducerId);
    EXPECT_TRUE(*Distribute(sink, "1"));
    EXPECT_TRUE(*Distribute(sink, "2"));
    CommitEpoch(sink, stateManager);
    ASSERT_EQ(sink->Deliveries.size(), 1u);
    EXPECT_EQ(sink->Deliveries.front(), std::make_tuple(sink->ProducerId, TMessageId("2"), i64(1)));

    RestartSink(sink, stateManager, /*atMostOnce*/ false);
    EXPECT_EQ(sink->ProducerId, orderedProducerId);
    EXPECT_TRUE(*Distribute(sink, "2"));
    CommitEpoch(sink, stateManager);
    EXPECT_TRUE(sink->Deliveries.empty());
    acknowledged = Distribute(sink, "3");
    CommitEpoch(sink, stateManager);
    ASSERT_EQ(sink->Deliveries.size(), 1u);
    EXPECT_EQ(sink->Deliveries.front(), std::make_tuple(orderedProducerId, TMessageId("3"), i64(2)));
    CommitEpoch(sink, stateManager);
    EXPECT_TRUE(*acknowledged);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
