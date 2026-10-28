#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/queue/sink.h>
#include <yt/yt/flow/library/cpp/connectors/queue/tablet_router.h>

#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/suspendable_invoker.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <atomic>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;
using namespace testing;

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncMultiClusterQueueSinkParametersTest, RejectsDuplicateClusters)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TAsyncMultiClusterQueueSinkParametersPtr>(TYsonStringBuf(R"({
            queue_path = "<clusters=[\"cluster-a\";\"cluster-a\"]>//queue";
            producer_path = "<clusters=[\"cluster-a\";\"cluster-a\"]>//producer";
            use_clusters = %true;
        })")),
        "Duplicate cluster");
}

////////////////////////////////////////////////////////////////////////////////

class TFixedClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TFixedClientsCache(IClientPtr client)
        : Client_(std::move(client))
    { }

    IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return Client_;
    }

private:
    const IClientPtr Client_;
};

class TMapClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TMapClientsCache(THashMap<std::string, IClientPtr> clients)
        : Clients_(std::move(clients))
    { }

    IClientPtr GetClient(TStringBuf clusterUrl) override
    {
        auto it = Clients_.find(std::string(clusterUrl));
        YT_VERIFY(it != Clients_.end());
        return it->second;
    }

private:
    const THashMap<std::string, IClientPtr> Clients_;
};

DEFINE_REFCOUNTED_TYPE(TMapClientsCache);

class TQueueWriterTest
{
public:
    NConcurrency::TActionQueuePtr Queue;
    TSinkContextPtr Context = New<TSinkContext>();
    TAsyncQueueWriterParametersBasePtr Parameters = New<TAsyncQueueWriterParametersBase>();
    TDynamicAsyncQueueWriterParametersPtr DynamicParameters = New<TDynamicAsyncQueueWriterParameters>();
    TNameTablePtr NameTable = New<TNameTable>();
    IStatusProfilerPtr StatusProfiler = CreateSyncStatusProfiler();
    TStateManagerMockPtr StateManager = New<TStateManagerMock>();
    TAsyncQueueWriterPtr Writer;

    TQueueWriterTest(
        const char* name,
        NClient::NCache::IClientsCachePtr clientsCache,
        std::vector<std::string> clusters = {"test"})
        : Queue(New<NConcurrency::TActionQueue>(name))
        , Clusters_(std::move(clusters))
        , Logger_(name)
        , ValueColumn_(NameTable->RegisterNameOrThrow("value"))
    {
        Context->ClientsCache = std::move(clientsCache);
        Context->SerializedInvoker = Queue->GetInvoker();
        Context->PoolInvoker = Queue->GetInvoker();
        Parameters->QueuePath = NYPath::TRichYPath("<cluster=test>//queue");
        Parameters->ProducerPath = NYPath::TRichYPath("<cluster=test>//producer");
        DynamicParameters->WritePeriod = TDuration::Seconds(1);
        DynamicParameters->MaxRowsPerWrite = 100;
        DynamicParameters->MaxBytesPerWrite = 1_MB;
        DynamicParameters->BackoffDuration = TDuration::MilliSeconds(1);
    }

    ~TQueueWriterTest()
    {
        Writer.Reset();
        Queue->Shutdown(/*graceful*/ true);
    }

    void InitWriter(bool atMostOnce = false)
    {
        Writer = New<TAsyncQueueWriter>(
            Context,
            Parameters,
            Clusters_,
            DynamicParameters,
            NameTable,
            StatusProfiler->ErrorState("/write"),
            Logger_);
        Writer->Init(StateManager->CreateContext(), atMostOnce);
    }

    TFuture<void> Write(TUnversionedOwningRow row, TMessageId messageId)
    {
        auto promise = NewPromise<void>();
        Writer->Write(std::move(row), std::move(messageId), BIND([promise] {
            promise.Set();
        }));
        return promise.ToFuture();
    }

    TUnversionedOwningRow MakeRow(i64 value)
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(value, ValueColumn_));
        return builder.FinishRow();
    }

    TAsyncQueueSinkPtr InitSink(bool atMostOnce = false, TDuration gracePeriod = TDuration::Zero())
    {
        const TStreamId streamId("input");
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = New<TTableSchema>(std::vector{TColumnSchema("value", EValueType::Int64)});
        THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
        streamSpecs[streamId][TStreamSpecId(1)] = streamSpec;
        Context->StreamSpecStorage = New<TComputationStreamSpecStorage>(
            New<TStreamSpecs>(std::move(streamSpecs)),
            New<TTableSchema>(),
            /*evaluatorCache*/ nullptr);
        Context->SinkSpec = ConvertTo<TSinkSpecPtr>(TYsonStringBuf(Format(R"({
            sink_class_name = "NYT::NFlow::TAsyncQueueSink";
            input_stream_ids = [input];
            parameters = {
                queue_path = "<cluster=test>//queue";
                producer_path = "<cluster=test>//producer";
                at_most_once_strategy = {enabled = %v;};
            };
        })",
            atMostOnce ? "%true" : "%false")));
        Context->Logger = Logger_;
        Context->StatusProfiler = StatusProfiler;
        auto dynamicContext = New<TDynamicSinkContext>();
        dynamicContext->DynamicSinkSpec = New<TDynamicSinkSpec>();
        dynamicContext->DynamicSinkSpec->Parameters = ConvertToNode(DynamicParameters)->AsMap();
        auto strategy = New<TAtMostOnceStrategyDynamicParameters>();
        strategy->SuspendDestructionDuration = gracePeriod;
        dynamicContext->DynamicSinkSpec->Parameters->AddChild("at_most_once_strategy", ConvertToNode(strategy));
        auto sink = New<TAsyncQueueSink>(Context, dynamicContext);
        sink->Init(StateManager->CreateContext());
        return sink;
    }

    TFuture<void> Distribute(const TAsyncQueueSinkPtr& sink, TStringBuf messageId)
    {
        const TStreamId streamId("input");
        TMessageBuilder builder(streamId, Context->StreamSpecStorage->GetSchema(streamId));
        builder.SetMessageId(TMessageId(messageId));
        builder.SetSystemTimestamp(TSystemTimestamp(1));
        builder.SetEventTimestamp(TSystemTimestamp(1));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1));
        builder.Payload().Set<i64>(1, "value");
        auto promise = NewPromise<void>();
        sink->Distribute(
            New<TOutputMessage>(builder.Finish(), Context->StreamSpecStorage),
            TOnDistributedCallback::FromCallback(BIND([promise] {
                promise.Set();
            })));
        return promise.ToFuture();
    }

private:
    const std::vector<std::string> Clusters_;
    const NLogging::TLogger Logger_;
    const int ValueColumn_;
};

TEST(TSyncQueueSinkInitTest, ResolvesTabletCountOnFirstNonemptySync)
{
    const TStreamId streamId("input");
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("tablet", EValueType::Uint64),
        TColumnSchema("value", EValueType::String),
    });
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    streamSpecs[streamId][TStreamSpecId(1)] = streamSpec;

    auto queue = New<NConcurrency::TActionQueue>("QueueSinkInitTest");
    std::atomic<int> getNodeCalls = 0;
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, GetNode("//queue/@tablet_count", _))
        .WillRepeatedly([&] {
            ++getNodeCalls;
            return MakeFuture(ConvertToYsonString(4));
        });
    auto context = New<TSinkContext>();
    context->SinkSpec = ConvertTo<TSinkSpecPtr>(TYsonStringBuf(R"({
        sink_class_name = "NYT::NFlow::TSyncQueueSink";
        input_stream_ids = [input];
        parameters = {
            queue_path = "<cluster=test>//queue";
            tablet_index_expression = "tablet";
        };
    })"));
    context->ClientsCache = New<TFixedClientsCache>(client);
    context->PipelinePath = NYPath::TRichYPath("<cluster=test>//pipeline");
    context->PoolInvoker = queue->GetInvoker();
    auto streamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(streamSpecs)),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);
    context->StreamSpecStorage = streamSpecStorage;
    context->Logger = NLogging::TLogger("SyncQueueSinkInitTest");
    context->StatusProfiler = CreateSyncStatusProfiler();

    auto dynamicContext = New<TDynamicSinkContext>();
    dynamicContext->DynamicSinkSpec = ConvertTo<TDynamicSinkSpecPtr>(TYsonStringBuf("{}"));

    auto sink = New<TSyncQueueSink>(context, dynamicContext);
    auto stateManager = New<TStateManagerMock>();
    EXPECT_NO_THROW(sink->Init(stateManager->CreateContext()));
    EXPECT_EQ(getNodeCalls.load(), 0);

    auto transaction = New<NiceMock<TMockTransaction>>();
    sink->Sync(transaction);
    EXPECT_EQ(getNodeCalls.load(), 0);

    auto makeMessage = [&] (TStringBuf messageId) {
        TMessageBuilder builder(streamId, schema);
        builder.SetMessageId(TMessageId(messageId));
        builder.SetSystemTimestamp(TSystemTimestamp(1));
        builder.SetEventTimestamp(TSystemTimestamp(1));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1));
        builder.Payload().Set<ui64>(2, "tablet");
        builder.Payload().Set<std::string>("value", "value");
        return New<TOutputMessage>(builder.Finish(), streamSpecStorage);
    };

    bool distributed = false;
    sink->Distribute(
        makeMessage("message-1"),
        TOnDistributedCallback::FromCallback(BIND([&] {
            distributed = true;
        })));
    EXPECT_TRUE(distributed);

    sink->Sync(transaction);
    EXPECT_EQ(getNodeCalls.load(), 1);

    sink->Distribute(
        makeMessage("message-2"),
        TOnDistributedCallback::FromCallback(BIND([&] {
            distributed = true;
        })));
    sink->Sync(transaction);
    EXPECT_EQ(getNodeCalls.load(), 1);
}

TEST(TTabletRouterTest, DefersAndStartsPeriodicRefresh)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("tablet", EValueType::Uint64),
    });
    auto queue = New<NConcurrency::TActionQueue>("TabletRouterTest");
    auto suspendedInvoker = NConcurrency::CreateSuspendableInvoker(queue->GetInvoker());
    NConcurrency::WaitFor(suspendedInvoker->Suspend()).ThrowOnError();
    auto getNodeCalls = std::make_shared<std::atomic<int>>(0);
    auto refreshed = NewPromise<void>();
    auto client = New<NiceMock<TMockClient>>();
    ON_CALL(*client, GetNode("//queue/@tablet_count", _))
        .WillByDefault([getNodeCalls, refreshed] {
            if (++*getNodeCalls == 2) {
                refreshed.TrySet();
            }
            return MakeFuture(ConvertToYsonString(4));
        });

    auto context = New<TSinkContext>();
    context->ClientsCache = New<TFixedClientsCache>(client);
    context->PoolInvoker = suspendedInvoker;

    auto parameters = New<TQueueSinkTabletRoutingParameters>();
    parameters->TabletIndexExpression = "tablet";
    auto router = CreateTabletRouter(
        *parameters,
        NYPath::TRichYPath("<cluster=test>//queue"),
        TDuration::Hours(1),
        schema,
        context,
        NLogging::TLogger("TabletRouterTest"));

    router->Start();

    NConcurrency::WaitFor(NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(10)))
        .ThrowOnError();
    suspendedInvoker->Resume();
    auto invokerDrained = NewPromise<void>();
    queue->GetInvoker()->Invoke(BIND([invokerDrained] {
        invokerDrained.Set();
    }));
    NConcurrency::WaitFor(invokerDrained.ToFuture()).ThrowOnError();
    EXPECT_EQ(getNodeCalls->load(), 1);

    router.Reset();
    getNodeCalls->store(0);
    router = CreateTabletRouter(
        *parameters,
        NYPath::TRichYPath("<cluster=test>//queue"),
        TDuration::MilliSeconds(50),
        schema,
        context,
        NLogging::TLogger("TabletRouterTest"));
    router->Start();
    EXPECT_TRUE(NConcurrency::WaitFor(refreshed.ToFuture().WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_GE(getNodeCalls->load(), 2);

    router.Reset();
    queue->Shutdown(/*graceful*/ true);
}

TEST(TAsyncQueueSinkTest, AtMostOnceAcknowledgesBeforeWriteAndDropsAfterOvershoot)
{
    const TStreamId streamId("input");
    auto schema = New<TTableSchema>(std::vector{TColumnSchema("value", EValueType::String)});
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    streamSpecs[streamId][TStreamSpecId(1)] = streamSpec;
    auto specStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(streamSpecs)),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);

    auto queue = New<NConcurrency::TActionQueue>("AtMostOnceQueueSinkTest");
    auto client = New<StrictMock<TMockClient>>();
    std::vector<NQueueClient::TQueueProducerSessionId> sessionIds;
    auto sessionEpochs = std::make_shared<THashMap<NQueueClient::TQueueProducerSessionId, int>>();
    auto restartedSessionCreated = NewPromise<void>();
    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .Times(2)
        .WillRepeatedly([&] (
            const NYPath::TRichYPath&,
            const NYPath::TRichYPath&,
            const NQueueClient::TQueueProducerSessionId& sessionId,
            const TCreateQueueProducerSessionOptions&) {
            sessionIds.push_back(sessionId);
            auto epoch = NQueueClient::TQueueProducerEpoch(++(*sessionEpochs)[sessionId]);
            if (sessionIds.size() == 2) {
                restartedSessionCreated.TrySet();
            }
            return MakeFuture(TCreateQueueProducerSessionResult{
                .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(0),
                .Epoch = epoch,
                .UserMeta = nullptr,
            });
        });
    auto transaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*client, StartTransaction(_, _))
        .Times(2)
        .WillRepeatedly(Return(MakeFuture<ITransactionPtr>(transaction)));
    auto pushStarted = NewPromise<void>();
    auto pushResult = NewPromise<TPushQueueProducerResult>();
    int pushCount = 0;
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .Times(2)
        .WillRepeatedly([&] (
            const NYPath::TRichYPath&,
            const NYPath::TRichYPath&,
            const NQueueClient::TQueueProducerSessionId& sessionId,
            NQueueClient::TQueueProducerEpoch epoch,
            TNameTablePtr,
            TSharedRange<TUnversionedRow>
                rows,
            const TPushQueueProducerOptions& options) {
            EXPECT_EQ(rows.Size(), 1);
            EXPECT_EQ(
                options.UserMeta->AsMap()->GetChildValueOrThrow<std::string>("last_message_id"),
                ++pushCount == 1 ? "id1" : "id4");
            if (pushCount == 1) {
                pushStarted.TrySet();
                return pushResult.ToFuture().Apply(BIND([sessionEpochs, sessionId, epoch] (const TPushQueueProducerResult& result) {
                    THROW_ERROR_EXCEPTION_IF(
                        GetOrCrash(*sessionEpochs, sessionId) != epoch.Underlying(),
                        NQueueClient::EErrorCode::ZombieEpoch,
                        "Producer session was fenced");
                    return result;
                }));
            }
            return MakeFuture(TPushQueueProducerResult{
                .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(1),
                .SkippedRowCount = 0,
            });
        });
    auto commitFinished = NewPromise<void>();
    auto restartedCommitFinished = NewPromise<void>();
    EXPECT_CALL(*transaction, Commit(_))
        .WillOnce([commitFinished] (const TTransactionCommitOptions&) {
            commitFinished.TrySet();
            return MakeFuture(TTransactionCommitResult{});
        })
        .WillOnce([restartedCommitFinished] (const TTransactionCommitOptions&) {
            restartedCommitFinished.TrySet();
            return MakeFuture(TTransactionCommitResult{});
        });

    auto context = New<TSinkContext>();
    context->SinkSpec = ConvertTo<TSinkSpecPtr>(TYsonStringBuf(R"({
        sink_class_name = "NYT::NFlow::TAsyncQueueSink";
        input_stream_ids = [input];
        parameters = {
            queue_path = "<cluster=test>//queue";
            producer_path = "<cluster=test>//producer";
            at_most_once_strategy = {enabled = %true;};
        };
    })"));
    context->ClientsCache = New<TFixedClientsCache>(client);
    context->SerializedInvoker = queue->GetInvoker();
    context->PoolInvoker = queue->GetInvoker();
    context->StreamSpecStorage = specStorage;
    context->Logger = NLogging::TLogger("AtMostOnceQueueSinkTest");
    context->StatusProfiler = CreateSyncStatusProfiler();
    auto dynamicContext = New<TDynamicSinkContext>();
    dynamicContext->DynamicSinkSpec = New<TDynamicSinkSpec>();
    auto strategy = New<TAtMostOnceStrategyDynamicParameters>();
    strategy->TotalQueueBytesLimit = NYTree::TSize(1);
    strategy->SuspendDestructionDuration = TDuration::Minutes(1);
    dynamicContext->DynamicSinkSpec->Parameters->AddChild("at_most_once_strategy", ConvertToNode(strategy));

    auto stateManager = New<TStateManagerMock>();
    auto sink = New<TAsyncQueueSink>(context, dynamicContext);
    sink->Init(stateManager->CreateContext());
    int callbackCount = 0;
    auto distribute = [&] (const TAsyncQueueSinkPtr& target, int id) {
        TMessageBuilder builder(streamId, schema);
        builder.SetMessageId(TMessageId(Format("id%v", id)));
        builder.SetSystemTimestamp(TSystemTimestamp(1));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1));
        builder.SetEventTimestamp(TSystemTimestamp(1));
        builder.Payload().Set<std::string>("a payload larger than one byte", "value");
        auto message = New<TOutputMessage>(builder.Finish(), specStorage);
        ASSERT_GT(message->ByteSize, 1);
        target->Distribute(message, TOnDistributedCallback::FromCallback([&callbackCount] {
            ++callbackCount;
        }));
    };
    for (int id = 1; id <= 3; ++id) {
        distribute(sink, id);
    }
    EXPECT_EQ(callbackCount, 3);
    NConcurrency::WaitFor(BIND([sink, stateManager] {
        sink->Sync(nullptr);
        stateManager->Sync();
        sink->Commit();
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    NConcurrency::WaitFor(pushStarted.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    sink->Terminate();
    // Let both the sink and writer termination callbacks run before completing the write.
    for (int callbackIndex = 0; callbackIndex < 2; ++callbackIndex) {
        NConcurrency::WaitFor(BIND([] {
        }).AsyncVia(queue->GetInvoker())
                .Run())
            .ThrowOnError();
    }
    EXPECT_FALSE(pushResult.IsSet());
    sink.Reset();
    auto restoredStateManager = New<TStateManagerMock>();
    restoredStateManager->SetStorage(stateManager->GetStorage());
    auto restartedSink = New<TAsyncQueueSink>(context, dynamicContext);
    restartedSink->Init(restoredStateManager->CreateContext());
    NConcurrency::WaitFor(restartedSessionCreated.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    EXPECT_NE(sessionIds[0], sessionIds[1]);
    EXPECT_FALSE(commitFinished.IsSet());
    pushResult.Set(TPushQueueProducerResult{
        .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(1),
        .SkippedRowCount = 0,
    });
    NConcurrency::WaitFor(commitFinished.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    NConcurrency::WaitFor(BIND([&] {
        restartedSink->Sync(nullptr);
        restoredStateManager->Sync();
        for (int id = 1; id <= 4; ++id) {
            distribute(restartedSink, id);
        }
        EXPECT_EQ(callbackCount, 7);
        restartedSink->Sync(nullptr);
        restartedSink->Commit();
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    NConcurrency::WaitFor(restartedCommitFinished.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    EXPECT_EQ(pushCount, 2);
    restartedSink.Reset();
    queue->Shutdown(/*graceful*/ true);
}

TEST(TAsyncQueueSinkTest, AtMostOnceDropsEpochWhenQueueLimitIsZero)
{
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{})));
    auto transaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*client, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture<ITransactionPtr>(transaction)));
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .WillOnce(WithArgs<5, 6>([] (TSharedRange<TUnversionedRow> rows, const TPushQueueProducerOptions& options) {
            EXPECT_EQ(rows.Size(), 1);
            EXPECT_EQ(options.UserMeta->AsMap()->GetChildValueOrThrow<std::string>("last_message_id"), "m2");
            return MakeFuture(TPushQueueProducerResult{
                .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(1),
                .SkippedRowCount = 0,
            });
        }));
    auto committed = NewPromise<void>();
    EXPECT_CALL(*transaction, Commit(_))
        .WillOnce([committed] (const TTransactionCommitOptions&) {
            committed.Set();
            return MakeFuture(TTransactionCommitResult{});
        });

    TQueueWriterTest test("AtMostOnceZeroQueueLimitTest", New<TFixedClientsCache>(client));
    test.DynamicParameters->MaxRowsPerWrite = 1;
    auto sink = test.InitSink(/*atMostOnce*/ true);
    NConcurrency::WaitFor(BIND([&] {
        auto dynamicContext = sink->GetDynamicContext();
        auto parameters = ConvertTo<TDynamicAsyncQueueSinkParametersPtr>(dynamicContext->DynamicSinkSpec->Parameters);
        parameters->AtMostOnceStrategy->TotalQueueBytesLimit = NYTree::TSize(0);
        dynamicContext->DynamicSinkSpec->Parameters = ConvertToNode(parameters)->AsMap();
        sink->Reconfigure(dynamicContext);

        auto dropped = test.Distribute(sink, "m1");
        EXPECT_TRUE(dropped.IsSet() && dropped.GetOrCrash().IsOK());
        sink->Sync(nullptr);
        test.StateManager->Sync();
        sink->Commit();
        EXPECT_EQ(ConvertTo<std::string>(test.StateManager->Get("/v1/at_most_once_watermark")), "m1");

        parameters->AtMostOnceStrategy->TotalQueueBytesLimit = NYTree::TSize(1_MB);
        dynamicContext->DynamicSinkSpec->Parameters = ConvertToNode(parameters)->AsMap();
        sink->Reconfigure(dynamicContext);

        auto control = test.Distribute(sink, "m2");
        EXPECT_TRUE(control.IsSet() && control.GetOrCrash().IsOK());
        sink->Sync(nullptr);
        test.StateManager->Sync();
        sink->Commit();
        EXPECT_EQ(ConvertTo<std::string>(test.StateManager->Get("/v1/at_most_once_watermark")), "m2");
    }).AsyncVia(test.Queue->GetInvoker())
            .Run())
        .ThrowOnError();
    NConcurrency::WaitFor(committed.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
}

TEST(TAsyncQueueSinkTest, TerminateCancelsRetriedSessionAndReleasesRequests)
{
    auto client = New<StrictMock<TMockClient>>();
    auto session = NewPromise<TCreateQueueProducerSessionResult>();
    auto sessionStarted = NewPromise<void>();
    auto canceled = NewPromise<void>();
    session.OnCanceled(BIND([canceled] (const TError&) {
        canceled.TrySet();
    }));
    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .WillOnce(Return(MakeFuture<TCreateQueueProducerSessionResult>(TError("Session failed"))))
        .WillOnce([session, sessionStarted] (const auto&, const auto&, const auto&, const auto&) {
            sessionStarted.Set();
            return session.ToFuture().ToImmediatelyCancelable();
        });
    TQueueWriterTest test("TerminateQueueSessionTest", New<TFixedClientsCache>(client));
    test.DynamicParameters->MaxRowsPerWrite = 1;
    test.DynamicParameters->BackoffDuration = TDuration::MilliSeconds(1);
    auto sink = test.InitSink();
    std::vector<TFuture<void>> results;
    NConcurrency::WaitFor(BIND([&] {
        results.push_back(test.Distribute(sink, "m1"));
        sink->Commit();
        results.push_back(test.Distribute(sink, "m2"));
        sink->Commit();
        results.push_back(test.Distribute(sink, "m3"));
    }).AsyncVia(test.Queue->GetInvoker())
            .Run())
        .ThrowOnError();
    NConcurrency::WaitFor(sessionStarted.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    sink->Terminate();
    NConcurrency::WaitFor(canceled.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    for (const auto& result : results) {
        auto error = NConcurrency::WaitFor(result.WithTimeout(TDuration::Seconds(5)));
        EXPECT_FALSE(error.IsOK());
        EXPECT_FALSE(error.FindMatching(NYT::EErrorCode::Timeout));
    }
}

TEST(TAsyncQueueSinkTest, TerminateCancelsInflightWriteAndBackoff)
{
    for (auto [atMostOnce, writeFailed] : {std::pair{false, false}, {true, false}, {false, true}}) {
        auto client = New<StrictMock<TMockClient>>();
        EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
            .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{})));
        auto transaction = New<StrictMock<TMockTransaction>>();
        EXPECT_CALL(*client, StartTransaction(_, _))
            .WillOnce(Return(MakeFuture<ITransactionPtr>(transaction)));
        auto push = NewPromise<TPushQueueProducerResult>();
        auto pushStarted = NewPromise<void>();
        auto canceled = NewPromise<void>();
        push.OnCanceled(BIND([canceled] (const TError&) {
            canceled.TrySet();
        }));
        EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
            .WillOnce([push, pushStarted] (const auto&, const auto&, const auto&, auto, auto, auto, const auto&) {
                pushStarted.Set();
                return push.ToFuture().ToImmediatelyCancelable();
            });
        TQueueWriterTest test("TerminateQueueWriteTest", New<TFixedClientsCache>(client));
        test.DynamicParameters->MaxRowsPerWrite = 1;
        test.DynamicParameters->BackoffDuration = TDuration::Hours(1);
        auto sink = test.InitSink(atMostOnce, TDuration::MilliSeconds(10));
        TFuture<void> distributed;
        NConcurrency::WaitFor(BIND([&] {
            distributed = test.Distribute(sink, "m1");
            sink->Commit();
        }).AsyncVia(test.Queue->GetInvoker())
                .Run())
            .ThrowOnError();
        NConcurrency::WaitFor(pushStarted.ToFuture().WithTimeout(TDuration::Seconds(5)))
            .ThrowOnError();
        if (writeFailed) {
            push.Set(TError("Retriable write failure"));
            NConcurrency::WaitFor(BIND([] {
            }).AsyncVia(test.Queue->GetInvoker())
                    .Run())
                .ThrowOnError();
        }
        sink->Terminate();
        if (!writeFailed) {
            NConcurrency::WaitFor(canceled.ToFuture().WithTimeout(TDuration::Seconds(5)))
                .ThrowOnError();
        }
        auto result = NConcurrency::WaitFor(distributed.WithTimeout(TDuration::Seconds(5)));
        EXPECT_EQ(result.IsOK(), atMostOnce);
        EXPECT_FALSE(result.FindMatching(NYT::EErrorCode::Timeout));
    }
}

TEST(TAsyncQueueSinkTest, RejectsNonIncreasingMessageIdsAfterCommittedEpoch)
{
    for (TStringBuf messageId : {"m1", "m2"}) {
        SCOPED_TRACE(std::string(messageId));
        auto client = New<StrictMock<TMockClient>>();
        EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
            .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{
                .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(0),
                .Epoch = NQueueClient::TQueueProducerEpoch(1),
                .UserMeta = nullptr,
            })));
        auto transaction = New<StrictMock<TMockTransaction>>();
        EXPECT_CALL(*client, StartTransaction(_, _))
            .WillOnce(Return(MakeFuture<ITransactionPtr>(transaction)));
        EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
            .WillOnce(WithArgs<5, 6>([] (TSharedRange<TUnversionedRow> rows, const TPushQueueProducerOptions& options) {
                EXPECT_EQ(rows.Size(), 1);
                EXPECT_EQ(options.UserMeta->AsMap()->GetChildValueOrThrow<std::string>("last_message_id"), "m2");
                return MakeFuture(TPushQueueProducerResult{
                    .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(1),
                    .SkippedRowCount = 0,
                });
            }));
        EXPECT_CALL(*transaction, Commit(_))
            .WillOnce(Return(MakeFuture(TTransactionCommitResult{})));

        TQueueWriterTest test("AsyncQueueSinkOrderingTest", New<TFixedClientsCache>(client));
        test.DynamicParameters->MaxRowsPerWrite = 1;
        auto sink = test.InitSink();
        TFuture<void> distributed;
        NConcurrency::WaitFor(BIND([&] {
            distributed = test.Distribute(sink, "m2");
            sink->Sync(nullptr);
            test.StateManager->Sync();
            sink->Commit();
        }).AsyncVia(test.Queue->GetInvoker())
                .Run())
            .ThrowOnError();
        NConcurrency::WaitFor(distributed.WithTimeout(TDuration::Seconds(5)))
            .ThrowOnError();

        NConcurrency::WaitFor(BIND([&] {
            distributed = test.Distribute(sink, messageId);
            sink->Sync(nullptr);
            test.StateManager->Sync();
            EXPECT_THROW_WITH_SUBSTRING(sink->Commit(), "must strictly increase");
        }).AsyncVia(test.Queue->GetInvoker())
                .Run())
            .ThrowOnError();
    }
}

TEST(TAsyncQueueSinkTest, RestoresProducerAndFiltersReplayAfterExternalCommit)
{
    auto client = New<StrictMock<TMockClient>>();
    TQueueWriterTest test("AsyncQueueSinkReplayTest", New<TFixedClientsCache>(client));
    const TStreamId streamId("input");
    auto schema = New<TTableSchema>(std::vector{TColumnSchema("value", EValueType::Int64)});
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    streamSpecs[streamId][TStreamSpecId(1)] = streamSpec;
    test.Context->StreamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(streamSpecs)),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);
    test.Context->SinkSpec = ConvertTo<TSinkSpecPtr>(TYsonStringBuf(R"({
        sink_class_name = "NYT::NFlow::TAsyncQueueSink";
        input_stream_ids = [input];
        parameters = {
            queue_path = "<cluster=test>//queue";
            producer_path = "<cluster=test>//producer";
        };
    })"));
    test.Context->Logger = NLogging::TLogger("AsyncQueueSinkReplayTest");
    test.Context->StatusProfiler = test.StatusProfiler;
    auto dynamicContext = New<TDynamicSinkContext>();
    dynamicContext->DynamicSinkSpec = ConvertTo<TDynamicSinkSpecPtr>(TYsonStringBuf(R"({
        parameters = {max_rows_per_write = 2; write_period = "1ms";};
    })"));

    std::vector<NQueueClient::TQueueProducerSessionId> sessionIds;
    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .Times(2)
        .WillRepeatedly(WithArg<2>([&] (const NQueueClient::TQueueProducerSessionId& sessionId) {
            sessionIds.push_back(sessionId);
            bool restarted = std::ssize(sessionIds) == 2;
            return MakeFuture(TCreateQueueProducerSessionResult{
                .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(restarted ? 2 : 0),
                .Epoch = NQueueClient::TQueueProducerEpoch(std::ssize(sessionIds)),
                .UserMeta = restarted
                    ? ConvertToNode(THashMap<std::string, std::string>{{"last_message_id", "m2"}})
                    : nullptr,
            });
        }));
    auto transaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*client, StartTransaction(_, _))
        .Times(2)
        .WillRepeatedly(Return(MakeFuture<ITransactionPtr>(transaction)));
    std::vector<i64> writtenValues;
    int pushCount = 0;
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .Times(2)
        .WillRepeatedly(WithArgs<5, 6>([&] (TSharedRange<TUnversionedRow> rows, const TPushQueueProducerOptions& options) {
            ++pushCount;
            EXPECT_EQ(rows.Size(), pushCount == 1 ? 2 : 1);
            EXPECT_EQ(options.SequenceNumber->Underlying(), pushCount == 1 ? 1 : 3);
            EXPECT_EQ(
                options.UserMeta->AsMap()->GetChildValueOrThrow<std::string>("last_message_id"),
                pushCount == 1 ? "m2" : "m3");
            for (auto row : rows) {
                writtenValues.push_back(FromUnversionedValue<i64>(row[0]));
            }
            return MakeFuture(TPushQueueProducerResult{
                .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(pushCount == 1 ? 2 : 3),
                .SkippedRowCount = 0,
            });
        }));
    EXPECT_CALL(*transaction, Commit(_))
        .Times(2)
        .WillRepeatedly(Return(MakeFuture(TTransactionCommitResult{})));

    THashMap<std::string, TYsonString> checkpoint;
    for (int attempt = 0; attempt < 2; ++attempt) {
        if (attempt == 1) {
            test.StateManager = New<TStateManagerMock>();
            test.StateManager->SetStorage(THashMap<std::string, TYsonString>(checkpoint));
        }
        auto sink = New<TAsyncQueueSink>(test.Context, dynamicContext);
        sink->Init(test.StateManager->CreateContext());
        if (attempt == 0) {
            test.StateManager->Sync();
            checkpoint = test.StateManager->GetStorage();
        }
        auto allDistributed = NewPromise<void>();
        auto callbackCount = std::make_shared<std::atomic<int>>(0);
        int messageCount = attempt == 0 ? 2 : 3;
        NConcurrency::WaitFor(BIND([&] {
            for (int id = 1; id <= messageCount; ++id) {
                TMessageBuilder builder(streamId, schema);
                builder.SetMessageId(TMessageId(Format("m%v", id)));
                builder.SetSystemTimestamp(TSystemTimestamp(1));
                builder.SetAlignmentTimestamp(TSystemTimestamp(1));
                builder.SetEventTimestamp(TSystemTimestamp(1));
                builder.Payload().Set<i64>(id, "value");
                sink->Distribute(
                    New<TOutputMessage>(builder.Finish(), test.Context->StreamSpecStorage),
                    TOnDistributedCallback::FromCallback([callbackCount, allDistributed, messageCount] {
                        if (++*callbackCount == messageCount) {
                            allDistributed.TrySet();
                        }
                    }));
            }
            sink->Sync(nullptr);
        }).AsyncVia(test.Queue->GetInvoker())
                .Run())
            .ThrowOnError();
        // Let a prematurely enqueued full batch run before checking the commit boundary.
        NConcurrency::WaitFor(BIND([&] {
            EXPECT_EQ(pushCount, attempt);
            EXPECT_EQ(std::ssize(writtenValues), attempt == 0 ? 0 : 2);
            EXPECT_EQ(callbackCount->load(), 0);
            sink->Commit();
        }).AsyncVia(test.Queue->GetInvoker())
                .Run())
            .ThrowOnError();
        NConcurrency::WaitFor(allDistributed.ToFuture().WithTimeout(TDuration::Seconds(5)))
            .ThrowOnError();
        EXPECT_EQ(callbackCount->load(), messageCount);
        EXPECT_EQ(pushCount, attempt + 1);
        sink.Reset();
    }
    ASSERT_EQ(std::ssize(sessionIds), 2);
    EXPECT_EQ(sessionIds[0], sessionIds[1]);
    EXPECT_EQ(writtenValues, (std::vector<i64>{1, 2, 3}));
}

TEST(TAsyncQueueWriterTest, ReleasesQueuedRequestsWhenDestroyed)
{
    for (bool preparedBatch : {false, true}) {
        auto client = New<StrictMock<TMockClient>>();
        TQueueWriterTest test("DestroyedQueuedWriterTest", New<TFixedClientsCache>(client));
        auto invoker = NConcurrency::CreateSuspendableInvoker(test.Queue->GetInvoker());
        NConcurrency::WaitFor(invoker->Suspend())
            .ThrowOnError();
        test.Context->SerializedInvoker = invoker;
        test.Context->PoolInvoker = invoker;
        test.DynamicParameters->WritePeriod = TDuration::Hours(1);
        test.DynamicParameters->MaxRowsPerWrite = preparedBatch ? 1 : 100;
        test.InitWriter();

        auto semaphore = New<NConcurrency::TAsyncSemaphore>(/*totalSlots*/ 2);
        auto probe = std::make_shared<int>(0);
        auto weakProbe = std::weak_ptr(probe);
        auto acknowledgedCount = std::make_shared<std::atomic<int>>(0);
        for (int id = 1; id <= 2; ++id) {
            test.Writer->Write(test.MakeRow(id), TMessageId(Format("m%v", id)), BIND([guard = NConcurrency::TAsyncSemaphoreGuard::Acquire(semaphore), probe, acknowledgedCount] () mutable {
                ++*acknowledgedCount;
                guard.Release();
            }));
        }
        EXPECT_EQ(semaphore->GetUsed(), 2);
        probe.reset();
        test.Writer.Reset();
        EXPECT_EQ(acknowledgedCount->load(), 0);
        EXPECT_TRUE(semaphore->IsFree());
        EXPECT_TRUE(weakProbe.expired());

        invoker->Resume();
        NConcurrency::WaitFor(BIND([] {
        }).AsyncVia(invoker)
                .Run())
            .ThrowOnError();
        EXPECT_EQ(acknowledgedCount->load(), 0);
    }
}

TEST(TAsyncQueueWriterTest, ReleasesActiveBatchWhenWriterIsAbandoned)
{
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{
            .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(0),
            .Epoch = NQueueClient::TQueueProducerEpoch(1),
            .UserMeta = nullptr,
        })));
    auto transaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*client, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture<ITransactionPtr>(transaction)));
    auto pushStarted = NewPromise<void>();
    auto pushResult = NewPromise<TPushQueueProducerResult>();
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .WillOnce([pushStarted, pushResult] {
            pushStarted.Set();
            return pushResult.ToFuture();
        });
    EXPECT_CALL(*transaction, Commit(_)).Times(0);

    TQueueWriterTest test("AbandonedActiveWriterTest", New<TFixedClientsCache>(client));
    test.DynamicParameters->MaxRowsPerWrite = 1;
    test.DynamicParameters->BackoffDuration = TDuration::Hours(1);
    test.InitWriter();
    auto semaphore = New<NConcurrency::TAsyncSemaphore>(/*totalSlots*/ 1);
    auto probe = std::make_shared<int>(0);
    auto weakProbe = std::weak_ptr(probe);
    auto acknowledgedCount = std::make_shared<std::atomic<int>>(0);
    auto finished = NewPromise<void>();
    auto finishedFuture = finished.ToFuture();
    test.Writer->Write(test.MakeRow(1), TMessageId("m1"), BIND([guard = NConcurrency::TAsyncSemaphoreGuard::Acquire(semaphore), probe, acknowledgedCount, finished] () mutable {
        ++*acknowledgedCount;
        guard.Release();
        finished.Set();
    }));
    finished = {};
    NConcurrency::WaitFor(pushStarted.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    probe.reset();
    auto weakWriter = MakeWeak(test.Writer);
    test.Writer.Reset();
    EXPECT_FALSE(weakWriter.IsExpired());
    EXPECT_EQ(semaphore->GetUsed(), 1);
    pushResult.Set(TError("Retriable write failure"));
    auto result = NConcurrency::WaitFor(finishedFuture.WithTimeout(TDuration::Seconds(5)));
    EXPECT_FALSE(result.IsOK());
    EXPECT_FALSE(result.FindMatching(NYT::EErrorCode::Timeout));
    NConcurrency::WaitFor(BIND([] {
    }).AsyncVia(test.Queue->GetInvoker())
            .Run())
        .ThrowOnError();
    EXPECT_TRUE(weakWriter.IsExpired());
    EXPECT_EQ(acknowledgedCount->load(), 0);
    EXPECT_TRUE(semaphore->IsFree());
    EXPECT_TRUE(weakProbe.expired());
}

TEST(TAsyncQueueWriterTest, InitializesSessionAsynchronouslyAndRetriesOneMutation)
{
    auto client = New<StrictMock<TMockClient>>();
    auto firstAttempt = NewPromise<TCreateQueueProducerSessionResult>();
    NRpc::TMutationId mutationId;

    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .WillOnce([&] (
            const NYPath::TRichYPath& /*producerPath*/,
            const NYPath::TRichYPath& /*queuePath*/,
            const NQueueClient::TQueueProducerSessionId& /*sessionId*/,
            const TCreateQueueProducerSessionOptions& options) {
            EXPECT_FALSE(options.Retry);
            mutationId = options.MutationId;
            return firstAttempt.ToFuture();
        })
        .WillOnce([&] (
            const NYPath::TRichYPath& /*producerPath*/,
            const NYPath::TRichYPath& /*queuePath*/,
            const NQueueClient::TQueueProducerSessionId& /*sessionId*/,
            const TCreateQueueProducerSessionOptions& options) {
            EXPECT_TRUE(options.Retry);
            EXPECT_EQ(options.MutationId, mutationId);
            return MakeFuture(TCreateQueueProducerSessionResult{
                .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(42),
                .Epoch = NQueueClient::TQueueProducerEpoch(7),
                .UserMeta = nullptr,
            });
        });

    TQueueWriterTest test("AsyncQueueWriterTest", New<TFixedClientsCache>(client));
    test.Parameters->RequireSyncReplica = true;
    test.InitWriter();
    auto initialMessageId = test.Writer->GetInitialMessageId();
    EXPECT_FALSE(initialMessageId.IsSet());

    firstAttempt.Set(TError("Injected create-session failure"));
    EXPECT_EQ(
        NConcurrency::WaitFor(initialMessageId.WithTimeout(TDuration::Seconds(5)))
            .ValueOrThrow(),
        TMessageId());
}

TEST(TAsyncQueueWriterTest, RefreshesSessionAfterUnknownCommitOutcome)
{
    auto client = New<StrictMock<TMockClient>>();
    int sessionCount = 0;
    std::optional<NQueueClient::TQueueProducerSessionId> initialSessionId;
    NRpc::TMutationId mutationId;
    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .Times(2)
        .WillRepeatedly([&] (
            const NYPath::TRichYPath&,
            const NYPath::TRichYPath&,
            const NQueueClient::TQueueProducerSessionId& sessionId,
            const TCreateQueueProducerSessionOptions& options) {
            if (++sessionCount == 1) {
                initialSessionId = sessionId;
                mutationId = options.MutationId;
                EXPECT_FALSE(mutationId.IsEmpty());
                EXPECT_FALSE(options.Retry);
            } else {
                EXPECT_EQ(sessionId, *initialSessionId);
                EXPECT_EQ(options.MutationId, mutationId);
                EXPECT_TRUE(options.Retry);
            }
            return MakeFuture(TCreateQueueProducerSessionResult{
                .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(sessionCount == 1 ? 10 : 12),
                .Epoch = NQueueClient::TQueueProducerEpoch(7),
                .UserMeta = ConvertToNode(THashMap<std::string, std::string>{{"last_message_id", sessionCount == 1 ? "m5" : "m7"}}),
            });
        });

    auto transaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*client, StartTransaction(_, _))
        .Times(2)
        .WillRepeatedly(Return(MakeFuture<ITransactionPtr>(transaction)));
    int pushCount = 0;
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .Times(2)
        .WillRepeatedly([&] (
            const NYPath::TRichYPath& /*producerPath*/,
            const NYPath::TRichYPath& /*queuePath*/,
            const NQueueClient::TQueueProducerSessionId& /*sessionId*/,
            NQueueClient::TQueueProducerEpoch /*epoch*/,
            TNameTablePtr /*nameTable*/,
            TSharedRange<TUnversionedRow>
                rows,
            const TPushQueueProducerOptions& options) {
            const int call = pushCount++;
            EXPECT_TRUE(options.SequenceNumber);
            if (options.SequenceNumber) {
                EXPECT_EQ(options.SequenceNumber->Underlying(), call == 1 ? 13 : 11);
            }
            EXPECT_EQ(
                options.UserMeta->AsMap()->GetChildValueOrThrow<std::string>("last_message_id"),
                call == 1 ? "m8" : "m7");
            EXPECT_EQ(rows.Size(), call == 1 ? 1 : 2);
            return MakeFuture(TPushQueueProducerResult{
                .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(call == 1 ? 13 : 12),
                .SkippedRowCount = 0,
            });
        });
    EXPECT_CALL(*transaction, Commit(_))
        .WillOnce(Return(MakeFuture<TTransactionCommitResult>(TError("Lost commit response"))))
        .WillOnce(Return(MakeFuture(TTransactionCommitResult{})));

    TQueueWriterTest test("AsyncQueueWriterMessageIdTest", New<TFixedClientsCache>(client));
    test.Parameters->RequireSyncReplica = true;
    test.DynamicParameters->MaxRowsPerWrite = 2;
    test.InitWriter();
    EXPECT_EQ(
        NConcurrency::WaitFor(test.Writer->GetInitialMessageId())
            .ValueOrThrow(),
        TMessageId("m5"));
    auto first = test.Write(test.MakeRow(1), TMessageId("m6"));
    auto second = test.Write(test.MakeRow(2), TMessageId("m7"));
    NConcurrency::WaitFor(first.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    NConcurrency::WaitFor(second.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    NConcurrency::WaitFor(test.Write(test.MakeRow(3), TMessageId("m8"))
            .WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    EXPECT_EQ(pushCount, 2);
}

TEST(TAsyncQueueSinkTest, FailsSyncAndCommitAfterProducerIsFenced)
{
    for (auto errorCode : {NQueueClient::EErrorCode::ZombieEpoch, NQueueClient::EErrorCode::InvalidEpoch}) {
        SCOPED_TRACE(static_cast<int>(errorCode));
        auto client = New<StrictMock<TMockClient>>();
        EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
            .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{
                .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(0),
                .Epoch = NQueueClient::TQueueProducerEpoch(1),
                .UserMeta = nullptr,
            })));
        auto transaction = New<StrictMock<TMockTransaction>>();
        EXPECT_CALL(*client, StartTransaction(_, _))
            .WillOnce(Return(MakeFuture<ITransactionPtr>(transaction)));
        EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
            .WillOnce(Return(MakeFuture<TPushQueueProducerResult>(TError(errorCode, "Producer was fenced"))));
        EXPECT_CALL(*transaction, Commit(_)).Times(0);

        TQueueWriterTest test("FencedQueueSinkTest", New<TFixedClientsCache>(client));
        test.DynamicParameters->MaxRowsPerWrite = 1;
        auto sink = test.InitSink();
        TFuture<void> distributed;
        NConcurrency::WaitFor(BIND([&] {
            distributed = test.Distribute(sink, "m1");
            sink->Sync(nullptr);
            test.StateManager->Sync();
            sink->Commit();
        }).AsyncVia(test.Queue->GetInvoker())
                .Run())
            .ThrowOnError();
        auto result = NConcurrency::WaitFor(distributed.WithTimeout(TDuration::Seconds(5)));
        EXPECT_FALSE(result.IsOK());
        EXPECT_FALSE(result.FindMatching(NYT::EErrorCode::Timeout));

        auto syncError = NConcurrency::WaitFor(BIND([sink] {
            sink->Sync(nullptr);
        }).AsyncVia(test.Queue->GetInvoker())
                .Run());
        EXPECT_TRUE(syncError.FindMatching(errorCode));
        auto commitError = NConcurrency::WaitFor(BIND([sink] {
            sink->Commit();
        }).AsyncVia(test.Queue->GetInvoker())
                .Run());
        EXPECT_TRUE(commitError.FindMatching(errorCode));
    }
}

TEST(TAsyncQueueWriterTest, FailsMessageIdRequestsAfterProducerIsFenced)
{
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{
            .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(0),
            .Epoch = NQueueClient::TQueueProducerEpoch(1),
            .UserMeta = nullptr,
        })));
    auto transaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*client, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture<ITransactionPtr>(transaction)));
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .WillOnce(Return(MakeFuture<TPushQueueProducerResult>(TError(
            NQueueClient::EErrorCode::ZombieEpoch,
            "Producer was fenced"))));
    EXPECT_CALL(*transaction, Commit(_)).Times(0);

    TQueueWriterTest test("FencedQueueWriterTest", New<TFixedClientsCache>(client));
    test.DynamicParameters->WritePeriod = TDuration::MilliSeconds(1);
    test.InitWriter();
    auto row = test.MakeRow(1);
    auto first = NConcurrency::WaitFor(test.Write(row, TMessageId("m1"))
            .WithTimeout(TDuration::Seconds(5)));
    EXPECT_FALSE(first.IsOK());
    EXPECT_FALSE(first.FindMatching(NYT::EErrorCode::Timeout));
    auto second = NConcurrency::WaitFor(test.Write(row, TMessageId("m2"))
            .WithTimeout(TDuration::Seconds(5)));
    EXPECT_FALSE(second.IsOK());
    EXPECT_FALSE(second.FindMatching(NYT::EErrorCode::Timeout));
    auto fatal = NConcurrency::WaitFor(BIND(&TAsyncQueueWriter::ThrowIfFatalError, test.Writer)
            .AsyncVia(test.Queue->GetInvoker())
            .Run());
    EXPECT_TRUE(fatal.FindMatching(NQueueClient::EErrorCode::ZombieEpoch));
    EXPECT_TRUE(GetOrCrash(test.StatusProfiler->GetStatus().Errors, "/write").FindMatching(NQueueClient::EErrorCode::ZombieEpoch));
}

TEST(TAsyncQueueWriterTest, FailsAtMostOnceRequestsAfterProducerIsFenced)
{
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{
            .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(0),
            .Epoch = NQueueClient::TQueueProducerEpoch(1),
            .UserMeta = nullptr,
        })));
    auto transaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*client, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture<ITransactionPtr>(transaction)));
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .WillOnce(Return(MakeFuture<TPushQueueProducerResult>(TError(
            NQueueClient::EErrorCode::ZombieEpoch,
            "Producer was fenced"))));

    TQueueWriterTest test("FencedAtMostOnceQueueWriterTest", New<TFixedClientsCache>(client));
    test.DynamicParameters->WritePeriod = TDuration::MilliSeconds(1);
    test.InitWriter(/*atMostOnce*/ true);
    auto row = test.MakeRow(1);
    auto first = NConcurrency::WaitFor(test.Write(row, TMessageId("m1")).WithTimeout(TDuration::Seconds(5)));
    auto second = NConcurrency::WaitFor(test.Write(row, TMessageId("m2")).WithTimeout(TDuration::Seconds(5)));
    EXPECT_FALSE(first.IsOK());
    EXPECT_FALSE(first.FindMatching(NYT::EErrorCode::Timeout));
    EXPECT_FALSE(second.IsOK());
    EXPECT_FALSE(second.FindMatching(NYT::EErrorCode::Timeout));
    auto fatal = NConcurrency::WaitFor(BIND(&TAsyncQueueWriter::ThrowIfFatalError, test.Writer)
            .AsyncVia(test.Queue->GetInvoker())
            .Run());
    EXPECT_TRUE(fatal.FindMatching(NQueueClient::EErrorCode::ZombieEpoch));
    EXPECT_TRUE(GetOrCrash(test.StatusProfiler->GetStatus().Errors, "/write").FindMatching(NQueueClient::EErrorCode::ZombieEpoch));
}

TEST(TAsyncQueueWriterTest, DoesNotFailOverAfterProducerIsFenced)
{
    auto firstClient = New<StrictMock<TMockClient>>();
    auto secondClient = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*firstClient, CreateQueueProducerSession(_, _, _, _))
        .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{
            .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(0),
            .Epoch = NQueueClient::TQueueProducerEpoch(1),
            .UserMeta = nullptr,
        })));
    EXPECT_CALL(*secondClient, CreateQueueProducerSession(_, _, _, _)).Times(0);
    auto transaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*firstClient, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture<ITransactionPtr>(transaction)));
    EXPECT_CALL(*secondClient, StartTransaction(_, _)).Times(0);
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .WillOnce(Return(MakeFuture<TPushQueueProducerResult>(TError(
            NQueueClient::EErrorCode::InvalidEpoch,
            "Producer was fenced"))));

    TQueueWriterTest test(
        "FencedMultiClusterQueueWriterTest",
        New<TMapClientsCache>(THashMap<std::string, IClientPtr>{
            {"cluster-a", firstClient},
            {"cluster-b", secondClient},
        }),
        {"cluster-a", "cluster-b"});
    test.Parameters->QueuePath = NYPath::TRichYPath("//queue");
    test.Parameters->ProducerPath = NYPath::TRichYPath("//producer");
    test.DynamicParameters->WritePeriod = TDuration::MilliSeconds(1);
    test.InitWriter();
    auto result = NConcurrency::WaitFor(test.Write(test.MakeRow(1), TMessageId("m1"))
            .WithTimeout(TDuration::Seconds(5)));
    EXPECT_FALSE(result.IsOK());
    EXPECT_FALSE(result.FindMatching(NYT::EErrorCode::Timeout));
    auto fatal = NConcurrency::WaitFor(BIND(&TAsyncQueueWriter::ThrowIfFatalError, test.Writer)
            .AsyncVia(test.Queue->GetInvoker())
            .Run());
    EXPECT_TRUE(fatal.FindMatching(NQueueClient::EErrorCode::InvalidEpoch));
    EXPECT_TRUE(GetOrCrash(test.StatusProfiler->GetStatus().Errors, "/write").FindMatching(NQueueClient::EErrorCode::InvalidEpoch));
}

TEST(TAsyncQueueWriterTest, RecoversAtMostOnceSessionAndRetriesPhysicalRows)
{
    auto client = New<StrictMock<TMockClient>>();
    int sessionCount = 0;
    std::optional<NQueueClient::TQueueProducerSessionId> initialSessionId;
    NRpc::TMutationId mutationId;
    EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
        .Times(3)
        .WillRepeatedly([&] (
            const NYPath::TRichYPath&,
            const NYPath::TRichYPath&,
            const NQueueClient::TQueueProducerSessionId& sessionId,
            const TCreateQueueProducerSessionOptions& options) {
            if (++sessionCount == 1) {
                initialSessionId = sessionId;
                mutationId = options.MutationId;
                EXPECT_FALSE(mutationId.IsEmpty());
                EXPECT_FALSE(options.Retry);
            } else {
                EXPECT_EQ(sessionId, *initialSessionId);
                EXPECT_EQ(options.MutationId, mutationId);
                EXPECT_TRUE(options.Retry);
            }
            return MakeFuture(TCreateQueueProducerSessionResult{
                .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(sessionCount == 1 ? 7 : -1),
                .Epoch = NQueueClient::TQueueProducerEpoch(sessionCount == 1 ? 2 : 0),
                .UserMeta = nullptr,
            });
        });
    auto transaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*client, StartTransaction(_, _))
        .Times(3)
        .WillRepeatedly(Return(MakeFuture<ITransactionPtr>(transaction)));
    int pushCount = 0;
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .Times(3)
        .WillRepeatedly([&] (
            const NYPath::TRichYPath&,
            const NYPath::TRichYPath&,
            const NQueueClient::TQueueProducerSessionId&,
            NQueueClient::TQueueProducerEpoch epoch,
            TNameTablePtr,
            TSharedRange<TUnversionedRow>
                rows,
            const TPushQueueProducerOptions& options) {
            const int call = pushCount++;
            EXPECT_EQ(rows.Size(), 1);
            EXPECT_TRUE(options.SequenceNumber);
            if (options.SequenceNumber) {
                EXPECT_EQ(options.SequenceNumber->Underlying(), call == 0 ? 8 : 0);
            }
            EXPECT_EQ(epoch.Underlying(), call == 0 ? 2 : 0);
            EXPECT_EQ(options.UserMeta->AsMap()->GetChildValueOrThrow<std::string>("last_message_id"), "m1");
            if (call == 0) {
                return MakeFuture<TPushQueueProducerResult>(TError("Unknown queue producer session"));
            }
            return MakeFuture(TPushQueueProducerResult{
                .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(0),
                .SkippedRowCount = 0,
            });
        });
    EXPECT_CALL(*transaction, Commit(_))
        .WillOnce(Return(MakeFuture<TTransactionCommitResult>(TError("Lost commit response"))))
        .WillOnce(Return(MakeFuture(TTransactionCommitResult{})));

    TQueueWriterTest test("AtMostOnceQueueWriterTest", New<TFixedClientsCache>(client));
    test.DynamicParameters->WritePeriod = TDuration::MilliSeconds(1);
    test.DynamicParameters->MaxRowsPerWrite = 1;
    test.InitWriter(/*atMostOnce*/ true);
    NConcurrency::WaitFor(test.Write(test.MakeRow(1), TMessageId("m1"))
            .WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    EXPECT_EQ(pushCount, 3);
}

TEST(TAsyncQueueWriterTest, AtMostOnceFailsOverOnlyBeforeCommit)
{
    for (bool commitStarted : {false, true}) {
        auto firstClient = New<StrictMock<TMockClient>>();
        auto secondClient = New<StrictMock<TMockClient>>();
        for (const auto& client : {firstClient, secondClient}) {
            EXPECT_CALL(*client, CreateQueueProducerSession(_, _, _, _))
                .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{
                    .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(0),
                    .Epoch = NQueueClient::TQueueProducerEpoch(1),
                    .UserMeta = nullptr,
                })));
        }
        auto firstTransaction = New<StrictMock<TMockTransaction>>();
        auto secondTransaction = New<StrictMock<TMockTransaction>>();
        EXPECT_CALL(*firstClient, StartTransaction(_, _))
            .WillOnce(Return(MakeFuture<ITransactionPtr>(firstTransaction)));
        EXPECT_CALL(*secondClient, StartTransaction(_, _))
            .Times(commitStarted ? 1 : 2)
            .WillRepeatedly(Return(MakeFuture<ITransactionPtr>(secondTransaction)));

        auto attemptStarted = NewPromise<void>();
        auto pushResponse = NewPromise<TPushQueueProducerResult>();
        auto commitResponse = NewPromise<TTransactionCommitResult>();
        std::vector<i64> pendingValues;
        std::vector<i64> firstClusterValues;
        EXPECT_CALL(*firstTransaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
            .WillOnce(WithArg<5>([&] (TSharedRange<TUnversionedRow> rows) {
                EXPECT_EQ(rows.Size(), 2);
                for (auto row : rows) {
                    pendingValues.push_back(FromUnversionedValue<i64>(row[0]));
                }
                if (!commitStarted) {
                    attemptStarted.Set();
                    return pushResponse.ToFuture();
                }
                return MakeFuture(TPushQueueProducerResult{
                    .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(2),
                    .SkippedRowCount = 0,
                });
            }));
        EXPECT_CALL(*firstTransaction, Commit(_))
            .Times(commitStarted ? 1 : 0)
            .WillRepeatedly([&] (const TTransactionCommitOptions&) {
                firstClusterValues = pendingValues;
                attemptStarted.Set();
                return commitResponse.ToFuture();
            });
        std::vector<i64> secondClusterValues;
        EXPECT_CALL(*secondTransaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
            .Times(commitStarted ? 1 : 2)
            .WillRepeatedly(WithArgs<5, 6>([&] (TSharedRange<TUnversionedRow> rows, const TPushQueueProducerOptions& options) {
                EXPECT_EQ(rows.Size(), 2);
                EXPECT_EQ(options.SequenceNumber->Underlying(), std::ssize(secondClusterValues) + 1);
                for (auto row : rows) {
                    secondClusterValues.push_back(FromUnversionedValue<i64>(row[0]));
                }
                return MakeFuture(TPushQueueProducerResult{
                    .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(std::ssize(secondClusterValues)),
                    .SkippedRowCount = 0,
                });
            }));
        EXPECT_CALL(*secondTransaction, Commit(_))
            .Times(commitStarted ? 1 : 2)
            .WillRepeatedly(Return(MakeFuture(TTransactionCommitResult{})));

        TQueueWriterTest test(
            "AtMostOnceFailoverTest",
            New<TMapClientsCache>(THashMap<std::string, IClientPtr>{
                {"cluster-a", firstClient},
                {"cluster-b", secondClient},
            }),
            {"cluster-a", "cluster-b"});
        test.DynamicParameters->WritePeriod = TDuration::Hours(1);
        test.DynamicParameters->MaxRowsPerWrite = 2;
        test.InitWriter(/*atMostOnce*/ true);
        auto semaphore = New<NConcurrency::TAsyncSemaphore>(/*totalSlots*/ 4);
        std::vector<TFuture<void>> writes;
        for (int id = 1; id <= 4; ++id) {
            auto promise = NewPromise<void>();
            writes.push_back(promise.ToFuture());
            test.Writer->Write(test.MakeRow(id), TMessageId(Format("m%v", id)), BIND([promise, guard = NConcurrency::TAsyncSemaphoreGuard::Acquire(semaphore)] () mutable {
                guard.Release();
                promise.Set();
            }));
            if (id == 2) {
                NConcurrency::WaitFor(attemptStarted.ToFuture().WithTimeout(TDuration::Seconds(5)))
                    .ThrowOnError();
            }
        }
        if (commitStarted) {
            commitResponse.Set(TError("Lost commit response"));
        } else {
            pushResponse.Set(TError("Push failed before commit"));
        }
        for (int index = 0; index < std::ssize(writes); ++index) {
            auto result = NConcurrency::WaitFor(writes[index].WithTimeout(TDuration::Seconds(5)));
            EXPECT_EQ(result.IsOK(), !commitStarted || index >= 2);
            EXPECT_FALSE(result.FindMatching(NYT::EErrorCode::Timeout));
        }
        EXPECT_TRUE(semaphore->IsFree());
        EXPECT_EQ(firstClusterValues, commitStarted ? (std::vector<i64>{1, 2}) : std::vector<i64>{});
        EXPECT_EQ(secondClusterValues, commitStarted ? (std::vector<i64>{3, 4}) : (std::vector<i64>{1, 2, 3, 4}));
    }
}

TEST(TAsyncQueueWriterTest, FailsOverWhenCreatingSessionFails)
{
    auto firstClient = New<NiceMock<TMockClient>>();
    auto secondClient = New<NiceMock<TMockClient>>();
    std::atomic<int> createSessionCalls = 0;
    std::atomic<int> failedCluster = -1;
    std::atomic<bool> retriedInitialCluster = false;
    NRpc::TMutationId failedMutationId;
    NRpc::TMutationId successfulMutationIds[2];

    auto createSession = [&] (int clusterIndex) {
        return [&, clusterIndex] (
            const NYPath::TRichYPath& /*producerPath*/,
            const NYPath::TRichYPath& /*queuePath*/,
            const NQueueClient::TQueueProducerSessionId& /*sessionId*/,
            const TCreateQueueProducerSessionOptions& options) {
            ++createSessionCalls;
            int expected = -1;
            if (failedCluster.compare_exchange_strong(expected, clusterIndex)) {
                EXPECT_FALSE(options.Retry);
                failedMutationId = options.MutationId;
                return MakeFuture<TCreateQueueProducerSessionResult>(TError("Injected create-session failure"));
            }
            if (failedCluster.load() == clusterIndex) {
                EXPECT_TRUE(options.Retry);
                EXPECT_EQ(options.MutationId, failedMutationId);
                retriedInitialCluster = true;
            }
            const bool reinitialized = !successfulMutationIds[clusterIndex].IsEmpty();
            if (reinitialized) {
                EXPECT_FALSE(options.Retry);
                EXPECT_NE(options.MutationId, successfulMutationIds[clusterIndex]);
            }
            successfulMutationIds[clusterIndex] = options.MutationId;
            return MakeFuture(TCreateQueueProducerSessionResult{
                .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(reinitialized ? 1 : 0),
                .Epoch = NQueueClient::TQueueProducerEpoch(1),
                .UserMeta = reinitialized
                    ? ConvertToNode(THashMap<std::string, std::string>{{"last_message_id", "m1"}})
                    : nullptr,
            });
        };
    };
    EXPECT_CALL(*firstClient, CreateQueueProducerSession(_, _, _, _))
        .WillRepeatedly(createSession(0));
    EXPECT_CALL(*secondClient, CreateQueueProducerSession(_, _, _, _))
        .WillRepeatedly(createSession(1));

    auto transaction = New<NiceMock<TMockTransaction>>();
    std::atomic<int> startTransactionCalls[2] = {};
    std::atomic<int> writeCluster = -1;
    auto startTransaction = [&] (int clusterIndex) {
        return [&, clusterIndex] (
            NTransactionClient::ETransactionType /*type*/,
            const TTransactionStartOptions& /*options*/) {
            int callIndex = startTransactionCalls[clusterIndex].fetch_add(1);
            if (callIndex == 1) {
                return MakeFuture<ITransactionPtr>(TError("Injected start-transaction failure"));
            }
            writeCluster = clusterIndex;
            return MakeFuture<ITransactionPtr>(transaction);
        };
    };
    ON_CALL(*firstClient, StartTransaction(_, _))
        .WillByDefault(startTransaction(0));
    ON_CALL(*secondClient, StartTransaction(_, _))
        .WillByDefault(startTransaction(1));
    int pushCount = 0;
    EXPECT_CALL(*transaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .Times(3)
        .WillRepeatedly([&] (
            const NYPath::TRichYPath&,
            const NYPath::TRichYPath&,
            const NQueueClient::TQueueProducerSessionId&,
            NQueueClient::TQueueProducerEpoch,
            TNameTablePtr,
            TSharedRange<TUnversionedRow>,
            const TPushQueueProducerOptions& options) {
            int sequenceNumber = ++pushCount == 3 ? 2 : 1;
            EXPECT_EQ(options.SequenceNumber->Underlying(), sequenceNumber);
            return MakeFuture(TPushQueueProducerResult{
                .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(sequenceNumber),
                .SkippedRowCount = 0,
            });
        });
    EXPECT_CALL(*transaction, Commit(_))
        .Times(3)
        .WillRepeatedly(Return(MakeFuture(TTransactionCommitResult{})));

    TQueueWriterTest test(
        "AsyncMultiClusterQueueWriterTest",
        New<TMapClientsCache>(THashMap<std::string, IClientPtr>{
            {"cluster-a", firstClient},
            {"cluster-b", secondClient},
        }),
        {"cluster-a", "cluster-b"});
    NYPath::TRichYPath queuePath("//queue");
    queuePath.SetClusters({"cluster-a", "cluster-b"});
    NYPath::TRichYPath producerPath("//producer");
    producerPath.SetClusters({"cluster-a", "cluster-b"});
    test.Parameters->QueuePath = std::move(queuePath);
    test.Parameters->ProducerPath = std::move(producerPath);
    test.Parameters->RequireSyncReplica = true;
    test.DynamicParameters->WritePeriod = TDuration::MilliSeconds(1);
    test.InitWriter();
    auto writeResult = NConcurrency::WaitFor(test.Write(test.MakeRow(1), TMessageId("m1")).WithTimeout(TDuration::Seconds(5)));
    ASSERT_TRUE(writeResult.IsOK());
    EXPECT_EQ(createSessionCalls.load(), 2);
    EXPECT_FALSE(retriedInitialCluster.load());
    EXPECT_NE(writeCluster.load(), failedCluster.load());

    int initialWriteCluster = writeCluster.load();
    auto secondWriteResult = NConcurrency::WaitFor(test.Write(test.MakeRow(2), TMessageId("m2")).WithTimeout(TDuration::Seconds(5)));
    ASSERT_TRUE(secondWriteResult.IsOK());
    EXPECT_EQ(createSessionCalls.load(), 3);
    EXPECT_TRUE(retriedInitialCluster.load());
    EXPECT_EQ(writeCluster.load(), failedCluster.load());
    EXPECT_NE(writeCluster.load(), initialWriteCluster);

    auto thirdWriteResult = NConcurrency::WaitFor(test.Write(test.MakeRow(3), TMessageId("m3")).WithTimeout(TDuration::Seconds(5)));
    ASSERT_TRUE(thirdWriteResult.IsOK());
    EXPECT_EQ(createSessionCalls.load(), 4);
    EXPECT_EQ(writeCluster.load(), initialWriteCluster);
}

TEST(TAsyncQueueWriterTest, AppliesSelectedClusterMessageIdBoundary)
{
    auto firstClient = New<StrictMock<TMockClient>>();
    auto secondClient = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*firstClient, CreateQueueProducerSession(_, _, _, _))
        .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{
            .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(10),
            .Epoch = NQueueClient::TQueueProducerEpoch(1),
            .UserMeta = ConvertToNode(THashMap<std::string, std::string>{{"last_message_id", "m1"}}),
        })));
    EXPECT_CALL(*secondClient, CreateQueueProducerSession(_, _, _, _))
        .WillOnce(Return(MakeFuture(TCreateQueueProducerSessionResult{
            .SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(20),
            .Epoch = NQueueClient::TQueueProducerEpoch(1),
            .UserMeta = ConvertToNode(THashMap<std::string, std::string>{{"last_message_id", "m2"}}),
        })));

    auto firstTransaction = New<StrictMock<TMockTransaction>>();
    auto secondTransaction = New<StrictMock<TMockTransaction>>();
    EXPECT_CALL(*firstClient, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture<ITransactionPtr>(firstTransaction)));
    EXPECT_CALL(*secondClient, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture<ITransactionPtr>(secondTransaction)));
    EXPECT_CALL(*firstTransaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .WillOnce([] (
            const NYPath::TRichYPath&,
            const NYPath::TRichYPath&,
            const NQueueClient::TQueueProducerSessionId&,
            NQueueClient::TQueueProducerEpoch,
            TNameTablePtr,
            TSharedRange<TUnversionedRow>
                rows,
            const TPushQueueProducerOptions& options) {
            EXPECT_EQ(rows.Size(), 2);
            EXPECT_EQ(options.SequenceNumber->Underlying(), 11);
            EXPECT_EQ(options.UserMeta->AsMap()->GetChildValueOrThrow<std::string>("last_message_id"), "m3");
            return MakeFuture<TPushQueueProducerResult>(TError("Injected first-cluster failure"));
        });
    EXPECT_CALL(*firstTransaction, Commit(_)).Times(0);
    EXPECT_CALL(*secondTransaction, PushQueueProducer(_, _, _, _, _, A<TSharedRange<TUnversionedRow>>(), _))
        .WillOnce([] (
            const NYPath::TRichYPath&,
            const NYPath::TRichYPath&,
            const NQueueClient::TQueueProducerSessionId&,
            NQueueClient::TQueueProducerEpoch,
            TNameTablePtr,
            TSharedRange<TUnversionedRow>
                rows,
            const TPushQueueProducerOptions& options) {
            EXPECT_EQ(rows.Size(), 1);
            EXPECT_EQ(options.SequenceNumber->Underlying(), 21);
            EXPECT_EQ(options.UserMeta->AsMap()->GetChildValueOrThrow<std::string>("last_message_id"), "m3");
            return MakeFuture(TPushQueueProducerResult{
                .LastSequenceNumber = NQueueClient::TQueueProducerSequenceNumber(21),
                .SkippedRowCount = 0,
            });
        });
    EXPECT_CALL(*secondTransaction, Commit(_))
        .WillOnce(Return(MakeFuture(TTransactionCommitResult{})));

    TQueueWriterTest test(
        "MultiClusterMessageIdBoundaryTest",
        New<TMapClientsCache>(THashMap<std::string, IClientPtr>{
            {"cluster-a", firstClient},
            {"cluster-b", secondClient},
        }),
        {"cluster-a", "cluster-b"});
    test.Parameters->QueuePath = NYPath::TRichYPath("//queue");
    test.Parameters->ProducerPath = NYPath::TRichYPath("//producer");
    test.DynamicParameters->MaxRowsPerWrite = 3;
    test.InitWriter();

    auto write = [&] (i64 value, TStringBuf messageId) {
        return test.Write(test.MakeRow(value), TMessageId(messageId));
    };
    auto first = write(1, "m1");
    auto second = write(2, "m2");
    auto third = write(3, "m3");
    NConcurrency::WaitFor(first.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    NConcurrency::WaitFor(second.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    NConcurrency::WaitFor(third.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
