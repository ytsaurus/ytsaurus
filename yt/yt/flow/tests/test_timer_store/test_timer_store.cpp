#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>

#include <yt/yt/flow/library/cpp/tables/timers.h>
#include <yt/yt/flow/library/cpp/tables/transaction_manager.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client_spec.h>
#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/client/api/transaction.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NClient::NCache;
using namespace NConcurrency;
using namespace NCppTests;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NYPath;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TFixedClientCache
    : public IClientsCache
{
public:
    explicit TFixedClientCache(IClientPtr client)
        : Client_(std::move(client))
    { }

    IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return Client_;
    }

private:
    IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "CppTests");

////////////////////////////////////////////////////////////////////////////////

class TTestTimerStore
    : public TDynamicTablesTestBase
{
public:
    TTestTimerStore()
    { }

protected:
    void SetUp() override
    {
        LeaseTransaction_ = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Master))
            .ValueOrThrow();
        ActionQueue_ = New<TActionQueue>();

        WaitFor(Client_->CreateNode(GetPipelinePath(), EObjectType::Pipeline))
            .ThrowOnError();
        WaitUntilEqual(YPathJoin(GetPipelinePath(), "timers") + "/@tablet_state", "mounted");
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetPipelinePath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iterationCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
        if (LeaseTransaction_) {
            WaitFor(LeaseTransaction_->Abort())
                .ThrowOnError();
            LeaseTransaction_.Reset();
        }
        if (ActionQueue_) {
            ActionQueue_->Shutdown();
        }
    }

    static TYPath GetPipelinePath()
    {
        return "//pipeline";
    }

    bool MessagesEqualWithoutSchema(const TMessage& left, const TMessage& right)
    {
        return std::tie(left.MessageId, left.SystemTimestamp, left.EventTimestamp, left.StreamId, left.Payload) ==
            std::tie(right.MessageId, right.SystemTimestamp, right.EventTimestamp, right.StreamId, right.Payload);
    }

    bool TimersEqualWithoutSchema(const TTimer& left, const TTimer& right)
    {
        return std::tie(left.MessageId, left.SystemTimestamp, left.EventTimestamp, left.StreamId, left.Key, left.TriggerTimestamp) ==
            std::tie(right.MessageId, right.SystemTimestamp, right.EventTimestamp, right.StreamId, right.Key, right.TriggerTimestamp);
    }

    NTables::TContextPtr PrepareTablesContext()
    {
        auto context = New<NTables::TContext>();
        context->Client = Client_;
        context->PipelinePath = GetPipelinePath();
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>(
            CreateNamedUnlimitedThroughputThrottler("test", NProfiling::TProfiler()),
            Logger(),
            NProfiling::TProfiler());
        context->Logger = Logger();
        context->Profiler = NProfiling::TProfiler();
        return context;
    }

    std::pair<TPartitionPtr, NTables::TTransactionManagerContextPtr> PreparePartitionAndTransactionContext(TKey lowerKey, TKey upperKey)
    {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->ComputationId = TComputationId("TestComputation");
        partition->LowerKey = lowerKey;
        partition->UpperKey = upperKey;

        auto context = New<NTables::TTransactionManagerContext>();
        context->Client = Client_;
        context->PipelinePath = GetPipelinePath();
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>(
            CreateNamedUnlimitedThroughputThrottler("test", NProfiling::TProfiler()),
            Logger(),
            NProfiling::TProfiler());
        context->Logger = Logger();
        context->Profiler = NProfiling::TProfiler();
        context->LeaseId = LeaseTransaction_->GetId();
        context->PartitionId = partition->PartitionId;

        return {partition, context};
    }

    IRetryableClientPtr PrepareRetryableClient()
    {
        return CreateRetryableClient(Client_, ActionQueue_->GetInvoker(), CreateStatusProfiler(), TLogger("retryable_client_test"));
    }

    TTableSchemaPtr PrepareDefaultPayloadSchema()
    {
        return New<TTableSchema>(
            std::vector<TColumnSchema>{
                TColumnSchema("key_hash", EValueType::Uint64, ESortOrder::Ascending),
                TColumnSchema("key", EValueType::String, ESortOrder::Ascending),
                TColumnSchema("data", EValueType::String),
            },
            /*strict*/ true);
    }

    TTableSchemaPtr PrepareDefaultKeySchema()
    {
        return New<TTableSchema>(
            std::vector<TColumnSchema>{
                TColumnSchema("key_hash", EValueType::Uint64),
                TColumnSchema("key", EValueType::String),
            },
            /*strict*/ true);
    }

    ITransactionPtr LeaseTransaction_;
    TActionQueuePtr ActionQueue_;
};

TEST_W(TTestTimerStore, PartitionStoreBasic)
{
    auto partition1 = New<TPartition>();
    partition1->PartitionId = TPartitionId(TGuid::Create());
    partition1->ComputationId = TComputationId("TestComputation1");
    partition1->LowerKey = MakeKey<ui64>(0);
    partition1->UpperKey = MakeKey<ui64>(10);

    auto partition2 = New<TPartition>();
    partition2->PartitionId = TPartitionId(TGuid::Create());
    partition2->ComputationId = TComputationId("TestComputation2");
    partition2->LowerKey = MakeKey<ui64>(11);
    partition2->UpperKey = MakeKey<ui64>(20);

    auto tablesContext = PrepareTablesContext();
    auto timersTable = New<NTables::TTimers>(tablesContext, New<TDynamicTableRequestSpec>());

    auto tx1 = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();
    auto tx2 = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    auto payloadSchema = PrepareDefaultKeySchema();

    auto row1 = YsonToSchemafulRow("key_hash=1u;key=\"test key1\"", *payloadSchema, true);
    auto row2 = YsonToSchemafulRow("key_hash=11u;key=\"test key2\"", *payloadSchema, true);
    auto row3 = YsonToSchemafulRow("key_hash=19u;key=\"test key3\"", *payloadSchema, true);

    auto makeTimer = [&] (const std::string& messageId, TUnversionedOwningRow row) {
        return TTimer{
            TMessageMeta{
                .MessageId = TMessageId(messageId),
                .SystemTimestamp = TSystemTimestamp(1734683888),
                .EventTimestamp = TSystemTimestamp(1734683777),
                .StreamId = TStreamId("timer"),
            },
            TKey(TKey::TUnderlying(row)),
            payloadSchema,
            TSystemTimestamp(1734683999)};
    };

    auto timer1 = makeTimer("timer1", row1);
    auto timer2 = makeTimer("timer2", row2);
    auto timer3 = makeTimer("timer3", row3);

    timersTable->Write(tx1, partition1->ComputationId, std::vector<TTimer>{timer1});
    timersTable->Write(tx2, partition2->ComputationId, std::vector<TTimer>{timer2, timer3});

    WaitFor(tx1->Commit())
        .ThrowOnError();
    WaitFor(tx2->Commit())
        .ThrowOnError();

    auto loadResult1 = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
            .ComputationId = partition1->ComputationId,
            .LowerKey = partition1->LowerKey,
            .UpperKey = partition1->UpperKey}))
        .ValueOrThrow();

    ASSERT_EQ(std::ssize(loadResult1), 1);
    ASSERT_TRUE(TimersEqualWithoutSchema(timer1, loadResult1[0].second));

    auto loadResult2 = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
            .ComputationId = partition2->ComputationId,
            .LowerKey = partition2->LowerKey,
            .UpperKey = partition2->UpperKey}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(loadResult2), 2);
    ASSERT_TRUE(TimersEqualWithoutSchema(timer2, loadResult2[0].second));
    ASSERT_TRUE(TimersEqualWithoutSchema(timer3, loadResult2[1].second));

    auto eraseTx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();
    timersTable->Erase(eraseTx, std::vector<NTables::TTimers::TTableKey>{{partition2->ComputationId, timer3.Key, timer3.MessageId}});
    WaitFor(eraseTx->Commit())
        .ThrowOnError();

    loadResult2 = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
            .ComputationId = partition2->ComputationId,
            .LowerKey = partition2->LowerKey,
            .UpperKey = partition2->UpperKey}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(loadResult2), 1);
    ASSERT_TRUE(TimersEqualWithoutSchema(timer2, loadResult2[0].second));
}

TEST_W(TTestTimerStore, BasicTimers)
{
    auto spec = New<TComputationSpec>();
    spec->GroupBySchema = PrepareDefaultKeySchema();

    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = TComputationId("TestComputation");
    partition->LowerKey = MakeKey<ui64>(0);
    partition->UpperKey = MakeKey<ui64>(10);

    auto tablesContext = PrepareTablesContext();
    auto timersTable = New<NTables::TTimers>(tablesContext, New<TDynamicTableRequestSpec>());

    auto timerContext = New<TTimerStoreContext>();
    timerContext->Logger = Logger();
    timerContext->Profiler = NProfiling::TProfiler();
    timerContext->Partition = partition;
    timerContext->KeySchema = spec->GroupBySchema;
    timerContext->StreamsDependency["timer"].insert("test_input");
    timerContext->TimerSpecs["timer"] = New<TTimerSpec>();
    timerContext->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
    timerContext->TimersTable = timersTable;

    TTimer timer;
    timer.MessageId = TMessageId("timer_1");
    timer.StreamId = "timer";
    timer.EventTimestamp = TSystemTimestamp(1734683888);
    timer.SystemTimestamp = TSystemTimestamp(1734683895);
    timer.TriggerTimestamp = TSystemTimestamp(1734683900);
    timer.KeySchema = spec->GroupBySchema;
    timer.Key = TKey(TKey::TUnderlying(YsonToSchemafulRow("key_hash=1u;key=\"test key1\"", *timer.KeySchema, true)));

    auto dynamicContext = New<TDynamicTimerStoreContext>();
    dynamicContext->DynamicTimerStoreSpec = New<TDynamicTimerStoreSpec>();
    const auto timerStore = CreateTimerStore(timerContext, dynamicContext);
    WaitFor(timerStore->Init()).ThrowOnError();
    EXPECT_EQ(timerStore->GetCount(), 0);

    {
        timerStore->Register({timer});

        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        timerStore->Sync(tx);
        WaitFor(tx->Commit())
            .ThrowOnError();
        auto loadResult = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
                .ComputationId = partition->ComputationId,
                .LowerKey = partition->LowerKey,
                .UpperKey = partition->UpperKey}))
            .ValueOrThrow();
        ASSERT_EQ(loadResult.size(), 1u);
        EXPECT_TRUE(TimersEqualWithoutSchema(timer, loadResult[0].second));
    }

    {
        timerStore->Unregister({New<TInputTimer>(TTimer(timer))});

        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        timerStore->Sync(tx);
        WaitFor(tx->Commit())
            .ThrowOnError();
        auto loadResult = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
                .ComputationId = partition->ComputationId,
                .LowerKey = partition->LowerKey,
                .UpperKey = partition->UpperKey}))
            .ValueOrThrow();
        ASSERT_EQ(loadResult.size(), 0u);
    }
}

TEST_W(TTestTimerStore, DeduplicateTimers)
{
    auto spec = New<TComputationSpec>();
    spec->GroupBySchema = PrepareDefaultKeySchema();

    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = TComputationId("TestComputation");
    partition->LowerKey = MakeKey<ui64>(0);
    partition->UpperKey = MakeKey<ui64>(10);

    auto tablesContext = PrepareTablesContext();
    auto timersTable = New<NTables::TTimers>(tablesContext, New<TDynamicTableRequestSpec>());

    auto timerContext = New<TTimerStoreContext>();
    timerContext->Logger = Logger();
    timerContext->Profiler = NProfiling::TProfiler();
    timerContext->Partition = partition;
    timerContext->KeySchema = spec->GroupBySchema;
    timerContext->StreamsDependency["timer"].insert("test_input");
    timerContext->TimerSpecs["timer"] = New<TTimerSpec>();
    timerContext->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
    timerContext->TimersTable = timersTable;

    TTimer firstTimer;
    firstTimer.MessageId = TMessageId("timer_1");
    firstTimer.StreamId = "timer";
    firstTimer.EventTimestamp = TSystemTimestamp(1734683888);
    firstTimer.SystemTimestamp = TSystemTimestamp(1734683895);
    firstTimer.TriggerTimestamp = TSystemTimestamp(1734683900);
    firstTimer.KeySchema = spec->GroupBySchema;
    firstTimer.Key = TKey(TKey::TUnderlying(YsonToSchemafulRow("key_hash=1u;key=\"test key1\"", *firstTimer.KeySchema, true)));

    TTimer secondTimer = firstTimer;
    secondTimer.MessageId = TMessageId("timer_2");
    secondTimer.EventTimestamp = TSystemTimestamp(1734683887);

    TTimer thirdTimer = firstTimer;
    thirdTimer.MessageId = TMessageId("timer_3");
    thirdTimer.EventTimestamp = TSystemTimestamp(1734683885);

    auto dynamicContext = New<TDynamicTimerStoreContext>();
    dynamicContext->DynamicTimerStoreSpec = New<TDynamicTimerStoreSpec>();
    const auto timerStore = CreateTimerStore(timerContext, dynamicContext);
    WaitFor(timerStore->Init()).ThrowOnError();
    EXPECT_EQ(timerStore->GetCount(), 0);

    {
        timerStore->Register({firstTimer});

        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        timerStore->Sync(tx);
        WaitFor(tx->Commit())
            .ThrowOnError();
        auto loadResult = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
                .ComputationId = partition->ComputationId,
                .LowerKey = partition->LowerKey,
                .UpperKey = partition->UpperKey}))
            .ValueOrThrow();
        ASSERT_EQ(loadResult.size(), 1u);
        EXPECT_TRUE(TimersEqualWithoutSchema(loadResult[0].second, firstTimer));
    }

    {
        timerStore->Register({secondTimer});

        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        timerStore->Sync(tx);
        WaitFor(tx->Commit())
            .ThrowOnError();
        auto loadResult = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
                .ComputationId = partition->ComputationId,
                .LowerKey = partition->LowerKey,
                .UpperKey = partition->UpperKey}))
            .ValueOrThrow();
        ASSERT_EQ(loadResult.size(), 1u);
        EXPECT_TRUE(TimersEqualWithoutSchema(loadResult[0].second, secondTimer));
    }

    {
        timerStore->Register({thirdTimer});
        timerStore->Unregister({New<TInputTimer>(TTimer(secondTimer))});
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        timerStore->Sync(tx);
        WaitFor(tx->Commit())
            .ThrowOnError();
        auto loadResult = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
                .ComputationId = partition->ComputationId,
                .LowerKey = partition->LowerKey,
                .UpperKey = partition->UpperKey}))
            .ValueOrThrow();
        ASSERT_EQ(loadResult.size(), 1u);
        EXPECT_TRUE(TimersEqualWithoutSchema(loadResult[0].second, thirdTimer));
    }

    {
        timerStore->Unregister({New<TInputTimer>(TTimer(thirdTimer))});
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        timerStore->Sync(tx);
        WaitFor(tx->Commit())
            .ThrowOnError();
        auto loadResult = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
                .ComputationId = partition->ComputationId,
                .LowerKey = partition->LowerKey,
                .UpperKey = partition->UpperKey}))
            .ValueOrThrow();
        ASSERT_EQ(loadResult.size(), 0u);
    }

    {
        timerStore->Register({secondTimer, firstTimer, thirdTimer});
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        timerStore->Sync(tx);
        WaitFor(tx->Commit())
            .ThrowOnError();
        auto loadResult = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
                .ComputationId = partition->ComputationId,
                .LowerKey = partition->LowerKey,
                .UpperKey = partition->UpperKey}))
            .ValueOrThrow();
        ASSERT_EQ(loadResult.size(), 1u);
        EXPECT_TRUE(TimersEqualWithoutSchema(loadResult[0].second, thirdTimer));
    }
}

TEST_W(TTestTimerStore, NoDeduplicationTimers)
{
    auto spec = New<TComputationSpec>();
    spec->GroupBySchema = PrepareDefaultKeySchema();

    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = TComputationId("TestComputation");
    partition->LowerKey = MakeKey<ui64>(0);
    partition->UpperKey = MakeKey<ui64>(10);

    auto tablesContext = PrepareTablesContext();
    auto timersTable = New<NTables::TTimers>(tablesContext, New<TDynamicTableRequestSpec>());

    auto timerContext = New<TTimerStoreContext>();
    timerContext->Logger = Logger();
    timerContext->Profiler = NProfiling::TProfiler();
    timerContext->Partition = partition;
    timerContext->KeySchema = spec->GroupBySchema;
    timerContext->StreamsDependency["timer"].insert("test_input");
    timerContext->TimerSpecs["timer"] = New<TTimerSpec>();
    timerContext->TimerSpecs["timer"]->DeduplicateEqualTimestamps = false;
    timerContext->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
    timerContext->TimersTable = timersTable;

    TTimer firstTimer;
    firstTimer.MessageId = TMessageId("timer_1");
    firstTimer.StreamId = "timer";
    firstTimer.EventTimestamp = TSystemTimestamp(1734683888);
    firstTimer.SystemTimestamp = TSystemTimestamp(1734683895);
    firstTimer.TriggerTimestamp = TSystemTimestamp(1734683900);
    firstTimer.KeySchema = spec->GroupBySchema;
    firstTimer.Key = TKey(TKey::TUnderlying(YsonToSchemafulRow("key_hash=1u;key=\"test key1\"", *firstTimer.KeySchema, true)));

    TTimer secondTimer = firstTimer;
    secondTimer.MessageId = TMessageId("timer_2");
    secondTimer.EventTimestamp = TSystemTimestamp(1734683887);

    auto dynamicContext = New<TDynamicTimerStoreContext>();
    dynamicContext->DynamicTimerStoreSpec = New<TDynamicTimerStoreSpec>();
    const auto timerStore = CreateTimerStore(timerContext, dynamicContext);
    WaitFor(timerStore->Init()).ThrowOnError();
    EXPECT_EQ(timerStore->GetCount(), 0);

    {
        timerStore->Register({secondTimer, firstTimer});
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        timerStore->Sync(tx);
        WaitFor(tx->Commit())
            .ThrowOnError();
        auto loadResult = WaitFor(timersTable->LoadAll(NTables::TTimers::TFilter{
                .ComputationId = partition->ComputationId,
                .LowerKey = partition->LowerKey,
                .UpperKey = partition->UpperKey}))
            .ValueOrThrow();
        ASSERT_EQ(loadResult.size(), 2u);
        std::vector<TTimer> persistedTimers;
        for (const auto& [key, timer] : loadResult) {
            persistedTimers.push_back(timer);
        }
        std::sort(persistedTimers.begin(), persistedTimers.end(), [] (const auto& l, const auto& r) {
            return l.MessageId < r.MessageId;
        });
        EXPECT_TRUE(TimersEqualWithoutSchema(persistedTimers[0], firstTimer));
        EXPECT_TRUE(TimersEqualWithoutSchema(persistedTimers[1], secondTimer));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
