#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/timer_page.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/flow/library/cpp/computation/stores/timer_page_catalogue.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/flow/library/cpp/pipeline_tables/public.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/timer_pages.h>
#include <yt/yt/flow/library/cpp/tables/timers.h>
#include <yt/yt/flow/library/cpp/tables/transaction_manager.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/client/api/transaction.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NCppTests;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, Logger, "CppTests");

////////////////////////////////////////////////////////////////////////////////

//! The paged backend against a real YT: the page tables are created from the catalogue
//! definitions, as the packaged pipeline node may predate them.
class TTestPagedTimerStore
    : public TDynamicTablesTestBase
{
protected:
    static constexpr i64 PageSize = 16;
    static constexpr TStringBuf TimerStreamId = "timer";
    static constexpr TStringBuf InputStreamId = "test_input";

    TActionQueuePtr ActionQueue_;
    ITransactionPtr LeaseTransaction_;
    TTimerPageCataloguePtr Catalogue_;
    NTables::TTimerPagesPtr TimerPages_;
    NTables::TTimersPtr Timers_;
    ITimeProviderPtr TimeProvider_ = New<TFakeTimeProvider>();
    TTableSchemaPtr KeySchema_ = New<TTableSchema>(std::vector<TColumnSchema>{TColumnSchema("key", EValueType::Uint64)}, /*strict*/ true);

    void SetUp() override
    {
        ActionQueue_ = New<TActionQueue>();
        LeaseTransaction_ = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Master)).ValueOrThrow();
        WaitFor(Client_->CreateNode(GetPipelinePath(), EObjectType::Pipeline)).ThrowOnError();
        WaitUntilEqual(YPathJoin(GetPipelinePath(), "timers") + "/@tablet_state", "mounted");

        const auto& definitions = GetPipelineTableDefinitions();
        std::vector<TStringBuf> names{TimerPagesTableName, TimerPageMetaTableName, TimerPageJournalTableName};
        for (auto name : names) {
            auto path = YPathJoin(GetPipelinePath(), name);
            if (WaitFor(Client_->NodeExists(path)).ValueOrThrow()) {
                continue;
            }
            auto it = definitions.Tables.find(std::string(name));
            const auto& definition = it != definitions.Tables.end() ? it->second : GetOrCrash(definitions.Queues, std::string(name));
            TCreateNodeOptions options;
            options.Attributes = BuildPipelineTableAttributes(definition);
            WaitFor(Client_->CreateNode(path, EObjectType::Table, options)).ThrowOnError();
            WaitFor(Client_->MountTable(path)).ThrowOnError();
        }
        for (auto name : names) {
            WaitUntilEqual(YPathJoin(GetPipelinePath(), name) + "/@tablet_state", "mounted");
        }

        auto tablesContext = PrepareTablesContext();
        TimerPages_ = New<NTables::TTimerPages>(tablesContext, New<TDynamicTableRequestSpec>());
        Timers_ = New<NTables::TTimers>(tablesContext, New<TDynamicTableRequestSpec>());
        RecreateCatalogue();
    }

    void RecreateCatalogue()
    {
        if (Catalogue_) {
            WaitFor(Catalogue_->Stop()).ThrowOnError();
        }
        TTimerPageCatalogueOptions options;
        options.JournalPollPeriod = TDuration::MilliSeconds(100);
        options.EmptyPageCollectionPeriod = std::nullopt;
        auto client = Client_;
        Catalogue_ = New<TTimerPageCatalogue>(
            TimerPages_,
            [client] {
                return client->StartTransaction(NTransactionClient::ETransactionType::Tablet);
            },
            ActionQueue_->GetInvoker(),
            Logger(),
            NProfiling::TProfiler(),
            options);
    }

    void TearDown() override
    {
        if (Catalogue_) {
            WaitFor(Catalogue_->Stop()).ThrowOnError();
        }
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetPipelinePath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iterationCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
        if (LeaseTransaction_) {
            WaitFor(LeaseTransaction_->Abort()).ThrowOnError();
            LeaseTransaction_.Reset();
        }
        if (ActionQueue_) {
            ActionQueue_->Shutdown();
        }
    }

    static TYPath GetPipelinePath()
    {
        return "//pipeline_paged_timers";
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

    TTimerStoreContextPtr MakeContext(
        ETimerStoreBackend backend,
        bool deduplicate = false,
        ui64 lowerKey = 0,
        std::optional<ui64> upperKey = 100)
    {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->ComputationId = TComputationId("TestComputation");
        partition->LowerKey = MakeUintKey(lowerKey);
        partition->UpperKey = MakeUintKey(upperKey);

        auto timerSpec = New<TTimerSpec>();
        timerSpec->TimeType = ETimeType::SystemTime;
        timerSpec->DeduplicateEqualTimestamps = deduplicate;

        auto context = New<TTimerStoreContext>();
        context->Logger = Logger();
        context->Profiler = NProfiling::TProfiler();
        context->Partition = partition;
        context->KeySchema = KeySchema_;
        context->StreamsDependency[TStreamId(TimerStreamId)].insert(TStreamId(InputStreamId));
        context->TimerSpecs[TStreamId(TimerStreamId)] = timerSpec;
        context->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
        context->Backend = backend;
        context->TimersTable = Timers_;
        context->TimerPageSize = PageSize;
        context->TimerPagesTable = TimerPages_;
        context->PageCatalogue = Catalogue_;
        context->TimeProvider = TimeProvider_;

        // The conversions of the init commit under the lease, like the job's own epochs.
        auto transactionContext = New<NTables::TTransactionManagerContext>();
        transactionContext->Client = Client_;
        transactionContext->PipelinePath = GetPipelinePath();
        transactionContext->LoadThroughputThrottler = PrepareTablesContext()->LoadThroughputThrottler;
        transactionContext->Logger = Logger();
        transactionContext->Profiler = NProfiling::TProfiler();
        transactionContext->LeaseId = LeaseTransaction_->GetId();
        transactionContext->PartitionId = partition->PartitionId;
        transactionContext->StatusProfiler = CreateSyncStatusProfiler();
        context->TransactionManager = New<NTables::TTransactionManager>(transactionContext, New<TDynamicRetryableRequestSpec>());
        return context;
    }

    ITimerStorePtr CreateStore(
        ETimerStoreBackend backend,
        bool deduplicate = false,
        ui64 lowerKey = 0,
        std::optional<ui64> upperKey = 100)
    {
        auto dynamicContext = New<TDynamicTimerStoreContext>();
        dynamicContext->DynamicTimerStoreSpec = New<TDynamicTimerStoreSpec>();
        auto store = CreateTimerStore(MakeContext(backend, deduplicate, lowerKey, upperKey), dynamicContext);
        WaitFor(store->Init()).ThrowOnError();
        return store;
    }

    TTimer MakeTimer(const std::string& id, ui64 key, ui64 trigger, ui64 event = 10, ui64 system = 10)
    {
        TTimer timer;
        timer.MessageId = TMessageId(id);
        timer.StreamId = TStreamId(TimerStreamId);
        timer.EventTimestamp = TSystemTimestamp(event);
        timer.SystemTimestamp = TSystemTimestamp(system);
        timer.AlignmentTimestamp = timer.SystemTimestamp;
        timer.TriggerTimestamp = TSystemTimestamp(trigger);
        timer.KeySchema = KeySchema_;
        timer.Key = MakeKey(ui64(key));
        return timer;
    }

    void SetWatermark(const ITimerStorePtr& store, ui64 timestamp)
    {
        auto watermarks = New<TWatermarks>();
        watermarks->SystemWatermark = TSystemTimestamp(timestamp);
        watermarks->EventWatermark = TSystemTimestamp(timestamp);
        auto state = New<TWatermarkState>();
        state->Streams[TStreamId(InputStreamId)] = watermarks;
        store->UpdateWatermarkState(state);
    }

    //! One epoch with a real transaction, in the order of the run iteration.
    std::vector<TInputTimerConstPtr> Iterate(const ITimerStorePtr& store, ui64 watermark, std::vector<TTimer> outputs = {}, i64 maxRows = 1000)
    {
        WaitFor(store->Preload()).ThrowOnError();
        SetWatermark(store, watermark);
        auto batch = store->GetNextBatch({TStreamId(TimerStreamId)}, maxRows, std::numeric_limits<i64>::max());
        store->Unregister(batch);
        store->Register(std::move(outputs));
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)).ValueOrThrow();
        store->Sync(tx);
        WaitFor(tx->Commit()).ThrowOnError();
        store->Commit();
        return batch;
    }

    static std::vector<std::string> Ids(const std::vector<TInputTimerConstPtr>& timers)
    {
        std::vector<std::string> ids;
        for (const auto& timer : timers) {
            ids.push_back(std::string(timer->MessageId.Underlying()));
        }
        return ids;
    }

    //! The pages of the computation by family and begin.
    std::vector<TTimerPageMeta> LoadPages()
    {
        auto pages = WaitFor(TimerPages_->LoadMeta({})).ValueOrThrow();
        std::sort(pages.begin(), pages.end(), [] (const auto& lhs, const auto& rhs) {
            return std::tuple(lhs.FamilyId, lhs.Begin) < std::tuple(rhs.FamilyId, rhs.Begin);
        });
        return pages;
    }

    std::vector<TTimerPageRow> ReadPage(const TTimerPageMeta& page)
    {
        return WaitFor(TimerPages_->ReadPage(page, std::nullopt)).ValueOrThrow();
    }

    i64 CountPageRows()
    {
        i64 rows = 0;
        for (const auto& page : LoadPages()) {
            rows += std::ssize(ReadPage(page));
        }
        return rows;
    }

    i64 CountTimerRows()
    {
        return std::ssize(WaitFor(Timers_->LoadAll({.ComputationId = TComputationId("TestComputation")})).ValueOrThrow());
    }

    i64 CountCataloguePages()
    {
        WaitFor(Catalogue_->Synchronize()).ThrowOnError();
        return Catalogue_->GetSnapshot().GetPageCount();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_W(TTestPagedTimerStore, FireSplitAndRestart)
{
    auto store = CreateStore(ETimerStoreBackend::Paged);
    EXPECT_TRUE(store->IsEmpty());

    // Register out of trigger order.
    std::vector<TTimer> timers;
    for (i64 index = 2 * PageSize + 4; index-- > 0;) {
        timers.push_back(MakeTimer(Format("t-%v", index), index, 100 + index, 20 + index, 30 + index));
    }
    Iterate(store, 0, std::move(timers));
    EXPECT_EQ(CountPageRows(), 2 * PageSize + 4);
    auto pages = LoadPages();
    ASSERT_EQ(pages.size(), 2u);
    EXPECT_EQ(pages[0].Count, PageSize);
    EXPECT_EQ(pages[1].Count, PageSize + 4);
    EXPECT_EQ(pages[0].MinEvent, TSystemTimestamp(20));
    EXPECT_EQ(pages[0].MinSystem, TSystemTimestamp(30));
    EXPECT_FALSE(pages[0].Flags.OnceShared);
    EXPECT_FALSE(pages[0].Flags.DedupSlots);

    // The next preload splits the remaining tail by changing metadata.
    Iterate(store, 0);
    pages = LoadPages();
    ASSERT_EQ(pages.size(), 3u);
    EXPECT_EQ(pages[0].Count, PageSize);
    EXPECT_EQ(pages[1].Count, PageSize);
    EXPECT_EQ(pages[2].Count, 4);
    EXPECT_EQ(pages[0].End, pages[1].Begin);
    EXPECT_EQ(pages[1].End, pages[2].Begin);
    EXPECT_FALSE(pages[2].End);
    EXPECT_EQ(CountPageRows(), 2 * PageSize + 4);
    EXPECT_EQ(CountCataloguePages(), 3);

    // A restart sees the pages through the journal and fires in order.
    store = CreateStore(ETimerStoreBackend::Paged);
    EXPECT_EQ(store->GetCount(), 2 * PageSize + 4);
    EXPECT_EQ(GetOrCrash(store->BuildInflight(), TStreamId(TimerStreamId))->MinSystemTimestamp, TSystemTimestamp(30));
    auto batch = Iterate(store, 100 + PageSize + 1, {}, /*maxRows*/ 10);
    EXPECT_EQ(Ids(batch), (std::vector<std::string>{"t-0", "t-1", "t-2", "t-3", "t-4", "t-5", "t-6", "t-7", "t-8", "t-9"}));
    batch = Iterate(store, 100 + PageSize + 1);
    EXPECT_EQ(std::ssize(batch), PageSize + 2 - 10);
    EXPECT_EQ(CountPageRows(), PageSize + 2);
    // The empty head merges with its neighbor, preserving family coverage.
    pages = LoadPages();
    ASSERT_EQ(pages.size(), 2u);
    EXPECT_EQ(pages[0].Count, PageSize - 2);
    EXPECT_EQ(pages[1].Count, 4);
    batch = Iterate(store, 1000);
    EXPECT_EQ(std::ssize(batch), PageSize + 2);
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(CountPageRows(), 0);
    auto inflight = GetOrCrash(store->BuildInflight(), TStreamId(TimerStreamId));
    EXPECT_TRUE(inflight->Empty);
}

TEST_W(TTestPagedTimerStore, AbortedEpochRestoresRowsAndMetadata)
{
    auto store = CreateStore(ETimerStoreBackend::Paged);
    std::vector<TTimer> original;
    std::vector<std::string> expected;
    for (int index = 0; index < PageSize; ++index) {
        auto id = Format("original-%v", index);
        original.push_back(MakeTimer(id, index, 100 + index));
        expected.push_back(id);
    }
    Iterate(store, 0, original);
    WaitFor(store->Preload()).ThrowOnError();
    SetWatermark(store, 100);
    auto fired = store->GetNextBatch({TStreamId(TimerStreamId)}, 1, std::numeric_limits<i64>::max());
    ASSERT_EQ(fired.size(), 1u);
    store->Unregister(fired);
    std::vector<TTimer> added;
    for (int index = 0; index < 3 * PageSize; ++index) {
        added.push_back(MakeTimer(Format("aborted-%v", index), index, 200 + index));
    }
    store->Register(std::move(added));
    auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)).ValueOrThrow();
    store->Sync(tx);
    WaitFor(tx->Abort()).ThrowOnError();
    store.Reset();

    RecreateCatalogue();
    store = CreateStore(ETimerStoreBackend::Paged);
    EXPECT_EQ(store->GetCount(), PageSize);
    EXPECT_EQ(CountPageRows(), PageSize);
    auto pages = LoadPages();
    ASSERT_EQ(pages.size(), 1u);
    EXPECT_EQ(pages.front().Count, PageSize);
    EXPECT_EQ(Ids(Iterate(store, 1000)), expected);
    EXPECT_TRUE(store->IsEmpty());
}

TEST_W(TTestPagedTimerStore, DeduplicationAndReregistration)
{
    auto store = CreateStore(ETimerStoreBackend::Paged, /*deduplicate*/ true);
    Iterate(store, 0, {MakeTimer("first", 5, 100, 30), MakeTimer("second", 5, 100, 20)});
    EXPECT_EQ(CountPageRows(), 1);
    auto pages = LoadPages();
    ASSERT_EQ(pages.size(), 1u);
    EXPECT_TRUE(pages[0].Flags.DedupSlots);

    // The stored timer wins against a later one with a higher event and loses to a lower one.
    Iterate(store, 0, {MakeTimer("higher", 5, 100, 25)});
    EXPECT_EQ(CountPageRows(), 1);
    Iterate(store, 0, {MakeTimer("lower", 5, 100, 15)});
    EXPECT_EQ(CountPageRows(), 1);
    EXPECT_EQ(LoadPages()[0].Count, 1);

    // A fired slot re-registered in the same iteration: the erase and the write share the transaction.
    EXPECT_EQ(Ids(Iterate(store, 100, {MakeTimer("again", 5, 100, 50)})), (std::vector<std::string>{"lower"}));
    EXPECT_EQ(CountPageRows(), 1);
    EXPECT_EQ(Ids(Iterate(store, 100)), (std::vector<std::string>{"again"}));
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(CountPageRows(), 0);
}

TEST_W(TTestPagedTimerStore, SplitIntoChildrenAndMergeBack)
{
    // The parent writes timers on both sides of the split key.
    auto parent = CreateStore(ETimerStoreBackend::Paged);
    Iterate(parent, 0, {MakeTimer("left", 10, 100), MakeTimer("right", 60, 50), MakeTimer("left-late", 20, 200)});
    auto pages = LoadPages();
    ASSERT_EQ(pages.size(), 1u);

    // Without registrations, the child keeps its own portion resident in the shared page.
    auto left = CreateStore(ETimerStoreBackend::Paged, false, 0, 50);
    EXPECT_EQ(left->GetCount(), 2);
    EXPECT_TRUE(Iterate(left, 0).empty());
    pages = LoadPages();
    ASSERT_EQ(pages.size(), 1u);
    EXPECT_TRUE(pages[0].Flags.OnceShared);
    EXPECT_EQ(pages[0].Count, 3);
    EXPECT_EQ(Ids(Iterate(left, 100)), (std::vector<std::string>{"left"}));

    // The right child fires its row out of the shared page.
    auto right = CreateStore(ETimerStoreBackend::Paged, /*deduplicate*/ false, /*lowerKey*/ 50, /*upperKey*/ 100);
    EXPECT_EQ(right->GetCount(), 1);
    EXPECT_EQ(Ids(Iterate(right, 50)), (std::vector<std::string>{"right"}));
    EXPECT_TRUE(right->IsEmpty());
    Iterate(right, 50, {MakeTimer("right-late", 70, 300)});
    EXPECT_EQ(CountPageRows(), 2);

    auto merged = CreateStore(ETimerStoreBackend::Paged);
    EXPECT_EQ(merged->GetCount(), 2);
    EXPECT_TRUE(Iterate(merged, 100).empty());
    EXPECT_EQ(LoadPages().size(), 2u);

    // Two mandatory writes fund at most two transfers at the default multiplier.
    Iterate(merged, 100, {MakeTimer("funding-1", 1, 400), MakeTimer("funding-2", 2, 500)});
    Catalogue_->CollectEmptyPages();
    pages = LoadPages();
    ASSERT_EQ(pages.size(), 1u);
    EXPECT_EQ(pages[0].KeyRange, MakeUintKeyRange(0, 100));
    EXPECT_FALSE(pages[0].Flags.OnceShared);
    EXPECT_EQ(pages[0].Count, 4);
    EXPECT_EQ(CountPageRows(), 4);
    EXPECT_EQ(Ids(Iterate(merged, 1000)),
        (std::vector<std::string>{"left-late", "right-late", "funding-1", "funding-2"}));
    EXPECT_TRUE(merged->IsEmpty());
    EXPECT_EQ(CountPageRows(), 0);
}

TEST_W(TTestPagedTimerStore, ConvertsBetweenBackends)
{
    // Timers of the in-memory backend move into pages at the paged init and back at the in-memory init.
    {
        auto inMemory = CreateStore(ETimerStoreBackend::InMemory);
        inMemory->Register({MakeTimer("a", 1, 100), MakeTimer("b", 2, 200)});
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet)).ValueOrThrow();
        inMemory->Sync(tx);
        WaitFor(tx->Commit()).ThrowOnError();
        inMemory->Commit();
    }
    EXPECT_EQ(CountTimerRows(), 2);

    auto paged = CreateStore(ETimerStoreBackend::Paged);
    EXPECT_EQ(CountTimerRows(), 0);
    EXPECT_EQ(CountPageRows(), 2);
    EXPECT_EQ(paged->GetCount(), 2);
    EXPECT_EQ(Ids(Iterate(paged, 100, {MakeTimer("c", 3, 150)})), (std::vector<std::string>{"a"}));
    EXPECT_EQ(CountPageRows(), 2);

    auto inMemory = CreateStore(ETimerStoreBackend::InMemory);
    EXPECT_EQ(CountPageRows(), 0);
    EXPECT_EQ(CountTimerRows(), 2);
    EXPECT_EQ(inMemory->GetInMemoryCount(), 2);
    SetWatermark(inMemory, 200);
    EXPECT_EQ(Ids(inMemory->GetNextBatch({TStreamId(TimerStreamId)}, 10, std::numeric_limits<i64>::max())), (std::vector<std::string>{"c", "b"}));
}

TEST_W(TTestPagedTimerStore, ConvertsPagesOnDeduplicationModeChange)
{
    // Without deduplication a slot keeps every timer; turning it on rewrites the pages as
    // slots at the init and keeps the best timer of each slot.
    auto plain = CreateStore(ETimerStoreBackend::Paged);
    Iterate(plain, 0, {MakeTimer("a-worse", 5, 100, 30), MakeTimer("a-best", 5, 100, 5), MakeTimer("b", 6, 100, 7)});
    EXPECT_EQ(CountPageRows(), 3);

    auto deduplicating = CreateStore(ETimerStoreBackend::Paged, /*deduplicate*/ true);
    auto pages = LoadPages();
    ASSERT_EQ(pages.size(), 1u);
    EXPECT_TRUE(pages[0].Flags.DedupSlots);
    EXPECT_EQ(pages[0].Count, 2);
    EXPECT_EQ(pages[0].MinEvent, TSystemTimestamp(5));
    for (const auto& row : ReadPage(pages[0])) {
        EXPECT_TRUE(row.RowKey.RowId.Underlying().empty());
    }
    EXPECT_EQ(deduplicating->GetCount(), 2);
    Iterate(deduplicating, 0, {MakeTimer("a-later", 5, 100, 20)});
    EXPECT_EQ(CountPageRows(), 2);

    // Turning it off rewrites the slots as plain rows; duplicates are allowed again.
    plain = CreateStore(ETimerStoreBackend::Paged);
    pages = LoadPages();
    ASSERT_EQ(pages.size(), 1u);
    EXPECT_FALSE(pages[0].Flags.DedupSlots);
    EXPECT_EQ(pages[0].Count, 2);
    for (const auto& row : ReadPage(pages[0])) {
        EXPECT_FALSE(row.RowKey.RowId.Underlying().empty());
    }
    Iterate(plain, 0, {MakeTimer("a-dup", 5, 100, 20)});
    EXPECT_EQ(CountPageRows(), 3);
    EXPECT_EQ(Ids(Iterate(plain, 100)), (std::vector<std::string>{"a-best", "a-dup", "b"}));
    EXPECT_TRUE(plain->IsEmpty());
    EXPECT_EQ(CountPageRows(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
