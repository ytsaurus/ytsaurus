#include <yt/yt/flow/library/cpp/computation/stores/paged_timer_store.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_page_catalogue.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_page_data.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_store_conversion.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/timer_pages.h>
#include <yt/yt/flow/library/cpp/tables/unittests/mock/timers.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <util/generic/scope.h>

#include <algorithm>
#include <atomic>
#include <memory>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EInterruptedOperation,
    (Load)
    (Write)
);

//! Fails the call number |FailOnCall| of |Load| or |Write|, which is how the tests interrupt a
//! conversion between two batches: the batches before it are fully committed.
class TInterruptibleTimers
    : public NTables::TInMemoryTimers
{
public:
    TInterruptibleTimers(EInterruptedOperation operation, int failOnCall)
        : Operation_(operation)
        , FailOnCall_(failOnCall)
    { }

    TFuture<TLoadResult> Load(
        TFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) override
    {
        if (Operation_ == EInterruptedOperation::Load && ++CallCount_ == FailOnCall_) {
            THROW_ERROR_EXCEPTION("Injected interruption");
        }
        return TInMemoryTimers::Load(std::move(filter), limit, std::move(offsetExclusive));
    }

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        const std::vector<TTimer>& timers) override
    {
        if (Operation_ == EInterruptedOperation::Write && ++CallCount_ == FailOnCall_) {
            THROW_ERROR_EXCEPTION("Injected interruption");
        }
        TInMemoryTimers::Write(std::move(transaction), computationId, timers);
    }

private:
    const EInterruptedOperation Operation_;
    const int FailOnCall_;
    int CallCount_ = 0;
};

class TPagedTimerStoreTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId = TComputationId("test-computation");
    const TStreamId TimerStreamId = TStreamId("test-timer");
    const TStreamId InputStreamId = TStreamId("test-input");
    static constexpr i64 PageSize = 16;

    TActionQueuePtr Queue_ = New<TActionQueue>("Test");
    NTables::TInMemoryTimerPagesPtr Tables_ = New<NTables::TInMemoryTimerPages>();
    TTimerPageCataloguePtr Catalogue_;
    ITimeProviderPtr TimeProvider_ = New<TFakeTimeProvider>();
    TTableSchemaPtr KeySchema_ = New<TTableSchema>(std::vector<TColumnSchema>{TColumnSchema("key", EValueType::Uint64)});

    void SetUp() override
    {
        TTimerPageCatalogueOptions options;
        options.EmptyPageCollectionPeriod = std::nullopt;
        Catalogue_ = New<TTimerPageCatalogue>(
            Tables_,
            [] {
                return MakeFuture(NApi::ITransactionPtr());
            },
            Queue_->GetInvoker(),
            NLogging::TLogger("Test"),
            NProfiling::TProfiler(),
            options);
    }

    void TearDown() override
    {
        WaitFor(Catalogue_->Stop()).ThrowOnError();
        Queue_->Shutdown();
    }

    TTimerStoreContextPtr MakeContext(
        ui64 lowerKey = 0,
        std::optional<ui64> upperKey = 100,
        bool deduplicateEqualTimestamps = false)
    {
        auto partition = New<TPartition>();
        partition->ComputationId = ComputationId;
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->LowerKey = MakeUintKey(lowerKey);
        partition->UpperKey = MakeUintKey(upperKey);

        auto timerSpec = New<TTimerSpec>();
        timerSpec->TimeType = ETimeType::SystemTime;
        timerSpec->DeduplicateEqualTimestamps = deduplicateEqualTimestamps;

        auto context = New<TTimerStoreContext>();
        context->Partition = std::move(partition);
        context->KeySchema = KeySchema_;
        context->StreamsDependency[TimerStreamId].insert(InputStreamId);
        context->TimerSpecs[TimerStreamId] = std::move(timerSpec);
        context->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
        context->Logger = NLogging::TLogger("Test");
        context->Profiler = NProfiling::TProfiler();
        context->Backend = ETimerStoreBackend::Paged;
        context->TimerPageSize = PageSize;
        context->TimerPagesTable = Tables_;
        context->PageCatalogue = Catalogue_;
        context->TimeProvider = TimeProvider_;
        return context;
    }

    static TDynamicTimerStoreContextPtr MakeDynamicContext(i64 maxRowsPerBatch = 1000)
    {
        auto dynamicContext = New<TDynamicTimerStoreContext>();
        dynamicContext->DynamicTimerStoreSpec = New<TDynamicTimerStoreSpec>();
        dynamicContext->MaxRowsPerBatch = maxRowsPerBatch;
        return dynamicContext;
    }

    ITimerStorePtr CreateStore(TTimerStoreContextPtr context = nullptr, i64 maxRowsPerBatch = 1000)
    {
        auto store = CreateTimerStore(context ? context : MakeContext(), MakeDynamicContext(maxRowsPerBatch));
        WaitFor(store->Init()).ThrowOnError();
        return store;
    }

    TFuture<ITimerStorePtr> StartWaitingChild()
    {
        WaitFor(Catalogue_->Synchronize()).ThrowOnError();
        auto childFiber = std::make_shared<std::atomic<TFiberId>>(0);
        auto waiting = NewPromise<void>();
        Tables_->SetHook([childFiber, waiting] (TStringBuf method) {
            if (method == "LoadMeta") {
                *childFiber = GetCurrentFiberId();
            } else if (method == "GetJournalRowRange" && *childFiber == GetCurrentFiberId()) {
                waiting.TrySet();
            }
        });
        auto clearHook = Finally([this] {
            Tables_->SetHook({});
        });
        auto result = BIND([this] {
            return CreateStore(MakeContext(50, 100, /*deduplicateEqualTimestamps*/ true));
        })
            .AsyncVia(Queue_->GetInvoker())
            .Run();
        WaitFor(waiting.ToFuture()).ThrowOnError();
        EXPECT_FALSE(result.IsSet());
        return result;
    }

    TTimer MakeTimer(const std::string& messageId, ui64 key, ui64 trigger, ui64 system = 10, ui64 event = 10)
    {
        TTimer timer;
        timer.MessageId = TMessageId(messageId);
        timer.StreamId = TimerStreamId;
        timer.SystemTimestamp = TSystemTimestamp(system);
        timer.AlignmentTimestamp = timer.SystemTimestamp;
        timer.EventTimestamp = TSystemTimestamp(event);
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
        auto watermarkState = New<TWatermarkState>();
        watermarkState->Streams[InputStreamId] = watermarks;
        store->UpdateWatermarkState(watermarkState);
    }

    //! Advances only the current timestamp; the stream watermarks stay at zero.
    void SetCurrentTimestamp(const ITimerStorePtr& store, ui64 timestamp)
    {
        auto watermarkState = New<TWatermarkState>();
        watermarkState->CurrentTimestamp = TSystemTimestamp(timestamp);
        store->UpdateWatermarkState(watermarkState);
    }

    //! One epoch: fires what is due, registers |outputs|, persists.
    std::vector<TInputTimerConstPtr> Iterate(
        const ITimerStorePtr& store,
        ui64 watermark,
        std::vector<TTimer> outputs = {},
        i64 maxRows = 1000)
    {
        WaitFor(store->Preload()).ThrowOnError();
        SetWatermark(store, watermark);
        auto batch = store->GetNextBatch({TimerStreamId}, maxRows, std::numeric_limits<i64>::max());
        store->Unregister(batch);
        store->Register(std::move(outputs));
        store->Sync(nullptr);
        store->Commit();
        return batch;
    }

    //! Fires everything; the pages come into memory a few per iteration.
    i64 FireAll(const ITimerStorePtr& store)
    {
        i64 fired = 0;
        for (int iteration = 0; iteration < 1000 && !store->IsEmpty(); ++iteration) {
            fired += std::ssize(Iterate(store, 1'000'000));
        }
        return fired;
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
        auto pages = WaitFor(Tables_->LoadMeta({.ComputationId = ComputationId})).ValueOrThrow();
        std::sort(pages.begin(), pages.end(), [] (const auto& lhs, const auto& rhs) {
            return std::tuple(lhs.FamilyId, lhs.Begin) < std::tuple(rhs.FamilyId, rhs.Begin);
        });
        return pages;
    }

    std::vector<TTimerPageRow> ReadPage(const TTimerPageMeta& page, std::optional<TKeyRange> keyRange = std::nullopt)
    {
        return WaitFor(Tables_->ReadPage(page, keyRange)).ValueOrThrow();
    }

    TTimerPageMeta MakePage(ui64 familyId, ui64 lowerKey, std::optional<ui64> upperKey, bool dedupSlots = false)
    {
        return TTimerPageMeta{
            .ComputationId = ComputationId,
            .FamilyId = TTimerFamilyId(familyId),
            .StreamId = TimerStreamId,
            .KeyRange = MakeUintKeyRange(lowerKey, upperKey),
            .Kind = ETimerPageKind::Trigger,
            .Flags = {.DedupSlots = dedupSlots},
        };
    }

    //! Writes the page with its rows, the count and the minima filled in.
    void WritePage(TTimerPageMeta page, const std::vector<TTimer>& timers)
    {
        std::vector<TTimerPageRow> rows;
        page.Count = std::ssize(timers);
        for (const auto& timer : timers) {
            if (rows.empty() || timer.EventTimestamp < page.MinEvent) {
                page.MinEvent = timer.EventTimestamp;
            }
            if (rows.empty() || timer.SystemTimestamp < page.MinSystem) {
                page.MinSystem = timer.SystemTimestamp;
            }
            rows.push_back(TTimerPageRow{
                .RowKey = MakeTimerPageRowKey(page.FamilyId, ETimerPageKind::Trigger, timer, page.Flags.DedupSlots),
                .Timer = timer,
            });
        }
        Tables_->WriteRows(nullptr, rows);
        Tables_->WritePage(nullptr, page);
        Tables_->WriteJournal(nullptr, {TTimerJournalEntry::Create(page)});
    }

    //! Writes a family with |pages| pages of |perPage| timers each, triggers counting from |firstTrigger|.
    void SeedFamily(ui64 familyId, ui64 lowerKey, std::optional<ui64> upperKey, int pages, int perPage, ui64 firstTrigger = 1, ui64 key = 5, bool dedupSlots = false)
    {
        ui64 trigger = firstTrigger;
        for (int pageIndex = 0; pageIndex < pages; ++pageIndex) {
            auto page = MakePage(familyId, lowerKey, upperKey, dedupSlots);
            page.Begin = TSystemTimestamp(pageIndex == 0 ? 0 : trigger);
            std::vector<TTimer> timers;
            for (int index = 0; index < perPage; ++index) {
                // Zero timestamps are invalid, so the system and event timestamps trail the trigger by one.
                timers.push_back(MakeTimer(Format("seed-%v-%v", familyId, trigger), key, trigger, trigger + 1, trigger + 1));
                ++trigger;
            }
            if (pageIndex + 1 != pages) {
                page.End = TSystemTimestamp(trigger);
            }
            WritePage(page, timers);
        }
    }

    struct TQueryCounters
    {
        std::atomic<int> Medians = 0;
        std::atomic<int> SplitAggregates = 0;
    };

    std::shared_ptr<TQueryCounters> CountSplitQueries()
    {
        auto counters = std::make_shared<TQueryCounters>();
        Tables_->SetHook([counters] (TStringBuf method) {
            if (method == "FindMedianTimestamp") {
                ++counters->Medians;
            } else if (method == "AggregateSplit") {
                ++counters->SplitAggregates;
            }
        });
        return counters;
    }

    //! Reads the page counters of the store in one dump: every dump advances the window.
    static std::pair<i64, i64> ReadSplitsAndClosures(const NProfiling::TSolomonRegistryPtr& registry)
    {
        registry->ProcessRegistrations();
        registry->Collect();
        const auto dump = registry->DumpSensors();
        i64 splits = 0;
        i64 closures = 0;
        for (const auto& cube : dump.cubes()) {
            for (const auto& projection : cube.projections()) {
                if (!projection.has_counter()) {
                    continue;
                }
                if (cube.name() == "yt/test/paged_timer_store/splits") {
                    splits = std::max<i64>(splits, projection.counter());
                } else if (cube.name() == "yt/test/paged_timer_store/page_closures") {
                    closures = std::max<i64>(closures, projection.counter());
                }
            }
        }
        return {splits, closures};
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPagedTimerStoreTest, AllowedStreamsApplyTimerGroups)
{
    auto context = MakeContext();
    const TStreamId blockedId("blocked");
    context->TimerSpecs[blockedId] = context->TimerSpecs[TimerStreamId];
    context->StreamsDependency[blockedId].insert(InputStreamId);
    auto store = CreateStore(context);
    store->Register({MakeTimer("timer", 5, 100)});
    SetWatermark(store, 1000);
    EXPECT_TRUE(store->GetNextBatch({TimerStreamId}, 100, 1_MB).empty());
    EXPECT_EQ(Ids(store->GetNextBatch({TimerStreamId, blockedId}, 100, 1_MB)),
        (std::vector<std::string>{"timer"}));
}

TEST_F(TPagedTimerStoreTest, KnownTimerCompletesEarlierPagesAcrossStreams)
{
    for (int index = 0; index < 12; ++index) {
        auto page = MakePage(1, 0, 100);
        page.Begin = TSystemTimestamp(index * 10);
        if (index < 11) {
            page.End = TSystemTimestamp((index + 1) * 10);
        }
        std::vector<TTimer> rows{MakeTimer(Format("neighbor-%v", index), 60, index * 10 + 5)};
        if (index == 11) {
            rows.push_back(MakeTimer("early", 5, 115));
        }
        WritePage(page, rows);
    }

    const TStreamId otherId("other-timer");
    auto otherPage = MakePage(2, 0, 50);
    otherPage.StreamId = otherId;
    auto late = MakeTimer("late", 5, 1000);
    late.StreamId = otherId;
    WritePage(otherPage, {late});
    auto context = MakeContext(0, 50);
    context->TimerSpecs[otherId] = context->TimerSpecs[TimerStreamId];
    context->StreamsDependency[otherId].insert(InputStreamId);
    i64 reads = Tables_->GetPageReadCount();
    auto store = CreateStore(context);
    SetWatermark(store, 1000);
    EXPECT_EQ(Tables_->GetPageReadCount() - reads, 13);
    EXPECT_EQ(Ids(store->GetNextBatch({TimerStreamId, otherId}, 100, 1_MB)),
        (std::vector<std::string>{"early", "late"}));
}

TEST_F(TPagedTimerStoreTest, KnownTimerWindowCompletesEarlierPagesAcrossStreams)
{
    const TStreamId otherId("other-timer");
    for (int index = 0; index < 200; ++index) {
        auto page = MakePage(1, 0, 100);
        page.StreamId = otherId;
        page.Begin = TSystemTimestamp(index == 0 ? 0 : index + 1);
        if (index < 199) {
            page.End = TSystemTimestamp(index + 2);
        }
        auto timer = MakeTimer(Format("neighbor-%v", index), 60, index + 1);
        timer.StreamId = otherId;
        WritePage(page, {timer});
    }
    std::vector<TTimer> timers;
    std::vector<std::string> expected;
    for (int trigger = 100; trigger < 120; ++trigger) {
        auto messageId = Format("t-%v", trigger);
        timers.push_back(MakeTimer(messageId, 5, trigger));
        if (trigger < 110) {
            expected.push_back(messageId);
        }
    }
    WritePage(MakePage(2, 0, 50), timers);
    auto context = MakeContext(0, 50);
    context->TimerSpecs[otherId] = context->TimerSpecs[TimerStreamId];
    context->StreamsDependency[otherId].insert(InputStreamId);
    i64 reads = Tables_->GetPageReadCount();
    auto store = CreateStore(context, /*maxRowsPerBatch*/ 10);
    // Complete the known window through 119, leaving pages starting at 120 unread.
    EXPECT_EQ(Tables_->GetPageReadCount() - reads, 120);
    SetWatermark(store, 1000);
    EXPECT_EQ(Ids(store->GetNextBatch({TimerStreamId, otherId}, 10, 1_MB)), expected);
}

TEST_F(TPagedTimerStoreTest, LargeResidentLimitStillLoadsHeads)
{
    SeedFamily(7, 0, 100, 2, PageSize);
    auto dynamicContext = MakeDynamicContext(std::numeric_limits<i64>::max());
    auto store = CreateTimerStore(MakeContext(), dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_EQ(store->GetInMemoryCount(), 2 * PageSize);
    EXPECT_EQ(FireAll(store), 2 * PageSize);
}

TEST_F(TPagedTimerStoreTest, SharedPageReadsGrowGeometricallyAndStopAtTheCap)
{
    SeedFamily(1, 0, 100, /*pages*/ 240, /*perPage*/ 1, /*firstTrigger*/ 1, /*key*/ 60);
    i64 reads = Tables_->GetPageReadCount();
    auto store = CreateStore(MakeContext(0, 50));
    EXPECT_EQ(Tables_->GetPageReadCount() - reads, 4);
    for (int batchSize : {8, 16, 32, 64, 64, 52}) {
        reads = Tables_->GetPageReadCount();
        WaitFor(store->Preload()).ThrowOnError();
        EXPECT_EQ(Tables_->GetPageReadCount() - reads, batchSize);
        EXPECT_EQ(store->GetInMemoryCount(), 0);
    }
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(Tables_->GetRowCount(), 240);
}

TEST_F(TPagedTimerStoreTest, SharedPageBatchResetsAfterFindingOwnTimers)
{
    for (int index = 0; index < 100; ++index) {
        auto page = MakePage(1, 0, 100);
        page.Begin = TSystemTimestamp(index * 10);
        if (index < 99) {
            page.End = TSystemTimestamp((index + 1) * 10);
        }
        WritePage(page, {MakeTimer(Format("t-%v", index), index == 12 ? 5 : 60, index * 10 + 5)});
    }
    auto store = CreateStore(MakeContext(0, 50), /*maxRowsPerBatch*/ 1);
    EXPECT_EQ(Tables_->GetPageReadCount(), 4);
    for (int batchSize : {8, 16, 4, 8}) {
        i64 reads = Tables_->GetPageReadCount();
        WaitFor(store->Preload()).ThrowOnError();
        EXPECT_EQ(Tables_->GetPageReadCount() - reads, batchSize);
    }
    EXPECT_EQ(store->GetInMemoryCount(), 1);
    SetWatermark(store, 1000);
    EXPECT_EQ(Ids(store->GetNextBatch({TimerStreamId}, 1, 1_MB)), (std::vector<std::string>{"t-12"}));
}

TEST_F(TPagedTimerStoreTest, NearestTimersBoundSharedPageReads)
{
    for (int index = 0; index < 20; ++index) {
        auto page = MakePage(1, 0, 100);
        page.Begin = TSystemTimestamp(index * 10);
        if (index < 19) {
            page.End = TSystemTimestamp((index + 1) * 10);
        }
        WritePage(page, {MakeTimer(Format("t-%v", index), index == 13 ? 5 : 60, index * 10 + 5)});
    }
    WritePage(MakePage(2, 0, 50), {MakeTimer("early", 5, 85), MakeTimer("late", 5, 1000)});
    i64 reads = Tables_->GetPageReadCount();
    auto store = CreateStore(MakeContext(0, 50), /*maxRowsPerBatch*/ 1);
    EXPECT_EQ(Tables_->GetPageReadCount() - reads, 15);
    reads = Tables_->GetPageReadCount();
    WaitFor(store->Preload()).ThrowOnError();
    EXPECT_EQ(Tables_->GetPageReadCount(), reads);
    SetWatermark(store, 1000);
    EXPECT_EQ(Ids(store->GetNextBatch({TimerStreamId}, 100, 1_MB)), (std::vector<std::string>{"early", "t-13"}));
}

TEST_F(TPagedTimerStoreTest, KnownTimerCompletesEqualTriggerAcrossPageBoundary)
{
    for (int index = 0; index < 8; ++index) {
        auto page = MakePage(1, 0, 100);
        page.Begin = TSystemTimestamp(index * 10);
        if (index < 7) {
            page.End = TSystemTimestamp((index + 1) * 10);
        }
        WritePage(page, {index == 4 ? MakeTimer("a", 5, 40) : MakeTimer(Format("neighbor-%v", index), 60, index * 10 + 5)});
    }
    WritePage(MakePage(2, 0, 50), {MakeTimer("y", 5, 40), MakeTimer("z", 5, 40)});
    i64 reads = Tables_->GetPageReadCount();
    auto store = CreateStore(MakeContext(0, 50), /*maxRowsPerBatch*/ 1);
    EXPECT_EQ(Tables_->GetPageReadCount() - reads, 6);
    SetWatermark(store, 1000);
    EXPECT_EQ(Ids(store->GetNextBatch({TimerStreamId}, 100, 1_MB)), (std::vector<std::string>{"a", "y", "z"}));
}

TEST_F(TPagedTimerStoreTest, EarlierTimersOfAnotherStreamDoNotLimitPreload)
{
    const TStreamId otherId("other-timer");
    const TStreamId otherInputId("other-input");
    auto makeOtherTimer = [&] (const std::string& messageId, ui64 key, ui64 trigger) {
        auto timer = MakeTimer(messageId, key, trigger);
        timer.StreamId = otherId;
        return timer;
    };
    for (int index = 0; index < 20; ++index) {
        auto page = MakePage(1, 0, 100);
        page.StreamId = otherId;
        page.Begin = TSystemTimestamp(index * 10);
        if (index < 19) {
            page.End = TSystemTimestamp((index + 1) * 10);
        }
        WritePage(page, {makeOtherTimer(Format("t-%v", index), index == 13 ? 5 : 60, index * 10 + 5)});
    }
    WritePage(MakePage(2, 0, 50), {MakeTimer("blocked-10", 5, 10), MakeTimer("blocked-20", 5, 20)});
    auto otherPage = MakePage(3, 0, 50);
    otherPage.StreamId = otherId;
    WritePage(otherPage, {makeOtherTimer("early", 5, 85), makeOtherTimer("late", 5, 1000)});

    auto context = MakeContext(0, 50);
    context->TimerSpecs[otherId] = context->TimerSpecs[TimerStreamId];
    context->StreamsDependency[otherId].insert(otherInputId);
    auto store = CreateStore(context, /*maxRowsPerBatch*/ 1);
    auto watermarkState = New<TWatermarkState>();
    watermarkState->Streams[InputStreamId] = New<TWatermarks>();
    auto watermarks = New<TWatermarks>();
    watermarks->SystemWatermark = TSystemTimestamp(1000);
    watermarks->EventWatermark = TSystemTimestamp(1000);
    watermarkState->Streams[otherInputId] = watermarks;
    store->UpdateWatermarkState(watermarkState);
    EXPECT_EQ(Ids(store->GetNextBatch({TimerStreamId, otherId}, 100, 1_MB)),
        (std::vector<std::string>{"early", "t-13"}));
}

TEST_F(TPagedTimerStoreTest, ZeroBatchLimitStillLoadsHeads)
{
    SeedFamily(7, 0, 100, 1, 1);
    auto store = CreateStore(nullptr, 0);
    EXPECT_EQ(store->GetInMemoryCount(), 1);
    EXPECT_EQ(FireAll(store), 1);
}

TEST_F(TPagedTimerStoreTest, LargePageSizeDoesNotForceSplits)
{
    auto context = MakeContext();
    context->TimerPageSize = std::numeric_limits<i64>::max();
    auto store = CreateStore(context);
    std::vector<TTimer> timers;
    for (int index = 0; index < 2 * PageSize; ++index) {
        timers.push_back(MakeTimer(Format("t-%v", index), index, index + 1));
    }
    Iterate(store, 0, std::move(timers));
    EXPECT_EQ(LoadPages().size(), 1u);
    EXPECT_EQ(FireAll(store), 2 * PageSize);
}

TEST_F(TPagedTimerStoreTest, ReportsMinTriggerTimestampOfUnprocessedTimers)
{
    auto store = CreateStore();
    auto getMinTriggerTimestamp = [&] {
        return GetOrCrash(store->BuildInflight(), TimerStreamId)->MinTriggerTimestamp;
    };
    EXPECT_FALSE(getMinTriggerTimestamp());

    Iterate(store, /*watermark*/ 0, {MakeTimer("t-200", 1, 200), MakeTimer("t-100", 2, 100), MakeTimer("t-300", 3, 300)});
    EXPECT_EQ(getMinTriggerTimestamp(), TSystemTimestamp(100));

    // A triggered timer is unprocessed until unregistered.
    SetWatermark(store, 150);
    EXPECT_EQ(getMinTriggerTimestamp(), TSystemTimestamp(100));

    auto batch = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 1000, std::numeric_limits<i64>::max());
    ASSERT_EQ(Ids(batch), std::vector<std::string>{"t-100"});
    store->Unregister(batch);
    EXPECT_EQ(getMinTriggerTimestamp(), TSystemTimestamp(200));

    batch = Iterate(store, /*watermark*/ 1000);
    EXPECT_EQ(std::ssize(batch), 2);
    EXPECT_FALSE(getMinTriggerTimestamp());
}

TEST_F(TPagedTimerStoreTest, ReportsBeginOfNonResidentPageAsMinTriggerTimestamp)
{
    SeedFamily(7, 0, 100, /*pages*/ 2, /*perPage*/ PageSize);
    // The resident limit of two batches keeps the second page out of memory.
    auto store = CreateStore(nullptr, /*maxRowsPerBatch*/ 4);
    EXPECT_EQ(GetOrCrash(store->BuildInflight(), TimerStreamId)->MinTriggerTimestamp, TSystemTimestamp(1));

    SetWatermark(store, PageSize);
    auto batch = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 1000, std::numeric_limits<i64>::max());
    ASSERT_EQ(std::ssize(batch), PageSize);
    store->Unregister(batch);
    ASSERT_LT(store->GetInMemoryCount(), store->GetCount());
    EXPECT_EQ(GetOrCrash(store->BuildInflight(), TimerStreamId)->MinTriggerTimestamp, TSystemTimestamp(PageSize + 1));
}

TEST_F(TPagedTimerStoreTest, RegisterFireRoundTrip)
{
    auto store = CreateStore();
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(store->GetCount(), 0);
    EXPECT_EQ(store->GetInMemoryCount(), 0);

    Iterate(store, 0, {MakeTimer("t-100", 1, 100, 11, 21), MakeTimer("t-300", 2, 300, 13, 23), MakeTimer("t-200", 3, 200, 12, 22)});
    EXPECT_EQ(Tables_->GetRowCount(), 3);
    EXPECT_FALSE(store->IsEmpty());
    EXPECT_EQ(store->GetCount(), 3);
    EXPECT_EQ(store->GetInMemoryCount(), 3);
    EXPECT_GT(store->GetInMemoryByteSize(), 0);

    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].KeyRange, MakeUintKeyRange(0, 100));
    EXPECT_EQ(pages[0].StreamId, TimerStreamId);
    EXPECT_EQ(pages[0].Count, 3);
    EXPECT_EQ(pages[0].Begin, ZeroSystemTimestamp);
    EXPECT_FALSE(pages[0].End);
    EXPECT_FALSE(pages[0].Flags.DedupSlots);
    EXPECT_EQ(pages[0].MinEvent, TSystemTimestamp(21));
    EXPECT_EQ(pages[0].MinSystem, TSystemTimestamp(11));
    WaitFor(Catalogue_->Synchronize()).ThrowOnError();
    EXPECT_EQ(Catalogue_->GetSnapshot().GetPageCount(), 1);

    auto inflight = store->BuildInflight();
    ASSERT_TRUE(inflight.contains(TimerStreamId));
    EXPECT_FALSE(inflight[TimerStreamId]->Empty);
    EXPECT_EQ(inflight[TimerStreamId]->InflightMetrics->Count, 3);
    EXPECT_EQ(inflight[TimerStreamId]->MinEventTimestamp, TSystemTimestamp(21));
    EXPECT_EQ(inflight[TimerStreamId]->MinSystemTimestamp, TSystemTimestamp(11));

    // Nothing is due below the first trigger; then the timers fire in trigger order.
    EXPECT_TRUE(Iterate(store, 99).empty());
    EXPECT_EQ(Ids(Iterate(store, 250)), (std::vector<std::string>{"t-100", "t-200"}));
    EXPECT_EQ(Tables_->GetRowCount(), 1);
    pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Count, 1);
    EXPECT_EQ(pages[0].MinEvent, TSystemTimestamp(23));
    EXPECT_EQ(pages[0].MinSystem, TSystemTimestamp(13));

    // A timer is offered once even when the watermark is reported again.
    SetWatermark(store, 250);
    SetWatermark(store, 250);
    EXPECT_EQ(Ids(Iterate(store, 300)), (std::vector<std::string>{"t-300"}));
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(Tables_->GetRowCount(), 0);
    EXPECT_TRUE(store->BuildInflight()[TimerStreamId]->Empty);
    // No timer rows or page metadata remain.
    EXPECT_TRUE(LoadPages().empty());
}

TEST_F(TPagedTimerStoreTest, BatchLimitAndOrderWithinTrigger)
{
    auto store = CreateStore();
    Iterate(store, 0, {
            MakeTimer("b", 1, 100, 5, 9),
            MakeTimer("a", 2, 100, 5, 9),
            MakeTimer("c", 3, 100, 4, 9),
            MakeTimer("d", 4, 100, 9, 1),
                      });
    // Order within a trigger: by id, like the in-memory store.
    EXPECT_EQ(Ids(Iterate(store, 100, {}, /*maxRows*/ 3)), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(Ids(Iterate(store, 100, {}, /*maxRows*/ 3)), (std::vector<std::string>{"d"}));
}

TEST_F(TPagedTimerStoreTest, RestartLoadsPersistedTimers)
{
    {
        auto store = CreateStore();
        Iterate(store, 0, {MakeTimer("t-100", 1, 100), MakeTimer("t-200", 2, 200)});
    }
    auto store = CreateStore();
    EXPECT_FALSE(store->IsEmpty());
    EXPECT_EQ(store->GetInMemoryCount(), 2);
    EXPECT_EQ(Ids(Iterate(store, 150)), (std::vector<std::string>{"t-100"}));
    Iterate(store, 150, {MakeTimer("t-120", 3, 120)});
    EXPECT_EQ(Ids(Iterate(store, 150)), (std::vector<std::string>{"t-120"}));
    EXPECT_EQ(Ids(Iterate(store, 200)), (std::vector<std::string>{"t-200"}));
    EXPECT_TRUE(LoadPages().empty());
}

TEST_F(TPagedTimerStoreTest, RegisteringTimerTwiceKeepsOneCopy)
{
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ false));
    Iterate(store, /*watermark*/ 0, {MakeTimer("timer", 5, 100)});
    ASSERT_EQ(store->GetCount(), 1);
    ASSERT_EQ(store->GetInMemoryCount(), 1);

    // A Swift computation adds the timer again when the upstream replays its committed input.
    EXPECT_NO_THROW(Iterate(store, /*watermark*/ 0, {MakeTimer("timer", 5, 100)}));
    EXPECT_EQ(store->GetCount(), 1);
    EXPECT_EQ(store->GetInMemoryCount(), 1);
    EXPECT_EQ(Tables_->GetRowCount(), 1);

    EXPECT_EQ(Ids(Iterate(store, /*watermark*/ 100)), std::vector<std::string>{"timer"});
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(Tables_->GetRowCount(), 0);
}

TEST_F(TPagedTimerStoreTest, DeduplicationUsesResidentSlots)
{
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true));
    // Duplicates of one iteration: the lowest event wins.
    Iterate(store, 0, {MakeTimer("first", 5, 100, 10, 30), MakeTimer("second", 5, 100, 10, 20), MakeTimer("third", 5, 100, 10, 40)});
    EXPECT_EQ(Tables_->GetRowCount(), 1);
    EXPECT_EQ(store->GetInMemoryCount(), 1);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_TRUE(pages[0].Flags.DedupSlots);
    auto rows = ReadPage(pages[0]);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].Timer.MessageId, TMessageId("second"));
    EXPECT_TRUE(rows[0].RowKey.RowId.Underlying().empty());

    // A later registration with a higher event loses to the stored timer; a lower one replaces it.
    i64 lookups = Tables_->GetLookupCount();
    Iterate(store, 0, {MakeTimer("higher", 5, 100, 10, 25)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(Tables_->GetRowCount(), 1);
    EXPECT_EQ(ReadPage(LoadPages()[0])[0].Timer.MessageId, TMessageId("second"));
    Iterate(store, 0, {MakeTimer("lower", 5, 100, 10, 15)});
    EXPECT_EQ(Tables_->GetRowCount(), 1);
    EXPECT_EQ(ReadPage(LoadPages()[0])[0].Timer.MessageId, TMessageId("lower"));
    EXPECT_EQ(LoadPages()[0].Count, 1);
    EXPECT_EQ(store->GetInMemoryCount(), 1);

    // Another key or trigger is a different slot.
    Iterate(store, 0, {MakeTimer("other-key", 6, 100, 10, 15), MakeTimer("other-trigger", 5, 101, 10, 15)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(Tables_->GetRowCount(), 3);
    EXPECT_EQ(Ids(Iterate(store, 200)), (std::vector<std::string>{"lower", "other-key", "other-trigger"}));
    EXPECT_TRUE(store->IsEmpty());

    // A stream without deduplication keeps every timer and costs no lookup.
    auto plain = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ false));
    lookups = Tables_->GetLookupCount();
    Iterate(plain, 0, {MakeTimer("x", 5, 100, 10, 30), MakeTimer("y", 5, 100, 10, 20)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(Ids(Iterate(plain, 100)), (std::vector<std::string>{"x", "y"}));
}

TEST_F(TPagedTimerStoreTest, ResidentSlotsAreResolvedWithoutTableLookups)
{
    // A window computation registers many keys at one trigger over successive batches.
    constexpr int KeyCount = 2000;
    constexpr int BatchSize = 100;
    auto store = CreateStore(MakeContext(0, KeyCount, /*deduplicateEqualTimestamps*/ true));
    i64 lookups = Tables_->GetLookupCount();
    for (int first = 0; first < KeyCount; first += BatchSize) {
        std::vector<TTimer> timers;
        for (int key = first; key < first + BatchSize; ++key) {
            timers.push_back(MakeTimer(Format("t-%v", key), key, 1000));
        }
        Iterate(store, 0, std::move(timers));
    }
    EXPECT_EQ(store->GetInMemoryCount(), KeyCount);
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);

    // Replacements of all slots find their stored timers in memory.
    for (int first = 0; first < KeyCount; first += BatchSize) {
        std::vector<TTimer> timers;
        for (int key = first; key < first + BatchSize; ++key) {
            timers.push_back(MakeTimer(Format("r-%v", key), key, 1000, 10, 5));
        }
        Iterate(store, 0, std::move(timers));
    }
    EXPECT_EQ(store->GetInMemoryCount(), KeyCount);
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(std::ssize(Iterate(store, 1000, {}, KeyCount)), KeyCount);
}

TEST_F(TPagedTimerStoreTest, ResidentSlotIndexFollowsEvictionAndReload)
{
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true), /*maxRowsPerBatch*/ 8);
    for (int round = 0; round < 16; ++round) {
        std::vector<TTimer> timers;
        for (int index = 0; index < 8; ++index) {
            i64 number = round * 8 + index;
            timers.push_back(MakeTimer(Format("t-%v", number), number % 50, 1000 + number));
        }
        Iterate(store, 0, std::move(timers));
    }
    for (int iteration = 0; iteration < 32; ++iteration) {
        Iterate(store, 0);
    }
    ASSERT_EQ(store->GetCount(), 128);
    ASSERT_LE(store->GetInMemoryCount(), 2 * PageSize);

    // A slot of a resident page is found in memory, with the winner by the deduplication rule.
    i64 lookups = Tables_->GetLookupCount();
    Iterate(store, 0, {MakeTimer("head-loser", 0, 1000, 10, 50), MakeTimer("head-winner", 1, 1001, 10, 5)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(store->GetCount(), 128);

    // A slot of an evicted page goes to the table.
    Iterate(store, 0, {MakeTimer("tail-winner", 20, 1120, 10, 5)});
    EXPECT_GT(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(store->GetCount(), 128);

    // Fire the head, so that the page of the slot is loaded; its slots are found in memory again.
    for (int iteration = 0; iteration < 40; ++iteration) {
        Iterate(store, 1100, {}, /*maxRows*/ 8);
    }
    Iterate(store, 1100);
    lookups = Tables_->GetLookupCount();
    Iterate(store, 1100, {MakeTimer("late-winner", 20, 1120, 10, 1)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(store->GetCount(), 128 - 101);

    // Fired and unregistered slots leave the index: a new timer of such a slot is stored again.
    Iterate(store, 1100, {MakeTimer("again", 0, 1000)});
    EXPECT_EQ(store->GetCount(), 128 - 101 + 1);
    auto fired = Iterate(store, 1000);
    ASSERT_EQ(std::ssize(fired), 1);
    EXPECT_EQ(fired[0]->MessageId, TMessageId("again"));
}

TEST_F(TPagedTimerStoreTest, WindowSlotsUseResidentData)
{
    auto store = CreateStore(MakeContext(0, 1000, /*deduplicateEqualTimestamps*/ true));
    for (int round = 0; round < 4; ++round) {
        std::vector<TTimer> timers;
        for (int index = 0; index < 2 * PageSize; ++index) {
            i64 key = round * 2 * PageSize + index;
            timers.push_back(MakeTimer(Format("t-%v", key), key, 1000));
        }
        i64 lookups = Tables_->GetLookupCount();
        Iterate(store, 0, std::move(timers));
        EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    }
    EXPECT_EQ(LoadPages().size(), 1u);
    EXPECT_EQ(Tables_->GetRowWriteCount(), 8 * PageSize);
    std::vector<TTimer> replacements;
    std::vector<std::string> expected;
    for (int key = 0; key < 8 * PageSize; ++key) {
        replacements.push_back(MakeTimer(Format("replacement-%v", key), key, 1000, 10, key % 2 ? 11 : 9));
        expected.push_back(key % 2 ? Format("t-%v", key) : Format("replacement-%v", key));
    }
    i64 lookups = Tables_->GetLookupCount();
    Iterate(store, 0, std::move(replacements));
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(Tables_->GetRowWriteCount(), 12 * PageSize);
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(Ids(Iterate(store, 1000)), expected);
    EXPECT_TRUE(store->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, DeduplicationUsesResidentSharedSlice)
{
    auto page = MakePage(1, 0, 100, /*dedupSlots*/ true);
    WritePage(page, {MakeTimer("own", 5, 100, 10, 20), MakeTimer("neighbor", 60, 100, 10, 1)});
    auto dynamicContext = MakeDynamicContext();
    dynamicContext->DynamicTimerStoreSpec->TimerWriteMultiplier = 1.0;
    auto store = CreateTimerStore(MakeContext(0, 50, /*deduplicateEqualTimestamps*/ true), dynamicContext);
    WaitFor(store->Init())
        .ThrowOnError();
    ASSERT_EQ(store->GetInMemoryCount(), 1);

    i64 lookups = Tables_->GetLookupCount();
    Iterate(store, 0, {MakeTimer("loser", 5, 100, 10, 25), MakeTimer("b-missing", 6, 100)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(Tables_->GetRowCount(), 3);
    EXPECT_EQ(ReadPage(page).size(), 2u);

    Iterate(store, 0, {MakeTimer("a-replacement", 5, 100, 10, 15)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(Ids(Iterate(store, 100)), (std::vector<std::string>{"a-replacement", "b-missing"}));
    auto rows = ReadPage(page);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].Timer.MessageId, TMessageId("neighbor"));
    EXPECT_EQ(Tables_->GetRowCount(), 1);
}

TEST_F(TPagedTimerStoreTest, DeduplicationUsesPendingReplacement)
{
    auto page = MakePage(1, 0, 100, /*dedupSlots*/ true);
    WritePage(page, {MakeTimer("stored", 5, 100, 10, 30)});
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true));
    i64 lookups = Tables_->GetLookupCount();
    store->Register({MakeTimer("replacement", 5, 100, 10, 20)});
    store->Register({MakeTimer("loser", 5, 100, 10, 25), MakeTimer("inserted", 6, 100, 10, 20)});
    store->Register({MakeTimer("equal", 5, 100, 1, 20), MakeTimer("inserted-loser", 6, 100, 10, 25)});
    store->Sync(nullptr);
    store->Commit();
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(Tables_->GetRowCount(), 2);
    EXPECT_EQ(Ids(Iterate(store, 100)), (std::vector<std::string>{"inserted", "replacement"}));
}

TEST_F(TPagedTimerStoreTest, DeduplicationRemovesResidentDuplicatesAcrossFamilies)
{
    WritePage(
        MakePage(1, 0, 100, /*dedupSlots*/ true),
        {MakeTimer("a", 5, 100, 10, 10), MakeTimer("b-loser", 6, 100, 10, 30)});
    WritePage(
        MakePage(2, 0, 50, /*dedupSlots*/ true),
        {MakeTimer("a-loser", 5, 100, 10, 30), MakeTimer("b", 6, 100, 10, 10)});
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true));
    ASSERT_EQ(store->GetInMemoryCount(), 4);

    i64 lookups = Tables_->GetLookupCount();
    Iterate(store, 0, {MakeTimer("new-a", 5, 100, 10, 40), MakeTimer("new-b", 6, 100, 10, 40)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(Tables_->GetRowCount(), 2);
    EXPECT_EQ(store->GetInMemoryCount(), 2);
    EXPECT_EQ(Ids(Iterate(store, 100)), (std::vector<std::string>{"a", "b"}));
}

TEST_F(TPagedTimerStoreTest, DeduplicationLooksUpNonResidentSlots)
{
    // The slot of the stored timer lives in a non-resident page: one lookup finds it.
    SeedFamily(1, 0, 100, /*pages*/ 2, /*perPage*/ PageSize, /*firstTrigger*/ 1, /*key*/ 5, /*dedupSlots*/ true);
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true), /*maxRowsPerBatch*/ 8);
    EXPECT_EQ(store->GetInMemoryCount(), PageSize);
    i64 lookups = Tables_->GetLookupCount();
    Iterate(store, 0, {MakeTimer("resident-winner", 5, 3, 10, 1), MakeTimer("loser", 5, PageSize + 3, 10, 100), MakeTimer("winner", 5, PageSize + 4, 10, 1)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups + 1);
    auto headRows = ReadPage(LoadPages()[0]);
    ASSERT_EQ(headRows.size(), PageSize);
    EXPECT_EQ(headRows[2].Timer.MessageId, TMessageId("resident-winner"));
    auto tail = LoadPages()[1];
    EXPECT_EQ(tail.Count, PageSize);
    auto rows = ReadPage(tail);
    std::vector<std::string> ids;
    for (const auto& row : rows) {
        ids.push_back(std::string(row.Timer.MessageId.Underlying()));
    }
    EXPECT_EQ(std::count(ids.begin(), ids.end(), "loser"), 0);
    EXPECT_EQ(std::count(ids.begin(), ids.end(), "winner"), 1);
}

TEST_F(TPagedTimerStoreTest, InOrderRegistrationsOpenPagesWithoutMovingRows)
{
    auto store = CreateStore();
    for (i64 round = 0; round < 6; ++round) {
        std::vector<TTimer> timers;
        for (i64 index = 0; index < 8; ++index) {
            i64 trigger = 100 + round * 8 + index;
            timers.push_back(MakeTimer(Format("t-%v", trigger), index, trigger));
        }
        Iterate(store, 0, std::move(timers));
    }
    EXPECT_EQ(Tables_->GetRowWriteCount(), 48);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 3);
    for (const auto& page : pages) {
        EXPECT_EQ(page.Count, PageSize);
    }
    EXPECT_EQ(FireAll(store), 48);
}

TEST_F(TPagedTimerStoreTest, DeduplicationSkipsLookupsPastTheStoredRows)
{
    // Pages filled in this run know their last row even after they leave memory.
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true), /*maxRowsPerBatch*/ 8);
    for (i64 round = 0; round < 6; ++round) {
        std::vector<TTimer> timers;
        for (i64 index = 0; index < 8; ++index) {
            i64 trigger = 100 + round * 8 + index;
            timers.push_back(MakeTimer(Format("t-%v", trigger), index, trigger));
        }
        Iterate(store, 0, std::move(timers));
    }
    Iterate(store, 0);
    EXPECT_LT(store->GetInMemoryCount(), 48);

    // Past every stored row: the slot cannot be stored, no lookup.
    i64 lookups = Tables_->GetLookupCount();
    Iterate(store, 0, {MakeTimer("later", 7, 5000)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    // Inside the rows of a page out of memory: looked up.
    Iterate(store, 0, {MakeTimer("inside", 7, 130)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups + 1);
    EXPECT_EQ(FireAll(store), 50);
}

TEST_F(TPagedTimerStoreTest, ForcedSplitsAreCappedPerIteration)
{
    // A smaller page size makes a stored page forty times too big: it is cut a few times per
    // iteration rather than read and rewritten whole at the start.
    auto page = MakePage(1, 0, 100);
    std::vector<TTimer> timers;
    for (int index = 0; index < 40 * PageSize; ++index) {
        timers.push_back(MakeTimer(Format("t-%v", index), index % 50, 100 + index));
    }
    WritePage(page, timers);
    auto dynamicContext = MakeDynamicContext(/*maxRowsPerBatch*/ 8);
    auto store = CreateTimerStore(MakeContext(), dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    store->Sync(nullptr);
    store->Commit();
    i64 initialPages = std::ssize(LoadPages());
    Iterate(store, 0);
    EXPECT_LE(std::ssize(LoadPages()), initialPages + 8);
    EXPECT_LT(LoadPages().size(), 40u);
    for (int iteration = 0; iteration < 20; ++iteration) {
        Iterate(store, 0);
    }
    auto pages = LoadPages();
    EXPECT_GE(std::ssize(pages), 20);
    for (const auto& page : pages) {
        EXPECT_LE(page.Count, 2 * PageSize);
    }
    // The resident head page is cut down to the page size for free.
    for (int iteration = 0; iteration < 100; ++iteration) {
        Iterate(store, 0);
    }
    EXPECT_LE(LoadPages().front().Count, PageSize);
    EXPECT_EQ(FireAll(store), 40 * PageSize);
}

TEST_F(TPagedTimerStoreTest, EvictedOfferedTimerIsNotUnregistered)
{
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true));
    Iterate(store, 0, {MakeTimer("stored", 5, 100, 10, 30)});

    // The stored timer is offered and taken, and its handler registers a better duplicate
    // of the same slot before the unregister.
    SetWatermark(store, 100);
    auto batch = store->GetNextBatch({TimerStreamId}, 10, std::numeric_limits<i64>::max());
    ASSERT_EQ(Ids(batch), (std::vector<std::string>{"stored"}));
    WaitFor(store->Preload()).ThrowOnError();
    store->Register({MakeTimer("replacement", 5, 100, 10, 20)});
    store->Unregister(batch);
    store->Sync(nullptr);
    store->Commit();
    EXPECT_EQ(Tables_->GetRowCount(), 1);
    EXPECT_EQ(ReadPage(LoadPages()[0])[0].Timer.MessageId, TMessageId("replacement"));
    EXPECT_EQ(LoadPages()[0].Count, 1);
}

TEST_F(TPagedTimerStoreTest, FiredSlotReregisteredInOneIteration)
{
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true));
    Iterate(store, 0, {MakeTimer("old", 5, 100, 10, 10)});
    // The fired timer's row is still stored when the lookup runs; it must not win.
    i64 lookups = Tables_->GetLookupCount();
    EXPECT_EQ(Ids(Iterate(store, 100, {MakeTimer("new", 5, 100, 20, 50)})), (std::vector<std::string>{"old"}));
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    EXPECT_EQ(Tables_->GetRowCount(), 1);
    EXPECT_EQ(ReadPage(LoadPages()[0])[0].Timer.MessageId, TMessageId("new"));
    EXPECT_EQ(LoadPages()[0].Count, 1);
    EXPECT_EQ(Ids(Iterate(store, 100)), (std::vector<std::string>{"new"}));
    EXPECT_TRUE(store->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, FiringDeduplicatesAcrossPages)
{
    // A page of the old row form (ids in the keys) and a slot page of another family hold the
    // same slot: the lookup cannot see the first, the firing keeps one timer by the rule.
    auto plain = MakePage(1, 0, 100, /*dedupSlots*/ false);
    WritePage(plain, {MakeTimer("old-worse", 5, 100, 10, 30), MakeTimer("old-better", 6, 100, 10, 5), MakeTimer("old-equal", 7, 100, 10, 10)});
    auto slots = MakePage(2, 0, 100, /*dedupSlots*/ true);
    slots.Flags.OnceShared = true;
    WritePage(slots, {MakeTimer("new-5", 5, 100, 10, 20), MakeTimer("new-6", 6, 100, 10, 20), MakeTimer("new-7", 7, 100, 9, 10)});

    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true));
    // Conversion preserves the six rows; firing resolves the inherited duplicates.
    EXPECT_EQ(store->GetInMemoryCount(), 6);
    // Lower event wins; equal event: lower system wins. The winners fire by id.
    EXPECT_EQ(Ids(Iterate(store, 100)), (std::vector<std::string>{"new-5", "new-7", "old-better"}));
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(Tables_->GetRowCount(), 0);
}

TEST_F(TPagedTimerStoreTest, SplitsAndRemovesEmptyPages)
{
    auto store = CreateStore();
    std::vector<TTimer> timers;
    for (i64 index = 2 * PageSize + 8; index-- > 0;) {
        timers.push_back(MakeTimer(Format("t-%v", index), index, 100 + index));
    }
    Iterate(store, 0, std::move(timers));
    EXPECT_EQ(std::ssize(LoadPages()), 2);

    // Oversized resident pages are split during Sync, without spending the write allowance.
    Iterate(store, 0);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 3);
    EXPECT_EQ(pages[0].Count, PageSize);
    EXPECT_EQ(pages[1].Count, PageSize);
    EXPECT_EQ(pages[0].End, pages[1].Begin);
    EXPECT_FALSE(pages[2].End);
    EXPECT_EQ(pages[1].KeyRange, pages[0].KeyRange);
    EXPECT_EQ(ReadPage(pages[0]).size(), static_cast<size_t>(PageSize));
    EXPECT_EQ(ReadPage(pages[1]).size(), static_cast<size_t>(PageSize));
    EXPECT_EQ(Tables_->GetRowCount(), 2 * PageSize + 8);

    EXPECT_EQ(pages[2].Count, 8);

    // The empty head merges with its successor without changing the family coverage.
    auto batch = Iterate(store, 100 + PageSize - 1);
    EXPECT_EQ(std::ssize(batch), PageSize);
    pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    EXPECT_EQ(pages[0].Begin, ZeroSystemTimestamp);
    EXPECT_EQ(pages[0].Count, PageSize);
    WaitFor(Catalogue_->Synchronize()).ThrowOnError();
    EXPECT_EQ(Catalogue_->GetSnapshot().GetPageCount(), 2);

    batch = Iterate(store, 1000);
    EXPECT_EQ(std::ssize(batch), PageSize + 8);
    EXPECT_TRUE(store->IsEmpty());
    pages = LoadPages();
    EXPECT_TRUE(pages.empty());
    EXPECT_EQ(Tables_->GetRowCount(), 0);

    // A registration into the gap grows the surviving page back to the beginning.
    Iterate(store, 0, {MakeTimer("late", 1, 5)});
    pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Begin, ZeroSystemTimestamp);
    EXPECT_FALSE(pages[0].End);
    EXPECT_EQ(Ids(Iterate(store, 5)), (std::vector<std::string>{"late"}));
}

TEST_F(TPagedTimerStoreTest, ClosesGapsByGrowingTheNearestNeighbour)
{
    auto head = MakePage(1, 0, 100);
    head.End = TSystemTimestamp(100);
    WritePage(head, {MakeTimer("a", 1, 10), MakeTimer("b", 2, 20), MakeTimer("c", 3, 30)});
    auto tail = MakePage(2, 0, 100);
    tail.Begin = TSystemTimestamp(200);
    WritePage(tail, {MakeTimer("z", 4, 300)});
    auto store = CreateStore();
    auto dynamicContext = MakeDynamicContext();
    dynamicContext->DynamicTimerStoreSpec->TimerWriteMultiplier = 1;
    store->Reconfigure(dynamicContext);

    // The nearer page grows backward by deleting its old metadata key and creating a new one.
    Iterate(store, 0, {MakeTimer("gap", 5, 175)});
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    EXPECT_EQ(pages[0].End, TSystemTimestamp(100));
    EXPECT_EQ(pages[1].Begin, TSystemTimestamp(100));
    EXPECT_EQ(pages[1].Count, 2);
    EXPECT_EQ(Ids(Iterate(store, 175)), (std::vector<std::string>{"a", "b", "c", "gap"}));

    Iterate(store, 175, {MakeTimer("early", 6, 50)});
    pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Begin, ZeroSystemTimestamp);
    EXPECT_FALSE(pages[0].End);
    EXPECT_EQ(Ids(Iterate(store, 1000)), (std::vector<std::string>{"early", "z"}));
}

TEST_F(TPagedTimerStoreTest, EvictsPagesPastTheResidentWindow)
{
    // Steady registrations of future timers without a restart: the resident window is two
    // batches, everything past it is split off and dropped from memory until needed.
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    for (i64 round = 0; round < 16; ++round) {
        std::vector<TTimer> timers;
        for (i64 index = 0; index < 8; ++index) {
            timers.push_back(MakeTimer(Format("t-%v", round * 8 + index), index, 1000 + round * 8 + index));
        }
        Iterate(store, 0, std::move(timers));
    }
    for (int iteration = 0; iteration < 32; ++iteration) {
        Iterate(store, 0);
    }
    EXPECT_EQ(store->GetCount(), 128);
    EXPECT_LE(store->GetInMemoryCount(), 2 * PageSize);
    EXPECT_GE(LoadPages().size(), 4u);

    // Everything still fires in order, page by page.
    std::vector<std::string> fired;
    for (int iteration = 0; iteration < 40 && !store->IsEmpty(); ++iteration) {
        for (const auto& timer : Iterate(store, 2000, {}, /*maxRows*/ 8)) {
            fired.push_back(std::string(timer->MessageId.Underlying()));
        }
    }
    ASSERT_EQ(fired.size(), 128u);
    for (int index = 0; index < 128; ++index) {
        EXPECT_EQ(fired[index], Format("t-%v", index));
    }
}

TEST_F(TPagedTimerStoreTest, AppendsPagesWithoutScanningFullTail)
{
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    i64 reads = Tables_->GetPageReadCount();
    i64 aggregates = Tables_->GetAggregateCount();
    for (int round = 0; round < 8; ++round) {
        std::vector<TTimer> timers;
        for (int index = 0; index < PageSize; ++index) {
            i64 number = round * PageSize + index;
            timers.push_back(MakeTimer(Format("timer-%v", number), index, 1000 + number));
        }
        Iterate(store, 0, std::move(timers));
    }
    auto pages = LoadPages();
    ASSERT_EQ(pages.size(), 8u);
    for (int index = 0; index < std::ssize(pages); ++index) {
        EXPECT_EQ(pages[index].Count, PageSize);
        if (index + 1 < std::ssize(pages)) {
            EXPECT_EQ(pages[index].End, pages[index + 1].Begin);
        }
    }
    EXPECT_FALSE(pages.back().End);
    EXPECT_EQ(Tables_->GetPageReadCount(), reads);
    EXPECT_EQ(Tables_->GetAggregateCount(), aggregates);

    store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    Iterate(store, 0, {MakeTimer("late", 99, 1001)});
    auto storedPages = LoadPages();
    EXPECT_EQ(storedPages.front().Count, PageSize + 1);
    EXPECT_EQ(FireAll(store), 8 * PageSize + 1);
    EXPECT_TRUE(LoadPages().empty());
}

TEST_F(TPagedTimerStoreTest, EqualSecondRegistrationsCloseWithoutScans)
{
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto context = MakeContext();
    context->Profiler = NProfiling::TProfiler(registry, "/test");
    auto store = CreateStore(context, /*maxRowsPerBatch*/ 8);
    auto counters = CountSplitQueries();
    auto clearHook = Finally([this] {
        Tables_->SetHook({});
    });

    std::vector<std::string> registered;
    for (int iteration = 0; iteration < 24; ++iteration) {
        std::vector<TTimer> timers;
        for (int index = 0; index < 10; ++index) {
            int number = iteration * 10 + index;
            auto id = Format("timer-%05d", number);
            timers.push_back(MakeTimer(id, number % 50, 10'000 + number / 5));
            registered.push_back(id);
        }
        Iterate(store, 0, std::move(timers));
    }
    EXPECT_EQ(counters->Medians.load(), 0);
    EXPECT_EQ(counters->SplitAggregates.load(), 0);
    auto [splits, closures] = ReadSplitsAndClosures(registry);
    EXPECT_EQ(splits, 0);
    EXPECT_GE(closures, 1);

    auto pages = LoadPages();
    ASSERT_GT(std::ssize(pages), 1);
    for (const auto& page : pages) {
        if (page.End) {
            EXPECT_GE(page.Count, PageSize) << page.Begin.Underlying();
            EXPECT_LT(page.Count, 2 * PageSize) << page.Begin.Underlying();
        }
    }

    std::vector<std::string> fired;
    for (int iteration = 0; iteration < 200 && !store->IsEmpty(); ++iteration) {
        for (const auto& timer : Iterate(store, 1'000'000, {}, /*maxRows*/ 8)) {
            fired.push_back(std::string(timer->MessageId.Underlying()));
        }
    }
    EXPECT_EQ(fired, registered);
}

TEST_F(TPagedTimerStoreTest, LateInsertsBelowHardLimitDoNotSplitNonresident)
{
    SeedFamily(1, 0, 100, /*pages*/ 3, /*perPage*/ PageSize);
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto context = MakeContext();
    context->Profiler = NProfiling::TProfiler(registry, "/test");
    auto store = CreateStore(context, /*maxRowsPerBatch*/ 8);
    auto counters = CountSplitQueries();
    auto clearHook = Finally([this] {
        Tables_->SetHook({});
    });

    // The middle page covers the triggers [17, 33).
    for (int round = 0; round < 8; ++round) {
        Iterate(store, 0, {MakeTimer(Format("late-%v", round), round, 20 + round)});
    }
    for (int iteration = 0; iteration < 8; ++iteration) {
        Iterate(store, 0);
    }
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 3);
    EXPECT_EQ(pages[1].Count, PageSize + 8);
    EXPECT_EQ(counters->Medians.load(), 0);
    EXPECT_EQ(counters->SplitAggregates.load(), 0);
    EXPECT_EQ(ReadSplitsAndClosures(registry).first, 0);

    for (int round = 8; round < 20; ++round) {
        Iterate(store, 0, {MakeTimer(Format("late-%v", round), round, 20 + round % 10)});
    }
    for (int iteration = 0; iteration < 8; ++iteration) {
        Iterate(store, 0);
    }
    EXPECT_EQ(counters->Medians.load(), 1);
    EXPECT_EQ(ReadSplitsAndClosures(registry).first, 1);
    pages = LoadPages();
    EXPECT_EQ(std::ssize(pages), 4);
    EXPECT_EQ(store->GetCount(), 3 * PageSize + 20);
}

TEST_F(TPagedTimerStoreTest, OpenPageWithUnknownMaxIsNotMedianSplitBelowHardLimit)
{
    SeedFamily(1, 0, 100, /*pages*/ 2, /*perPage*/ PageSize);
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto context = MakeContext();
    context->Profiler = NProfiling::TProfiler(registry, "/test");
    auto store = CreateStore(context, /*maxRowsPerBatch*/ 8);
    auto counters = CountSplitQueries();
    auto clearHook = Finally([this] {
        Tables_->SetHook({});
    });

    for (int round = 0; round < 10; ++round) {
        Iterate(store, 0, {MakeTimer(Format("late-%v", round), round, 1000 + round)});
    }
    for (int iteration = 0; iteration < 8; ++iteration) {
        Iterate(store, 0);
    }
    EXPECT_EQ(counters->Medians.load(), 0);
    EXPECT_EQ(counters->SplitAggregates.load(), 0);
    EXPECT_EQ(ReadSplitsAndClosures(registry).first, 0);
    EXPECT_EQ(std::ssize(LoadPages()), 2);
    EXPECT_EQ(store->GetCount(), 2 * PageSize + 10);
}

TEST_F(TPagedTimerStoreTest, MaxTimestampSurvivesRestart)
{
    {
        // The first page is closed by in-order timers; one trigger never closes the open page, so
        // it ends up above the page size.
        auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
        std::vector<TTimer> timers;
        for (int index = 0; index < PageSize; ++index) {
            timers.push_back(MakeTimer(Format("head-%v", index), index, 1000 + index));
        }
        for (int index = 0; index < PageSize + 4; ++index) {
            timers.push_back(MakeTimer(Format("tail-%v", index), index, 2000));
        }
        Iterate(store, 0, std::move(timers));
    }
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    EXPECT_EQ(pages[0].MaxTimestamp, TSystemTimestamp(1000 + PageSize - 1));
    EXPECT_EQ(pages[1].MaxTimestamp, TSystemTimestamp(2000));
    EXPECT_EQ(pages[1].Count, PageSize + 4);
    EXPECT_FALSE(pages[1].End);

    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto context = MakeContext();
    context->Profiler = NProfiling::TProfiler(registry, "/test");
    auto store = CreateStore(context, /*maxRowsPerBatch*/ 8);
    auto counters = CountSplitQueries();
    auto clearHook = Finally([this] {
        Tables_->SetHook({});
    });
    i64 aggregates = Tables_->GetAggregateCount();
    for (int iteration = 0; iteration < 4; ++iteration) {
        Iterate(store, 0);
    }
    EXPECT_EQ(counters->Medians.load(), 0);
    EXPECT_EQ(counters->SplitAggregates.load(), 0);
    EXPECT_EQ(Tables_->GetAggregateCount(), aggregates);
    EXPECT_GE(ReadSplitsAndClosures(registry).second, 1);
    EXPECT_EQ(FireAll(store), 2 * PageSize + 4);
}

TEST_F(TPagedTimerStoreTest, ClosedPageSurvivesIdleIterations)
{
    {
        // A head page of distinct triggers keeps the tail nonresident after the restart.
        auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
        std::vector<TTimer> timers;
        for (int index = 0; index < PageSize; ++index) {
            timers.push_back(MakeTimer(Format("head-%v", index), index, 1000 + index));
        }
        for (int index = 0; index < PageSize + 4; ++index) {
            timers.push_back(MakeTimer(Format("tail-%v", index), index, 2000));
        }
        Iterate(store, 0, std::move(timers));
    }
    ASSERT_EQ(std::ssize(LoadPages()), 2);

    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto context = MakeContext();
    context->Profiler = NProfiling::TProfiler(registry, "/test");
    auto store = CreateStore(context, /*maxRowsPerBatch*/ 8);
    Iterate(store, 0);
    // The full open tail is closed after its maximum; the new empty tail is the insertion point.
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 3);
    ASSERT_EQ(pages[1].End, TSystemTimestamp(2001));
    EXPECT_EQ(pages[1].Count, PageSize + 4);
    EXPECT_EQ(pages[2].Count, 0);
    EXPECT_FALSE(pages[2].End);

    i64 writes = Tables_->GetPageWriteCount();
    for (int iteration = 0; iteration < 5; ++iteration) {
        Iterate(store, 0);
        auto current = LoadPages();
        ASSERT_EQ(std::ssize(current), 3);
        EXPECT_EQ(current[1].End, pages[1].End);
        EXPECT_EQ(current[1].Count, PageSize + 4);
        EXPECT_EQ(current[2].Count, 0);
    }
    EXPECT_EQ(Tables_->GetPageWriteCount(), writes);
    EXPECT_EQ(ReadSplitsAndClosures(registry).second, 1);
    EXPECT_EQ(FireAll(store), 2 * PageSize + 4);
}

TEST_F(TPagedTimerStoreTest, LegacyPageGetsBoundOnLoad)
{
    // The first page becomes resident and gets its bound; the others stay as they were stored.
    SeedFamily(1, 0, 100, /*pages*/ 3, /*perPage*/ PageSize);
    ASSERT_FALSE(LoadPages().front().MaxTimestamp);
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    Iterate(store, 0);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 3);
    EXPECT_EQ(pages[0].MaxTimestamp, TSystemTimestamp(PageSize));
    EXPECT_FALSE(pages[1].MaxTimestamp);
    EXPECT_FALSE(pages[2].MaxTimestamp);
}

TEST_F(TPagedTimerStoreTest, LegacyPageIsNotMergedWithBoundedPage)
{
    SeedFamily(1, 0, 100, /*pages*/ 2, /*perPage*/ PageSize);
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    // The first page fires out and is bounded; the second one is nonresident and has no bound.
    EXPECT_EQ(std::ssize(Iterate(store, 1000, {}, /*maxRows*/ PageSize)), PageSize);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    EXPECT_EQ(pages[0].Count, 0);
    EXPECT_TRUE(pages[0].MaxTimestamp);
    EXPECT_FALSE(pages[1].MaxTimestamp);
}

TEST_F(TPagedTimerStoreTest, OnceSharedOpenPageIsNotClosed)
{
    auto head = MakePage(1, 0, 100);
    head.End = TSystemTimestamp(PageSize + 1);
    std::vector<TTimer> headTimers;
    for (int index = 0; index < PageSize; ++index) {
        headTimers.push_back(MakeTimer(Format("head-%v", index), index, index + 1));
    }
    WritePage(head, headTimers);
    auto tail = MakePage(1, 0, 100);
    tail.Begin = TSystemTimestamp(PageSize + 1);
    tail.Flags.OnceShared = true;
    tail.MaxTimestamp = TSystemTimestamp(2000);
    std::vector<TTimer> timers;
    for (int index = 0; index < PageSize + 4; ++index) {
        timers.push_back(MakeTimer(Format("shared-%v", index), index, 2000));
    }
    WritePage(tail, timers);

    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto context = MakeContext();
    context->Profiler = NProfiling::TProfiler(registry, "/test");
    auto store = CreateStore(context, /*maxRowsPerBatch*/ 8);
    for (int iteration = 0; iteration < 3; ++iteration) {
        Iterate(store, 0);
    }
    EXPECT_EQ(ReadSplitsAndClosures(registry).second, 0);
    for (const auto& page : LoadPages()) {
        if (page.Flags.OnceShared) {
            EXPECT_FALSE(page.End);
        }
    }
}

TEST_F(TPagedTimerStoreTest, OpenPageWithPendingRowsIsNotClosed)
{
    {
        // The tail is not full yet, so the restart leaves it open.
        auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
        std::vector<TTimer> timers;
        for (int index = 0; index < PageSize; ++index) {
            timers.push_back(MakeTimer(Format("head-%v", index), index, 1000 + index));
        }
        for (int index = 0; index < PageSize - 2; ++index) {
            timers.push_back(MakeTimer(Format("tail-%v", index), index, 2000));
        }
        Iterate(store, 0, std::move(timers));
    }
    auto registry = New<NProfiling::TSolomonRegistry>();
    registry->SetWindowSize(12);
    auto context = MakeContext();
    context->Profiler = NProfiling::TProfiler(registry, "/test");
    auto store = CreateStore(context, /*maxRowsPerBatch*/ 8);
    EXPECT_EQ(std::ssize(LoadPages()), 2);

    // The nonresident tail becomes full by rows that are not stored yet.
    std::vector<TTimer> late;
    for (int index = 0; index < 4; ++index) {
        late.push_back(MakeTimer(Format("late-%v", index), 50 + index, 2000));
    }
    store->Register(std::move(late));
    WaitFor(store->Preload()).ThrowOnError();
    EXPECT_EQ(ReadSplitsAndClosures(registry).second, 0);

    store->Sync(nullptr);
    store->Commit();
    WaitFor(store->Preload()).ThrowOnError();
    EXPECT_EQ(ReadSplitsAndClosures(registry).second, 1);
}

TEST_F(TPagedTimerStoreTest, MaxTimestampSkipsLookupsAfterRestart)
{
    {
        auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true), /*maxRowsPerBatch*/ 8);
        std::vector<TTimer> timers;
        for (int index = 0; index < PageSize + 8; ++index) {
            timers.push_back(MakeTimer(Format("timer-%v", index), index, 1000 + index));
        }
        Iterate(store, 0, std::move(timers));
    }
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true), /*maxRowsPerBatch*/ 8);
    Iterate(store, 0);
    EXPECT_LT(store->GetInMemoryCount(), PageSize + 8);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    auto bound = pages.back().MaxTimestamp;
    ASSERT_EQ(bound, TSystemTimestamp(1000 + PageSize + 7));

    i64 lookups = Tables_->GetLookupCount();
    Iterate(store, 0, {MakeTimer("above", 7, bound->Underlying() + 1)});
    Iterate(store, 0, {MakeTimer("far-above", 8, 5000)});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups);
    // The nonresident page still may hold a slot at the bound itself.
    Iterate(store, 0, {MakeTimer("at-bound", 9, bound->Underlying())});
    EXPECT_EQ(Tables_->GetLookupCount(), lookups + 1);
    EXPECT_EQ(FireAll(store), PageSize + 8 + 3);
}

TEST_F(TPagedTimerStoreTest, MaxTimestampIsAnUpperBound)
{
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    std::vector<TTimer> timers;
    for (int index = 0; index < 4; ++index) {
        timers.push_back(MakeTimer(Format("old-%v", index), index, 1000 + index));
    }
    Iterate(store, 0, std::move(timers));
    EXPECT_EQ(LoadPages().at(0).MaxTimestamp, TSystemTimestamp(1003));

    // The old timers fire and the page is refilled below its bound within one iteration; the
    // equal triggers keep the page from being split.
    timers.clear();
    for (int index = 0; index < PageSize + 2; ++index) {
        timers.push_back(MakeTimer(Format("new-%v", index), index, 1000));
    }
    EXPECT_EQ(std::ssize(Iterate(store, 1'000'000, std::move(timers))), 4);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Count, PageSize + 2);
    EXPECT_EQ(pages[0].MaxTimestamp, TSystemTimestamp(1003));

    // Below the bound the full page keeps taking timers.
    Iterate(store, 0, {MakeTimer("below", 50, 1000)});
    pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Count, PageSize + 3);
    EXPECT_EQ(pages[0].MaxTimestamp, TSystemTimestamp(1003));

    // Past the bound it is closed.
    Iterate(store, 0, {MakeTimer("above", 51, 1004)});
    pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    EXPECT_EQ(pages[0].End, TSystemTimestamp(1004));
    EXPECT_EQ(pages[0].MaxTimestamp, TSystemTimestamp(1003));
    EXPECT_EQ(pages[1].MaxTimestamp, TSystemTimestamp(1004));
}

TEST_F(TPagedTimerStoreTest, UnsupportedFormatVersionFailsInit)
{
    auto supported = MakePage(1, 0, 100);
    supported.FormatVersion = CurrentTimerPageFormatVersion;
    WritePage(supported, {MakeTimer("supported", 5, 10)});
    EXPECT_EQ(CreateStore()->GetCount(), 1);

    auto newer = MakePage(2, 0, 100);
    newer.FormatVersion = CurrentTimerPageFormatVersion + 1;
    WritePage(newer, {MakeTimer("newer", 6, 20)});
    i64 rowWrites = Tables_->GetRowWriteCount();
    auto store = CreateTimerStore(MakeContext(), MakeDynamicContext());
    try {
        WaitFor(store->Init()).ThrowOnError();
        ADD_FAILURE() << "Init accepted an unsupported page format version";
    } catch (const TErrorException& ex) {
        EXPECT_THAT(ex.what(), testing::HasSubstr("Timer page format version 2 is not supported; the current version is 1"));
        EXPECT_EQ(ex.Error().Attributes().Get<ui64>("family_id"), 2u);
        EXPECT_EQ(ex.Error().Attributes().Get<ui64>("begin_timestamp"), 0u);
        EXPECT_EQ(ex.Error().Attributes().Get<std::string>("computation_id"), "test-computation");
    }
    EXPECT_EQ(Tables_->GetRowWriteCount(), rowWrites);
}

TEST_F(TPagedTimerStoreTest, AppendingDoesNotSeparateEqualTimestamps)
{
    auto store = CreateStore();
    std::vector<TTimer> timers;
    for (int index = 0; index < 3 * PageSize; ++index) {
        timers.push_back(MakeTimer(Format("same-%v", index), index, 100));
    }
    Iterate(store, 0, std::move(timers));
    ASSERT_EQ(LoadPages().size(), 1u);
    Iterate(store, 0, {MakeTimer("later", 99, 101)});
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    EXPECT_EQ(pages[0].Count, 3 * PageSize);
    EXPECT_EQ(pages[0].End, TSystemTimestamp(101));
    EXPECT_EQ(pages[1].Count, 1);
    EXPECT_EQ(FireAll(store), 3 * PageSize + 1);
}

TEST_F(TPagedTimerStoreTest, RemovesEmptyOwnedPageWithoutScanningTimers)
{
    auto store = CreateStore();
    Iterate(store, 0, {MakeTimer("only", 1, 100)});
    i64 aggregates = Tables_->GetAggregateCount();
    EXPECT_EQ(Ids(Iterate(store, 100)), (std::vector<std::string>{"only"}));
    EXPECT_TRUE(LoadPages().empty());
    EXPECT_EQ(Tables_->GetRowCount(), 0);
    EXPECT_EQ(Tables_->GetAggregateCount(), aggregates);
}

TEST_F(TPagedTimerStoreTest, SplitsNonResidentPageWithMajorityAtMinimum)
{
    SeedFamily(1, 0, 100, /*pages*/ 1, /*perPage*/ PageSize);
    auto head = LoadPages().front();
    head.End = TSystemTimestamp(100);
    Tables_->WritePage(nullptr, head);
    auto tail = MakePage(1, 0, 100);
    tail.Begin = TSystemTimestamp(100);
    std::vector<TTimer> timers;
    for (int index = 0; index < 2 * PageSize; ++index) {
        timers.push_back(MakeTimer(Format("same-%v", index), index, 100));
    }
    timers.push_back(MakeTimer("later", 99, 200));
    WritePage(tail, timers);
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    store->Sync(nullptr);
    store->Commit();
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 3);
    EXPECT_EQ(pages[1].Count, 2 * PageSize);
    EXPECT_EQ(pages[1].End, TSystemTimestamp(101));
    EXPECT_EQ(pages[2].Count, 1);
    EXPECT_EQ(FireAll(store), 3 * PageSize + 1);
}

TEST_F(TPagedTimerStoreTest, SplitsOversizedNonResidentPage)
{
    SeedFamily(1, 0, 100, /*pages*/ 2, /*perPage*/ PageSize);
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    EXPECT_EQ(store->GetInMemoryCount(), PageSize);
    for (i64 round = 0; round < 8; ++round) {
        std::vector<TTimer> timers;
        for (i64 index = 0; index < 8; ++index) {
            timers.push_back(MakeTimer(Format("late-%v", round * 8 + index), index, 100 + round * 8 + index));
        }
        Iterate(store, 0, std::move(timers));
    }
    for (int iteration = 0; iteration < 64; ++iteration) {
        Iterate(store, 0);
    }
    auto pages = LoadPages();
    EXPECT_GE(pages.size(), 5u);
    for (const auto& page : pages) {
        EXPECT_LE(page.Count, 2 * PageSize) << page.Begin.Underlying();
    }
    EXPECT_EQ(store->GetCount(), 2 * PageSize + 64);
}

TEST_F(TPagedTimerStoreTest, SplitsClusteredNonResidentPageAtMedian)
{
    auto head = MakePage(1, 0, 100);
    head.End = TSystemTimestamp(100);
    std::vector<TTimer> headTimers;
    for (i64 index = 0; index < PageSize; ++index) {
        headTimers.push_back(MakeTimer(Format("head-%v", index), index, 10 + index));
    }
    WritePage(head, headTimers);
    auto tail = MakePage(1, 0, 100);
    tail.Begin = TSystemTimestamp(100);
    std::vector<TTimer> tailTimers;
    for (i64 index = 0; index < 64; ++index) {
        tailTimers.push_back(MakeTimer(Format("cluster-%v", index), index, 10'000 + index));
    }
    tailTimers.push_back(MakeTimer("outlier", 65, 900'000));
    WritePage(tail, tailTimers);

    std::atomic<int> medianQueries = 0;
    std::atomic<int> splitQueries = 0;
    std::atomic<int> fullAggregates = 0;
    Tables_->SetHook([&] (TStringBuf method) {
        if (method == "FindMedianTimestamp") {
            ++medianQueries;
        } else if (method == "AggregateSplit") {
            ++splitQueries;
        } else if (method == "Aggregate") {
            ++fullAggregates;
        }
    });
    auto clearHook = Finally([&] {
        Tables_->SetHook({});
    });

    auto dynamicContext = MakeDynamicContext(/*maxRowsPerBatch*/ 8);
    auto store = CreateTimerStore(MakeContext(), dynamicContext);
    i64 reads = Tables_->GetPageReadCount();
    i64 writes = Tables_->GetRowWriteCount();
    WaitFor(store->Init()).ThrowOnError();
    store->Sync(nullptr);
    store->Commit();

    auto pages = LoadPages();
    ASSERT_GE(pages.size(), 3u);
    EXPECT_EQ(pages[0].Count, PageSize);
    i64 tailCount = 0;
    for (int index = 1; index < std::ssize(pages); ++index) {
        EXPECT_GT(pages[index].Count, 0);
        EXPECT_LE(pages[index].Count, 2 * PageSize);
        tailCount += pages[index].Count;
    }
    EXPECT_EQ(tailCount, 65);
    EXPECT_EQ(Tables_->GetPageReadCount(), reads + 1);
    EXPECT_EQ(Tables_->GetRowWriteCount(), writes);
    EXPECT_EQ(medianQueries.load(), 2);
    EXPECT_EQ(splitQueries.load(), 2);
    EXPECT_EQ(fullAggregates.load(), 0);
    EXPECT_TRUE(std::ranges::any_of(pages, [] (const auto& page) {
        return page.Begin == TSystemTimestamp(10'032);
    }));
    EXPECT_EQ(FireAll(CreateStore()), PageSize + 65);
}

TEST_F(TPagedTimerStoreTest, UncuttablePageIsNotRereadWhileIdle)
{
    // A full head and a tail of 32 timers sharing one trigger: the tail is twice the page
    // size yet cannot be cut. Past the resident window it is evicted, but not reread for
    // another attempt on every idle iteration.
    auto head = MakePage(1, 0, 100);
    head.End = TSystemTimestamp(100);
    std::vector<TTimer> headTimers;
    for (i64 index = 0; index < 16; ++index) {
        headTimers.push_back(MakeTimer(Format("h-%v", index), index, 10 + index));
    }
    WritePage(head, headTimers);
    auto tail = MakePage(1, 0, 100);
    tail.Begin = TSystemTimestamp(100);
    std::vector<TTimer> tailTimers;
    for (i64 index = 0; index < 32; ++index) {
        tailTimers.push_back(MakeTimer(Format("t-%v", index), index, 500));
    }
    WritePage(tail, tailTimers);

    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    Iterate(store, 0);
    i64 reads = Tables_->GetPageReadCount();
    for (int iteration = 0; iteration < 10; ++iteration) {
        Iterate(store, 0);
    }
    EXPECT_EQ(Tables_->GetPageReadCount(), reads);
    EXPECT_EQ(std::ssize(LoadPages()), 2);
    EXPECT_EQ(store->GetInMemoryCount(), 16);

    // A timer with another trigger makes the tail cuttable: one more read at most, one split.
    Iterate(store, 0, {MakeTimer("later", 50, 600)});
    Iterate(store, 0);
    EXPECT_EQ(LoadPages().size(), 3u);
    EXPECT_LE(Tables_->GetPageReadCount(), reads + 1);
    EXPECT_EQ(store->GetCount(), 49);
}

TEST_F(TPagedTimerStoreTest, InitRejectsRowsOutsideFamilyCoverage)
{
    auto page = MakePage(1, 0, 100);
    page.Begin = TSystemTimestamp(100);
    WritePage(page, {MakeTimer("orphan", 1, 50)});
    EXPECT_THROW(CreateStore(), std::exception);
}

TEST_F(TPagedTimerStoreTest, InitRejectsGapsWithinFamily)
{
    auto head = MakePage(1, 0, 100);
    head.End = TSystemTimestamp(100);
    WritePage(head, {MakeTimer("head", 1, 50)});
    auto tail = MakePage(1, 0, 100);
    tail.Begin = TSystemTimestamp(200);
    WritePage(tail, {MakeTimer("tail", 1, 250)});
    EXPECT_THROW(CreateStore(), std::exception);
}

TEST_F(TPagedTimerStoreTest, NonResidentTailAndFrontier)
{
    // Three pages of 16; a batch of 8 keeps 16 timers resident, so the tail is read on demand.
    SeedFamily(1, 0, 100, /*pages*/ 3, /*perPage*/ PageSize);
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    EXPECT_EQ(store->GetInMemoryCount(), PageSize);
    EXPECT_EQ(store->GetCount(), 3 * PageSize);
    EXPECT_EQ(store->BuildInflight()[TimerStreamId]->InflightMetrics->Count, 3 * PageSize);
    EXPECT_EQ(store->BuildInflight()[TimerStreamId]->MinSystemTimestamp, TSystemTimestamp(2));

    // Everything is due; the timers fire in order across the pages, never beyond the frontier.
    std::vector<std::string> fired;
    for (int iteration = 0; iteration < 20 && !store->IsEmpty(); ++iteration) {
        auto batch = Iterate(store, 1000, {}, /*maxRows*/ 8);
        EXPECT_LE(std::ssize(batch), 8);
        for (const auto& timer : batch) {
            fired.push_back(std::string(timer->MessageId.Underlying()));
        }
    }
    ASSERT_EQ(std::ssize(fired), 3 * PageSize);
    for (int index = 0; index < 3 * PageSize; ++index) {
        EXPECT_EQ(fired[index], Format("seed-1-%v", index + 1));
    }
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(Tables_->GetRowCount(), 0);
    EXPECT_TRUE(LoadPages().empty());
}

TEST_F(TPagedTimerStoreTest, FrontierHoldsBackLaterFamilies)
{
    // Family 1 has a resident head with triggers 1 and 2 and a non-resident tail with 3 and 4;
    // family 2 holds trigger 100 in one page. Nothing past the frontier fires until the tail
    // of family 1 is read, however due it is.
    SeedFamily(1, 0, 100, /*pages*/ 2, /*perPage*/ 2, /*firstTrigger*/ 1, /*key*/ 5);
    auto nested = MakePage(2, 10, 20);
    WritePage(nested, {MakeTimer("far", 15, 100)});
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 1);
    EXPECT_EQ(store->GetInMemoryCount(), 3);
    EXPECT_EQ(store->GetCount(), 5);

    std::vector<std::vector<std::string>> batches;
    for (int iteration = 0; iteration < 10 && !store->IsEmpty(); ++iteration) {
        batches.push_back(Ids(Iterate(store, 1000, {}, /*maxRows*/ 10)));
    }
    // The tail is read by the preload of the second iteration, before its firing.
    EXPECT_EQ(batches, (std::vector<std::vector<std::string>>{{"seed-1-1", "seed-1-2"}, {"seed-1-3", "seed-1-4", "far"}}));
    EXPECT_TRUE(store->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, SharedPagesAfterSplit)
{
    // The parent page covers [0, 100) with keys 10 and 60.
    auto parent = MakePage(7, 0, 100);
    auto left = MakeTimer("left", 10, 100, 1, 1);
    auto right = MakeTimer("right", 60, 50, 2, 2);
    WritePage(parent, {left, right});

    // The left child holds the page's key begin and fences it; only its own row is resident,
    // and it uses that subset without any additional timer writes.
    auto leftStore = CreateStore(MakeContext(0, 50));
    EXPECT_EQ(leftStore->GetInMemoryCount(), 1);
    EXPECT_FALSE(leftStore->IsEmpty());
    i64 writes = Tables_->GetRowWriteCount();
    EXPECT_TRUE(Iterate(leftStore, 99).empty());
    EXPECT_EQ(Tables_->GetRowWriteCount(), writes);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_TRUE(pages[0].Flags.OnceShared);
    EXPECT_EQ(pages[0].Count, 2);
    WaitFor(Catalogue_->Synchronize()).ThrowOnError();
    EXPECT_TRUE(Catalogue_->GetSnapshot().FindPage(parent.FamilyId, parent.Kind, parent.Begin)->OnceShared);
    // The shared page, empty for the child now, no longer bounds anything: the count is exact.
    EXPECT_EQ(leftStore->BuildInflight()[TimerStreamId]->InflightMetrics->Count, 1);
    EXPECT_EQ(leftStore->BuildInflight()[TimerStreamId]->MinSystemTimestamp, TSystemTimestamp(1));

    // New timers of the child go to its canonical page too.
    Iterate(leftStore, 99, {MakeTimer("child", 20, 120)});
    pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    EXPECT_EQ(Ids(Iterate(leftStore, 100)), (std::vector<std::string>{"left"}));
    EXPECT_EQ(Ids(Iterate(leftStore, 120)), (std::vector<std::string>{"child"}));
    EXPECT_TRUE(leftStore->IsEmpty());

    // The parent's page keeps the right child's row and an exact count.
    for (const auto& page : LoadPages()) {
        if (page.FamilyId == parent.FamilyId) {
            EXPECT_EQ(page.Count, 1);
            EXPECT_EQ(ReadPage(page).size(), 1u);
        }
    }

    // The right child does not touch the flag and fires its own row.
    auto rightStore = CreateStore(MakeContext(50, 100));
    EXPECT_EQ(rightStore->GetInMemoryCount(), 1);
    EXPECT_EQ(Ids(Iterate(rightStore, 50)), (std::vector<std::string>{"right"}));
    EXPECT_TRUE(rightStore->IsEmpty());
    EXPECT_EQ(Tables_->GetRowCount(), 0);
}

TEST_F(TPagedTimerStoreTest, OnceSharedPageIsNotAdoptedAgain)
{
    auto page = MakePage(7, 0, 100);
    page.Flags.OnceShared = true;
    WritePage(page, {MakeTimer("old", 10, 100)});
    auto store = CreateStore();
    Iterate(store, 0, {MakeTimer("new", 20, 100)});
    Catalogue_->CollectEmptyPages();
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_NE(pages[0].FamilyId, page.FamilyId);
    EXPECT_FALSE(pages[0].Flags.OnceShared);
    EXPECT_EQ(pages[0].Count, 2);
    // Equal timestamps: the ids decide the order.
    EXPECT_EQ(Ids(Iterate(store, 100)), (std::vector<std::string>{"new", "old"}));
    EXPECT_TRUE(store->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, WriteLimitIsStrictAndDoesNotAccumulate)
{
    SeedFamily(3, 10, 20, 1, 64, 100, 15);
    auto dynamicContext = MakeDynamicContext();
    dynamicContext->DynamicTimerStoreSpec->TimerWriteMultiplier = 1.0;
    auto store = CreateTimerStore(MakeContext(), dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    i64 writes = Tables_->GetRowWriteCount();
    for (int iteration = 0; iteration < 10; ++iteration) {
        Iterate(store, 0);
    }
    EXPECT_EQ(Tables_->GetRowWriteCount(), writes);
    Iterate(store, 0, {MakeTimer("mandatory", 30, 1000)});
    EXPECT_EQ(Tables_->GetRowWriteCount() - writes, 1);
    writes = Tables_->GetRowWriteCount();
    dynamicContext->DynamicTimerStoreSpec->TimerWriteMultiplier = 2.0;
    store->Reconfigure(dynamicContext);
    Iterate(store, 0, {MakeTimer("next", 31, 1001)});
    EXPECT_EQ(Tables_->GetRowWriteCount() - writes, 2);
    writes = Tables_->GetRowWriteCount();
    Iterate(store, 0);
    EXPECT_EQ(Tables_->GetRowWriteCount(), writes);
    store = CreateStore();
    auto fired = FireAll(store);
    EXPECT_EQ(fired, 66);
    EXPECT_EQ(Tables_->GetRowCount(), 0);
    EXPECT_EQ(Tables_->GetRowWriteCount(), writes);
}

TEST_F(TPagedTimerStoreTest, WriteLimitUsesNetWritesAfterDeduplication)
{
    auto page = MakePage(3, 10, 20, true);
    WritePage(page, {MakeTimer("incumbent", 15, 100, 10, 1), MakeTimer("other", 16, 200)});
    auto dynamicContext = MakeDynamicContext();
    dynamicContext->DynamicTimerStoreSpec->TimerWriteMultiplier = 1.5;
    auto store = CreateTimerStore(MakeContext(0, 100, true), dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    i64 writes = Tables_->GetRowWriteCount();
    std::vector<TTimer> rejected;
    for (int index = 0; index < 10; ++index) {
        rejected.push_back(MakeTimer(Format("rejected-%v", index), 15, 100, 10, 2));
    }
    Iterate(store, 0, std::move(rejected));
    EXPECT_EQ(Tables_->GetRowWriteCount(), writes);
    store->Register({MakeTimer("first", 30, 1000, 10, 5)});
    store->Register({MakeTimer("replacement", 30, 1000, 10, 4)});
    store->Sync(nullptr);
    store->Commit();
    EXPECT_EQ(Tables_->GetRowWriteCount() - writes, 1);
    writes = Tables_->GetRowWriteCount();
    Iterate(store, 0, {MakeTimer("a", 31, 1001), MakeTimer("b", 32, 1002), MakeTimer("c", 33, 1003)});
    EXPECT_EQ(Tables_->GetRowWriteCount() - writes, 4);
    writes = Tables_->GetRowWriteCount();
    EXPECT_EQ(FireAll(store), 6);
    EXPECT_EQ(Tables_->GetRowWriteCount(), writes);
}

TEST_F(TPagedTimerStoreTest, CanonicalizationFitsTheWriteLimit)
{
    SeedFamily(3, 10, 20, /*pages*/ 1, /*perPage*/ 64, /*firstTrigger*/ 100, /*key*/ 15);
    auto dynamicContext = MakeDynamicContext();
    auto store = CreateTimerStore(MakeContext(), dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_EQ(store->GetCount(), 64);

    auto nestedExists = [&] {
        for (const auto& page : LoadPages()) {
            if (page.FamilyId == TTimerFamilyId(3)) {
                return true;
            }
        }
        return false;
    };
    int iterations = 0;
    while (nestedExists() && iterations < 10) {
        std::vector<TTimer> timers;
        for (i64 index = 0; index < 30; ++index) {
            timers.push_back(MakeTimer(Format("t-%v-%v", iterations, index), 30 + index, 1000 + iterations));
        }
        Iterate(store, 0, std::move(timers));
        ++iterations;
    }
    EXPECT_LE(iterations, 5);
    for (const auto& page : LoadPages()) {
        EXPECT_EQ(page.KeyRange, MakeUintKeyRange(0, 100));
    }
}

TEST_F(TPagedTimerStoreTest, CanonicalizesOwnRowsOfSharedPage)
{
    // The parent page holds a row of each child.
    auto parent = MakePage(7, 0, 100);
    WritePage(parent, {MakeTimer("left", 10, 100), MakeTimer("right", 60, 50, 2, 2)});

    auto leftStore = CreateStore(MakeContext(0, 50));
    // The registration funds moving the child's row in the same transaction.
    EXPECT_TRUE(Iterate(leftStore, 0, {MakeTimer("new", 11, 150)}).empty());
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    for (const auto& page : pages) {
        EXPECT_EQ(page.Count, page.FamilyId == parent.FamilyId ? 1 : 2);
        auto rows = ReadPage(page);
        ASSERT_EQ(std::ssize(rows), page.Count);
        EXPECT_EQ(rows[0].Timer.MessageId, page.FamilyId == parent.FamilyId ? TMessageId("right") : TMessageId("left"));
    }
    EXPECT_EQ(Ids(Iterate(leftStore, 100)), (std::vector<std::string>{"left"}));
    EXPECT_EQ(Ids(Iterate(leftStore, 150)), (std::vector<std::string>{"new"}));
    EXPECT_TRUE(leftStore->IsEmpty());

    auto rightStore = CreateStore(MakeContext(50, 100));
    EXPECT_EQ(Ids(Iterate(rightStore, 50)), (std::vector<std::string>{"right"}));
    EXPECT_TRUE(rightStore->IsEmpty());
    EXPECT_EQ(Tables_->GetRowCount(), 0);
}

TEST_F(TPagedTimerStoreTest, CanonicalizationSkipsOfferedTimers)
{
    SeedFamily(3, 10, 20, /*pages*/ 1, /*perPage*/ 4, /*firstTrigger*/ 100, /*key*/ 15);
    auto dynamicContext = MakeDynamicContext();
    auto store = CreateTimerStore(MakeContext(), dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    auto countOf = [&] (ui64 familyId) {
        for (const auto& page : LoadPages()) {
            if (page.FamilyId == TTimerFamilyId(familyId)) {
                return page.Count;
            }
        }
        return i64(-1);
    };
    // Idle epochs do not finance relocation.
    Iterate(store, 0);
    EXPECT_EQ(countOf(3), 4);

    // Everything is due and offered; the preload must leave the offered timers in place.
    WaitFor(store->Preload()).ThrowOnError();
    SetWatermark(store, 200);
    auto batch = store->GetNextBatch({TimerStreamId}, 10, std::numeric_limits<i64>::max());
    EXPECT_EQ(Ids(batch), (std::vector<std::string>{"seed-3-100", "seed-3-101", "seed-3-102", "seed-3-103"}));
    store->Register({MakeTimer("funding", 40, 1000)});
    store->Sync(nullptr);
    store->Commit();
    WaitFor(store->Preload()).ThrowOnError();
    store->Sync(nullptr);
    store->Commit();
    EXPECT_EQ(countOf(3), 4);

    store->Unregister(batch);
    store->Sync(nullptr);
    store->Commit();
    EXPECT_EQ(Ids(Iterate(store, 1000)), (std::vector<std::string>{"funding"}));
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(countOf(3), -1);
    EXPECT_EQ(Tables_->GetRowCount(), 0);
}

TEST_F(TPagedTimerStoreTest, MovesSurviveTheLoadOfTheirDestination)
{
    auto head = MakePage(1, 0, 100);
    head.End = TSystemTimestamp(100);
    std::vector<TTimer> headTimers;
    for (i64 index = 0; index < 16; ++index) {
        headTimers.push_back(MakeTimer(Format("h-%v", index), index, 10 + index));
    }
    WritePage(head, headTimers);
    auto tail = MakePage(1, 0, 100);
    tail.Begin = TSystemTimestamp(100);
    std::vector<TTimer> tailTimers;
    for (i64 index = 0; index < 31; ++index) {
        tailTimers.push_back(MakeTimer(Format("t-%v", index), index, 100 + index));
    }
    WritePage(tail, tailTimers);
    auto nested = MakePage(2, 10, 20);
    WritePage(nested, {MakeTimer("moved", 15, 1000)});

    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    EXPECT_EQ(store->GetCount(), 48);
    Iterate(store, 0, {MakeTimer("funding", 40, 1500)});
    EXPECT_EQ(store->GetCount(), 49);
    EXPECT_EQ(Tables_->GetRowCount(), 49);
    for (const auto& page : LoadPages()) {
        EXPECT_EQ(page.Count, std::ssize(ReadPage(page))) << page.Begin.Underlying();
        EXPECT_EQ(page.KeyRange, MakeUintKeyRange(0, 100));
    }

    store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    EXPECT_EQ(store->GetCount(), 49);
    std::vector<std::string> fired;
    for (int iteration = 0; iteration < 20 && !store->IsEmpty(); ++iteration) {
        for (const auto& timer : Iterate(store, 2000, {}, /*maxRows*/ 8)) {
            fired.push_back(std::string(timer->MessageId.Underlying()));
        }
    }
    ASSERT_EQ(fired.size(), 49u);
    EXPECT_EQ(fired[fired.size() - 2], "moved");
    EXPECT_EQ(fired.back(), "funding");
}

TEST_F(TPagedTimerStoreTest, CanonicalizationResolvesSlotCollisions)
{
    // A canonical slot page and a nested slot page hold the same slot: inherited duplicates
    // from before a deduplication mode change. Canonicalizing the nested one must apply the
    // stored-timer rule, whichever side holds the better timer.
    auto canonical = MakePage(1, 0, 100, /*dedupSlots*/ true);
    WritePage(canonical, {MakeTimer("winner", 15, 100, 10, 5), MakeTimer("stale", 16, 200, 10, 30)});
    auto nested = MakePage(2, 10, 20, /*dedupSlots*/ true);
    WritePage(nested, {MakeTimer("loser", 15, 100, 10, 30), MakeTimer("better", 16, 200, 10, 5)});

    auto dynamicContext = MakeDynamicContext();
    auto store = CreateTimerStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true), dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    Iterate(store, 0, {MakeTimer("funding-1", 40, 2000), MakeTimer("funding-2", 41, 2000)});
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Count, 4);
    std::vector<std::string> stored;
    for (const auto& row : ReadPage(pages[0])) {
        stored.push_back(std::string(row.Timer.MessageId.Underlying()));
    }
    std::sort(stored.begin(), stored.end());
    EXPECT_EQ(stored, (std::vector<std::string>{"better", "funding-1", "funding-2", "winner"}));

    store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true));
    EXPECT_EQ(Ids(Iterate(store, 1000)), (std::vector<std::string>{"winner", "better"}));
    EXPECT_EQ(Ids(Iterate(store, 2000)), (std::vector<std::string>{"funding-1", "funding-2"}));
    EXPECT_TRUE(store->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, CanonicalizationTakesTurnsBetweenFamilies)
{
    // Two nested families and a budget of one row per iteration: both make progress.
    SeedFamily(3, 10, 20, /*pages*/ 1, /*perPage*/ 64, /*firstTrigger*/ 100, /*key*/ 15);
    SeedFamily(4, 20, 30, /*pages*/ 1, /*perPage*/ 64, /*firstTrigger*/ 100, /*key*/ 25);
    auto dynamicContext = MakeDynamicContext();
    auto store = CreateTimerStore(MakeContext(), dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    for (int iteration = 0; iteration < 10; ++iteration) {
        Iterate(store, 0, {MakeTimer(Format("new-%v", iteration), 40, 1000 + iteration)});
    }
    auto countOf = [&] (ui64 familyId) {
        i64 count = 0;
        for (const auto& page : LoadPages()) {
            if (page.FamilyId == TTimerFamilyId(familyId)) {
                count += page.Count;
            }
        }
        return count;
    };
    EXPECT_EQ(countOf(3), 59);
    EXPECT_EQ(countOf(4), 59);
    EXPECT_EQ(countOf(3) + countOf(4), 128 - 10);
}

TEST_F(TPagedTimerStoreTest, CanonicalizesPagesDemotedAtAdoption)
{
    // Family A covers [0, inf); both pages of family B overlap it and are demoted at adoption.
    auto a1 = MakePage(1, 0, 100);
    a1.End = TSystemTimestamp(100);
    auto a2 = MakePage(1, 0, 100);
    a2.Begin = TSystemTimestamp(100);
    WritePage(a1, {MakeTimer("a-50", 1, 50)});
    WritePage(a2, {MakeTimer("a-400", 2, 400)});
    auto b1 = MakePage(2, 0, 100);
    b1.Begin = TSystemTimestamp(100);
    b1.End = TSystemTimestamp(200);
    auto b2 = MakePage(2, 0, 100);
    b2.Begin = TSystemTimestamp(200);
    b2.End = TSystemTimestamp(350);
    WritePage(b1, {MakeTimer("b-150", 3, 150)});
    WritePage(b2, {MakeTimer("b-250", 4, 250)});

    auto store = CreateStore();
    for (int iteration = 0; iteration < 5; ++iteration) {
        Iterate(store, 0, {MakeTimer(Format("funding-%v", iteration), 40, 2000 + iteration)});
    }
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    for (const auto& page : pages) {
        EXPECT_NE(page.Begin, b2.Begin);
    }
    EXPECT_EQ(Ids(Iterate(store, 1000)), (std::vector<std::string>{"a-50", "b-150", "b-250", "a-400"}));
}

TEST_F(TPagedTimerStoreTest, PruningNestedEdgePreservesCanonicalPages)
{
    for (int index = 0; index < 3; ++index) {
        auto page = MakePage(1, 0, 100);
        page.Begin = TSystemTimestamp(30 * index);
        page.End = TSystemTimestamp(index == 2 ? 100 : 30 * (index + 1));
        WritePage(page, {MakeTimer(Format("small-%v", index), index, 30 * index + 10)});
    }
    auto nested = MakePage(2, 0, 100);
    nested.End = TSystemTimestamp(100);
    WritePage(nested, {MakeTimer("nested", 0, 20)});
    for (i64 begin : {100, 200}) {
        auto page = MakePage(2, 0, 100);
        page.Begin = TSystemTimestamp(begin);
        if (begin == 100) {
            page.End = TSystemTimestamp(200);
        }
        std::vector<TTimer> timers;
        for (int index = 0; index < 17; ++index) {
            timers.push_back(MakeTimer(Format("dense-%v-%v", begin, index), index, begin + 10 + index));
        }
        WritePage(page, timers);
    }
    auto context = MakeContext();
    context->TimerPageSize = 32;
    auto store = CreateStore(context);
    i64 writes = Tables_->GetRowWriteCount();
    for (int index = 0; index < 5; ++index) {
        Iterate(store, 0, {MakeTimer(Format("funding-%v", index), 50, 150 + index)});
    }
    // One nested row and the four rows of the small family move; the dense pages stay.
    EXPECT_EQ(Tables_->GetRowWriteCount() - writes, 10);
    for (const auto& page : LoadPages()) {
        EXPECT_EQ(page.FamilyId, TTimerFamilyId(2));
    }
    EXPECT_EQ(FireAll(store), 43);
}

TEST_F(TPagedTimerStoreTest, CrossFamilyReplacementRaisesSourceEventMinimum)
{
    auto head = MakePage(7, 0, 40, true);
    head.End = TSystemTimestamp(200);
    std::vector<TTimer> timers;
    for (i64 index = 0; index < PageSize; ++index) {
        timers.push_back(MakeTimer(Format("head-%v", index), index, 50 + index, 30, 30));
    }
    WritePage(head, timers);
    auto tail = MakePage(7, 0, 40, true);
    tail.Begin = TSystemTimestamp(200);
    WritePage(tail, {MakeTimer("old", 5, 200, 10, 10), MakeTimer("survivor", 6, 300, 20, 20)});
    auto dynamicContext = MakeDynamicContext(8);
    dynamicContext->DynamicTimerStoreSpec->TimerWriteMultiplier = 1;
    auto store = CreateTimerStore(MakeContext(0, 100, true), dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    Iterate(store, 0, {MakeTimer("winner", 5, 200, 5, 5)});
    auto pages = LoadPages();
    auto source = std::ranges::find_if(pages, [&] (const auto& page) {
        return page.FamilyId == tail.FamilyId && page.Begin == tail.Begin;
    });
    ASSERT_NE(source, pages.end());
    EXPECT_TRUE(source->Flags.DirtyMinima);
    EXPECT_EQ(source->MinEvent, TSystemTimestamp(10));
    Iterate(store, 0);
    pages = LoadPages();
    source = std::ranges::find_if(pages, [&] (const auto& page) {
        return page.FamilyId == tail.FamilyId && page.Begin == tail.Begin;
    });
    ASSERT_NE(source, pages.end());
    EXPECT_FALSE(source->Flags.DirtyMinima);
    EXPECT_EQ(source->MinEvent, TSystemTimestamp(20));
    Iterate(store, 200);
    EXPECT_EQ(Ids(Iterate(store, 200)), (std::vector<std::string>{"winner"}));
    EXPECT_EQ(GetOrCrash(store->BuildInflight(), TimerStreamId)->MinEventTimestamp, TSystemTimestamp(20));
    EXPECT_EQ(Ids(Iterate(store, 1000)), (std::vector<std::string>{"survivor"}));
}

TEST_F(TPagedTimerStoreTest, DirtyMinimaOfNonResidentPage)
{
    // Two pages of 16, only the head resident; the tail's stored minima are lower bounds.
    SeedFamily(1, 0, 100, /*pages*/ 2, /*perPage*/ PageSize, /*firstTrigger*/ 1, /*key*/ 5, /*dedupSlots*/ true);
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true), /*maxRowsPerBatch*/ 8);
    EXPECT_EQ(store->GetInMemoryCount(), PageSize);
    auto tail = LoadPages()[1];
    EXPECT_EQ(tail.MinSystem, TSystemTimestamp(PageSize + 2));
    EXPECT_EQ(tail.MinEvent, TSystemTimestamp(PageSize + 2));

    // The tail's first timer (system 18, event 18) is replaced through its slot by a timer with a
    // lower event but a higher system timestamp: the stored system minimum becomes stale.
    Iterate(store, 0, {MakeTimer("replacement", 5, PageSize + 1, /*system*/ 500, /*event*/ 3)});
    tail = LoadPages()[1];
    EXPECT_EQ(tail.Count, PageSize);
    EXPECT_EQ(tail.MinEvent, TSystemTimestamp(3));
    EXPECT_TRUE(tail.Flags.DirtyMinima);
    EXPECT_EQ(ReadPage(tail).size(), static_cast<size_t>(PageSize));
    auto inflight = store->BuildInflight()[TimerStreamId];
    EXPECT_EQ(inflight->MinEventTimestamp, TSystemTimestamp(2));
    EXPECT_LE(inflight->MinSystemTimestamp, TSystemTimestamp(PageSize + 2));

    // The recompute under the maintenance budget fixes the bound.
    for (int iteration = 0; iteration < 40 && LoadPages().back().Flags.DirtyMinima; ++iteration) {
        Iterate(store, 0);
    }
    tail = LoadPages().back();
    EXPECT_FALSE(tail.Flags.DirtyMinima);
    EXPECT_EQ(tail.MinSystem, TSystemTimestamp(PageSize + 3));
    EXPECT_EQ(tail.MinEvent, TSystemTimestamp(3));
}

TEST_F(TPagedTimerStoreTest, DirtyMinimaOfOversizedPage)
{
    auto head = MakePage(1, 0, 100, /*dedupSlots*/ true);
    head.End = TSystemTimestamp(100);
    std::vector<TTimer> headTimers;
    for (i64 index = 0; index < 16; ++index) {
        headTimers.push_back(MakeTimer(Format("h-%v", index), index, 10 + index, /*system*/ 50, /*event*/ 50));
    }
    WritePage(head, headTimers);
    auto tail = MakePage(1, 0, 100, /*dedupSlots*/ true);
    tail.Begin = TSystemTimestamp(100);
    std::vector<TTimer> tailTimers;
    for (i64 index = 0; index < 64; ++index) {
        tailTimers.push_back(MakeTimer(Format("t-%v", index), index, 500, /*system*/ index == 7 ? 1 : 200, /*event*/ 60));
    }
    WritePage(tail, tailTimers);
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true), /*maxRowsPerBatch*/ 8);
    for (int iteration = 0; iteration < 4; ++iteration) {
        Iterate(store, 0);
    }
    EXPECT_EQ(store->GetInMemoryCount(), 16);

    // The slot of the timer holding the system minimum is replaced by a later timer with a
    // lower event: the stored system minimum of the non-resident tail goes stale.
    Iterate(store, 0, {MakeTimer("replacement", 7, 500, /*system*/ 500, /*event*/ 3)});
    EXPECT_TRUE(LoadPages()[1].Flags.DirtyMinima);
    for (int iteration = 0; iteration < 40 && LoadPages()[1].Flags.DirtyMinima; ++iteration) {
        Iterate(store, 0);
    }
    auto page = LoadPages()[1];
    EXPECT_FALSE(page.Flags.DirtyMinima);
    EXPECT_EQ(page.MinSystem, TSystemTimestamp(200));
    EXPECT_EQ(page.MinEvent, TSystemTimestamp(3));
    EXPECT_EQ(store->BuildInflight()[TimerStreamId]->MinSystemTimestamp, TSystemTimestamp(50));
}

TEST_F(TPagedTimerStoreTest, ImportIntoNonResidentTailPreservesStatistics)
{
    SeedFamily(1, 0, 100, 2, PageSize, 1, 5, true);
    auto timers = New<NTables::TInMemoryTimers>();
    std::vector<TTimer> imported{MakeTimer("replacement", 5, PageSize + 1, 500, 3)};
    for (i64 index = 0; index < 2 * PageSize; ++index) {
        imported.push_back(MakeTimer(Format("import-%v", index), 40, 100 + index, 100, 4));
    }
    timers->Write(nullptr, ComputationId, imported);
    auto context = MakeContext(0, 100, true);
    context->TimersTable = timers;
    auto store = CreateStore(context, 8);
    EXPECT_EQ(store->GetCount(), 4 * PageSize);
    for (int iteration = 0; iteration < 4; ++iteration) {
        Iterate(store, 0);
    }
    for (const auto& page : LoadPages()) {
        auto rows = ReadPage(page);
        EXPECT_EQ(page.Count, std::ssize(rows));
        if (!rows.empty()) {
            auto minimum = std::min_element(rows.begin(), rows.end(), [] (const auto& lhs, const auto& rhs) {
                return lhs.Timer.EventTimestamp < rhs.Timer.EventTimestamp;
            })->Timer.EventTimestamp;
            EXPECT_EQ(page.MinEvent, minimum);
        }
    }
    EXPECT_EQ(FireAll(store), 4 * PageSize);
}

TEST_F(TPagedTimerStoreTest, RearmingLastTimerPreservesFamily)
{
    auto store = CreateStore();
    Iterate(store, 0, {MakeTimer("first", 5, 100)});
    auto familyId = LoadPages().front().FamilyId;
    i64 aggregates = Tables_->GetAggregateCount();
    for (i64 index = 0; index < 10; ++index) {
        EXPECT_EQ(Iterate(store, 100 + index, {MakeTimer(Format("next-%v", index), 5, 101 + index)}).size(), 1u);
        ASSERT_EQ(LoadPages().size(), 1u);
        EXPECT_EQ(LoadPages().front().FamilyId, familyId);
    }
    EXPECT_EQ(Tables_->GetAggregateCount(), aggregates);
    EXPECT_EQ(FireAll(store), 1);
}

TEST_F(TPagedTimerStoreTest, LargeWriteMultiplierSaturates)
{
    WritePage(MakePage(1, 10, 20), {MakeTimer("old", 15, 100)});
    auto store = CreateStore();
    auto dynamicContext = MakeDynamicContext();
    dynamicContext->DynamicTimerStoreSpec->TimerWriteMultiplier = 1e20;
    store->Reconfigure(dynamicContext);
    i64 writes = Tables_->GetRowWriteCount();
    Iterate(store, 0, {MakeTimer("new", 40, 200)});
    EXPECT_EQ(Tables_->GetRowWriteCount() - writes, 2);
    EXPECT_EQ(FireAll(store), 2);
}

TEST_F(TPagedTimerStoreTest, ImportsTimersTableAtInit)
{
    // The rows of the partition move into pages, in batches; a neighbour's row stays.
    auto timers = New<NTables::TInMemoryTimers>();
    std::vector<TTimer> rows{MakeTimer("out", 500, 10)};
    for (i64 index = 0; index < 2 * PageSize + 5; ++index) {
        rows.push_back(MakeTimer(Format("in-%v", index), index % 100, 100 + index));
    }
    timers->Write(nullptr, ComputationId, rows);
    auto context = MakeContext();
    context->TimersTable = timers;
    auto store = CreateStore(context);
    EXPECT_EQ(store->GetCount(), 2 * PageSize + 5);
    EXPECT_EQ(Tables_->GetRowCount(), 2 * PageSize + 5);
    auto left = WaitFor(timers->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(left.size(), 1u);
    EXPECT_EQ(left[0].second.MessageId, TMessageId("out"));
    for (const auto& page : LoadPages()) {
        EXPECT_LE(page.Count, 2 * PageSize);
    }
    std::vector<std::string> fired;
    for (int iteration = 0; iteration < 20 && !store->IsEmpty(); ++iteration) {
        for (const auto& timer : Iterate(store, 1000, {}, /*maxRows*/ 8)) {
            fired.push_back(std::string(timer->MessageId.Underlying()));
        }
    }
    ASSERT_EQ(std::ssize(fired), 2 * PageSize + 5);
    EXPECT_EQ(fired.front(), "in-0");
    EXPECT_TRUE(store->IsEmpty());

    // A second init finds nothing to import.
    auto again = CreateStore(context);
    EXPECT_TRUE(again->IsEmpty());
    EXPECT_EQ(WaitFor(timers->LoadAll({.ComputationId = ComputationId})).ValueOrThrow().size(), 1u);
}

TEST_F(TPagedTimerStoreTest, ImportSkipsRemovedStreamsAcrossBatches)
{
    auto timers = New<NTables::TInMemoryTimers>();
    std::vector<TTimer> removed;
    for (i64 index = 0; index <= TimerConversionBatchSize; ++index) {
        auto timer = MakeTimer(Format("removed-%v", index), 0, 100);
        timer.StreamId = TStreamId("removed");
        removed.push_back(std::move(timer));
    }
    timers->Write(nullptr, ComputationId, removed);
    timers->Write(nullptr, ComputationId, {MakeTimer("active", 1, 100)});
    auto context = MakeContext();
    context->TimersTable = timers;

    auto store = CreateStore(context);
    EXPECT_EQ(Ids(Iterate(store, 1000)), (std::vector<std::string>{"active"}));
    auto retained = WaitFor(timers->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    EXPECT_EQ(retained.size(), removed.size());
    for (const auto& [key, timer] : retained) {
        EXPECT_EQ(timer.StreamId, TStreamId("removed"));
    }
    EXPECT_TRUE(CreateStore(context)->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, ExportsPagesAtInMemoryInit)
{
    {
        auto store = CreateStore();
        Iterate(store, 0, {MakeTimer("p-1", 5, 100), MakeTimer("p-2", 7, 50)});
    }
    auto timers = New<NTables::TInMemoryTimers>();
    auto context = MakeContext();
    context->Backend = ETimerStoreBackend::InMemory;
    context->TimersTable = timers;
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_EQ(store->GetInMemoryCount(), 2);
    EXPECT_EQ(WaitFor(timers->LoadAll({.ComputationId = ComputationId})).ValueOrThrow().size(), 2u);
    EXPECT_EQ(Tables_->GetRowCount(), 0);
    // Nothing is left behind that would export again at the next start.
    EXPECT_TRUE(LoadPages().empty());
    SetWatermark(store, 100);
    EXPECT_EQ(Ids(store->GetNextBatch({TimerStreamId}, 10, std::numeric_limits<i64>::max())), (std::vector<std::string>{"p-2", "p-1"}));
}

TEST_F(TPagedTimerStoreTest, ExportsPagesWhileDraining)
{
    {
        auto store = CreateStore();
        Iterate(store, 0, {MakeTimer("p-1", 5, 100)});
    }
    auto timers = New<NTables::TInMemoryTimers>();
    auto context = MakeContext();
    context->Backend = ETimerStoreBackend::InMemory;
    context->TimersTable = timers;
    auto dynamicContext = MakeDynamicContext();
    dynamicContext->Draining = true;
    auto store = CreateTimerStore(context, dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_EQ(store->GetInMemoryCount(), 1);
    EXPECT_EQ(Tables_->GetRowCount(), 0);
}

TEST_F(TPagedTimerStoreTest, ExportsTimersBeyondDelayedWatermarks)
{
    ui64 trigger = InfinitySystemTimestamp.Underlying() - 1;
    WritePage(MakePage(1, 0, 100), {MakeTimer("last", 5, trigger)});
    auto timers = New<NTables::TInMemoryTimers>();
    auto context = MakeContext();
    context->Backend = ETimerStoreBackend::InMemory;
    context->TimersTable = timers;
    context->TimerSpecs[TimerStreamId]->StreamsWithDelays = {{InputStreamId, TDuration::Seconds(2)}};
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();
    auto exported = WaitFor(timers->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(exported.size(), 1u);
    EXPECT_EQ(exported.front().second.TriggerTimestamp.Underlying(), trigger);
    EXPECT_EQ(Tables_->GetRowCount(), 0);
    EXPECT_TRUE(LoadPages().empty());
}

TEST_F(TPagedTimerStoreTest, ResumedExportKeepsTheExportedRows)
{
    // An interrupted export left one timer in the timers table and one in a page.
    auto timers = New<NTables::TInMemoryTimers>();
    timers->Write(nullptr, ComputationId, {MakeTimer("exported", 5, 100)});
    WritePage(MakePage(1, 0, 100), {MakeTimer("left", 7, 50)});
    i64 pageWrites = Tables_->GetRowWriteCount();

    auto context = MakeContext();
    context->Backend = ETimerStoreBackend::InMemory;
    context->TimersTable = timers;
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_EQ(store->GetInMemoryCount(), 2);
    EXPECT_EQ(Tables_->GetRowCount(), 0);
    EXPECT_EQ(Tables_->GetRowWriteCount(), pageWrites);
    EXPECT_EQ(timers->GetWriteCount(), 2);
}

TEST_F(TPagedTimerStoreTest, ImportResumesAcrossBatchBoundary)
{
    constexpr i64 TimerCount = 2500;
    ASSERT_GT(TimerCount, 2 * TimerConversionBatchSize);

    // The load of the second batch fails, so the first batch is imported and erased, the rest is left.
    auto interrupted = New<TInterruptibleTimers>(EInterruptedOperation::Load, /*failOnCall*/ 2);
    std::vector<TTimer> rows;
    for (i64 index = 0; index < TimerCount; ++index) {
        rows.push_back(MakeTimer(Format("active-%04d", index), index % 100, 100 + index, 10 + index, 5000 + index));
    }
    interrupted->Write(nullptr, ComputationId, rows);
    auto context = MakeContext();
    context->TimersTable = interrupted;
    EXPECT_THROW(CreateStore(context), std::exception);
    auto left = WaitFor(interrupted->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    EXPECT_EQ(std::ssize(left), TimerCount - TimerConversionBatchSize);
    EXPECT_EQ(Tables_->GetRowCount(), TimerConversionBatchSize);

    auto store = CreateStore(context);
    EXPECT_EQ(store->GetCount(), TimerCount);
    EXPECT_TRUE(WaitFor(interrupted->LoadAll({.ComputationId = ComputationId})).ValueOrThrow().empty());

    THashMap<std::string, std::pair<ui64, ui64>> fired;
    i64 firedCount = 0;
    for (int iteration = 0; iteration < 1000 && !store->IsEmpty(); ++iteration) {
        for (const auto& timer : Iterate(store, 1'000'000)) {
            ++firedCount;
            fired[std::string(timer->MessageId.Underlying())] = {
                timer->TriggerTimestamp.Underlying(),
                timer->EventTimestamp.Underlying(),
            };
        }
    }
    EXPECT_EQ(firedCount, TimerCount);
    ASSERT_EQ(std::ssize(fired), TimerCount);
    for (i64 index = 0; index < TimerCount; ++index) {
        auto it = fired.find(Format("active-%04d", index));
        ASSERT_NE(it, fired.end()) << index;
        EXPECT_EQ(it->second.first, static_cast<ui64>(100 + index));
        EXPECT_EQ(it->second.second, static_cast<ui64>(5000 + index));
    }
}

TEST_F(TPagedTimerStoreTest, ExportResumesAcrossBatchBoundary)
{
    constexpr i64 TimerCount = 2500;
    ASSERT_GT(TimerCount, 2 * TimerConversionBatchSize);

    {
        auto store = CreateStore();
        std::vector<TTimer> timers;
        for (i64 index = 0; index < TimerCount; ++index) {
            timers.push_back(MakeTimer(Format("active-%04d", index), index % 100, 100 + index, 10 + index, 5000 + index));
        }
        Iterate(store, 0, std::move(timers));
        EXPECT_EQ(store->GetCount(), TimerCount);
    }

    // The write of the second batch fails, so the first batch is exported and the rest stays in pages.
    auto interrupted = New<TInterruptibleTimers>(EInterruptedOperation::Write, /*failOnCall*/ 2);
    auto context = MakeContext();
    context->Backend = ETimerStoreBackend::InMemory;
    context->TimersTable = interrupted;
    EXPECT_THROW(CreateStore(context), std::exception);
    auto exportedBefore = WaitFor(interrupted->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    EXPECT_EQ(std::ssize(exportedBefore), TimerConversionBatchSize);
    EXPECT_FALSE(LoadPages().empty());

    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_EQ(store->GetInMemoryCount(), TimerCount);
    EXPECT_EQ(Tables_->GetRowCount(), 0);
    EXPECT_TRUE(LoadPages().empty());
    auto exported = WaitFor(interrupted->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(exported), TimerCount);

    SetWatermark(store, 1'000'000);
    THashMap<std::string, std::pair<ui64, ui64>> fired;
    i64 firedCount = 0;
    for (const auto& timer : store->GetNextBatch({TimerStreamId}, 2 * TimerCount, std::numeric_limits<i64>::max())) {
        ++firedCount;
        fired[std::string(timer->MessageId.Underlying())] = {
            timer->TriggerTimestamp.Underlying(),
            timer->EventTimestamp.Underlying(),
        };
    }
    EXPECT_EQ(firedCount, TimerCount);
    ASSERT_EQ(std::ssize(fired), TimerCount);
    for (i64 index = 0; index < TimerCount; ++index) {
        auto it = fired.find(Format("active-%04d", index));
        ASSERT_NE(it, fired.end()) << index;
        EXPECT_EQ(it->second.first, static_cast<ui64>(100 + index));
        EXPECT_EQ(it->second.second, static_cast<ui64>(5000 + index));
    }
}

TEST_F(TPagedTimerStoreTest, ExportScansSharedPagesHoldingNothingForThePartition)
{
    // A child owns one timer in the last of many shared pages, read in bounded batches: the
    // export keeps going while pages are being read.
    constexpr int PageCount = 1200;
    for (int index = 0; index < PageCount; ++index) {
        auto page = MakePage(7, 0, 100);
        page.Begin = TSystemTimestamp(index * 10);
        if (index + 1 < PageCount) {
            page.End = TSystemTimestamp((index + 1) * 10);
        }
        bool last = index + 1 == PageCount;
        WritePage(page, {MakeTimer(Format("t-%v", index), last ? 10 : 60, index * 10 + 5)});
    }
    auto timers = New<NTables::TInMemoryTimers>();
    auto context = MakeContext(0, 50);
    context->Backend = ETimerStoreBackend::InMemory;
    context->TimersTable = timers;
    auto dynamicContext = MakeDynamicContext();
    auto store = CreateTimerStore(context, dynamicContext);
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_EQ(store->GetInMemoryCount(), 1);
    EXPECT_EQ(timers->GetWriteCount(), 1);
    EXPECT_EQ(Tables_->GetRowCount(), PageCount - 1);
}

TEST_F(TPagedTimerStoreTest, ConvertsRowFormAcrossEmptyHead)
{
    auto head = MakePage(1, 0, 100, false);
    head.End = TSystemTimestamp(100);
    WritePage(head, {});
    auto tail = MakePage(1, 0, 100, false);
    tail.Begin = TSystemTimestamp(100);
    WritePage(tail, {MakeTimer("converted", 7, 200)});
    auto store = CreateStore(MakeContext(0, 100, true));
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Begin, ZeroSystemTimestamp);
    EXPECT_TRUE(pages[0].Flags.DedupSlots);
    ASSERT_EQ(ReadPage(pages[0]).size(), 1u);
    EXPECT_TRUE(ReadPage(pages[0])[0].RowKey.RowId.Underlying().empty());
    EXPECT_EQ(Ids(Iterate(store, 200)), (std::vector<std::string>{"converted"}));
}

TEST_F(TPagedTimerStoreTest, ConvertsRowFormWhenDeduplicationIsTurnedOn)
{
    // Pages written without deduplication hold duplicates of a slot; turning it on rewrites
    // them as slots at the init, collapsing the duplicates within a page.
    auto head = MakePage(1, 0, 100, /*dedupSlots*/ false);
    head.End = TSystemTimestamp(150);
    WritePage(head, {MakeTimer("a-worse", 5, 100, 10, 30), MakeTimer("a-best", 5, 100, 10, 5), MakeTimer("b", 6, 100, 10, 7)});
    auto tail = MakePage(1, 0, 100, /*dedupSlots*/ false);
    tail.Begin = TSystemTimestamp(150);
    WritePage(tail, {MakeTimer("c-1", 7, 200, 10, 7), MakeTimer("c-2", 7, 200, 10, 7)});

    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true), /*maxRowsPerBatch*/ 8);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    for (const auto& page : pages) {
        EXPECT_TRUE(page.Flags.DedupSlots);
        for (const auto& row : ReadPage(page)) {
            EXPECT_TRUE(row.RowKey.RowId.Underlying().empty());
        }
    }
    EXPECT_EQ(pages[0].Count, 2);
    EXPECT_EQ(pages[0].MinEvent, TSystemTimestamp(5));
    EXPECT_EQ(pages[1].Count, 1);
    EXPECT_EQ(Tables_->GetRowCount(), 3);
    EXPECT_EQ(store->GetCount(), 3);

    // The slots deduplicate new registrations as usual, and everything fires.
    Iterate(store, 0, {MakeTimer("a-later", 5, 100, 10, 20)});
    EXPECT_EQ(Tables_->GetRowCount(), 3);
    EXPECT_EQ(Ids(Iterate(store, 1000)), (std::vector<std::string>{"a-best", "b", "c-1"}));
    EXPECT_TRUE(store->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, ConvertsRowFormWhenDeduplicationIsTurnedOff)
{
    auto page = MakePage(1, 0, 100, /*dedupSlots*/ true);
    WritePage(page, {MakeTimer("a", 5, 100), MakeTimer("b", 6, 100)});
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ false));
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_FALSE(pages[0].Flags.DedupSlots);
    EXPECT_EQ(pages[0].Count, 2);
    for (const auto& row : ReadPage(pages[0])) {
        EXPECT_EQ(row.RowKey.RowId, row.Timer.MessageId);
    }

    // Two timers of one slot coexist now, and the old rows are erased by their new keys.
    Iterate(store, 0, {MakeTimer("a-twin", 5, 100)});
    EXPECT_EQ(Tables_->GetRowCount(), 3);
    EXPECT_EQ(Ids(Iterate(store, 1000)), (std::vector<std::string>{"a", "a-twin", "b"}));
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_EQ(Tables_->GetRowCount(), 0);
}

TEST_F(TPagedTimerStoreTest, SharedPageIsConvertedByTheOwnerOfItsKeyBegin)
{
    // A shared page in the old form: the right child must wait until the left one, holding
    // the page's key begin, has converted it whole.
    auto parent = MakePage(7, 0, 100, /*dedupSlots*/ false);
    WritePage(parent, {MakeTimer("left-1", 10, 100, 10, 30), MakeTimer("left-2", 10, 100, 10, 5), MakeTimer("right", 60, 50)});

    auto rightInit = StartWaitingChild();
    auto leftStore = CreateStore(MakeContext(0, 50, /*deduplicateEqualTimestamps*/ true));
    auto rightStore = WaitFor(rightInit).ValueOrThrow();
    auto pages = LoadPages();
    for (const auto& page : pages) {
        EXPECT_TRUE(page.Flags.DedupSlots);
    }
    EXPECT_EQ(Tables_->GetRowCount(), 2);
    EXPECT_EQ(Ids(Iterate(leftStore, 1000)), (std::vector<std::string>{"left-2"}));
    EXPECT_EQ(Ids(Iterate(rightStore, 1000)), (std::vector<std::string>{"right"}));
    EXPECT_TRUE(leftStore->IsEmpty());
    EXPECT_TRUE(rightStore->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, WaitingChildReloadsTheFamilyOnce)
{
    // Three shared pages of one family in the old form: the right child waits for all of them
    // and reloads the family once.
    auto head = MakePage(7, 0, 100, /*dedupSlots*/ false);
    head.End = TSystemTimestamp(150);
    WritePage(head, {MakeTimer("left-1", 10, 100), MakeTimer("right-1", 60, 100)});
    auto middle = MakePage(7, 0, 100, /*dedupSlots*/ false);
    middle.Begin = TSystemTimestamp(150);
    middle.End = TSystemTimestamp(300);
    WritePage(middle, {MakeTimer("right-2", 60, 200)});
    auto tail = MakePage(7, 0, 100, /*dedupSlots*/ false);
    tail.Begin = TSystemTimestamp(300);
    WritePage(tail, {MakeTimer("right-3", 60, 300)});

    auto rightInit = StartWaitingChild();
    std::atomic<int> metaLoads = 0;
    auto stopCatalogue = Finally([this] {
        Tables_->SetHook({});
        if (Catalogue_) {
            WaitFor(Catalogue_->Stop()).ThrowOnError();
        }
    });
    Tables_->SetHook([&] (TStringBuf method) {
        if (method == "LoadMeta") {
            ++metaLoads;
        }
    });
    auto leftStore = CreateStore(MakeContext(0, 50, /*deduplicateEqualTimestamps*/ true));
    auto rightStore = WaitFor(rightInit).ValueOrThrow();
    Tables_->SetHook({});
    // The owner loads the family at Init; the waiting child reloads it once.
    EXPECT_EQ(metaLoads.load(), 2);
    EXPECT_EQ(Ids(Iterate(rightStore, 1000)), (std::vector<std::string>{"right-1", "right-2", "right-3"}));
    EXPECT_TRUE(rightStore->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, EmptySharedPageIsLeftToTheCollector)
{
    // The owner of the key begin finds the shared page empty while the collector removes it:
    // no flag is written, so the metadata row is not resurrected without its columns.
    auto page = MakePage(7, 0, 100, /*dedupSlots*/ false);
    page.Flags.OnceShared = true;
    WritePage(page, {});
    bool collected = false;
    auto stopCatalogue = Finally([this] {
        Tables_->SetHook({});
        if (Catalogue_) {
            WaitFor(Catalogue_->Stop()).ThrowOnError();
        }
    });
    Tables_->SetHook([&] (TStringBuf method) {
        if (method == "ReadPage" && !collected) {
            collected = true;
            Tables_->ErasePage(nullptr, ComputationId, page.FamilyId, page.Kind, page.Begin);
        }
    });
    auto store = CreateStore(MakeContext(0, 50, /*deduplicateEqualTimestamps*/ true));
    Tables_->SetHook({});
    EXPECT_TRUE(collected);
    EXPECT_TRUE(LoadPages().empty());
    EXPECT_TRUE(store->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, WaitingChildSkipsEmptyPages)
{
    // The right child does not wait for the owner on an empty page: nothing of its own is
    // stored in the old form there.
    auto page = MakePage(7, 0, 100, /*dedupSlots*/ false);
    page.Flags.OnceShared = true;
    WritePage(page, {});
    auto store = CreateStore(MakeContext(50, 100, /*deduplicateEqualTimestamps*/ true));
    EXPECT_TRUE(store->IsEmpty());
    EXPECT_FALSE(LoadPages()[0].Flags.DedupSlots);
}

TEST_F(TPagedTimerStoreTest, KeepsPageWhoseCountUnderstatesRows)
{
    // The nonresident sole page of the second family looks empty by its stored count, but a row
    // is there: pruning must not delete the page and orphan the row.
    SeedFamily(1, 0, 100, /*pages*/ 1, /*perPage*/ PageSize);
    auto hidden = MakePage(2, 0, 100);
    hidden.Begin = TSystemTimestamp(3000);
    WritePage(hidden, {MakeTimer("hidden", 5, 5000)});
    hidden.Count = 0;
    Tables_->WritePage(nullptr, hidden);

    i64 aggregates = Tables_->GetAggregateCount();
    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    EXPECT_EQ(store->GetInMemoryCount(), PageSize);
    store->Sync(nullptr);
    store->Commit();
    EXPECT_GT(Tables_->GetAggregateCount(), aggregates);
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 2);
    EXPECT_EQ(pages[1].FamilyId, hidden.FamilyId);
    EXPECT_EQ(pages[1].Count, 1);
    EXPECT_EQ(Tables_->GetRowCount(), PageSize + 1);
    EXPECT_EQ(FireAll(store), PageSize + 1);
}

TEST_F(TPagedTimerStoreTest, RowFormConversionCountsTheRowsItRead)
{
    // Three rows are read and two remain; the stored count was overstated by two, and the
    // conversion applies only its own difference.
    auto page = MakePage(1, 0, 100, /*dedupSlots*/ false);
    WritePage(page, {MakeTimer("a-worse", 5, 100, 10, 30), MakeTimer("a-best", 5, 100, 10, 5), MakeTimer("b", 6, 100, 10, 7)});
    page.Count = 5;
    Tables_->WritePage(nullptr, page);
    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true));
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_TRUE(pages[0].Flags.DedupSlots);
    EXPECT_EQ(pages[0].Count, 4);
    EXPECT_EQ(Tables_->GetRowCount(), 2);
}

TEST_F(TPagedTimerStoreTest, EmptyReadOfNonemptySharedPageStillFlipsTheRowForm)
{
    // The stored count promises rows that are gone: the owner flips the form all the same,
    // so that the waiting partitions are released.
    auto page = MakePage(7, 0, 100, /*dedupSlots*/ false);
    page.Flags.OnceShared = true;
    WritePage(page, {MakeTimer("gone", 10, 100)});
    Tables_->EraseRows(nullptr, {ReadPage(LoadPages().front()).front().RowKey});

    auto store = CreateStore(MakeContext(0, 50, /*deduplicateEqualTimestamps*/ true));
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_TRUE(pages[0].Flags.DedupSlots);
    EXPECT_EQ(pages[0].Count, 1);
}

TEST_F(TPagedTimerStoreTest, WaitingForRowFormTimesOut)
{
    // The owner of the key begin never starts.
    auto parent = MakePage(7, 0, 100, /*dedupSlots*/ false);
    WritePage(parent, {MakeTimer("left", 10, 100), MakeTimer("right", 60, 50)});
    auto context = MakeContext(50, 100, /*deduplicateEqualTimestamps*/ true);
    context->RowFormWaitTimeout = TDuration::MilliSeconds(100);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    auto error = WaitFor(store->Init());
    ASSERT_FALSE(error.IsOK());
    EXPECT_THAT(error.GetMessage(), ::testing::HasSubstr("Timed out waiting"));
    EXPECT_EQ(error.Attributes().Get<ui64>("family_id"), parent.FamilyId.Underlying());
    EXPECT_TRUE(error.Attributes().Contains("begin_timestamp"));
    EXPECT_TRUE(error.Attributes().Contains("owner_partition_key"));
}

TEST_F(TPagedTimerStoreTest, PageWithOverstatedCountIsNotSplit)
{
    // The second page is not resident and claims more rows than it has, so no median is found.
    SeedFamily(1, 0, 100, /*pages*/ 2, /*perPage*/ PageSize);
    auto tail = LoadPages().back();
    auto rows = ReadPage(tail);
    ASSERT_EQ(std::ssize(rows), PageSize);
    for (int index = 1; index < std::ssize(rows); ++index) {
        Tables_->EraseRows(nullptr, {rows[index].RowKey});
    }
    tail.Count = 3 * PageSize;
    Tables_->WritePage(nullptr, tail);

    auto store = CreateStore(MakeContext(), /*maxRowsPerBatch*/ 8);
    EXPECT_EQ(store->GetInMemoryCount(), PageSize);
    EXPECT_NO_THROW(WaitFor(store->Preload()).ThrowOnError());
    EXPECT_EQ(FireAll(store), PageSize + 1);
    EXPECT_TRUE(store->IsEmpty());
}

TEST_F(TPagedTimerStoreTest, CanonicalizationResolvesSlotDuplicatesOfOneBatch)
{
    // A nested page flagged as slots still holds two rows of one slot, as an unconverted page
    // would: only the preferred one may reach the canonical family.
    auto canonical = MakePage(1, 0, 100, /*dedupSlots*/ true);
    WritePage(canonical, {MakeTimer("anchor", 30, 300)});
    auto nested = MakePage(2, 10, 20, /*dedupSlots*/ true);
    WritePage(nested, {});
    std::vector<TTimerPageRow> rows;
    for (const auto& timer : {MakeTimer("dup-a", 15, 100, 10, 30), MakeTimer("dup-b", 15, 100, 10, 5)}) {
        rows.push_back(TTimerPageRow{
            .RowKey = MakeTimerPageRowKey(nested.FamilyId, ETimerPageKind::Trigger, timer, /*dedupSlots*/ false),
            .Timer = timer,
        });
    }
    Tables_->WriteRows(nullptr, rows);
    nested.Count = 2;
    Tables_->WritePage(nullptr, nested);

    auto store = CreateStore(MakeContext(0, 100, /*deduplicateEqualTimestamps*/ true));
    Iterate(store, 0, {MakeTimer("funding-1", 40, 2000), MakeTimer("funding-2", 41, 2000)});
    auto pages = LoadPages();
    ASSERT_EQ(std::ssize(pages), 1);
    EXPECT_EQ(pages[0].Count, 4);
    std::vector<std::string> stored;
    for (const auto& row : ReadPage(pages[0])) {
        stored.push_back(std::string(row.Timer.MessageId.Underlying()));
    }
    std::sort(stored.begin(), stored.end());
    EXPECT_EQ(stored, (std::vector<std::string>{"anchor", "dup-b", "funding-1", "funding-2"}));
    EXPECT_EQ(store->GetCount(), 4);
}

TEST_F(TPagedTimerStoreTest, SmallPagesAcrossAGapAreNotMerged)
{
    // The middle page is once-shared and loads empty, so it is dropped from memory, which
    // leaves a gap between its small neighbours.
    auto head = MakePage(1, 0, 100);
    head.End = TSystemTimestamp(100);
    WritePage(head, {MakeTimer("first", 10, 50)});
    auto middle = MakePage(1, 0, 100);
    middle.Begin = TSystemTimestamp(100);
    middle.End = TSystemTimestamp(200);
    middle.Flags.OnceShared = true;
    WritePage(middle, {MakeTimer("gone", 60, 150)});
    Tables_->EraseRows(nullptr, {ReadPage(middle).front().RowKey});
    auto tail = MakePage(1, 0, 100);
    tail.Begin = TSystemTimestamp(200);
    WritePage(tail, {MakeTimer("last", 10, 250)});

    auto store = CreateStore();
    SetWatermark(store, 0);
    for (int iteration = 0; iteration < 3; ++iteration) {
        WaitFor(store->Preload()).ThrowOnError();
        store->Sync(nullptr);
        store->Commit();
    }
    EXPECT_EQ(std::ssize(LoadPages()), 3);
    EXPECT_EQ(Ids(Iterate(store, 1000)), (std::vector<std::string>{"first", "last"}));
    EXPECT_EQ(Tables_->GetRowCount(), 0);
}

TEST_F(TPagedTimerStoreTest, DrainingBlocksBatch)
{
    auto store = CreateStore();
    Iterate(store, 0, {MakeTimer("t", 1, 100)});
    auto dynamicContext = MakeDynamicContext();
    dynamicContext->Draining = true;
    store->Reconfigure(dynamicContext);
    EXPECT_TRUE(Iterate(store, 100).empty());
    EXPECT_TRUE(store->BuildInflight()[TimerStreamId]->Suspended);
    store->Reconfigure(MakeDynamicContext());
    EXPECT_EQ(Ids(Iterate(store, 100)), (std::vector<std::string>{"t"}));
}

TEST_F(TPagedTimerStoreTest, UnknownStreamThrows)
{
    auto store = CreateStore();
    auto timer = MakeTimer("t", 1, 100);
    timer.StreamId = TStreamId("unknown");
    EXPECT_THROW(store->Register({timer}), TErrorException);
}

TEST_F(TPagedTimerStoreTest, CurrentTimeIgnoresStreamDelays)
{
    auto context = MakeContext();
    context->TimerSpecs[TimerStreamId]->TimeType = ETimeType::CurrentTime;
    context->TimerSpecs[TimerStreamId]->StreamsWithDelays = {{InputStreamId, TDuration::Seconds(30)}};
    auto store = CreateStore(context);
    Iterate(store, /*watermark*/ 0, {MakeTimer("timer-1", /*key*/ 5, /*trigger*/ 50)});

    SetCurrentTimestamp(store, /*timestamp*/ 49);
    WaitFor(store->Preload()).ThrowOnError();
    EXPECT_TRUE(store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, std::numeric_limits<i64>::max()).empty());

    SetCurrentTimestamp(store, /*timestamp*/ 50);
    auto batch = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, std::numeric_limits<i64>::max());
    EXPECT_EQ(Ids(batch), std::vector<std::string>{"timer-1"});
}

TEST_F(TPagedTimerStoreTest, CurrentTimeWithoutStreamsWaitsForTriggerTimestamp)
{
    auto context = MakeContext();
    context->TimerSpecs[TimerStreamId]->TimeType = ETimeType::CurrentTime;
    context->TimerSpecs[TimerStreamId]->Streams.emplace();
    auto store = CreateStore(context);
    Iterate(store, /*watermark*/ 0, {MakeTimer("timer-1", /*key*/ 5, /*trigger*/ 50)});

    SetCurrentTimestamp(store, /*timestamp*/ 49);
    WaitFor(store->Preload()).ThrowOnError();
    EXPECT_TRUE(store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, std::numeric_limits<i64>::max()).empty());

    SetCurrentTimestamp(store, /*timestamp*/ 50);
    auto batch = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, std::numeric_limits<i64>::max());
    EXPECT_EQ(Ids(batch), std::vector<std::string>{"timer-1"});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
