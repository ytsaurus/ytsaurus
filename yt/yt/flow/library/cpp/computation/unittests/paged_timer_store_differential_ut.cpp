#include <yt/yt/flow/library/cpp/computation/stores/paged_timer_store.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_page_catalogue.h>
#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>

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

#include <random>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// The paged store must behave like the in-memory one: the same registrations and watermarks
// fire the same timers in the same order, and the reported counts and minima agree. The two
// run side by side over a random workload with restarts and a split of the partition into two
// halves and back.
class TDifferentialTimerStoreTest
    : public ::testing::TestWithParam<std::pair<bool, int>>
{
protected:
    const TComputationId ComputationId = TComputationId("test-computation");
    const TStreamId TimerStreamId = TStreamId("test-timer");
    const TStreamId InputStreamId = TStreamId("test-input");
    static constexpr i64 PageSize = 16;
    static constexpr ui64 KeySpace = 100;

    TActionQueuePtr Queue_ = New<TActionQueue>("Test");
    NTables::TInMemoryTimerPagesPtr Pages_ = New<NTables::TInMemoryTimerPages>();
    NTables::TInMemoryTimersPtr Timers_ = New<NTables::TInMemoryTimers>();
    TTimerPageCataloguePtr Catalogue_;
    ITimeProviderPtr TimeProvider_ = New<TFakeTimeProvider>();
    TTableSchemaPtr KeySchema_ = New<TTableSchema>(std::vector<TColumnSchema>{TColumnSchema("key", EValueType::Uint64)});
    std::mt19937_64 Random_{static_cast<ui64>(GetParam().second)};
    int NextId_ = 0;

    void SetUp() override
    {
        TTimerPageCatalogueOptions options;
        options.EmptyPageCollectionPeriod = std::nullopt;
        Catalogue_ = New<TTimerPageCatalogue>(
            Pages_,
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

    bool Deduplicate() const
    {
        return GetParam().first;
    }

    //! A pair of stores over the same key range.
    struct TPair
    {
        ITimerStorePtr InMemory;
        ITimerStorePtr Paged;
        ui64 LowerKey = 0;
        ui64 UpperKey = 0;
    };

    TPair CreatePair(ui64 lowerKey, ui64 upperKey, i64 maxRowsPerBatch)
    {
        auto makeContext = [&] (ETimerStoreBackend backend) {
            auto partition = New<TPartition>();
            partition->ComputationId = ComputationId;
            partition->PartitionId = TPartitionId(TGuid::Create());
            partition->LowerKey = MakeUintKey(lowerKey);
            partition->UpperKey = MakeUintKey(upperKey);

            auto timerSpec = New<TTimerSpec>();
            timerSpec->TimeType = ETimeType::SystemTime;
            timerSpec->DeduplicateEqualTimestamps = Deduplicate();

            auto context = New<TTimerStoreContext>();
            context->Partition = std::move(partition);
            context->KeySchema = KeySchema_;
            context->StreamsDependency[TimerStreamId].insert(InputStreamId);
            context->TimerSpecs[TimerStreamId] = std::move(timerSpec);
            context->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
            context->Logger = NLogging::TLogger("Test");
            context->Profiler = NProfiling::TProfiler();
            context->Backend = backend;
            // The paged store would import the in-memory store's rows at its init: each
            // backend gets a timers table of its own.
            context->TimersTable = backend == ETimerStoreBackend::InMemory ? Timers_ : New<NTables::TInMemoryTimers>();
            context->TimeProvider = TimeProvider_;
            // And the in-memory store would export the paged store's pages at its init: it
            // gets no page tables.
            if (backend == ETimerStoreBackend::Paged) {
                context->TimerPageSize = PageSize;
                context->TimerPagesTable = Pages_;
                context->PageCatalogue = Catalogue_;
            }
            return context;
        };
        auto makeDynamicContext = [&] {
            auto dynamicContext = New<TDynamicTimerStoreContext>();
            dynamicContext->DynamicTimerStoreSpec = New<TDynamicTimerStoreSpec>();
            dynamicContext->MaxRowsPerBatch = maxRowsPerBatch;
            return dynamicContext;
        };
        TPair pair{
            .InMemory = CreateTimerStore(makeContext(ETimerStoreBackend::InMemory), makeDynamicContext()),
            .Paged = CreateTimerStore(makeContext(ETimerStoreBackend::Paged), makeDynamicContext()),
            .LowerKey = lowerKey,
            .UpperKey = upperKey,
        };
        WaitFor(pair.InMemory->Init()).ThrowOnError();
        WaitFor(pair.Paged->Init()).ThrowOnError();
        return pair;
    }

    TTimer MakeTimer(ui64 key, ui64 trigger, ui64 system, ui64 event)
    {
        TTimer timer;
        timer.MessageId = TMessageId(Format("t-%v", NextId_++));
        timer.StreamId = TimerStreamId;
        timer.SystemTimestamp = TSystemTimestamp(system);
        timer.AlignmentTimestamp = timer.SystemTimestamp;
        timer.EventTimestamp = TSystemTimestamp(event);
        timer.TriggerTimestamp = TSystemTimestamp(trigger);
        timer.KeySchema = KeySchema_;
        timer.Key = MakeKey(ui64(key));
        return timer;
    }

    TWatermarkStatePtr MakeWatermarkState(ui64 timestamp)
    {
        auto watermarks = New<TWatermarks>();
        watermarks->SystemWatermark = TSystemTimestamp(timestamp);
        watermarks->EventWatermark = TSystemTimestamp(timestamp);
        auto watermarkState = New<TWatermarkState>();
        watermarkState->Streams[InputStreamId] = watermarks;
        return watermarkState;
    }

    static std::vector<std::string> Ids(const std::vector<TInputTimerConstPtr>& timers)
    {
        std::vector<std::string> ids;
        for (const auto& timer : timers) {
            ids.push_back(std::string(timer->MessageId.Underlying()));
        }
        return ids;
    }

    //! One epoch of one store.
    std::vector<TInputTimerConstPtr> Iterate(const ITimerStorePtr& store, ui64 watermark, std::vector<TTimer> outputs, i64 maxRows)
    {
        WaitFor(store->Preload()).ThrowOnError();
        store->UpdateWatermarkState(MakeWatermarkState(watermark));
        auto batch = store->GetNextBatch({TimerStreamId}, maxRows, std::numeric_limits<i64>::max());
        store->Unregister(batch);
        store->Register(std::move(outputs));
        store->Sync(nullptr);
        store->Commit();
        return batch;
    }

    //! One epoch of both stores of the pair with the same inputs; the outputs must agree. The
    //! counts and minima agree exactly while every page belongs to the partition; after a
    //! split the shared pages are only bounds: the count from above, the minima from below.
    //! A system minimum left dirty by a deduplication is a lower bound too.
    void IterateAndCompare(const TPair& pair, ui64 watermark, const std::vector<TTimer>& outputs, i64 maxRows, bool exact, TStringBuf where)
    {
        auto expected = Ids(Iterate(pair.InMemory, watermark, outputs, maxRows));
        auto actual = Ids(Iterate(pair.Paged, watermark, outputs, maxRows));
        ASSERT_EQ(actual, expected) << where << ", watermark " << watermark;

        auto expectedInflight = pair.InMemory->BuildInflight()[TimerStreamId];
        auto actualInflight = pair.Paged->BuildInflight()[TimerStreamId];
        if (expectedInflight->Empty) {
            EXPECT_TRUE(actualInflight->Empty || !exact) << where;
        } else {
            EXPECT_FALSE(actualInflight->Empty) << where;
            EXPECT_FALSE(pair.Paged->IsEmpty()) << where;
            EXPECT_LE(actualInflight->MinEventTimestamp, expectedInflight->MinEventTimestamp) << where;
            EXPECT_LE(actualInflight->MinSystemTimestamp, expectedInflight->MinSystemTimestamp) << where;
            ASSERT_TRUE(actualInflight->MinTriggerTimestamp) << where;
            EXPECT_LE(actualInflight->MinTriggerTimestamp, expectedInflight->MinTriggerTimestamp) << where;
            // A non-resident page reports its range begin, so only a fully resident store is exact.
            if (exact && pair.Paged->GetInMemoryCount() == pair.Paged->GetCount()) {
                EXPECT_EQ(actualInflight->MinTriggerTimestamp, expectedInflight->MinTriggerTimestamp) << where;
            }
            if (exact) {
                EXPECT_EQ(actualInflight->MinEventTimestamp, expectedInflight->MinEventTimestamp) << where;
                if (!Deduplicate()) {
                    EXPECT_EQ(actualInflight->MinSystemTimestamp, expectedInflight->MinSystemTimestamp) << where;
                }
            }
        }
        EXPECT_GE(pair.Paged->GetCount(), pair.InMemory->GetCount()) << where;
        EXPECT_GE(actualInflight->InflightMetrics->Count, expectedInflight->InflightMetrics->Count) << where;
        if (exact) {
            EXPECT_EQ(pair.Paged->GetCount(), pair.InMemory->GetCount()) << where;
            EXPECT_EQ(actualInflight->InflightMetrics->Count, expectedInflight->InflightMetrics->Count) << where;
        }
    }

    std::vector<TTimer> RandomTimers(ui64 lowerKey, ui64 upperKey, ui64 now, int count)
    {
        std::vector<TTimer> timers;
        for (int index = 0; index < count; ++index) {
            ui64 key = lowerKey + Random_() % (upperKey - lowerKey);
            // Triggers cluster so that pages fill and split, and slots collide under deduplication.
            ui64 trigger = now + 1 + Random_() % 30;
            ui64 system = 1 + Random_() % 1000;
            ui64 event = 1 + Random_() % 20;
            timers.push_back(MakeTimer(key, trigger, system, event));
        }
        return timers;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TDifferentialTimerStoreTest, RandomWorkloadWithRestartsAndRepartitioning)
{
    constexpr int Rounds = 240;
    constexpr i64 MaxRows = 7;
    ui64 watermark = 0;

    std::optional<TPair> whole = CreatePair(0, KeySpace, MaxRows);
    std::optional<TPair> left;
    std::optional<TPair> right;
    // Until the first split every page belongs to the partition and the figures are exact.
    bool exact = true;

    for (int round = 0; round < Rounds; ++round) {
        // The watermark creeps, with occasional jumps that make many timers due at once.
        watermark += Random_() % 10 == 0 ? 25 : Random_() % 3;
        auto registrations = static_cast<int>(Random_() % 12);

        if (whole) {
            IterateAndCompare(*whole, watermark, RandomTimers(0, KeySpace, watermark, registrations), MaxRows, exact, Format("round %v, whole", round));
        } else {
            auto timers = RandomTimers(0, KeySpace, watermark, registrations);
            std::vector<TTimer> leftTimers;
            std::vector<TTimer> rightTimers;
            for (auto& timer : timers) {
                (timer.Key < MakeUintKey(KeySpace / 2) ? leftTimers : rightTimers).push_back(std::move(timer));
            }
            IterateAndCompare(*left, watermark, leftTimers, MaxRows, exact, Format("round %v, left", round));
            IterateAndCompare(*right, watermark, rightTimers, MaxRows, exact, Format("round %v, right", round));
        }

        // Every so often the jobs restart, or the partition splits in two, or the halves merge.
        int roll = Random_() % 20;
        if (roll == 0) {
            if (whole) {
                whole = CreatePair(0, KeySpace, MaxRows);
            } else {
                left = CreatePair(0, KeySpace / 2, MaxRows);
                right = CreatePair(KeySpace / 2, KeySpace, MaxRows);
            }
        } else if (roll == 1) {
            exact = false;
            if (whole) {
                whole.reset();
                left = CreatePair(0, KeySpace / 2, MaxRows);
                right = CreatePair(KeySpace / 2, KeySpace, MaxRows);
            } else {
                left.reset();
                right.reset();
                whole = CreatePair(0, KeySpace, MaxRows);
            }
        }
    }

    // Everything left fires in the same order.
    for (int round = 0; round < 40; ++round) {
        watermark += 100;
        if (whole) {
            IterateAndCompare(*whole, watermark, {}, MaxRows, exact, Format("drain %v", round));
        } else {
            IterateAndCompare(*left, watermark, {}, MaxRows, exact, Format("drain %v, left", round));
            IterateAndCompare(*right, watermark, {}, MaxRows, exact, Format("drain %v, right", round));
        }
    }
    if (whole) {
        EXPECT_TRUE(whole->InMemory->IsEmpty());
        EXPECT_TRUE(whole->Paged->IsEmpty());
    } else {
        EXPECT_TRUE(left->InMemory->IsEmpty());
        EXPECT_TRUE(right->InMemory->IsEmpty());
        EXPECT_TRUE(left->Paged->IsEmpty());
        EXPECT_TRUE(right->Paged->IsEmpty());
    }
    EXPECT_EQ(Pages_->GetRowCount(), 0);
}

INSTANTIATE_TEST_SUITE_P(
    DeduplicationAndSeed,
    TDifferentialTimerStoreTest,
    ::testing::Values(std::pair(false, 1), std::pair(false, 2), std::pair(false, 3), std::pair(true, 1), std::pair(true, 2), std::pair(true, 3)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
