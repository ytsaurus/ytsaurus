#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/flow/library/cpp/computation/key_visitor_store.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/key_visitor_states.h>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TKeyVisitorStoreTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId = TComputationId("c");
    const TStreamId StreamId = TStreamId("s");

    NTables::TInMemoryKeyVisitorStatesPtr MakeBackend()
    {
        return New<NTables::TInMemoryKeyVisitorStates>();
    }

    TKeyVisitorStorePtr MakeStore(
        TKeyRange partitionRange,
        int bucketCount,
        NTables::IKeyVisitorStatesPtr backend)
    {
        return New<TKeyVisitorStore>(
            ComputationId,
            StreamId,
            std::move(partitionRange),
            bucketCount,
            std::move(backend),
            NLogging::TLogger("Test"));
    }

    int CountIntervals(const TKeyVisitorStorePtr& store, TKeyVisitorStore::EIntervalState state)
    {
        int n = 0;
        for (const auto& bucket : store->GetBuckets()) {
            for (const auto& slot : bucket.Intervals) {
                if (slot.State == state) {
                    ++n;
                }
            }
        }
        return n;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyVisitorStoreTest, InitOnEmptyStorage)
{
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 2, MakeBackend());
    WaitFor(store->Init()).ThrowOnError();

    EXPECT_EQ(store->GetBuckets().size(), 2u);
    EXPECT_EQ(CountIntervals(store, TKeyVisitorStore::EIntervalState::Pending), 2);
    EXPECT_EQ(CountIntervals(store, TKeyVisitorStore::EIntervalState::Committed), 0);
    EXPECT_FALSE(store->IsAllCommitted());
}

TEST_F(TKeyVisitorStoreTest, MarkBufferedStaysInMemory)
{
    auto backend = MakeBackend();
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
    WaitFor(store->Init()).ThrowOnError();

    store->MarkBuffered(MakeUintKeyRange(1, 100), TSystemTimestamp(100));
    EXPECT_FALSE(store->IsAllCommitted());
    EXPECT_EQ(CountIntervals(store, TKeyVisitorStore::EIntervalState::Buffered), 1);

    store->Sync(/*transaction*/ nullptr);
    EXPECT_EQ(backend->GetRowCount(), 0);
    EXPECT_EQ(CountIntervals(store, TKeyVisitorStore::EIntervalState::Buffered), 1);
}

TEST_F(TKeyVisitorStoreTest, MarkCommittedAndSyncWritesRows)
{
    auto backend = MakeBackend();
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
    WaitFor(store->Init()).ThrowOnError();

    store->MarkCommitted(MakeUintKeyRange(1, 100));
    EXPECT_TRUE(store->IsAllCommitted());

    store->Sync(/*transaction*/ nullptr);
    EXPECT_EQ(backend->GetRowCount(), 2);
}

TEST_F(TKeyVisitorStoreTest, GetNextRangeProgressesAndExhausts)
{
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 2, MakeBackend());
    WaitFor(store->Init()).ThrowOnError();

    auto a = store->GetNextRange();
    auto b = store->GetNextRange();
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_NE(a->Lower, b->Lower);

    store->MarkBuffered(*a, TSystemTimestamp(100));
    store->MarkBuffered(*b, TSystemTimestamp(200));
    EXPECT_FALSE(store->GetNextRange().has_value());

    store->StartNewPass(/*finalPass*/ false);
    EXPECT_TRUE(store->GetNextRange().has_value());
}

TEST_F(TKeyVisitorStoreTest, SyncIsIdempotent)
{
    auto backend = MakeBackend();
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
    WaitFor(store->Init()).ThrowOnError();

    store->MarkCommitted(MakeUintKeyRange(10, 50));
    store->Sync(/*transaction*/ nullptr);
    auto writes = backend->GetWriteCount();

    store->Sync(/*transaction*/ nullptr);
    EXPECT_EQ(backend->GetWriteCount(), writes);
}

TEST_F(TKeyVisitorStoreTest, StartNewPassMakesAllFreeAndSyncDeletesRows)
{
    auto backend = MakeBackend();
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
    WaitFor(store->Init()).ThrowOnError();

    store->MarkCommitted(MakeUintKeyRange(1, 100));
    store->Sync(/*transaction*/ nullptr);
    EXPECT_TRUE(store->IsAllCommitted());
    EXPECT_EQ(backend->GetRowCount(), 2);

    store->StartNewPass(/*finalPass*/ false);
    EXPECT_FALSE(store->IsAllCommitted());
    EXPECT_EQ(CountIntervals(store, TKeyVisitorStore::EIntervalState::Pending), 1);

    store->Sync(/*transaction*/ nullptr);
    EXPECT_EQ(backend->GetRowCount(), 0);
}

TEST_F(TKeyVisitorStoreTest, StartNewPassFinalMarksIntervals)
{
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, MakeBackend());
    WaitFor(store->Init()).ThrowOnError();

    EXPECT_FALSE(store->IsCurrentPassFinal());
    store->StartNewPass(/*finalPass*/ true);
    EXPECT_TRUE(store->IsCurrentPassFinal());

    const auto& intervals = store->GetBuckets().at(0).Intervals;
    ASSERT_EQ(intervals.size(), 1u);
    EXPECT_EQ(intervals[0].State, TKeyVisitorStore::EIntervalState::Pending);
    EXPECT_TRUE(intervals[0].Interval->FinalPass);

    store->MarkBuffered(MakeUintKeyRange(1, 100), TSystemTimestamp(100));
    store->MarkCommitted(MakeUintKeyRange(1, 100));
    EXPECT_TRUE(store->IsCurrentPassFinal());
    EXPECT_TRUE(store->IsAllCommitted());
    const auto& committed = store->GetBuckets().at(0).Intervals;
    ASSERT_EQ(committed.size(), 1u);
    EXPECT_EQ(committed[0].State, TKeyVisitorStore::EIntervalState::Committed);
    EXPECT_TRUE(committed[0].Interval->FinalPass);
}

TEST_F(TKeyVisitorStoreTest, StartNewPassNonFinalKeepsFlagDown)
{
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, MakeBackend());
    WaitFor(store->Init()).ThrowOnError();

    store->StartNewPass(/*finalPass*/ false);
    EXPECT_FALSE(store->IsCurrentPassFinal());

    store->MarkBuffered(MakeUintKeyRange(1, 100), TSystemTimestamp(100));
    store->MarkCommitted(MakeUintKeyRange(1, 100));
    EXPECT_FALSE(store->IsCurrentPassFinal());
}

TEST_F(TKeyVisitorStoreTest, FinalPassSurvivesRestart)
{
    auto backend = MakeBackend();
    {
        auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
        WaitFor(store->Init()).ThrowOnError();
        store->StartNewPass(/*finalPass*/ true);
        store->MarkBuffered(MakeUintKeyRange(1, 100), TSystemTimestamp(42));
        store->MarkCommitted(MakeUintKeyRange(1, 100));
        store->Sync(/*transaction*/ nullptr);
    }

    auto restored = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
    WaitFor(restored->Init()).ThrowOnError();
    EXPECT_TRUE(restored->IsCurrentPassFinal());
    EXPECT_TRUE(restored->IsAllCommitted());
}

TEST_F(TKeyVisitorStoreTest, IntervalSplitCarriesFinalFlag)
{
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, MakeBackend());
    WaitFor(store->Init()).ThrowOnError();
    store->StartNewPass(/*finalPass*/ true);

    // Commit a middle slice → Pending / Committed / Pending; all three must
    // inherit FinalPass=true from the parent.
    store->MarkBuffered(MakeUintKeyRange(40, 60), TSystemTimestamp(100));
    store->MarkCommitted(MakeUintKeyRange(40, 60));

    const auto& intervals = store->GetBuckets().at(0).Intervals;
    ASSERT_EQ(intervals.size(), 3u);
    for (const auto& slot : intervals) {
        EXPECT_TRUE(slot.Interval->FinalPass)
            << "interval [" << NYson::ConvertToYsonString(slot.Interval->Lower).ToString()
            << ";" << NYson::ConvertToYsonString(slot.Interval->Upper).ToString()
            << ") lost FinalPass after split";
    }
}

TEST_F(TKeyVisitorStoreTest, OpenLeftPersistsOnlyUpperRow)
{
    auto backend = MakeBackend();
    auto store = MakeStore(TKeyRange{MinKey(), MakeUintKey(100)}, /*bucketCount*/ 1, backend);
    WaitFor(store->Init()).ThrowOnError();

    store->MarkCommitted(TKeyRange{MinKey(), MakeUintKey(50)});
    store->Sync(/*transaction*/ nullptr);

    auto rows = WaitFor(backend->ReadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].first.Key, MakeUintKey(50));
    EXPECT_FALSE(rows[0].first.IsLower);
}

TEST_F(TKeyVisitorStoreTest, OpenRightPersistsOnlyLowerRow)
{
    auto backend = MakeBackend();
    auto store = MakeStore(TKeyRange{MakeUintKey(0), MaxKey()}, /*bucketCount*/ 1, backend);
    WaitFor(store->Init()).ThrowOnError();

    store->MarkCommitted(TKeyRange{MakeUintKey(50), MaxKey()});
    store->Sync(/*transaction*/ nullptr);

    auto rows = WaitFor(backend->ReadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].first.Key, MakeUintKey(50));
    EXPECT_TRUE(rows[0].first.IsLower);
}

TEST_F(TKeyVisitorStoreTest, InitRestoresFromPersistedRows)
{
    auto backend = MakeBackend();
    {
        auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
        WaitFor(store->Init()).ThrowOnError();
        store->MarkCommitted(MakeUintKeyRange(20, 80));
        store->Sync(/*transaction*/ nullptr);
    }

    auto restored = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
    WaitFor(restored->Init()).ThrowOnError();

    ASSERT_EQ(restored->GetBuckets().size(), 1u);
    const auto& intervals = restored->GetBuckets()[0].Intervals;
    ASSERT_EQ(intervals.size(), 3u);
    EXPECT_EQ(intervals[0].State, TKeyVisitorStore::EIntervalState::Pending);
    EXPECT_EQ(intervals[1].State, TKeyVisitorStore::EIntervalState::Committed);
    EXPECT_EQ(intervals[1].Interval->Lower, MakeUintKey(20));
    EXPECT_EQ(intervals[1].Interval->Upper, MakeUintKey(80));
    EXPECT_EQ(intervals[2].State, TKeyVisitorStore::EIntervalState::Pending);
}

TEST_F(TKeyVisitorStoreTest, MarkCommittedAcrossMultipleBuckets)
{
    auto backend = MakeBackend();
    // 4 buckets over [10; 110) split evenly: [10;35), [35;60), [60;85), [85;110).
    auto store = MakeStore(MakeUintKeyRange(10, 110), /*bucketCount*/ 4, backend);
    WaitFor(store->Init()).ThrowOnError();

    // Range straddles buckets 1, 2, and partially bucket 3.
    store->MarkCommitted(MakeUintKeyRange(40, 90));

    const auto& buckets = store->GetBuckets();
    ASSERT_EQ(buckets.size(), 4u);
    // Bucket 0 untouched.
    ASSERT_EQ(buckets[0].Intervals.size(), 1u);
    EXPECT_EQ(buckets[0].Intervals.front().State, TKeyVisitorStore::EIntervalState::Pending);
    // Bucket 1: Free [35;40), Processed [40;60).
    ASSERT_EQ(buckets[1].Intervals.size(), 2u);
    EXPECT_EQ(buckets[1].Intervals[0].State, TKeyVisitorStore::EIntervalState::Pending);
    EXPECT_EQ(buckets[1].Intervals[1].State, TKeyVisitorStore::EIntervalState::Committed);
    EXPECT_EQ(buckets[1].Intervals[1].Interval->Lower, MakeUintKey(40));
    EXPECT_EQ(buckets[1].Intervals[1].Interval->Upper, MakeUintKey(60));
    // Bucket 2: Processed [60;85).
    ASSERT_EQ(buckets[2].Intervals.size(), 1u);
    EXPECT_EQ(buckets[2].Intervals.front().State, TKeyVisitorStore::EIntervalState::Committed);
    // Bucket 3: Processed [85;90), Free [90;110).
    ASSERT_EQ(buckets[3].Intervals.size(), 2u);
    EXPECT_EQ(buckets[3].Intervals[0].State, TKeyVisitorStore::EIntervalState::Committed);
    EXPECT_EQ(buckets[3].Intervals[0].Interval->Upper, MakeUintKey(90));
    EXPECT_EQ(buckets[3].Intervals[1].State, TKeyVisitorStore::EIntervalState::Pending);

    store->Sync(/*transaction*/ nullptr);
    // No cross-bucket merging: each bucket's Processed interval contributes
    // two rows on its own. Buckets 1, 2, 3 → 6 rows total.
    EXPECT_EQ(backend->GetRowCount(), 6);
}

TEST_F(TKeyVisitorStoreTest, InitWithDifferentBucketCountProducesJaggedCoverage)
{
    auto backend = MakeBackend();
    // First incarnation: 2 buckets over [10; 110) ([10;60), [60;110)),
    // persist two disjoint slices straddling the bucket boundary.
    {
        auto store = MakeStore(MakeUintKeyRange(10, 110), /*bucketCount*/ 2, backend);
        WaitFor(store->Init()).ThrowOnError();
        store->MarkCommitted(MakeUintKeyRange(20, 50));
        store->MarkCommitted(MakeUintKeyRange(70, 100));
        store->Sync(/*transaction*/ nullptr);
        EXPECT_EQ(backend->GetRowCount(), 4);
    }

    auto store = MakeStore(MakeUintKeyRange(10, 110), /*bucketCount*/ 5, backend);
    WaitFor(store->Init()).ThrowOnError();
    ASSERT_EQ(store->GetBuckets().size(), 5u);

    // Bucket 0 ([10;30)): Free [10;20), Processed [20;30).
    {
        const auto& iv = store->GetBuckets()[0].Intervals;
        ASSERT_EQ(iv.size(), 2u);
        EXPECT_EQ(iv[0].State, TKeyVisitorStore::EIntervalState::Pending);
        EXPECT_EQ(iv[1].State, TKeyVisitorStore::EIntervalState::Committed);
        EXPECT_EQ(iv[1].Interval->Lower, MakeUintKey(20));
    }
    // Bucket 1 ([30;50)): all Processed.
    {
        const auto& iv = store->GetBuckets()[1].Intervals;
        ASSERT_EQ(iv.size(), 1u);
        EXPECT_EQ(iv[0].State, TKeyVisitorStore::EIntervalState::Committed);
    }
    // Bucket 2 ([50;70)): all Free.
    {
        const auto& iv = store->GetBuckets()[2].Intervals;
        ASSERT_EQ(iv.size(), 1u);
        EXPECT_EQ(iv[0].State, TKeyVisitorStore::EIntervalState::Pending);
    }
    // Bucket 3 ([70;90)): all Processed.
    {
        const auto& iv = store->GetBuckets()[3].Intervals;
        ASSERT_EQ(iv.size(), 1u);
        EXPECT_EQ(iv[0].State, TKeyVisitorStore::EIntervalState::Committed);
    }
    // Bucket 4 ([90;110)): Processed [90;100), Free [100;110).
    {
        const auto& iv = store->GetBuckets()[4].Intervals;
        ASSERT_EQ(iv.size(), 2u);
        EXPECT_EQ(iv[0].State, TKeyVisitorStore::EIntervalState::Committed);
        EXPECT_EQ(iv[0].Interval->Upper, MakeUintKey(100));
        EXPECT_EQ(iv[1].State, TKeyVisitorStore::EIntervalState::Pending);
    }
}

TEST_F(TKeyVisitorStoreTest, ScannedFractionTracksProgress)
{
    auto store = MakeStore(MakeUintKeyRange(0, 100), /*bucketCount*/ 1, MakeBackend());
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_DOUBLE_EQ(store->GetScannedFraction(), 0.0);

    store->MarkBuffered(MakeUintKeyRange(0, 25), TSystemTimestamp(100));
    EXPECT_DOUBLE_EQ(store->GetScannedFraction(), 0.25);

    store->MarkCommitted(MakeUintKeyRange(0, 25));
    store->MarkBuffered(MakeUintKeyRange(25, 75), TSystemTimestamp(200));
    EXPECT_DOUBLE_EQ(store->GetScannedFraction(), 0.75);
}

TEST_F(TKeyVisitorStoreTest, IsRemoteStateEmptyMirrorsBackend)
{
    auto backend = MakeBackend();
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_TRUE(store->IsRemoteStateEmpty());

    store->MarkCommitted(MakeUintKeyRange(1, 100));
    store->Sync(/*transaction*/ nullptr);
    EXPECT_FALSE(store->IsRemoteStateEmpty());

    store->Reset();
    store->Sync(/*transaction*/ nullptr);
    EXPECT_TRUE(store->IsRemoteStateEmpty());
}

TEST_F(TKeyVisitorStoreTest, GetMinPassStartedAtNulloptWhenAllPending)
{
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 2, MakeBackend());
    WaitFor(store->Init()).ThrowOnError();
    EXPECT_EQ(store->GetMinPassStartedAt(), std::nullopt);
}

TEST_F(TKeyVisitorStoreTest, GetMinPassStartedAtTakesEarliestStamp)
{
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, MakeBackend());
    WaitFor(store->Init()).ThrowOnError();

    store->MarkBuffered(MakeUintKeyRange(1, 30), TSystemTimestamp(500));
    store->MarkBuffered(MakeUintKeyRange(30, 60), TSystemTimestamp(300));
    store->MarkBuffered(MakeUintKeyRange(60, 100), TSystemTimestamp(700));

    // CoalesceInplace merges the three same-state intervals into one with
    // PassStartedAt = min(500, 300, 700) = 300.
    auto stamp = store->GetMinPassStartedAt();
    ASSERT_TRUE(stamp);
    EXPECT_EQ(stamp->Underlying(), 300u);
}

TEST_F(TKeyVisitorStoreTest, MarkCommittedPreservesPassStartedAt)
{
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, MakeBackend());
    WaitFor(store->Init()).ThrowOnError();

    store->MarkBuffered(MakeUintKeyRange(1, 50), TSystemTimestamp(400));
    store->MarkCommitted(MakeUintKeyRange(1, 50));
    auto stamp = store->GetMinPassStartedAt();
    ASSERT_TRUE(stamp);
    EXPECT_EQ(stamp->Underlying(), 400u);
}

TEST_F(TKeyVisitorStoreTest, PassStartedAtSurvivesRestart)
{
    auto backend = MakeBackend();
    {
        auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
        WaitFor(store->Init()).ThrowOnError();
        store->MarkBuffered(MakeUintKeyRange(1, 50), TSystemTimestamp(1234));
        store->MarkCommitted(MakeUintKeyRange(1, 50));
        store->Sync(/*transaction*/ nullptr);
    }

    auto restored = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
    WaitFor(restored->Init()).ThrowOnError();
    auto stamp = restored->GetMinPassStartedAt();
    ASSERT_TRUE(stamp);
    EXPECT_EQ(stamp->Underlying(), 1234u);
}

TEST_F(TKeyVisitorStoreTest, ResetClearsPersistedAfterSync)
{
    auto backend = MakeBackend();
    auto store = MakeStore(MakeUintKeyRange(1, 100), /*bucketCount*/ 1, backend);
    WaitFor(store->Init()).ThrowOnError();
    store->MarkCommitted(MakeUintKeyRange(10, 50));
    store->Sync(/*transaction*/ nullptr);
    EXPECT_EQ(backend->GetRowCount(), 2);

    store->Reset();
    EXPECT_EQ(CountIntervals(store, TKeyVisitorStore::EIntervalState::Pending), 1);

    store->Sync(/*transaction*/ nullptr);
    EXPECT_EQ(backend->GetRowCount(), 0);
}

// Regression: filter on Init must NOT load the neighbour partition's edge rows.
// Adjacent partitions share the boundary key B: the lower partition writes
// (Key=B, IsLower=false), the upper one writes (Key=B, IsLower=true). If both
// load each other's edge rows the Sync diff turns them into DELETE+WRITE on
// the same primary key and the two transactions race-conflict.
TEST_F(TKeyVisitorStoreTest, InitDoesNotLoadNeighbourPartitionEdgeRows)
{
    auto backend = MakeBackend();
    // Lower neighbour: partition [10; 100). It commits all of it so the
    // upper-bound row (Key=100, IsLower=false) is persisted.
    {
        auto lower = MakeStore(MakeUintKeyRange(10, 100), /*bucketCount*/ 1, backend);
        WaitFor(lower->Init()).ThrowOnError();
        lower->MarkCommitted(MakeUintKeyRange(10, 100));
        lower->Sync(/*transaction*/ nullptr);
    }
    // Upper neighbour: partition [100; 200). Commits all of it: writes
    // (Key=100, IsLower=true) and (Key=200, IsLower=false).
    {
        auto upper = MakeStore(MakeUintKeyRange(100, 200), /*bucketCount*/ 1, backend);
        WaitFor(upper->Init()).ThrowOnError();
        upper->MarkCommitted(MakeUintKeyRange(100, 200));
        upper->Sync(/*transaction*/ nullptr);
    }
    // 4 rows: (10, true)+(100, false) from lower, (100, true)+(200, false)
    // from upper. The two (Key=100, *) rows have distinct IsLower so they
    // are distinct primary keys — that is exactly what the half-open filter
    // must preserve.
    ASSERT_EQ(backend->GetRowCount(), 4);

    // Re-init the lower partition: it must see ONLY its own (Key=100, IsLower=false)
    // — not the upper's (Key=100, IsLower=true). Re-syncing without any new
    // coverage must be a no-op (no DELETE/WRITE mutations).
    auto reopened = MakeStore(MakeUintKeyRange(10, 100), /*bucketCount*/ 1, backend);
    WaitFor(reopened->Init()).ThrowOnError();
    const auto& intervals = reopened->GetBuckets()[0].Intervals;
    ASSERT_EQ(intervals.size(), 1u);
    EXPECT_EQ(intervals[0].State, TKeyVisitorStore::EIntervalState::Committed);
    EXPECT_EQ(intervals[0].Interval->Lower, MakeUintKey(10));
    EXPECT_EQ(intervals[0].Interval->Upper, MakeUintKey(100));

    const auto writesBefore = backend->GetWriteCount();
    const auto deletesBefore = backend->GetDeleteCount();
    reopened->Sync(/*transaction*/ nullptr);
    EXPECT_EQ(backend->GetWriteCount(), writesBefore) << "must not re-write the boundary row";
    EXPECT_EQ(backend->GetDeleteCount(), deletesBefore) << "must not delete the neighbour's lower-row";
    EXPECT_EQ(backend->GetRowCount(), 4) << "all four persisted rows survive untouched";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
