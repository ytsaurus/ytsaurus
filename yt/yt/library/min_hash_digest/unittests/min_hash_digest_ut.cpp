#include <gtest/gtest.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/library/min_hash_digest/config.h>
#include <yt/yt/library/min_hash_digest/min_hash_digest.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////

template <class TTimestampComparator>
TMinHash<TTimestampComparator> MakeMinHash(
    int capacity,
    const std::vector<std::pair<TFingerprint, ui32>>& items)
{
    TMinHashItems<TTimestampComparator> minHashItems;
    minHashItems.reserve(items.size());
    for (auto [hash, primaryTs] : items) {
        minHashItems.push_back({
            .Hash = hash,
            .PrimaryTimestamp = primaryTs,
        });
    }
    return TMinHash<TTimestampComparator>(capacity, std::move(minHashItems));
}

TWriteMinHash MakeWriteMinHash(
    int capacity,
    const std::vector<std::pair<TFingerprint, ui32>>& items)
{
    return MakeMinHash<std::less<ui32>>(capacity, items);
}

TDeleteMinHash MakeDeleteMinHash(
    int capacity,
    const std::vector<std::pair<TFingerprint, ui32>>& items)
{
    return MakeMinHash<std::greater<ui32>>(capacity, items);
}

TMinHashDigestPtr MakeDigest(TWriteMinHash writes, TDeleteMinHash deletes)
{
    auto digest = New<TMinHashDigest>(/*memoryTracker*/ nullptr);
    digest->Initialize(std::move(writes), std::move(deletes));
    return digest;
}

TMinHashSimilarityConfigPtr MakeSimilarityConfig(double minSimilarity, int minRowCount)
{
    auto config = New<TMinHashSimilarityConfig>();
    config->MinSimilarity = minSimilarity;
    config->MinRowCount = minRowCount;
    return config;
}

////////////////////////////////////////////////////////////////////////////////
// TMinHashAccumulator tests
////////////////////////////////////////////////////////////////////////////////

// Write accumulator keeps the MINIMUM primary timestamp for duplicate hashes.
// Items are added so that buffer reaches 2*capacity to trigger compaction.
TEST(TMinHashAccumulator, WriteKeepsMinimumTimestampOnDuplicate)
{
    // capacity=2, 2*capacity=4; compaction triggers when buffer hits 4 items.
    TWriteMinHashAccumulator acc(2);

    // Add 4 items: hash 1 twice and hash 3 twice.
    acc.Add(/*hash*/ 1, /*timestamp*/ 10);
    acc.Add(/*hash*/ 3, /*timestamp*/ 5);
    acc.Add(/*hash*/ 1, /*timestamp*/ 3); // Lower timestamp for hash 1.
    acc.Add(/*hash*/ 3, /*timestamp*/ 8); // Higher timestamp for hash 3.

    auto result = acc.Finish();

    // Only 2 unique hashes, both fit in capacity=2.
    ASSERT_EQ(ssize(result.Items()), 2);
    // Sorted by hash ascending.
    EXPECT_EQ(result.Items()[0].Hash, 1u);
    EXPECT_EQ(result.Items()[0].PrimaryTimestamp, 3u); // Minimum kept.
    EXPECT_EQ(result.Items()[1].Hash, 3u);
    EXPECT_EQ(result.Items()[1].PrimaryTimestamp, 5u); // Minimum kept.
}

// Delete accumulator keeps the MAXIMUM primary timestamp for duplicate hashes.
TEST(TMinHashAccumulator, DeleteKeepsMaximumTimestampOnDuplicate)
{
    TDeleteMinHashAccumulator acc(2);

    acc.Add(/*hash*/ 1, /*timestamp*/ 10);
    acc.Add(/*hash*/ 3, /*timestamp*/ 5);
    acc.Add(/*hash*/ 1, /*timestamp*/ 3);  // Lower should be discarded.
    acc.Add(/*hash*/ 3, /*timestamp*/ 8);  // Higher should replace 5

    auto result = acc.Finish();

    ASSERT_EQ(ssize(result.Items()), 2);
    EXPECT_EQ(result.Items()[0].Hash, 1u);
    EXPECT_EQ(result.Items()[0].PrimaryTimestamp, 10u); // Maximum kept.
    EXPECT_EQ(result.Items()[1].Hash, 3u);
    EXPECT_EQ(result.Items()[1].PrimaryTimestamp, 8u);  // Maximum kept.
}

// When all items are unique and total count exceeds capacity, only the
// capacity-smallest hashes survive (because accumulator sorts by hash).
TEST(TMinHashAccumulator, CapacityLimitTruncatesLargestHashes)
{
    // capacity=3, 2*capacity=6; fill exactly 6 unique hashes to trigger compaction.
    TWriteMinHashAccumulator acc(3);

    acc.Add(5, 1);
    acc.Add(3, 2);
    acc.Add(1, 3);
    acc.Add(6, 4);
    acc.Add(2, 5);
    acc.Add(4, 6);
    // Finish triggers compaction because buffer size == 6 == 2*capacity.
    auto result = acc.Finish();

    // After sorting and trimming to capacity=3, we expect the 3 smallest hashes.
    ASSERT_EQ(result.GetCapacity(), 3);
    ASSERT_EQ(ssize(result.Items()), 3);
    EXPECT_EQ(result.Items()[0].Hash, 1u);
    EXPECT_EQ(result.Items()[1].Hash, 2u);
    EXPECT_EQ(result.Items()[2].Hash, 3u);
}

// Compaction triggers mid-stream when buffer reaches 2*capacity while adding.
// After mid-stream compaction the new item is appended, so final result can
// contain capacity+1 items before the next (not yet triggered) compaction.
TEST(TMinHashAccumulator, MidStreamCompaction)
{
    // capacity=2, 2*capacity=4.
    TWriteMinHashAccumulator acc(2);

    acc.Add(4, 10);
    acc.Add(2, 20);
    acc.Add(3, 30);
    acc.Add(1, 40);
    // At this point buffer=[{4,10},{2,20},{3,30},{1,40}], size=4.
    // Adding one more: EnsureCompacted sees size==4 >= 4, compacts to
    // sorted+trimmed [{1,40},{2,20}], then appends the new item.
    acc.Add(10, 50);
    // Buffer is now [{1,40},{2,20},{10,50}], size=3 < 4, no further compaction.

    auto result = acc.Finish();
    // Finish triggers compaction because size=3 < 4 → no compaction.
    // Items are returned as-is (already partly sorted).
    EXPECT_EQ(result.GetCapacity(), 2);
    // We only assert that hash=1 and hash=2 are present (the two smallest).
    ASSERT_GE(ssize(result.Items()), 2);
    EXPECT_EQ(result.Items()[0].Hash, 1u);
    EXPECT_EQ(result.Items()[1].Hash, 2u);
}

// Accumulator with all-duplicate hashes collapses to a single item.
TEST(TMinHashAccumulator, AllDuplicateHashes)
{
    TWriteMinHashAccumulator acc(3);

    for (int i = 0; i < 6; ++i) {
        acc.Add(42, i + 1);
    }

    auto result = acc.Finish();

    ASSERT_EQ(ssize(result.Items()), 1);
    EXPECT_EQ(result.Items()[0].Hash, 42u);
    EXPECT_EQ(result.Items()[0].PrimaryTimestamp, 1u); // Write keeps minimum.
}

TEST(TMinHashAccumulator, AllDuplicateHashesDelete)
{
    TDeleteMinHashAccumulator acc(3);

    for (int i = 0; i < 6; ++i) {
        acc.Add(42, i + 1);
    }

    auto result = acc.Finish();

    ASSERT_EQ(ssize(result.Items()), 1);
    EXPECT_EQ(result.Items()[0].Hash, 42u);
    EXPECT_EQ(result.Items()[0].PrimaryTimestamp, 6u); // Delete keeps maximum.
}

////////////////////////////////////////////////////////////////////////////////
// TMinHash::Merge tests
////////////////////////////////////////////////////////////////////////////////

// Merging a non-empty min-hash with an empty one yields the non-empty side.
TEST(TMinHashMerge, MergeWithEmptyRhs)
{
    auto lhs = MakeWriteMinHash(5, {{1, 10}, {3, 5}, {5, 1}});
    auto rhs = MakeWriteMinHash(5, {});

    auto result = TWriteMinHash::Merge(lhs, rhs);

    ASSERT_EQ(ssize(result.Items()), 3);
    EXPECT_EQ(result.Items()[0].Hash, 1u);
    EXPECT_EQ(result.Items()[1].Hash, 3u);
    EXPECT_EQ(result.Items()[2].Hash, 5u);
}

// Merging an empty min-hash with a non-empty one yields the non-empty side.
TEST(TMinHashMerge, MergeWithEmptyLhs)
{
    auto lhs = MakeWriteMinHash(5, {});
    auto rhs = MakeWriteMinHash(5, {{2, 20}, {4, 8}});

    auto result = TWriteMinHash::Merge(lhs, rhs);

    ASSERT_EQ(ssize(result.Items()), 2);
    EXPECT_EQ(result.Items()[0].Hash, 2u);
    EXPECT_EQ(result.Items()[1].Hash, 4u);
}

// When hashes don't overlap, items are merged in hash order up to capacity.
TEST(TMinHashMerge, MergeNonOverlappingHashes)
{
    // lhs has odd hashes, rhs has even hashes.
    auto lhs = MakeWriteMinHash(6, {{1, 10}, {3, 5}, {5, 1}});
    auto rhs = MakeWriteMinHash(6, {{2, 20}, {4, 8}, {6, 3}});

    auto result = TWriteMinHash::Merge(lhs, rhs);

    ASSERT_EQ(ssize(result.Items()), 6);
    // Expect interleaved in hash order.
    EXPECT_EQ(result.Items()[0].Hash, 1u);
    EXPECT_EQ(result.Items()[1].Hash, 2u);
    EXPECT_EQ(result.Items()[2].Hash, 3u);
    EXPECT_EQ(result.Items()[3].Hash, 4u);
    EXPECT_EQ(result.Items()[4].Hash, 5u);
    EXPECT_EQ(result.Items()[5].Hash, 6u);
    // No aux timestamps set for non-overlapping items.
    for (const auto& item : result.Items()) {
        EXPECT_EQ(item.AuxiliaryTimestamp, 0u);
    }
}

// Merging with capacity < sum of items truncates the result.
TEST(TMinHashMerge, CapacityConstrainsMerge)
{
    auto lhs = MakeWriteMinHash(3, {{1, 10}, {3, 5}, {5, 1}});
    auto rhs = MakeWriteMinHash(3, {{2, 20}, {4, 8}, {6, 3}});

    auto result = TWriteMinHash::Merge(lhs, rhs);

    EXPECT_EQ(result.GetCapacity(), 3);
    ASSERT_EQ(ssize(result.Items()), 3);
    EXPECT_EQ(result.Items()[0].Hash, 1u);
    EXPECT_EQ(result.Items()[1].Hash, 2u);
    EXPECT_EQ(result.Items()[2].Hash, 3u);
}

// The merged capacity is the minimum of the two input capacities.
TEST(TMinHashMerge, MergeUsesMinimumCapacity)
{
    auto lhs = MakeWriteMinHash(10, {{1, 1}, {2, 2}});
    auto rhs = MakeWriteMinHash(3,  {{3, 3}, {4, 4}});

    auto result = TWriteMinHash::Merge(lhs, rhs);

    EXPECT_EQ(result.GetCapacity(), 3);
}

// When the same hash appears in both sides, the write merge keeps the item with
// the LOWER primary timestamp and records the discarded item's timestamp as Auxiliary.
TEST(TMinHashMerge, WriteOverlapSetsAuxiliaryTimestamp)
{
    // lhs has hash=1 at ts=10; rhs has hash=1 at ts=20.
    auto lhs = MakeWriteMinHash(5, {{1, 10}, {3, 5}});
    auto rhs = MakeWriteMinHash(5, {{1, 20}, {5, 1}});

    auto result = TWriteMinHash::Merge(lhs, rhs);

    ASSERT_EQ(ssize(result.Items()), 3);
    // Hash=1: primary should be the smaller (10), auxiliary should be the larger (20).
    EXPECT_EQ(result.Items()[0].Hash, 1u);
    EXPECT_EQ(result.Items()[0].PrimaryTimestamp, 10u);
    EXPECT_EQ(result.Items()[0].AuxiliaryTimestamp, 20u);
    // Non-overlapping hashes have no auxiliary timestamp.
    EXPECT_EQ(result.Items()[1].Hash, 3u);
    EXPECT_EQ(result.Items()[1].AuxiliaryTimestamp, 0u);
    EXPECT_EQ(result.Items()[2].Hash, 5u);
    EXPECT_EQ(result.Items()[2].AuxiliaryTimestamp, 0u);
}

// When AuxiliaryTimestamp is already set and another merge happens, the auxiliary
// is updated to the best of the existing auxiliary and the new discarded primary
// (for writes: minimum; for deletes: maximum via comparator-based std::min).
TEST(TMinHashMerge, WriteTripleMergeAuxiliaryUpdated)
{
    // First merge: hash=1 at ts=10 (lhs) vs ts=50 (rhs) → primary=10, aux=50.
    auto step1Lhs = MakeWriteMinHash(5, {{1, 10}});
    auto step1Rhs = MakeWriteMinHash(5, {{1, 50}});
    auto step1 = TWriteMinHash::Merge(step1Lhs, step1Rhs);
    // step1.items[0] = {hash=1, primary=10, aux=50}

    // Second merge: step1 vs a new chunk with hash=1 at ts=30.
    // Discarded primary (30) is compared to existing aux (50): min(30, 50) = 30.
    auto step2Rhs = MakeWriteMinHash(5, {{1, 30}});
    auto step2 = TWriteMinHash::Merge(step1, step2Rhs);

    ASSERT_EQ(ssize(step2.Items()), 1);
    EXPECT_EQ(step2.Items()[0].Hash, 1u);
    EXPECT_EQ(step2.Items()[0].PrimaryTimestamp, 10u);
    EXPECT_EQ(step2.Items()[0].AuxiliaryTimestamp, 30u); // min(50, 30) = 30
}

////////////////////////////////////////////////////////////////////////////////
// TMinHashDigest serialization tests
////////////////////////////////////////////////////////////////////////////////

TEST(TMinHashDigestSerialization, RoundTrip)
{
    auto original = MakeDigest(
        MakeWriteMinHash(5, {{10, 100}, {20, 200}, {30, 300}}),
        MakeDeleteMinHash(5, {{10, 150}, {40, 400}}));

    auto serialized = original->BuildSerialized();

    auto restored = New<TMinHashDigest>(GetNullMemoryUsageTracker());
    restored->Initialize(serialized);

    ASSERT_TRUE(restored->IsInitialized());

    // Verify both digests produce the same similarity result.
    auto config = MakeSimilarityConfig(0.5, 1);
    EXPECT_EQ(
        original->CalculateWriteDeleteSimilarityTimestamp(config),
        restored->CalculateWriteDeleteSimilarityTimestamp(config));
    EXPECT_EQ(original->CalculateWriteDeleteSimilarityTimestamp(config), 150u);
}

TEST(TMinHashDigestSerialization, InvalidFormatVersionThrows)
{
    // Build a fake serialized blob with format version = 99.
    auto data = TSharedMutableRef::Allocate(sizeof(ui32));
    char* ptr = data.begin();
    WritePod(ptr, 99);

    auto digest = New<TMinHashDigest>(GetNullMemoryUsageTracker());
    EXPECT_THROW(digest->Initialize(TSharedRef(data)), std::exception);
}

TEST(TMinHashDigestSerialization, EmptyDigestRoundTrip)
{
    auto original = MakeDigest(
        MakeWriteMinHash(5, {}),
        MakeDeleteMinHash(5, {}));

    auto serialized = original->BuildSerialized();
    auto restored = New<TMinHashDigest>(GetNullMemoryUsageTracker());
    restored->Initialize(serialized);

    auto config = MakeSimilarityConfig(0.5, 1);
    // Both should return 0 (no data).
    EXPECT_EQ(original->CalculateWriteDeleteSimilarityTimestamp(config), 0u);
    EXPECT_EQ(restored->CalculateWriteDeleteSimilarityTimestamp(config), 0u);
}

////////////////////////////////////////////////////////////////////////////////
// CalculateWriteDeleteSimilarityTimestamp tests
////////////////////////////////////////////////////////////////////////////////

// If min(writeCount, deleteCount) < MinRowCount, return 0.
TEST(TMinHashDigestWriteDeleteSimilarity, BelowMinRowCountReturnsZero)
{
    auto digest = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {2, 20}, {3, 30}}),
        MakeDeleteMinHash(5, {{1, 15}, {2, 25}, {3, 35}}));

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);
    EXPECT_EQ(digest->CalculateWriteDeleteSimilarityTimestamp(config), 0u);
}

// No matching hashes between writes and deletes → intersection empty → return 0.
TEST(TMinHashDigestWriteDeleteSimilarity, NoIntersectionReturnsZero)
{
    auto digest = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {3, 20}, {5, 30}}),
        MakeDeleteMinHash(5, {{2, 15}, {4, 25}, {6, 35}}));

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 3);
    EXPECT_EQ(digest->CalculateWriteDeleteSimilarityTimestamp(config), 0u);
}

// When the delete timestamp precedes the write timestamp for matching hashes,
// they are NOT counted in the intersection (the record was not re-written after deletion).
TEST(TMinHashDigestWriteDeleteSimilarity, DeleteBeforeWriteNotCounted)
{
    auto digest = MakeDigest(
        MakeWriteMinHash(5, {{1, 100}, {2, 200}}),
        MakeDeleteMinHash(5, {{1, 50}, {2, 150}})); // deletes at lower timestamps (before the writes)

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 2);
    EXPECT_EQ(digest->CalculateWriteDeleteSimilarityTimestamp(config), 0u);
}

// Intersection count meets the threshold exactly — should return a non-zero timestamp.
TEST(TMinHashDigestWriteDeleteSimilarity, ExactThresholdReturnsTimestamp)
{
    // 5 writes, 5 deletes, 3 overlapping with write.ts < delete.ts.
    // MinSimilarity=0.5 → ceil(5*0.5)=3, so need exactly 3 intersections.
    auto digest = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {3, 5}, {5, 50}, {7, 1}, {9, 8}}),
        MakeDeleteMinHash(5, {{1, 20}, {3, 100}, {4, 7}, {7, 2}, {9, 3}}));

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);
    // Two-pointer walk:
    //   hash=1: write(10) < delete(20) → intersect, push 20
    //   hash=3: write(5)  < delete(100)→ intersect, push 100
    //   hash=4: only in deletes, skip
    //   hash=5: only in writes, skip
    //   hash=7: write(1)  < delete(2) → intersect, push 2
    //   hash=9: write(8)  > delete(3) → NOT counted
    // intersectionTimestamps = [20, 100, 2]; need 3rd smallest → nth_element → 100
    EXPECT_EQ(digest->CalculateWriteDeleteSimilarityTimestamp(config), 100u);
}

// When intersection count is one below the required threshold, return 0.
TEST(TMinHashDigestWriteDeleteSimilarity, OneShortOfThresholdReturnsZero)
{
    // 5 writes, 5 deletes, only 2 overlapping with write.ts < delete.ts.
    // MinSimilarity=0.5 → need ceil(5*0.5)=3 intersections.
    // hash=5: delete(3)<write(50); hash=7: delete(2)<write(100); hash=9: delete(3)<write(8)
    auto digest = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {3, 5}, {5, 50}, {7, 100}, {9, 8}}),
        MakeDeleteMinHash(5, {{1, 20}, {3, 100}, {5, 3}, {7, 2}, {9, 3}}));

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);
    // Two-pointer:
    //   hash=1: write(10) < delete(20) → push 20
    //   hash=3: write(5)  < delete(100)→ push 100
    //   hash=5: write(50) > delete(3)  → NOT counted
    //   hash=7: write(100)> delete(2)  → NOT counted
    //   hash=9: write(8)  > delete(3)  → NOT counted
    // intersectionTimestamps=[20,100], need 3 → insufficient → return 0
    EXPECT_EQ(digest->CalculateWriteDeleteSimilarityTimestamp(config), 0u);
}

// High similarity threshold (0.8) requires more intersections; verify correct timestamp.
TEST(TMinHashDigestWriteDeleteSimilarity, HighSimilarityThreshold)
{
    // 5 writes, 5 deletes, all 5 overlapping with write.ts < delete.ts.
    // MinSimilarity=0.8 → ceil(5*0.8)=4, need 4th-smallest intersection timestamp.
    auto digest = MakeDigest(
        MakeWriteMinHash(5, {{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}),
        MakeDeleteMinHash(5, {{1, 11}, {2, 22}, {3, 33}, {4, 44}, {5, 55}}));

    auto config = MakeSimilarityConfig(0.8, /*minRowCount*/ 5);
    // All 5 pairs intersect; timestamps = [11,22,33,44,55].
    // sufficientIntersectionSize = ceil(5*0.8) = 4 → index 3 → 4th smallest = 44.
    EXPECT_EQ(digest->CalculateWriteDeleteSimilarityTimestamp(config), 44u);
}

// Partial intersection: some hashes only in writes (not deletes) are skipped.
TEST(TMinHashDigestWriteDeleteSimilarity, WritesOnlyHashesAreIgnored)
{
    // hashes 100, 200 only in writes
    auto digest = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {2, 20}, {3, 30}, {100, 1}, {200, 2}}),
        MakeDeleteMinHash(5, {{1, 15}, {2, 25}, {3, 35}}));

    // comparisonPrefixSize = min(5, 3) = 3
    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 3);
    // hash=1: write(10) < delete(15) → push 15
    // hash=2: write(20) < delete(25) → push 25
    // hash=3: write(30) < delete(35) → push 35
    // intersectionTimestamps=[15,25,35], need ceil(3*0.5)=2, index=1 → 25
    EXPECT_EQ(digest->CalculateWriteDeleteSimilarityTimestamp(config), 25u);
}

////////////////////////////////////////////////////////////////////////////////
// CalculateWritesSimilarityTimestamp tests
////////////////////////////////////////////////////////////////////////////////

// Without any auxiliary timestamps (no hash appeared in more than one chunk),
// the intersection is empty and the result is 0.
TEST(TMinHashDigestWritesSimilarity, NoAuxTimestampsReturnsZero)
{
    auto digest = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}}),
        MakeDeleteMinHash(5, {}));

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 3);
    EXPECT_EQ(digest->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 0u);
    EXPECT_EQ(digest->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary), 0u);
}

// If write item count < MinRowCount, return 0 regardless of auxiliary timestamps.
TEST(TMinHashDigestWritesSimilarity, BelowMinRowCountReturnsZero)
{
    // Two digests each writing hashes {1, 2} at different timestamps.
    // After merge: both items get aux timestamps, but count=2 < MinRowCount=5.
    auto digest0 = MakeDigest(MakeWriteMinHash(5, {{1, 10}, {2, 20}}), MakeDeleteMinHash(5, {}));
    auto digest1 = MakeDigest(MakeWriteMinHash(5, {{1, 50}, {2, 60}}), MakeDeleteMinHash(5, {}));
    auto merged = TMinHashDigest::Merge(digest0, digest1);
    // Merged write items: hash=1 (primary=10, aux=50), hash=2 (primary=20, aux=60).

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 0u);
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary), 0u);
}

// Not enough items with auxiliary timestamps to meet the similarity threshold.
// Aux timestamps are produced by merging two digests where only hash=1 overlaps.
TEST(TMinHashDigestWritesSimilarity, InsufficientAuxTimestampsReturnsZero)
{
    // digest0 writes hashes {1, 2, 3, 4, 5}; digest1 writes only hash {1}.
    // After merge: only hash=1 gets an aux timestamp.
    // MinSimilarity=0.5 → ceil(5*0.5)=3 intersections needed, but only 1.
    auto digest0 = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}}),
        MakeDeleteMinHash(5, {}));
    auto digest1 = MakeDigest(
        MakeWriteMinHash(5, {{1, 20}}),
        MakeDeleteMinHash(5, {}));
    auto merged = TMinHashDigest::Merge(digest0, digest1);
    // Merged: hash=1 (primary=10, aux=20), hashes 2-5 (aux=0).

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 0u);
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary), 0u);
}

// Primary mode returns the primary timestamp of the threshold-th item.
// Aux timestamps are produced by merging two digests that share hashes {1, 2, 3}.
TEST(TMinHashDigestWritesSimilarity, PrimaryModeReturnsCorrectTimestamp)
{
    // digest0 writes hashes {1, 2, 3, 4, 5} at timestamps {10, 30, 50, 70, 90}.
    // digest1 writes hashes {1, 2, 3} at timestamps {20, 60, 90} (all larger → become aux).
    // After merge: hash=1 (primary=10, aux=20), hash=2 (primary=30, aux=60),
    //              hash=3 (primary=50, aux=90), hash=4 (primary=70, aux=0),
    //              hash=5 (primary=90, aux=0).
    auto digest0 = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {2, 30}, {3, 50}, {4, 70}, {5, 90}}),
        MakeDeleteMinHash(5, {}));
    auto digest1 = MakeDigest(
        MakeWriteMinHash(5, {{1, 20}, {2, 60}, {3, 90}}),
        MakeDeleteMinHash(5, {}));
    auto merged = TMinHashDigest::Merge(digest0, digest1);

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);
    // 3 items have aux != 0; intersectionTimestamps (primary) = [10, 30, 50].
    // sufficientIntersectionSize = ceil(5*0.5) = 3, index = 2 → 50 (3rd smallest).
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 50u);
}

// Auxiliary mode returns the auxiliary timestamp of the threshold-th item.
// Same merge setup as PrimaryModeReturnsCorrectTimestamp.
TEST(TMinHashDigestWritesSimilarity, AuxiliaryModeReturnsCorrectTimestamp)
{
    // Same digests as above.
    // After merge: hash=1 (aux=20), hash=2 (aux=60), hash=3 (aux=90).
    auto digest0 = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {2, 30}, {3, 50}, {4, 70}, {5, 90}}),
        MakeDeleteMinHash(5, {}));
    auto digest1 = MakeDigest(
        MakeWriteMinHash(5, {{1, 20}, {2, 60}, {3, 90}}),
        MakeDeleteMinHash(5, {}));
    auto merged = TMinHashDigest::Merge(digest0, digest1);

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);
    // intersectionTimestamps (aux) = [20, 60, 90].
    // sufficientIntersectionSize = 3, index = 2 → 90 (3rd smallest).
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary), 90u);
}

// Low similarity threshold means we need fewer intersections, returning an earlier timestamp.
// Aux timestamps are produced by merging two digests that share hashes {1,2,3}.
TEST(TMinHashDigestWritesSimilarity, LowThresholdReturnsSmallerTimestamp)
{
    // digest0 writes hashes {1, 2, 3, 4, 5} at timestamps {5, 10, 50, 70, 90}.
    // digest1 writes hashes {1, 2, 3} at timestamps {100, 200, 500} (all larger → become aux).
    // After merge: hash=1 (primary=5, aux=100), hash=2 (primary=10, aux=200),
    //              hash=3 (primary=50, aux=500), hash=4 (primary=70), hash=5 (primary=90).
    auto digest0 = MakeDigest(
        MakeWriteMinHash(5, {{1, 5}, {2, 10}, {3, 50}, {4, 70}, {5, 90}}),
        MakeDeleteMinHash(5, {}));
    auto digest1 = MakeDigest(
        MakeWriteMinHash(5, {{1, 100}, {2, 200}, {3, 500}}),
        MakeDeleteMinHash(5, {}));
    auto merged = TMinHashDigest::Merge(digest0, digest1);

    // MinSimilarity=0.2 → ceil(5*0.2)=1, index=0, 1st smallest primary.
    auto config = MakeSimilarityConfig(0.2, /*minRowCount*/ 5);
    // intersectionTimestamps (primary) = [5, 10, 50]; need 1st smallest = 5.
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 5u);
}

////////////////////////////////////////////////////////////////////////////////
// Merge then similarity
////////////////////////////////////////////////////////////////////////////////

TEST(TMinHashDigest, SingleDigestDelete)
{
    auto digest = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {3, 1}, {5, 5}, {7, 10}, {9, 9}}),
        MakeDeleteMinHash(5, {{1, 11}, {3, 0}, {5, 6}, {6, 20}, {9, 200}}));

    auto similarityConfig = New<TMinHashSimilarityConfig>();

    auto timestamp = digest->CalculateWriteDeleteSimilarityTimestamp(similarityConfig);

    EXPECT_EQ(timestamp, 200u);
}

// Merging three digests and checking both similarity modes.
TEST(TMinHashDigest, SimpleMultiDigest)
{
    auto digest0 = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {3, 1}, {5, 5}, {7, 10}, {9, 9}}),
        MakeDeleteMinHash(5, {}));
    auto digest1 = MakeDigest(
        MakeWriteMinHash(5, {{1, 100}, {3, 100}, {50, 5}, {70, 10}, {90, 9}}),
        MakeDeleteMinHash(5, {{1, 11}, {3, 2}, {5, 6}, {700, 10}, {900, 9}}));
    auto digest2 = MakeDigest(
        MakeWriteMinHash(5, {}),
        MakeDeleteMinHash(5, {{5, 6}, {7, 11}, {9, 42}, {1000, 10}, {3000, 1}}));

    auto mergedDigest = TMinHashDigest::Merge(digest0, digest1);
    mergedDigest = TMinHashDigest::Merge(mergedDigest, digest2);

    auto similarityConfig = New<TMinHashSimilarityConfig>();

    auto timestamp = mergedDigest->CalculateWriteDeleteSimilarityTimestamp(similarityConfig);
    EXPECT_EQ(timestamp, 11u);

    similarityConfig->MinRowCount = 2;
    similarityConfig->MinSimilarity = 0.19;

    timestamp = mergedDigest->CalculateWritesSimilarityTimestamp(similarityConfig, EMinHashWriteSimilarityMode::Primary);
    EXPECT_EQ(timestamp, 1u);

    timestamp = mergedDigest->CalculateWritesSimilarityTimestamp(similarityConfig, EMinHashWriteSimilarityMode::Auxiliary);
    EXPECT_EQ(timestamp, 100u);
}

// Merging two digests where the same hash has writes in both, then computing
// write similarity via the merged auxiliary timestamps.
TEST(TMinHashDigest, MergedWritesSimilarityEndToEnd)
{
    // digest0: hash=1 written at ts=5; hash=3 written at ts=10.
    // digest1: hash=1 written at ts=50; hash=3 written at ts=100; hash=5 written at ts=200.
    auto digest0 = MakeDigest(
        MakeWriteMinHash(5, {{1, 5}, {3, 10}}),
        MakeDeleteMinHash(5, {}));
    auto digest1 = MakeDigest(
        MakeWriteMinHash(5, {{1, 50}, {3, 100}, {5, 200}}),
        MakeDeleteMinHash(5, {}));

    auto merged = TMinHashDigest::Merge(digest0, digest1);

    // After merge, hash=1 → primary=5, aux=50; hash=3 → primary=10, aux=100; hash=5 → primary=200, aux=0.
    // MinSimilarity=0.5, MinRowCount=3: comparisonSize=3, ceil(3*0.5)=2.
    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 3);

    // Primary mode: intersectionTimestamps = [5, 10] (items with aux != 0), need 2nd smallest = 10.
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 10u);

    // Auxiliary mode: intersectionTimestamps = [50, 100], need 2nd smallest = 100.
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary), 100u);
}

// Merging two digests with write-delete similarity check after merge.
TEST(TMinHashDigest, MergedWriteDeleteSimilarityEndToEnd)
{
    // digest0: writes for hashes {1, 3, 5}; no deletes.
    // digest1: deletes for hashes {1, 3, 7}; no writes.
    // After merge, write min hash = {1→ts=10, 3→ts=20, 5→ts=30},
    //              delete min hash = {1→ts=15, 3→ts=25, 7→ts=100}.
    auto digest0 = MakeDigest(
        MakeWriteMinHash(5, {{1, 10}, {3, 20}, {5, 30}}),
        MakeDeleteMinHash(5, {}));
    auto digest1 = MakeDigest(
        MakeWriteMinHash(5, {}),
        MakeDeleteMinHash(5, {{1, 15}, {3, 25}, {7, 100}}));

    auto merged = TMinHashDigest::Merge(digest0, digest1);

    // comparisonPrefixSize = min(3, 3) = 3; MinRowCount=3.
    // Two-pointer:
    //   hash=1: write(10) < delete(15) → push 15
    //   hash=3: write(20) < delete(25) → push 25
    //   hash=5 vs hash=7: 5 < 7 → writeIndex++, loop ends (writeIndex hits 3)
    // intersectionTimestamps=[15, 25], sufficientIntersectionSize=ceil(3*0.5)=2, index=1 → 25.
    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 3);
    EXPECT_EQ(merged->CalculateWriteDeleteSimilarityTimestamp(config), 25u);
}

////////////////////////////////////////////////////////////////////////////////
// Multi-digest tests (10+ digests)
////////////////////////////////////////////////////////////////////////////////

// 10 digests, each with a single unique write hash. After merging all 10,
// no hash appears in more than one digest, so all auxiliary timestamps are 0.
// Write similarity should be 0; write-delete similarity should be 0 (no deletes).
TEST(TMinHashDigestMulti, TenDigestsUniqueHashes)
{
    constexpr int NumDigests = 10;
    constexpr int Capacity = 20;

    std::vector<TMinHashDigestPtr> digests;
    for (int i = 0; i < NumDigests; ++i) {
        TFingerprint hash = i + 1;
        ui32 ts = (i + 1) * 10;
        digests.push_back(MakeDigest(
            MakeWriteMinHash(Capacity, {{hash, ts}}),
            MakeDeleteMinHash(Capacity, {})));
    }

    auto merged = digests[0];
    for (int i = 1; i < NumDigests; ++i) {
        merged = TMinHashDigest::Merge(merged, digests[i]);
    }

    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);

    // No deletes at all → write-delete similarity = 0.
    EXPECT_EQ(merged->CalculateWriteDeleteSimilarityTimestamp(config), 0u);

    // All hashes unique across digests → no auxiliary timestamps → writes similarity = 0.
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 0u);
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary), 0u);
}

// 10 digests all writing the SAME hash with increasing timestamps.
// After sequential merging, the single item should have:
//   primary = smallest timestamp (from digest 0),
//   auxiliary = second-smallest timestamp (from digest 1),
// because each merge replaces aux with min(current_aux, discarded_primary).
TEST(TMinHashDigestMulti, TenDigestsSameHash)
{
    constexpr int NumDigests = 10;
    constexpr int Capacity = 5;

    std::vector<TMinHashDigestPtr> digests;
    for (int i = 0; i < NumDigests; ++i) {
        ui32 ts = (i + 1) * 100; // 100, 200, ..., 1000
        digests.push_back(MakeDigest(
            MakeWriteMinHash(Capacity, {{42, ts}}),
            MakeDeleteMinHash(Capacity, {})));
    }

    auto merged = digests[0];
    for (int i = 1; i < NumDigests; ++i) {
        merged = TMinHashDigest::Merge(merged, digests[i]);
    }

    // After merging digest0 (ts=100) with digest1 (ts=200): primary=100, aux=200.
    // Merging with digest2 (ts=300): discarded=300, aux=min(200,300)=200. Still aux=200.
    // All subsequent merges have larger timestamps, so aux stays at 200.
    auto config = MakeSimilarityConfig(0.1, /*minRowCount*/ 1);

    // Primary mode: only 1 item with aux!=0, intersectionTimestamps=[100], need ceil(1*0.1)=1 → 100.
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 100u);

    // Auxiliary mode: intersectionTimestamps=[200], need 1 → 200.
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary), 200u);
}

// 10 digests: odd-numbered digests have writes, even-numbered have deletes for the same hashes.
// Simulates a pattern where data is written in some chunks and deleted in others.
// After merging, write-delete similarity should detect the overlap.
TEST(TMinHashDigestMulti, TenDigestsAlternatingWritesAndDeletes)
{
    constexpr int Capacity = 10;

    // Hashes 1..5 appear in all digests.
    // Even digests (0, 2, 4, 6, 8) have writes at timestamps 10*digestIndex + hash.
    // Odd digests (1, 3, 5, 7, 9) have deletes at timestamps 10*digestIndex + hash + 1000.
    std::vector<TMinHashDigestPtr> digests;
    for (int i = 0; i < 10; ++i) {
        std::vector<std::pair<TFingerprint, ui32>> writeItems;
        std::vector<std::pair<TFingerprint, ui32>> deleteItems;

        for (int h = 1; h <= 5; ++h) {
            if (i % 2 == 0) {
                // Write digest: timestamp = 10*i + h (small values).
                writeItems.push_back({h, 10 * i + h});
            } else {
                // Delete digest: timestamp = 10*i + h + 1000 (large values, always after writes).
                deleteItems.push_back({h, 10 * i + h + 1000});
            }
        }

        digests.push_back(MakeDigest(
            MakeWriteMinHash(Capacity, writeItems),
            MakeDeleteMinHash(Capacity, deleteItems)));
    }

    auto merged = digests[0];
    for (int i = 1; i < 10; ++i) {
        merged = TMinHashDigest::Merge(merged, digests[i]);
    }

    // After merging:
    // Write min hash: for each hash h, primary = min over even digests of (10*i+h).
    //   Digest 0: ts = 0*10+h = h → primary = h for each hash.
    //   After merging digest 0 (primary=h) with digest 2 (primary=20+h): aux = 20+h.
    //   Subsequent merges have larger primaries, so aux stays at 20+h.
    //
    // Delete min hash: for each hash h, primary = max over odd digests of (10*i+h+1000).
    //   Digest 9: ts = 9*10+h+1000 = 1090+h → primary = 1090+h for each hash.
    //
    // Write-delete similarity: all 5 hashes match, write.ts < delete.ts for all.
    // comparisonPrefixSize = min(5, 5) = 5.
    // sufficientIntersectionSize = ceil(5*0.5) = 3, index = 2.
    // intersectionTimestamps = [1091, 1092, 1093, 1094, 1095] (delete primary timestamps).
    // 3rd smallest (index 2) = 1093.
    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);
    auto timestamp = merged->CalculateWriteDeleteSimilarityTimestamp(config);
    EXPECT_EQ(timestamp, 1093u);

    // Writes similarity: hashes appear in multiple write digests (0, 2, 4, 6, 8),
    // so auxiliary timestamps are set. Primary timestamps = [1, 2, 3, 4, 5].
    // sufficientIntersectionSize = 3, index = 2. 3rd smallest = 3.
    auto writeTimestamp = merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary);
    EXPECT_EQ(writeTimestamp, 3u);

    writeTimestamp = merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary);
    EXPECT_EQ(writeTimestamp, 23u);
}

// 10 digests with overlapping hash ranges: digest i writes hashes [i*3, i*3+4].
// This creates partial overlap between adjacent digests.
// After merging, hashes that appear in multiple digests get auxiliary timestamps.
TEST(TMinHashDigestMulti, TenDigestsOverlappingRanges)
{
    constexpr int Capacity = 50; // Large enough to hold all unique hashes.

    std::vector<TMinHashDigestPtr> digests;
    for (int i = 0; i < 10; ++i) {
        std::vector<std::pair<TFingerprint, ui32>> writeItems;
        for (int j = 0; j < 5; ++j) {
            writeItems.push_back({i * 3 + j, i * 100 + j}); // Unique timestamp per (digest, hash).
        }
        digests.push_back(MakeDigest(
            MakeWriteMinHash(Capacity, writeItems),
            MakeDeleteMinHash(Capacity, {})));
    }

    auto merged = digests[0];
    for (int i = 1; i < 10; ++i) {
        merged = TMinHashDigest::Merge(merged, digests[i]);
    }

    // Digest 0: hashes {0, 1, 2, 3, 4}
    // Digest 1: hashes {3, 4, 5, 6, 7}     → overlaps with digest 0 on {3, 4}
    // Digest 2: hashes {6, 7, 8, 9, 10}    → overlaps with digest 1 on {6, 7}
    // ...
    // Each adjacent pair shares 2 hashes. Total unique hashes = 5 + 9*3 = 32.
    // Hashes with aux timestamps: those appearing in 2+ digests.
    // Overlapping hashes: 3, 4 (digests 0, 1), 6, 7 (digests 1, 2), 9, 10 (digests 2, 3), ...
    // That's 2 overlapping hashes per adjacent pair = 2*9 = 18 hashes with aux.
    //
    // config: minSimilarity=0.3, minRowCount=10.
    // comparisonPrefixSize = min(32, 32) = 32 (capacity=50, all hashes fit).
    // sufficientIntersectionSize = ceil(32*0.3) = 10, index = 9.
    //
    // Hash i*3+j gets timestamp i*100+j. Overlapping hashes and their primary timestamps
    // (min of the two digests' timestamps):
    //   hash 3: min(0*100+3, 1*100+0) = min(3, 100) = 3
    //   hash 4: min(0*100+4, 1*100+1) = min(4, 101) = 4
    //   hash 6: min(1*100+3, 2*100+0) = min(103, 200) = 103
    //   hash 7: min(1*100+4, 2*100+1) = min(104, 201) = 104
    //   ...pattern: primary timestamps = 3, 4, 103, 104, 203, 204, 303, 304, 403, 404, ...
    // 10th smallest primary (index 9) = 404.
    //
    // Aux timestamps (the discarded/larger timestamp for each overlapping hash):
    //   hash 3→100, hash 4→101, hash 6→200, hash 7→201, ..., hash 15→500, hash 16→501.
    // 10th smallest aux (index 9) = 501.
    auto config = MakeSimilarityConfig(0.3, /*minRowCount*/ 10);
    auto writeTimestamp = merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary);
    EXPECT_EQ(writeTimestamp, 404u);

    auto auxTimestamp = merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary);
    EXPECT_EQ(auxTimestamp, 501u);
}

// 10 digests where writes and deletes are interleaved per hash.
// Hash h is written in digest h and deleted in digest h+1.
// This creates a chain: write(h) at ts=h*10, delete(h) at ts=h*10+50.
TEST(TMinHashDigestMulti, TenDigestsChainedWriteDelete)
{
    constexpr int Capacity = 15;

    std::vector<TMinHashDigestPtr> digests;
    for (int i = 0; i < 10; ++i) {
        std::vector<std::pair<TFingerprint, ui32>> writeItems;
        std::vector<std::pair<TFingerprint, ui32>> deleteItems;

        // Digest i writes hash=i at ts=i*10.
        writeItems.push_back({i, i * 10});

        // Digest i deletes hash=i-1 at ts=(i-1)*10+50 (if i > 0).
        if (i > 0) {
            deleteItems.push_back({i - 1, (i - 1) * 10 + 50});
        }

        digests.push_back(MakeDigest(
            MakeWriteMinHash(Capacity, writeItems),
            MakeDeleteMinHash(Capacity, deleteItems)));
    }

    auto merged = digests[0];
    for (int i = 1; i < 10; ++i) {
        merged = TMinHashDigest::Merge(merged, digests[i]);
    }

    // After merging:
    // Write hashes: {0, 1, 2, ..., 9} with timestamps {0, 10, 20, ..., 90}.
    // Delete hashes: {0, 1, 2, ..., 8} with timestamps {50, 60, 70, ..., 130}.
    // comparisonPrefixSize = min(10, 9) = 9.
    // For each hash h in [0...8]: write.ts = h*10, delete.ts = h*10+50.
    // write.ts < delete.ts always → all 9 hashes intersect.
    // intersectionTimestamps = [50, 60, 70, 80, 90, 100, 110, 120, 130].
    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 9);
    // sufficientIntersectionSize = ceil(9 * 0.5) = 5, index = 4.
    // 5th smallest of [50, 60, 70, 80, 90, 100, 110, 120, 130] = 90.
    EXPECT_EQ(merged->CalculateWriteDeleteSimilarityTimestamp(config), 90u);
}

// 12 digests: first 6 write the same set of hashes, last 6 delete them.
// All writes happen before all deletes. High overlap → high similarity.
TEST(TMinHashDigestMulti, TwelveDigestsFullOverlapWriteThenDelete)
{
    constexpr int Capacity = 10;
    constexpr int NumHashes = 8;

    std::vector<TMinHashDigestPtr> digests;

    // Digests 0-5: writes for hashes 1..8.
    for (int i = 0; i < 6; ++i) {
        std::vector<std::pair<TFingerprint, ui32>> writeItems;
        for (int h = 1; h <= NumHashes; ++h) {
            writeItems.push_back({h, i * 10 + h}); // ts in [1..58]
        }
        digests.push_back(MakeDigest(
            MakeWriteMinHash(Capacity, writeItems),
            MakeDeleteMinHash(Capacity, {})));
    }

    // Digests 6-11: deletes for hashes 1..8.
    for (int i = 6; i < 12; ++i) {
        std::vector<std::pair<TFingerprint, ui32>> deleteItems;
        for (int h = 1; h <= NumHashes; ++h) {
            deleteItems.push_back({h, i * 100 + h}); // ts in [601..1108], always > writes
        }
        digests.push_back(MakeDigest(
            MakeWriteMinHash(Capacity, {}),
            MakeDeleteMinHash(Capacity, deleteItems)));
    }

    auto merged = digests[0];
    for (int i = 1; i < ssize(digests); ++i) {
        merged = TMinHashDigest::Merge(merged, digests[i]);
    }

    // Write min hash: for each hash h, primary = min timestamp across write digests.
    //   Digest 0: ts = 0*10+h = h → primary = h.
    // Delete min hash: for each hash h, primary = max timestamp across delete digests.
    //   Digest 11: ts = 11*100+h = 1100+h → primary = 1100+h.
    // All 8 hashes intersect (write.ts < delete.ts).
    // comparisonPrefixSize = min(8, 8) = 8.
    // sufficientIntersectionSize = ceil(8*0.5) = 4, index = 3.
    // intersectionTimestamps = [1101, 1102, 1103, 1104, 1105, 1106, 1107, 1108].
    // 4th smallest (index 3) = 1104.
    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 8);
    auto timestamp = merged->CalculateWriteDeleteSimilarityTimestamp(config);
    EXPECT_EQ(timestamp, 1104u);

    // Writes similarity: each hash appears in 6 write digests → aux timestamps set.
    // Primary timestamps = [1, 2, 3, 4, 5, 6, 7, 8]. 4th smallest (index 3) = 4.
    // Aux timestamps: after merging digest 0 (primary=h) with digest 1 (primary=10+h),
    //   aux = 10+h. Subsequent merges have larger primaries, so aux stays at 10+h.
    // Aux timestamps = [11, 12, 13, 14, 15, 16, 17, 18]. 4th smallest (index 3) = 14.
    auto writesConfig = MakeSimilarityConfig(0.5, /*minRowCount*/ 8);
    auto writeTimestamp = merged->CalculateWritesSimilarityTimestamp(writesConfig, EMinHashWriteSimilarityMode::Primary);
    EXPECT_EQ(writeTimestamp, 4u);

    auto auxTimestamp = merged->CalculateWritesSimilarityTimestamp(writesConfig, EMinHashWriteSimilarityMode::Auxiliary);
    EXPECT_EQ(auxTimestamp, 14u);
}

// 10 digests with capacity=5. Each digest has 5 hashes, but after merging
// the capacity constraint limits the result to 5 items total.
TEST(TMinHashDigestMulti, TenDigestsCapacityConstraint)
{
    constexpr int Capacity = 5;

    std::vector<TMinHashDigestPtr> digests;
    for (int i = 0; i < 10; ++i) {
        std::vector<std::pair<TFingerprint, ui32>> writeItems;
        for (int j = 0; j < 5; ++j) {
            // Each digest has unique hashes: digest i has hashes [i*5, i*5+4].
            writeItems.push_back({i * 5 + j, i * 10 + j});
        }
        digests.push_back(MakeDigest(
            MakeWriteMinHash(Capacity, writeItems),
            MakeDeleteMinHash(Capacity, {})));
    }

    auto merged = digests[0];
    for (int i = 1; i < 10; ++i) {
        merged = TMinHashDigest::Merge(merged, digests[i]);
    }

    // After merging with capacity=5, only the 5 smallest hashes survive: {0,1,2,3,4}.
    // These all come from digest 0, so no auxiliary timestamps.
    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 0u);
}

// 10 digests where each digest writes the same 5 hashes but with different timestamps.
// After merging, all 5 hashes should have auxiliary timestamps.
// Verify exact primary and auxiliary timestamps after the full merge chain.
TEST(TMinHashDigestMulti, TenDigestsSameHashesExactTimestamps)
{
    constexpr int Capacity = 5;

    std::vector<TMinHashDigestPtr> digests;
    for (int i = 0; i < 10; ++i) {
        std::vector<std::pair<TFingerprint, ui32>> writeItems;
        for (int h = 1; h <= 5; ++h) {
            // Digest i writes hash h at timestamp = (i+1)*100 + h.
            // Digest 0: ts = 101,102,103,104,105
            // Digest 1: ts = 201,202,203,204,205
            // ...
            // Digest 9: ts = 1001,1002,1003,1004,1005
            writeItems.push_back({h, (i + 1) * 100 + h});
        }
        digests.push_back(MakeDigest(
            MakeWriteMinHash(Capacity, writeItems),
            MakeDeleteMinHash(Capacity, {})));
    }

    auto merged = digests[0];
    for (int i = 1; i < 10; ++i) {
        merged = TMinHashDigest::Merge(merged, digests[i]);
    }

    // For each hash h:
    //   Primary = min timestamp = digest0's ts = 100 + h.
    //   Auxiliary = second-smallest = digest1's ts = 200 + h.
    //   (Each subsequent merge has a larger discarded primary, so min(aux, discarded) stays at 200+h.)

    // Writes similarity with MinSimilarity=0.5, MinRowCount=5:
    // comparisonSize = 5, sufficientIntersectionSize = ceil(5*0.5) = 3.
    // All 5 items have aux != 0, so intersectionTimestamps has 5 entries.
    auto config = MakeSimilarityConfig(0.5, /*minRowCount*/ 5);

    // Primary mode: intersectionTimestamps = [101, 102, 103, 104, 105].
    // 3rd smallest (index 2) = 103.
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Primary), 103u);

    // Auxiliary mode: intersectionTimestamps = [201, 202, 203, 204, 205].
    // 3rd smallest (index 2) = 203.
    EXPECT_EQ(merged->CalculateWritesSimilarityTimestamp(config, EMinHashWriteSimilarityMode::Auxiliary), 203u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
