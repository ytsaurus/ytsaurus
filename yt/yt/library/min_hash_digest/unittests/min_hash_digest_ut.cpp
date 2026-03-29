#include <gtest/gtest.h>

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/library/min_hash_digest/config.h>
#include <yt/yt/library/min_hash_digest/min_hash_digest.h>

namespace NYT {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void InitializeDigest(
    TMinHashDigest* digest,
    const std::vector<std::pair<TFingerprint, ui32>>& writes,
    const std::vector<std::pair<TFingerprint, ui32>>& deletes,
    int writeMaxSize = -1,
    int deleteMaxSize = -1)
{
    auto getFilledMinHash = [] <class TTimestampComparator> (
        const std::vector<std::pair<TFingerprint, ui32>>& vector,
        int maxSize)
    {
        ui32 size = maxSize == -1
            ? vector.size()
            : maxSize;

        TMinHashItems<TTimestampComparator> items;
        items.reserve(size);
        for (auto element : vector) {
            items.push_back({
                .Hash = element.first,
                .PrimaryTimestamp = element.second,
            });
        }

        return TMinHash<TTimestampComparator>(size, std::move(items));
    };

    digest->Initialize(
        getFilledMinHash.template operator()<std::less<ui32>>(writes, writeMaxSize),
        getFilledMinHash.template operator()<std::greater<ui32>>(deletes, deleteMaxSize));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMinHashDigest, SingleDigestDelete)
{
    auto digest = New<TMinHashDigest>(GetNullMemoryUsageTracker());

    InitializeDigest(
        digest.Get(),
        {{1, 10}, {3, 1}, {5, 5}, {7, 10}, {9, 9}},
        {{1, 11}, {3, 0}, {5, 6}, {6, 20}, {9, 200}});

    auto retentionConfig = New<TRetentionConfig>();
    auto similarityConfig = New<TMinHashSimilarityConfig>();

    auto timestamp = digest->CalculateWriteDeleteSimilarityTimestamp(similarityConfig);

    EXPECT_EQ(timestamp, 200u);
}

TEST(TMinHashDigest, SimpleMultiDigest)
{
    auto digest0 = New<TMinHashDigest>(/*memoryTracker*/ nullptr);
    auto digest1 = New<TMinHashDigest>(/*memoryTracker*/ nullptr);
    auto digest2 = New<TMinHashDigest>(/*memoryTracker*/ nullptr);


    // 1 - 10, 100
    // 3 -  1, 100

    InitializeDigest(
        digest0.Get(),
        {{1, 10}, {3, 1}, {5, 5}, {7, 10}, {9, 9}},
        {},
        5,
        5);
    InitializeDigest(
        digest1.Get(),
        {{1, 100}, {3, 100}, {50, 5}, {70, 10}, {90, 9}},
        {{{1, 11}, {3, 2}, {5, 6}, {700, 10}, {900, 9}}},
        5,
        5);
    InitializeDigest(
        digest2.Get(),
        {},
        {{5, 6}, {7, 11}, {9, 42}, {1000, 10}, {3000, 1}},
        5,
        5);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
