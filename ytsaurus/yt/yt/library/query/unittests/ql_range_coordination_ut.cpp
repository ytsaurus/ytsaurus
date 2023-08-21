#include <yt/yt/core/test_framework/framework.h>
#include "ql_helpers.h"

#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/core/misc/range_formatters.h>

#include <util/random/fast.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Test MergeOverlappingRanges
// TODO(lukyan): Test for ranges, partial keys (ranges) and full keys (points)

struct TMyPartition
{
    TRow PivotKey;
    TRow NextPivotKey;
    std::vector<TRow> SampleKeys;
};

struct TMyTablet
{
    TRow PivotKey;
    TRow NextPivotKey;
    std::vector<TMyPartition> Partitions;
};

template <class TIter>
void FillRandomUniqueSequence(TFastRng64& rng, TIter begin, TIter end, size_t min, size_t max)
{
    // TODO: Use linear congruential generator without materialization of sequence.

    YT_VERIFY(end - begin <= static_cast<ssize_t>(max - min));

    if (begin == end) {
        return;
    }

    YT_VERIFY(min < max);

    TIter current = begin;

    do {
        while (current < end) {
            *current++ = rng.Uniform(min, max);
        }

        std::sort(begin, end);
        current = std::unique(begin, end);
    } while (current < end);
}

TSharedRange<size_t> GetRandomUniqueSequence(TFastRng64& rng, size_t length, size_t min, size_t max)
{
    std::vector<ui64> sequence(length);
    FillRandomUniqueSequence(rng, sequence.begin(), sequence.end(), min, max);
    return MakeSharedRange(std::move(sequence));
}

template <class T>
bool CheckRangesAreEquivalent(TRange<std::pair<T, T>> a, TRange<std::pair<T, T>> b)
{
    auto it1 = a.begin();
    auto it2 = b.begin();

    while (true) {
        bool end1 = it1 == a.end();
        bool end2 = it2 == b.end();

        if (end1 || end2) {
            return end1 && end2;
        }

        if (it1->first != it2->first) {
            return false;
        }

        while (true) {
            if (it1->second < it2->second) {
                auto lastBound = it1->second;
                if (++it1 == a.end() || it1->first != lastBound) {
                    return false;
                }
            } else if (it2->second < it1->second) {
                auto lastBound = it2->second;
                if (++it2 == b.end() || it2->first != lastBound) {
                    return false;
                }
            } else {
                ++it1;
                ++it2;
                break;
            }
        }
    }

    Y_UNREACHABLE();
}

TEST(TestHelpers, Step)
{
    auto step = [&] (size_t c, size_t s, size_t t) -> ssize_t {
        auto s1 = ((c * t / s + 1) * s + t - 1) / t - c;
        auto s2 = (s + t - 1 - c * t % s) / t;
        auto s3 = Step(c, s, t);

        YT_VERIFY(s1 == s2);
        YT_VERIFY(s2 == s3);
        return s1;
    };

    EXPECT_EQ(step(0, 4, 3), 2);
    EXPECT_EQ(step(1, 4, 3), 1);
    EXPECT_EQ(step(2, 4, 3), 1);
    EXPECT_EQ(step(3, 4, 3), 1);

    // [0 .. 3] -> [0 .. 3]

    for (size_t s = 1; s < 100; ++s) {
        for (size_t t = 1; t <= s; ++t) {
            for (size_t c = 0; c < s; ++c) {
                auto next = step(c, s, t) + c;

                EXPECT_EQ(next * t / s, c * t / s + 1);
            }
        }
    }
}

void TestSplitTablet(
    TRange<size_t> sampleValues,
    TRange<std::pair<size_t, size_t>> rangeValues,
    size_t maxSubsplitsPerTablet)
{
    auto rowBuffer = New<TRowBuffer>();
    auto makeRow = [&] (int value) {
        auto row = rowBuffer->AllocateUnversioned(1);
        row[0] = MakeInt64(value);
        return row;
    };
    auto makeRange = [&] (int a, int b) {
        return TRowRange(makeRow(a), makeRow(b));
    };

    std::vector<TMyPartition> partitions;
    auto& partition = partitions.emplace_back();
    partition.PivotKey = makeRow(0);
    partition.NextPivotKey = makeRow(110);

    for (auto value : sampleValues) {
        partition.SampleKeys.push_back(makeRow(value)); // Sample key can be equal to PivotKey
    }

    std::vector<TRowRange> ranges;
    for (auto [l, r] : rangeValues) {
        ranges.push_back(makeRange(l, r));
    }

    NLogging::TLogger logger;

    auto result = SplitTablet(
        MakeRange(partitions),
        MakeSharedRange(ranges),
        rowBuffer,
        maxSubsplitsPerTablet,
        true,
        logger);

    std::vector<TRowRange> mergedRanges;
    for (const auto& group : result) {
        mergedRanges.insert(mergedRanges.end(), group.begin(), group.end());
    }

    if (!CheckRangesAreEquivalent(MakeRange(ranges), MakeRange(mergedRanges))) {
        Cerr << Format("Source: %v, Samples: %v, MaxSubsplits: %v",
            MakeFormattableView(ranges, TRangeFormatter()),
            partition.SampleKeys,
            maxSubsplitsPerTablet) << Endl;
        Cerr << Format("Merged: %v", MakeFormattableView(mergedRanges, TRangeFormatter())) << Endl;

        for (const auto& group : result) {
            Cerr << Format("Group: %v", MakeFormattableView(group, TRangeFormatter())) << Endl;
        }

        GTEST_FAIL() << "Expected ranges are equivalent";
    }
}

TEST(TestHelpers, TestSplitTablet)
{
    {
        std::pair<size_t, size_t> ranges[] = {{17, 42}, {47, 60}, {64, 75}};
        size_t samples[] = {27, 36, 52, 57, 60, 70, 74, 85, 90};

        TestSplitTablet(samples, ranges, 6);
    }

    {
        std::pair<size_t, size_t> ranges[] = {{6, 17}, {21, 24}, {35, 58}, {68, 79}, {85, 88}};
        size_t samples[] = {5, 22, 23, 46, 49, 99, 100};

        TestSplitTablet(samples, ranges, 64);
    }

    TFastRng64 rng(42);

    for (size_t iteration = 0; iteration < 1000; ++iteration) {
        std::vector<std::pair<size_t, size_t>> ranges;
        std::vector<size_t> samples;

        for (auto index : GetRandomUniqueSequence(rng, rng.Uniform(0, 10), 0, 100)) {
            samples.push_back(index + 10); // Sample key can be equal to PivotKey
        }

        auto bounds = GetRandomUniqueSequence(rng, rng.Uniform(2, 10), 0, 100);
        for (size_t index = 1; index < bounds.size(); index += rng.GenRand64() % 3 ? 2 : 1) {
            ranges.emplace_back(bounds[index - 1], bounds[index]);
        }

        size_t maxSubsplitsPerTablet = rng.Uniform(1, 64);

        TestSplitTablet(samples, ranges, maxSubsplitsPerTablet);
    }

    for (size_t iteration = 0; iteration < 1000; ++iteration) {
        auto rowBuffer = New<TRowBuffer>();
        auto makeRow = [&] (size_t value) {
            auto row = rowBuffer->AllocateUnversioned(1);
            row[0] = MakeInt64(value);
            return row;
        };
        auto makeRange = [&] (size_t a, size_t b) {
            return TRowRange(makeRow(a), makeRow(b));
        };

        // Generate tablet structure.
        std::vector<TMyTablet> tablets;
        std::vector<size_t> keys(rng.Uniform(30, 70));
        FillRandomUniqueSequence(rng, keys.begin(), keys.end(), 0, 1000);

        std::vector<size_t> tabletPivotIds(rng.Uniform(1, 5) - 1);
        FillRandomUniqueSequence(rng, tabletPivotIds.begin(), tabletPivotIds.end(), 1, keys.size() - 1);
        tabletPivotIds.push_back(keys.size() - 1);

        size_t lastPivotId = 0;
        for (auto pivotId : tabletPivotIds) {
            auto& tablet = tablets.emplace_back();

            tablet.PivotKey = makeRow(keys[lastPivotId]);
            tablet.NextPivotKey = makeRow(keys[pivotId]);

            size_t maxPartCount = std::min(pivotId - lastPivotId, keys[pivotId] - keys[lastPivotId]);
            YT_VERIFY(maxPartCount > 0);

            std::vector<size_t> partPivotIds(rng.Uniform(0, maxPartCount));
            FillRandomUniqueSequence(rng, partPivotIds.begin(), partPivotIds.end(), lastPivotId + 1, pivotId);
            partPivotIds.push_back(pivotId);

            auto lastPartPivotId = lastPivotId;
            for (auto partPivotId : partPivotIds) {
                auto& partition = tablet.Partitions.emplace_back();
                partition.PivotKey = makeRow(keys[lastPartPivotId]);
                partition.NextPivotKey = makeRow(keys[partPivotId]);

                bool firstSampleIsPivot = false;

                for (size_t sampleId = lastPartPivotId + !firstSampleIsPivot; sampleId < partPivotId; ++sampleId) {
                    partition.SampleKeys.push_back(makeRow(keys[sampleId]));
                }

                lastPartPivotId = partPivotId;
            }

            YT_VERIFY(lastPartPivotId == pivotId);
            lastPivotId = pivotId;
        }

        YT_VERIFY(lastPivotId + 1 == keys.size());

        // Generate ranges.
        std::vector<size_t> bounds(std::min(rng.Uniform(1, 30), keys.back() - keys.front()));
        FillRandomUniqueSequence(rng, bounds.begin(), bounds.end(), keys.front(), keys.back());

        std::vector<TRowRange> ranges;
        for (size_t index = 1; index < bounds.size(); index += rng.GenRand64() % 3 ? 2 : 1) {
            ranges.push_back(makeRange(bounds[index - 1], bounds[index]));
        }

        size_t maxSubsplitsPerTablet = rng.Uniform(1, 100);

        NLogging::TLogger logger;
        std::vector<TRowRange> mergedRanges;

        SplitRangesByTablets(
            MakeRange(ranges),
            MakeRange(tablets),
            makeRow(0),
            makeRow(1000),
            [&] (auto shardIt, auto rangesIt, auto rangesItEnd) {

                const auto& tablet = *(shardIt - 1);

                std::vector<TRowRange> rangesSlice(rangesIt, rangesItEnd);

                auto groups = SplitTablet(
                    MakeRange(tablet.Partitions),
                    MakeSharedRange(rangesSlice),
                    rowBuffer,
                    maxSubsplitsPerTablet,
                    true,
                    logger);

                for (const auto& group : groups) {
                    mergedRanges.insert(mergedRanges.end(), group.begin(), group.end());
                }
            });

        if (!CheckRangesAreEquivalent(MakeRange(ranges), MakeRange(mergedRanges))) {
            Cerr << Format("Ranges: %v", MakeFormattableView(ranges, TRangeFormatter())) << Endl;
            Cerr << Format("Merged: %v", MakeFormattableView(mergedRanges, TRangeFormatter())) << Endl;

            Cerr << Format("Tablets: %v", MakeFormattableView(tablets, [] (auto* builder, const auto& tablet) {
                builder->AppendFormat("[%v .. %v]",
                    tablet.PivotKey,
                    tablet.NextPivotKey);
            })) << Endl;

            for (const auto& tablet : tablets) {
                Cerr << Format("Tablet [%v..%v] \t %v",
                    tablet.PivotKey,
                    tablet.NextPivotKey,
                    MakeFormattableView(tablet.Partitions, [] (auto* builder, const auto& partition) {
                        builder->AppendFormat("[%v .. %v] : %v",
                            partition.PivotKey,
                            partition.NextPivotKey,
                            partition.SampleKeys);
                    })) << Endl;
            }

            GTEST_FAIL() << "Expected ranges are equivalent";
        }
    }
}

TEST(TestHelpers, SplitByPivots)
{
    using TItemIt = const std::pair<int, int>*;
    using TShardIt = const int*;

    struct TPredicate
    {
        // itemIt PRECEDES shardIt
        bool operator() (TItemIt itemIt, TShardIt shardIt) const
        {
            return itemIt->second <= *shardIt;
        }

        // itemIt FOLLOWS shardIt
        bool operator() (TShardIt shardIt, TItemIt itemIt) const
        {
            return *shardIt <= itemIt->first;
        }

    };

    TFastRng64 rng(42);
    for (size_t iteration = 0; iteration < 1000; ++iteration) {
        // Generate ranges.
        std::vector<int> bounds(rng.Uniform(1, 30));
        FillRandomUniqueSequence(rng, bounds.begin(), bounds.end(), 0, 100);

        std::vector<std::pair<int, int>> ranges;
        for (size_t index = 1; index < bounds.size(); index += rng.GenRand64() % 3 ? 2 : 1) {
            ranges.emplace_back(bounds[index - 1], bounds[index]);
        }

        // Generate pivots.
        std::vector<int> pivots(rng.Uniform(1, 30));
        FillRandomUniqueSequence(rng, pivots.begin(), pivots.end(), 0, 100);

        std::vector<std::pair<int, int>> mergedRanges;

        SplitByPivots(
            MakeRange(ranges),
            MakeRange(pivots),
            TPredicate{},
            [&] (auto itemsIt, auto itemsItEnd, auto /*shardIt*/) {
                mergedRanges.insert(mergedRanges.end(), itemsIt, itemsItEnd);
            },
            [&] (auto shardIt, auto shardItEnd, auto itemsIt) {
                auto lastBound = itemsIt->first;

                for (auto it = shardIt; it != shardItEnd; ++it) {
                    mergedRanges.emplace_back(lastBound, *it);
                    lastBound = *it;
                }

                mergedRanges.emplace_back(lastBound, itemsIt->second);
            });

        if (!CheckRangesAreEquivalent(MakeRange(ranges), MakeRange(mergedRanges))) {
            struct TPairFormatter
            {
                void operator()(TStringBuilderBase* builder, std::pair<int, int> source) const
                {
                    builder->AppendFormat("[%v .. %v]",
                        source.first,
                        source.second);
                }
            };

            Cerr << Format("Ranges: %v", MakeFormattableView(ranges, TPairFormatter())) << Endl;
            Cerr << Format("Merged: %v", MakeFormattableView(mergedRanges, TPairFormatter())) << Endl;

            GTEST_FAIL() << "Expected ranges are equivalent";
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <>
[[maybe_unused]] TRow GetPivotKey(const TMyTablet& shard)
{
    return shard.PivotKey;
}

template <>
[[maybe_unused]] TRow GetPivotKey(const TMyPartition& shard)
{
    return shard.PivotKey;
}

template <>
[[maybe_unused]] TRow GetNextPivotKey(const TMyPartition& shard)
{
    return shard.NextPivotKey;
}

template <>
[[maybe_unused]] TRange<TRow> GetSampleKeys(const TMyPartition& shard)
{
    return shard.SampleKeys;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
