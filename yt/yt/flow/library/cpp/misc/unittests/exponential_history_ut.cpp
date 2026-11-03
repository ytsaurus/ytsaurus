#include <yt/yt/flow/library/cpp/misc/exponential_history.h>

#include <yt/yt/core/test_framework/framework.h>

#include <util/datetime/base.h>

#include <cmath>
#include <random>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TItem
{
    ui64 Time = 0;
    ui64 FirstTime = 0;
    int Count = 1;
};

void Append(std::vector<TItem>* history, ui64 time, int thinningFactor)
{
    AppendToExponentialHistory(
        history,
        TItem{.Time = time, .FirstTime = time},
        [] (const TItem& item) {
            return item.Time;
        },
        [] (const TItem& older, const TItem& newer) {
            return TItem{.Time = newer.Time, .FirstTime = older.FirstTime, .Count = older.Count + newer.Count};
        },
        thinningFactor);
}

//! Checks against the definition: every merged element spans at most 1/K of its age, and the elements
//! partition the appended times.
void ExpectThinned(const std::vector<TItem>& history, const std::vector<ui64>& appended, int thinningFactor)
{
    ui64 now = appended.back();
    int totalCount = 0;
    int appendedIndex = 0;
    for (int index = 0; index < std::ssize(history); ++index) {
        const auto& item = history[index];
        totalCount += item.Count;
        EXPECT_EQ(item.FirstTime, appended[appendedIndex]);
        appendedIndex += item.Count;
        EXPECT_EQ(item.Time, appended[appendedIndex - 1]);

        if (index > 0 && item.Count > 1) {
            ui64 span = item.Time - history[index - 1].Time;
            EXPECT_LE(span * thinningFactor, now - item.Time);
        }
    }

    EXPECT_EQ(totalCount, std::ssize(appended));
}

TEST(TExponentialHistoryTest, MergedSpansAreBoundedByAge)
{
    std::mt19937 random(42);
    for (int thinningFactor : {1, 2, 4}) {
        std::vector<TItem> history;
        std::vector<ui64> appended;
        ui64 time = 1000;
        for (int index = 0; index < 3000; ++index) {
            time += 1 + random() % 20;

            if (random() % 100 == 0) {
                time += random() % 5000;
            }

            appended.push_back(time);
            Append(&history, time, thinningFactor);

            if (index % 97 == 0) {
                ExpectThinned(history, appended, thinningFactor);
            }
        }

        ExpectThinned(history, appended, thinningFactor);
    }
}

TEST(TExponentialHistoryTest, SizeIsLogarithmic)
{
    constexpr ui64 Interval = 5;
    constexpr int SampleCount = 200'000;
    for (int thinningFactor : {2, 4}) {
        std::vector<TItem> history;
        for (int index = 0; index < SampleCount; ++index) {
            Append(&history, 1000 + index * Interval, thinningFactor);
        }

        double covered = static_cast<double>(history.back().Time - history.front().Time) / Interval;
        int bound = (2 * thinningFactor + 2) * static_cast<int>(std::ceil(std::log2(covered))) + 4 * thinningFactor;
        EXPECT_LE(std::ssize(history), bound) << "thinningFactor = " << thinningFactor;
    }
}

TEST(TExponentialHistoryTest, LongGapStaysUnmergedUntilKTimesOlder)
{
    constexpr int ThinningFactor = 4;
    constexpr ui64 Interval = 5;
    constexpr ui64 Gap = 3600;
    std::vector<TItem> history;
    ui64 time = 1000;
    for (int index = 0; index < 200; ++index) {
        Append(&history, time, ThinningFactor);
        time += Interval;
    }

    const ui64 postGapTime = time - Interval + Gap;
    time = postGapTime;
    auto findPostGap = [&] {
        return std::find_if(history.begin(), history.end(), [&] (const TItem& item) {
            return item.Time >= postGapTime;
        });
    };
    while (time - postGapTime < ThinningFactor * Gap) {
        Append(&history, time, ThinningFactor);
        auto it = findPostGap();
        ASSERT_NE(it, history.end());
        EXPECT_EQ(it->FirstTime, postGapTime) << "time = " << time;
        time += Interval;
    }

    for (int index = 0; index < 4000; ++index) {
        Append(&history, time, ThinningFactor);
        time += Interval;
    }

    EXPECT_LT(findPostGap()->FirstTime, postGapTime);
}

TEST(TExponentialHistoryTest, AnchorIsNeverMerged)
{
    std::vector<TItem> history;
    for (int index = 0; index < 10'000; ++index) {
        Append(&history, 1000 + 7 * static_cast<ui64>(index), /*thinningFactor*/ 1);
    }

    ASSERT_GE(std::ssize(history), 2);
    EXPECT_EQ(history[0].Time, 1000u);
    EXPECT_EQ(history[0].Count, 1);
    EXPECT_GT(history[1].Count, 1);
}

TEST(TExponentialHistoryTest, InstantTime)
{
    std::vector<TInstant> history;
    for (int index = 0; index < 1000; ++index) {
        AppendToExponentialHistory(
            &history,
            TInstant::Seconds(1000 + index),
            [] (TInstant instant) {
                return instant;
            },
            [] (TInstant /*older*/, TInstant newer) {
                return newer;
            },
            /*thinningFactor*/ 4);
    }

    EXPECT_EQ(history.front(), TInstant::Seconds(1000));
    EXPECT_EQ(history.back(), TInstant::Seconds(1999));
    EXPECT_LT(std::ssize(history), 100);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
