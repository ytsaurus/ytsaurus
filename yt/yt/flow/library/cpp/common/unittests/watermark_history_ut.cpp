#include <yt/yt/flow/library/cpp/common/trigger_condition.h>
#include <yt/yt/flow/library/cpp/common/watermark_history.h>

#include <yt/yt/core/test_framework/framework.h>

#include <random>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int ThinningFactor = 4;

std::optional<TSystemTimestamp> Reached(ui64 instant)
{
    return TSystemTimestamp(instant);
}

const std::optional<TSystemTimestamp> NotReached = InfinitySystemTimestamp;
const std::optional<TSystemTimestamp> BeforeHistory = std::nullopt;

void Append(TWatermarkHistory* history, ui64 instant, ui64 watermark)
{
    history->Append(TSystemTimestamp(instant), TSystemTimestamp(watermark), ThinningFactor);
}

std::optional<TSystemTimestamp> Find(const TWatermarkHistory& history, ui64 watermark)
{
    return history.FindReachInstant(TSystemTimestamp(watermark));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWatermarkHistoryTest, SingleSamplesAreExact)
{
    TWatermarkHistory history;
    EXPECT_EQ(Find(history, /*watermark*/ 1), BeforeHistory);

    Append(&history, /*instant*/ 100, /*watermark*/ 10);
    Append(&history, /*instant*/ 110, /*watermark*/ 20);
    Append(&history, /*instant*/ 120, /*watermark*/ 30);

    EXPECT_EQ(Find(history, /*watermark*/ 5), BeforeHistory);
    EXPECT_EQ(Find(history, /*watermark*/ 10), BeforeHistory);
    EXPECT_EQ(Find(history, /*watermark*/ 11), Reached(110));
    EXPECT_EQ(Find(history, /*watermark*/ 20), Reached(110));
    EXPECT_EQ(Find(history, /*watermark*/ 25), Reached(120));
    EXPECT_EQ(Find(history, /*watermark*/ 30), Reached(120));
    EXPECT_EQ(Find(history, /*watermark*/ 31), NotReached);
}

TEST(TWatermarkHistoryTest, IgnoresRegressingWatermarkAndNonIncreasingInstant)
{
    TWatermarkHistory history;
    Append(&history, /*instant*/ 100, /*watermark*/ 10);
    Append(&history, /*instant*/ 110, /*watermark*/ 30);
    Append(&history, /*instant*/ 120, /*watermark*/ 20);
    Append(&history, /*instant*/ 110, /*watermark*/ 40);
    Append(&history, /*instant*/ 105, /*watermark*/ 50);

    ASSERT_EQ(std::ssize(history.Elements), 2);
    EXPECT_EQ(history.Elements.back().Time, TSystemTimestamp(110));
    EXPECT_EQ(history.Elements.back().Watermark, TSystemTimestamp(30));
    EXPECT_EQ(Find(history, /*watermark*/ 25), Reached(110));
    EXPECT_EQ(Find(history, /*watermark*/ 31), NotReached);

    // A non-advancing watermark extends the time.
    Append(&history, /*instant*/ 130, /*watermark*/ 30);
    EXPECT_EQ(history.Elements.back().Time, TSystemTimestamp(130));
    EXPECT_EQ(Find(history, /*watermark*/ 25), Reached(110));
}

TEST(TWatermarkHistoryTest, RandomizedLagIsNeverUnderReported)
{
    std::mt19937 random(42);
    for (int iteration = 0; iteration < 20; ++iteration) {
        TWatermarkHistory history;
        // First instant of each watermark value, as published.
        std::vector<std::pair<ui64, ui64>> samples;
        ui64 instant = 1000;
        ui64 watermark = 500;
        for (int index = 0; index < 5000; ++index) {
            instant += 1 + random() % 20;

            if (random() % 50 == 0) {
                // A rare long gap, as after a leader downtime.
                instant += random() % 3600;
            }

            switch (random() % 4) {
                case 0:
                    break;

                case 1:
                    watermark += random() % 1000;
                    break;

                default:
                    watermark += random() % 20;
                    break;
            }

            Append(&history, instant, watermark);

            if (samples.empty() || watermark > samples.back().second) {
                samples.emplace_back(instant, watermark);
            }
        }

        const ui64 now = instant;

        const ui64 firstWatermark = samples.front().second;
        const ui64 lastWatermark = samples.back().second;
        for (int probe = 0; probe < 1000; ++probe) {
            ui64 key = firstWatermark + 1 + random() % (lastWatermark - firstWatermark + 10);
            auto reach = Find(history, key);
            auto truth = std::find_if(samples.begin(), samples.end(), [&] (const auto& sample) {
                return sample.second >= key;
            });
            if (truth == samples.end()) {
                EXPECT_EQ(reach, NotReached);
                continue;
            }

            ASSERT_TRUE(reach);
            ASSERT_NE(*reach, InfinitySystemTimestamp);
            ui64 trueInstant = truth->first;
            // The reported instant is never later than the true one, so the lag is never under-reported,
            // and it is earlier by at most 1/K of the true lag.
            ASSERT_LE(reach->Underlying(), trueInstant);
            EXPECT_LE((trueInstant - reach->Underlying()) * ThinningFactor, now - trueInstant);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TResolveTriggerReachTest
    : public ::testing::Test
{
protected:
    TPipelineWatermarkHistoryPtr History_ = New<TPipelineWatermarkHistory>();

    std::optional<TSystemTimestamp> Resolve(const TTimerTriggerCondition& condition, ui64 triggerTimestamp, ui64 now = 10'000)
    {
        return ResolveTriggerReach(condition, TSystemTimestamp(triggerTimestamp), *History_, TSystemTimestamp(now));
    }

    static TTimerTriggerCondition MakeCondition(ETimeType timeType, THashMap<TStreamId, TDuration> streamsWithDelays)
    {
        return {.TimeType = timeType, .StreamsWithDelays = std::move(streamsWithDelays)};
    }
};

TEST_F(TResolveTriggerReachTest, CurrentTimeIsDueAtTriggerTimestamp)
{
    // Watched streams and their delays do not apply to current-time timers.
    for (const auto& streamsWithDelays : {THashMap<TStreamId, TDuration>{{"a", TDuration::Seconds(10)}}, THashMap<TStreamId, TDuration>{}}) {
        auto condition = MakeCondition(ETimeType::CurrentTime, streamsWithDelays);
        EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 100, /*now*/ 130), Reached(100));
        EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 100, /*now*/ 100), Reached(100));
        EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 200, /*now*/ 115), NotReached);
        EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 0, /*now*/ 115), Reached(0));
    }
}

TEST_F(TResolveTriggerReachTest, EmptyConditionAndZeroTriggerTimestampAreReached)
{
    for (auto timeType : {ETimeType::EventTime, ETimeType::SystemTime}) {
        EXPECT_EQ(Resolve(MakeCondition(timeType, {}), /*triggerTimestamp*/ 5000), Reached(0));
        EXPECT_EQ(Resolve(MakeCondition(timeType, {{"missing", TDuration::Seconds(5)}}), /*triggerTimestamp*/ 0), Reached(0));
    }
}

TEST_F(TResolveTriggerReachTest, CombinesStreamsWithDelays)
{
    for (auto timeType : {ETimeType::EventTime, ETimeType::SystemTime}) {
        History_ = New<TPipelineWatermarkHistory>();
        auto& histories = History_->Get(timeType);
        for (int index = 0; index < 10; ++index) {
            Append(&histories["a"], /*instant*/ 1000 + 10 * index, /*watermark*/ 100 + 10 * index);
            Append(&histories["b"], /*instant*/ 1000 + 10 * index, /*watermark*/ 100 + 20 * index);
        }

        auto condition = MakeCondition(timeType, {{"a", TDuration::Seconds(5)}, {"b", TDuration::Seconds(30)}});
        // Key 140: stream a needs 145 (reached at 1050), stream b needs 170 (reached at 1040).
        EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 140), Reached(1050));
        // Stream a never reached 195.
        EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 190), NotReached);
        // A trigger timestamp plus a positive delay saturates instead of wrapping around to an early key.
        EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ InfinitySystemTimestamp.Underlying()), NotReached);

        // The other time type has no history.
        auto otherType = timeType == ETimeType::EventTime ? ETimeType::SystemTime : ETimeType::EventTime;
        EXPECT_EQ(Resolve(MakeCondition(otherType, {{"a", TDuration::Zero()}}), /*triggerTimestamp*/ 140), BeforeHistory);
    }
}

TEST_F(TResolveTriggerReachTest, MixedReachedAndBeforeHistory)
{
    for (int index = 0; index < 10; ++index) {
        Append(&History_->Event["early"], /*instant*/ 1000 + 10 * index, /*watermark*/ 100 + 10 * index);
        Append(&History_->Event["late"], /*instant*/ 1030 + 10 * index, /*watermark*/ 500 + 10 * index);
    }

    auto condition = MakeCondition(ETimeType::EventTime, {{"early", TDuration::Zero()}, {"late", TDuration::Zero()}});
    // Stream late reached 150 before its history started at 1030, so the maximum 1050 is determined.
    EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 150), Reached(1050));
    // Stream early reaches 120 at 1020, before late's history start: the maximum is undetermined.
    EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 120), BeforeHistory);
    // Every stream precedes its history.
    EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 50), BeforeHistory);
    // A stream without any history.
    auto withMissing = MakeCondition(ETimeType::EventTime, {{"early", TDuration::Zero()}, {"missing", TDuration::Zero()}});
    EXPECT_EQ(Resolve(withMissing, /*triggerTimestamp*/ 150), BeforeHistory);
    // Not reached takes precedence.
    EXPECT_EQ(Resolve(condition, /*triggerTimestamp*/ 195), NotReached);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
