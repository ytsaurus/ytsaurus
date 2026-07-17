#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////

//! Underlying throttler stub that records every Throttle() amount and never
//! blocks; the load throttler's size estimates are observed through it.
class TRecordingThrottler
    : public IReconfigurableThroughputThrottler
{
public:
    TFuture<void> Throttle(i64 amount) override
    {
        Amounts_.push_back(amount);
        return OKFuture;
    }

    const std::vector<i64>& Amounts() const
    {
        return Amounts_;
    }

    i64 LastAmount() const
    {
        YT_VERIFY(!Amounts_.empty());
        return Amounts_.back();
    }

    bool TryAcquire(i64 /*amount*/) override
    {
        return true;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        return amount;
    }

    void Acquire(i64 /*amount*/) override
    { }

    void Release(i64 /*amount*/) override
    { }

    bool IsOverdraft() override
    {
        return false;
    }

    i64 GetQueueTotalAmount() const override
    {
        return 0;
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        return TDuration::Zero();
    }

    i64 GetAvailable() const override
    {
        return 0;
    }

    void Reconfigure(TThroughputThrottlerConfigPtr /*config*/) override
    { }

    void SetLimit(std::optional<double> /*limit*/) override
    { }

    std::optional<double> GetLimit() const override
    {
        return std::nullopt;
    }

    TDuration GetPeriod() const override
    {
        return TDuration::Zero();
    }

    TFuture<void> GetAvailableFuture() override
    {
        return OKFuture;
    }

    TThroughputThrottlerConfigPtr GetConfig() const override
    {
        return New<TThroughputThrottlerConfig>();
    }

private:
    std::vector<i64> Amounts_;
};

using TRecordingThrottlerPtr = TIntrusivePtr<TRecordingThrottler>;

//! Alpha 0.5 makes every EWMA step an exact halving, so expected charges
//! below are exact integers.
TLoadThroughputThrottlerPtr MakeThrottler(const TRecordingThrottlerPtr& recording)
{
    auto throttler = New<TLoadThroughputThrottler>(recording, NLogging::TLogger("Test"), NProfiling::TProfiler());
    auto spec = New<TLoadThroughputThrottlerSpec>();
    spec->Alpha = 0.5;
    spec->InitialRowSize = 1'000;
    spec->InitialKeySize = 500;
    throttler->Reconfigure(std::move(spec));
    return throttler;
}

////////////////////////////////////////////////////////////////////////////////
// TLoadThroughputThrottler tests
////////////////////////////////////////////////////////////////////////////////

TEST(TLoadThroughputThrottlerTest, FirstThrottleChargesInitialEstimate)
{
    auto recording = New<TRecordingThrottler>();
    auto throttler = MakeThrottler(recording);

    // A tag never seen before must be charged with the spec's initial
    // estimates, not with a zero-initialized entry.
    throttler->ThrottleKeys("tag", 10).BlockingGet().ThrowOnError();
    EXPECT_EQ(recording->LastAmount(), 5'000);

    throttler->ThrottleRows("tag", 3).BlockingGet().ThrowOnError();
    EXPECT_EQ(recording->LastAmount(), 3'000);

    // Each ThrottleKeys/ThrottleRows call charges the underlying throttler
    // exactly once.
    EXPECT_EQ(std::ssize(recording->Amounts()), 2);
}

TEST(TLoadThroughputThrottlerTest, RegisteredSizesSurviveSubsequentThrottles)
{
    auto recording = New<TRecordingThrottler>();
    auto throttler = MakeThrottler(recording);

    // Seed the tag, then feed the EWMA with actual sizes.
    throttler->ThrottleKeys("tag", 1).BlockingGet().ThrowOnError();
    EXPECT_EQ(recording->LastAmount(), 500);

    // 500 -> 300 with alpha 0.5.
    throttler->RegisterKeys("tag", {100});
    throttler->ThrottleKeys("tag", 10).BlockingGet().ThrowOnError();
    EXPECT_EQ(recording->LastAmount(), 3'000);

    // 300 -> 200 -> 150; the estimate must accumulate across calls instead
    // of being reset to the initial value on every access.
    throttler->RegisterKeys("tag", {100, 100});
    throttler->ThrottleKeys("tag", 4).BlockingGet().ThrowOnError();
    EXPECT_EQ(recording->LastAmount(), 600);
}

TEST(TLoadThroughputThrottlerTest, RowAndKeyEstimatesAreIndependent)
{
    auto recording = New<TRecordingThrottler>();
    auto throttler = MakeThrottler(recording);

    // 1000 -> 750 with alpha 0.5.
    throttler->RegisterRows("tag", {500});
    throttler->ThrottleRows("tag", 2).BlockingGet().ThrowOnError();
    EXPECT_EQ(recording->LastAmount(), 1'500);

    // The key estimate is untouched by row registrations.
    throttler->ThrottleKeys("tag", 2).BlockingGet().ThrowOnError();
    EXPECT_EQ(recording->LastAmount(), 1'000);
}

TEST(TLoadThroughputThrottlerTest, TagsAreIndependent)
{
    auto recording = New<TRecordingThrottler>();
    auto throttler = MakeThrottler(recording);

    // 500 -> 275 with alpha 0.5.
    throttler->RegisterKeys("a", {50});
    throttler->ThrottleKeys("a", 2).BlockingGet().ThrowOnError();
    EXPECT_EQ(recording->LastAmount(), 550);

    // Tag "b" still starts from the initial estimate.
    throttler->ThrottleKeys("b", 2).BlockingGet().ThrowOnError();
    EXPECT_EQ(recording->LastAmount(), 1'000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
