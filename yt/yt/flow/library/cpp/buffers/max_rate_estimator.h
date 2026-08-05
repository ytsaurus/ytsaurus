#pragma once

#include <util/datetime/base.h>

#include <optional>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Windowed-max rate estimator over a monotonic cumulative counter, in the
//! style of TCP BBR (Bottleneck Bandwidth and Round-trip propagation time).
//! The window is split into #BucketCount buckets of |bucketDuration| each; the
//! estimate is the maximum per-bucket rate. Unlike an EMA, it does not decay
//! during idle gaps or between bursts shorter than the window, so a bursty
//! consumer (drain happens once per epoch) is estimated by its peak drain rate.
//! |bucketDuration| is supplied by the caller on every update, so the window
//! adapts to the consumer's measured cycle instead of a wall-clock constant.
class TMaxRateEstimator
{
public:
    static constexpr size_t DefaultBucketCount = 8;

    //! Changing the count drops the collected buckets: the estimate restarts.
    void SetBucketCount(size_t count);

    //! |cumulative| must be monotonically non-decreasing.
    void Update(double cumulative, TInstant now, TDuration bucketDuration);

    std::optional<double> GetMaxRate() const;

private:
    std::vector<double> BucketRates_ = std::vector<double>(DefaultBucketCount);
    size_t FilledBuckets_ = 0;
    size_t NextIndex_ = 0;

    std::optional<TInstant> BucketStart_;
    double BucketStartCumulative_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
