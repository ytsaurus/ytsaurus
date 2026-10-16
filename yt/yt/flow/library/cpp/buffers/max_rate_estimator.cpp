#include "max_rate_estimator.h"

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TMaxRateEstimator::SetBucketCount(size_t count)
{
    count = std::max<size_t>(count, 1);
    if (count == BucketRates_.size()) {
        return;
    }
    BucketRates_.assign(count, 0.0);
    FilledBuckets_ = 0;
    NextIndex_ = 0;
    BucketStart_.reset();
}

void TMaxRateEstimator::Update(double cumulative, TInstant now, TDuration bucketDuration)
{
    if (!BucketStart_) {
        BucketStart_ = now;
        BucketStartCumulative_ = cumulative;
        return;
    }
    auto elapsed = now - *BucketStart_;
    if (elapsed < bucketDuration) {
        return;
    }
    double rate = (cumulative - BucketStartCumulative_) / elapsed.SecondsFloat();
    BucketRates_[NextIndex_] = rate;
    NextIndex_ = (NextIndex_ + 1) % BucketRates_.size();
    FilledBuckets_ = std::min(FilledBuckets_ + 1, BucketRates_.size());
    BucketStart_ = now;
    BucketStartCumulative_ = cumulative;
}

std::optional<double> TMaxRateEstimator::GetMaxRate() const
{
    if (FilledBuckets_ == 0) {
        return std::nullopt;
    }
    return *std::max_element(BucketRates_.begin(), BucketRates_.begin() + FilledBuckets_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
