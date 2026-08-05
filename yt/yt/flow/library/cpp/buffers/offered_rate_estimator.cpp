#include "offered_rate_estimator.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TOfferedRateEstimator::TOfferedRateEstimator(TDuration window)
    : WindowSeconds_(std::max(window.SecondsFloat(), 1.0))
{ }

void TOfferedRateEstimator::SetWindow(TDuration window)
{
    WindowSeconds_ = std::max(window.SecondsFloat(), 1.0);
}

void TOfferedRateEstimator::RecordAccepted(i64 inflatedBytes, i64 timestampSeconds)
{
    if (Frontier_ < 0) {
        Frontier_ = timestampSeconds;
        FirstAcceptedTimestamp_ = timestampSeconds;
        DecayedBytes_ = inflatedBytes;
        return;
    }
    const i64 timestamp = std::min<i64>(timestampSeconds, Frontier_ + static_cast<i64>(WindowSeconds_));
    if (timestamp > Frontier_) {
        DecayedBytes_ *= std::exp(-static_cast<double>(timestamp - Frontier_) / WindowSeconds_);
        Frontier_ = timestamp;
    }
    DecayedBytes_ += inflatedBytes * std::exp(-static_cast<double>(Frontier_ - timestamp) / WindowSeconds_);
}

double TOfferedRateEstimator::EstimateRate(TConstArrayRef<std::pair<i64, i64>> pendingBuckets) const
{
    i64 pendingBytes = 0;
    i64 oldest = std::numeric_limits<i64>::max();
    i64 newest = std::numeric_limits<i64>::min();
    for (const auto& [timestamp, bytes] : pendingBuckets) {
        pendingBytes += bytes;
        oldest = std::min(oldest, timestamp);
        newest = std::max(newest, timestamp);
    }
    if (pendingBytes <= 0) {
        return 0;
    }
    if (Frontier_ < 0) {
        // No accepted history: fall back to the pure backlog-span estimate.
        return static_cast<double>(pendingBytes) / std::max<i64>(newest - oldest, 1);
    }

    const double offerEstimate = static_cast<double>(pendingBytes) / std::max<i64>(newest - Frontier_, 1);
    double pendingFolded = 0;
    for (const auto& [timestamp, bytes] : pendingBuckets) {
        const double relative = std::min(
            static_cast<double>(timestamp - Frontier_) / WindowSeconds_,
            1.0);
        pendingFolded += bytes * std::exp(relative);
    }
    const double fullEstimate = (DecayedBytes_ + pendingFolded) / WindowSeconds_;
    const double confidence = std::min(
        static_cast<double>(Frontier_ - FirstAcceptedTimestamp_) / WindowSeconds_,
        1.0);
    return (1 - confidence) * offerEstimate +
        confidence * std::min(offerEstimate, MaxHistoryGain * fullEstimate);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
