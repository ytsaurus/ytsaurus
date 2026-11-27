#include "offered_rate_estimator.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TOfferedRateEstimate::Register(TRegistrar registrar)
{
    registrar.Parameter("pending_inflated_bytes", &TThis::PendingInflatedBytes)
        .Default();
    registrar.Parameter("oldest_pending_timestamp", &TThis::OldestPendingTimestamp)
        .Default();
    registrar.Parameter("newest_pending_timestamp", &TThis::NewestPendingTimestamp)
        .Default();
    registrar.Parameter("window_seconds", &TThis::WindowSeconds)
        .Default();
    registrar.Parameter("decayed_bytes", &TThis::DecayedBytes)
        .Default();
    registrar.Parameter("frontier", &TThis::Frontier)
        .Default(-1);
    registrar.Parameter("first_accepted_timestamp", &TThis::FirstAcceptedTimestamp)
        .Default(-1);
    registrar.Parameter("offer_estimate", &TThis::OfferEstimate)
        .Default();
    registrar.Parameter("full_estimate", &TThis::FullEstimate)
        .Default();
    registrar.Parameter("confidence", &TThis::Confidence)
        .Default();
    registrar.Parameter("rate", &TThis::Rate)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TOfferedRateEstimator::TOfferedRateEstimator(TDuration window)
    : WindowSeconds_(std::max(window.SecondsFloat(), 1.0))
{ }

void TOfferedRateEstimator::SetWindow(TDuration window)
{
    WindowSeconds_.store(std::max(window.SecondsFloat(), 1.0), std::memory_order::relaxed);
}

void TOfferedRateEstimator::RecordAccepted(i64 inflatedBytes, i64 timestampSeconds)
{
    if (Frontier_ < 0) {
        Frontier_ = timestampSeconds;
        FirstAcceptedTimestamp_ = timestampSeconds;
        DecayedBytes_ = inflatedBytes;
        return;
    }

    const double windowSeconds = WindowSeconds_.load(std::memory_order::relaxed);
    const i64 timestamp = std::min<i64>(timestampSeconds, Frontier_ + static_cast<i64>(windowSeconds));
    if (timestamp > Frontier_) {
        DecayedBytes_ *= std::exp(-static_cast<double>(timestamp - Frontier_) / windowSeconds);
        Frontier_ = timestamp;
    }
    DecayedBytes_ += inflatedBytes * std::exp(-static_cast<double>(Frontier_ - timestamp) / windowSeconds);
}

double TOfferedRateEstimator::EstimateRate(
    TConstArrayRef<std::pair<i64, i64>> pendingBuckets,
    TOfferedRateEstimate* details) const
{
    const double windowSeconds = WindowSeconds_.load(std::memory_order::relaxed);
    i64 pendingBytes = 0;
    i64 oldest = std::numeric_limits<i64>::max();
    i64 newest = std::numeric_limits<i64>::min();
    for (const auto& [timestamp, bytes] : pendingBuckets) {
        pendingBytes += bytes;
        oldest = std::min(oldest, timestamp);
        newest = std::max(newest, timestamp);
    }

    auto finish = [&] (double rate, double offerEstimate, double fullEstimate, double confidence) {
        if (details) {
            const bool hasPending = !pendingBuckets.empty();
            details->WindowSeconds = windowSeconds;
            details->DecayedBytes = DecayedBytes_;
            details->Frontier = Frontier_;
            details->FirstAcceptedTimestamp = FirstAcceptedTimestamp_;
            details->PendingInflatedBytes = pendingBytes;
            details->OldestPendingTimestamp = hasPending ? oldest : 0;
            details->NewestPendingTimestamp = hasPending ? newest : 0;
            details->OfferEstimate = offerEstimate;
            details->FullEstimate = fullEstimate;
            details->Confidence = confidence;
            details->Rate = rate;
        }

        return rate;
    };

    if (pendingBytes <= 0) {
        return finish(0, 0, 0, 0);
    }
    if (Frontier_ < 0) {
        // No accepted history: fall back to the pure backlog-span estimate.
        const double spanEstimate = static_cast<double>(pendingBytes) / std::max<i64>(newest - oldest, 1);
        return finish(spanEstimate, spanEstimate, 0, 0);
    }

    const double offerEstimate = static_cast<double>(pendingBytes) / std::max<i64>(newest - Frontier_, 1);
    double pendingFolded = 0;
    for (const auto& [timestamp, bytes] : pendingBuckets) {
        const double relative = std::min(
            static_cast<double>(timestamp - Frontier_) / windowSeconds,
            1.0);
        pendingFolded += bytes * std::exp(relative);
    }
    const double fullEstimate = (DecayedBytes_ + pendingFolded) / windowSeconds;
    const double confidence = std::min(
        static_cast<double>(Frontier_ - FirstAcceptedTimestamp_) / windowSeconds,
        1.0);
    return finish(
        (1 - confidence) * offerEstimate + confidence * std::min(offerEstimate, MaxHistoryGain * fullEstimate),
        offerEstimate,
        fullEstimate,
        confidence);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
