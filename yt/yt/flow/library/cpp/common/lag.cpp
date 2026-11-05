#include "lag.h"

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

const std::vector<TDuration>& EventLagBuckets()
{
    static const std::vector<TDuration> buckets = {
        TDuration::Seconds(1),
        TDuration::Seconds(3),
        TDuration::Seconds(7),
        TDuration::Seconds(15),
        TDuration::Seconds(30),
        TDuration::Minutes(1),
        TDuration::Minutes(3),
        TDuration::Minutes(7),
        TDuration::Minutes(15),
        TDuration::Minutes(30),
        TDuration::Hours(1),
        TDuration::Hours(3),
        TDuration::Hours(6),
        TDuration::Hours(12),
        TDuration::Days(1),
    };
    return buckets;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStreamEventLagObserver::TStreamEventLagObserver(
    const NProfiling::TProfiler& profiler,
    const std::vector<TStreamId>& streamIds)
{
    for (const auto& streamId : streamIds) {
        auto streamProfiler = profiler.WithTag("stream_id", streamId.Underlying());
        Streams_[streamId] = TStream{
            .Histogram = streamProfiler.TimeHistogram("/lag", EventLagBuckets()),
            // Sparse: an idle stream emits no lag_max sample at all.
            .Max = streamProfiler.WithSparse().TimeGauge("/lag_max"),
            .EventTimestamps = {},
        };
    }
}

void TStreamEventLagObserver::DoObserve(const TStreamId& streamId, TSystemTimestamp eventTimestamp)
{
    auto it = Streams_.find(streamId);
    if (it == Streams_.end()) {
        return;
    }
    it->second.EventTimestamps.push_back(eventTimestamp);
}

void TStreamEventLagObserver::Observe(const TStreamId& streamId, TSystemTimestamp eventTimestamp)
{
    auto guard = Guard(Lock_);
    DoObserve(streamId, eventTimestamp);
}

void TStreamEventLagObserver::Flush(TInstant now)
{
    Flush(TSystemTimestamp(now.Seconds()));
}

void TStreamEventLagObserver::Flush(TSystemTimestamp now)
{
    auto guard = Guard(Lock_);
    for (auto& [streamId, stream] : Streams_) {
        TDuration maxLag;
        for (auto eventTimestamp : stream.EventTimestamps) {
            auto lag = ComputeLag(now, eventTimestamp);
            stream.Histogram.Record(lag);
            maxLag = std::max(maxLag, lag);
        }
        stream.EventTimestamps.clear();
        stream.Max.Update(maxLag);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
