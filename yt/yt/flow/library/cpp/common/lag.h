#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/datetime/base.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! now - timestamp, clamped to zero. Source clock and observer clock may
//! disagree by a few seconds; we treat that as zero rather than wrap.
TDuration ComputeLag(TInstant now, TSystemTimestamp timestamp);
TDuration ComputeLag(TSystemTimestamp now, TSystemTimestamp timestamp);

////////////////////////////////////////////////////////////////////////////////

//! Per-stream accumulator + sensors for the now() - EventTimestamp lag.
//! Caller pushes events via Observe and drains them with Flush at a natural
//! "now" boundary (epoch commit, sink ack, ...).
//!
//! The profiler must already carry the entity tags (computation_id, sink_id,
//! ...); only stream_id is added here. The lag_max gauge is sparse — idle
//! streams emit no sample so dashboards distinguish "stream is silent" from
//! "lag is small".
class TStreamEventLagObserver
{
public:
    TStreamEventLagObserver() = default;
    TStreamEventLagObserver(
        const NProfiling::TProfiler& profiler,
        const std::vector<TStreamId>& streamIds);

    void Observe(const TStreamId& streamId, TSystemTimestamp eventTimestamp);

    //! Observes a batch of messages. Each element must expose `.StreamId` and `.EventTimestamp`.
    //! Takes the lock once for the entire batch.
    template <class TRange>
    void ObserveBatch(const TRange& messages);

    void Flush(TInstant now);
    void Flush(TSystemTimestamp now);

private:
    struct TStream
    {
        NProfiling::TEventTimer Histogram;
        NProfiling::TTimeGauge Max;
        std::vector<TSystemTimestamp> EventTimestamps;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TStreamId, TStream> Streams_;

    void DoObserve(const TStreamId& streamId, TSystemTimestamp eventTimestamp);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define LAG_INL_H_
#include "lag-inl.h"
#undef LAG_INL_H_
