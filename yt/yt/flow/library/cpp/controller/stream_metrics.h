#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

class TLagGauge
    : public TRefCounted
{
public:
    static TIntrusivePtr<TLagGauge> Create(
        const NProfiling::TProfiler& profiler,
        const std::string& name);

    void Update(TSystemTimestamp watermark);

private:
    std::atomic<ui64> Watermark_{ZeroSystemTimestamp.Underlying()};
};

using TLagGaugePtr = TIntrusivePtr<TLagGauge>;

////////////////////////////////////////////////////////////////////////////////

//! The part of a stream's inflight volume that is ready for processing.
struct TReadyVolume
{
    i64 Count = 0;
    std::optional<i64> ByteSize;
};

//! Returns the global ids of the timer streams of |spec|.
THashSet<TStreamId> CollectTimerStreamIds(const TPipelineSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

class TStreamMetrics
{
public:
    TStreamMetrics(const TStreamId& streamId, const NProfiling::TProfiler& profiler);

    void Apply(const TSystemTimestamp now, const TStreamTraverseDataPtr& stream, const TReadyVolume& readyVolume);

private:
    NProfiling::TProfiler Profiler_;

    TLagGaugePtr TimeLagGauge_;
    TLagGaugePtr EventTimeLagGauge_;
    NProfiling::TGauge LagGauge_;
    std::optional<NProfiling::TGauge> ByteSizeLagGauge_;
    TLagGaugePtr ReadyTimeLagGauge_;
    NProfiling::TGauge ReadyCountLagGauge_;
    std::optional<NProfiling::TGauge> ReadyByteSizeLagGauge_;
};

void UpdateStreamMetrics(
    THashMap<TStreamId, TStreamMetrics>& streamMetrics,
    TSystemTimestamp now,
    const TPipelineTraverseDataPtr& traverseData,
    const THashSet<TStreamId>& timerStreamIds,
    bool enableUnitedStreamMetrics,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
