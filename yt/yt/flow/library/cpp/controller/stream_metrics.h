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

class TStreamMetrics
{
public:
    TStreamMetrics(const TStreamId& streamId, const NProfiling::TProfiler& profiler);

    void Apply(const TSystemTimestamp now, const TStreamTraverseDataPtr& stream);

private:
    NProfiling::TProfiler Profiler_;

    TLagGaugePtr TimeLagGauge_;
    TLagGaugePtr EventTimeLagGauge_;
    NProfiling::TGauge LagGauge_;
    std::optional<NProfiling::TGauge> ByteSizeLagGauge_;
};

void UpdateStreamMetrics(
    THashMap<TStreamId, TStreamMetrics>& streamMetrics,
    TSystemTimestamp now,
    const TPipelineTraverseDataPtr& traverseData,
    bool enableUnitedStreamMetrics,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
