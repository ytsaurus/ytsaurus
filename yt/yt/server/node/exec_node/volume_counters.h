#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/tagged_counters.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TVolumeProfilerCounters
{
public:
    TVolumeProfilerCounters();

    NProfiling::TCounter GetCounter(const NProfiling::TTagSet& tagSet, const TString& name);

    NProfiling::TGauge GetGauge(const NProfiling::TTagSet& tagSet, const TString& name);

    NProfiling::TEventTimer GetTimeHistogram(const NProfiling::TTagSet& tagSet, const TString& name);

    NProfiling::TEventTimer GetTimer(const NProfiling::TTagSet& tagSet, const TString& name);

    static NProfiling::TTagSet MakeTagSet(const TString& volumeType, const TString& volumeFilePath);

    static TVolumeProfilerCounters* Get();

private:
    using TKey = NProfiling::TTagList;

    const NProfiling::TProfiler VolumeProfiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TKey, NProfiling::TCounter> Counters_;
    THashMap<TKey, NProfiling::TGauge> Gauges_;
    THashMap<TKey, NProfiling::TEventTimer> EventTimers_;

    static TKey CreateKey(const NProfiling::TTagSet& tagSet, const TString& name);
};

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTaggedCounters<int>& VolumeCounters();

////////////////////////////////////////////////////////////////////////////////

struct TLayerLocationPerformanceCounters
{
    TLayerLocationPerformanceCounters() = default;

    explicit TLayerLocationPerformanceCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TGauge LayerCount;
    NProfiling::TGauge VolumeCount;

    NProfiling::TGauge TotalSpace;
    NProfiling::TGauge UsedSpace;
    NProfiling::TGauge AvailableSpace;
    NProfiling::TGauge Full;

    NProfiling::TEventTimer ImportLayerTimer;
};

////////////////////////////////////////////////////////////////////////////////

class TTmpfsLayerCacheCounters
{
public:
    explicit TTmpfsLayerCacheCounters(NProfiling::TProfiler profiler);

    NProfiling::TCounter GetCounter(const NProfiling::TTagSet& tagSet, const TString& name);

private:
    using TKey = NProfiling::TTagList;

    static TKey CreateKey(const NProfiling::TTagSet& tagSet, const TString& name);

    const NProfiling::TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TKey, NProfiling::TCounter> Counters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
