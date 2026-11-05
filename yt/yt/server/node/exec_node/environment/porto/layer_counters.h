#pragma once

#include <yt/yt/server/node/exec_node/volume_counters.h>

namespace NYT::NExecNode {

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

    NProfiling::TCounter EnospcRate;
};

////////////////////////////////////////////////////////////////////////////////

class TTmpfsLayerCacheCounters
{
public:
    explicit TTmpfsLayerCacheCounters(NProfiling::TProfiler profiler);

    NProfiling::TCounter GetCounter(const NProfiling::TTagSet& tagSet, const std::string& name);

private:
    using TKey = NProfiling::TTagList;

    static TKey CreateKey(const NProfiling::TTagSet& tagSet, const std::string& name);

    const NProfiling::TProfiler Profiler_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    THashMap<TKey, NProfiling::TCounter> Counters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
