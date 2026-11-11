#include "layer_counters.h"

namespace NYT::NExecNode {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TLayerLocationPerformanceCounters::TLayerLocationPerformanceCounters(const TProfiler& profiler)
{
    LayerCount = profiler.Gauge("/layer_count");
    VolumeCount = profiler.Gauge("/volume_count");

    UsedSpace = profiler.Gauge("/used_space");
    AvailableSpace = profiler.Gauge("/available_space");
    TotalSpace = profiler.Gauge("/total_space");
    Full = profiler.Gauge("/full");

    ImportLayerTimer = profiler.Timer("/import_layer_time");

    EnospcRate = profiler.Counter("/enospc_events");
}

////////////////////////////////////////////////////////////////////////////////

TTmpfsLayerCacheCounters::TTmpfsLayerCacheCounters(TProfiler profiler)
    : Profiler_(std::move(profiler))
{ }

TCounter TTmpfsLayerCacheCounters::GetCounter(const TTagSet& tagSet, const std::string& name)
{
    auto key = CreateKey(tagSet, name);

    auto guard = Guard(Lock_);
    auto [it, inserted] = Counters_.emplace(key, TCounter());
    if (inserted) {
        it->second = Profiler_.WithTags(tagSet).Counter(name);
    }

    return it->second;
}

TTmpfsLayerCacheCounters::TKey TTmpfsLayerCacheCounters::CreateKey(const TTagSet& tagSet, const std::string& name)
{
    auto key = tagSet.Tags();
    key.push_back({"name", name});
    return key;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
