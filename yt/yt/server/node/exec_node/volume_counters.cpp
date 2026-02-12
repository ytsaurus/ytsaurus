#include "volume_counters.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TVolumeProfilerCounters::TVolumeProfilerCounters()
    : VolumeProfiler_("/volumes")
{ }

TCounter TVolumeProfilerCounters::GetCounter(const TTagSet& tagSet, const TString& name)
{
    auto key = CreateKey(tagSet, name);

    auto guard = Guard(Lock_);
    auto [it, inserted] = Counters_.emplace(key, TCounter());
    if (inserted) {
        it->second = VolumeProfiler_.WithTags(tagSet).Counter(name);
    }

    return it->second;
}

TGauge TVolumeProfilerCounters::GetGauge(const TTagSet& tagSet, const TString& name)
{
    auto key = CreateKey(tagSet, name);

    auto guard = Guard(Lock_);
    auto [it, inserted] = Gauges_.emplace(key, TGauge());
    if (inserted) {
        it->second = VolumeProfiler_.WithTags(tagSet).Gauge(name);
    }

    return it->second;
}

TEventTimer TVolumeProfilerCounters::GetTimeHistogram(const TTagSet& tagSet, const TString& name)
{
    auto key = CreateKey(tagSet, name);

    auto guard = Guard(Lock_);
    auto [it, inserted] = EventTimers_.emplace(key, TEventTimer());
    if (inserted) {
        std::vector<TDuration> bounds{
            TDuration::Zero(),
            TDuration::MilliSeconds(100),
            TDuration::MilliSeconds(500),
            TDuration::Seconds(1),
            TDuration::Seconds(5),
            TDuration::Seconds(10),
        };
        it->second = VolumeProfiler_.WithTags(tagSet).TimeHistogram(name, std::move(bounds));
    }

    return it->second;
}

TEventTimer TVolumeProfilerCounters::GetTimer(const TTagSet& tagSet, const TString& name)
{
    auto key = CreateKey(tagSet, name);

    auto guard = Guard(Lock_);
    auto [it, inserted] = EventTimers_.emplace(key, TEventTimer());
    if (inserted) {
        it->second = VolumeProfiler_.WithTags(tagSet).Timer(name);
    }

    return it->second;
}

TTagSet TVolumeProfilerCounters::MakeTagSet(const TString& volumeType, const TString& volumeFilePath)
{
    return TTagSet({{"type", volumeType}, {"file_path", volumeFilePath}});
}

TVolumeProfilerCounters* TVolumeProfilerCounters::Get()
{
    return Singleton<TVolumeProfilerCounters>();
}

TVolumeProfilerCounters::TKey TVolumeProfilerCounters::CreateKey(const TTagSet& tagSet, const TString& name)
{
    auto key = tagSet.Tags();
    key.push_back({"name", name});
    return key;
}

////////////////////////////////////////////////////////////////////////////////

TTaggedCounters<int>& VolumeCounters()
{
    static TTaggedCounters<int> result;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
