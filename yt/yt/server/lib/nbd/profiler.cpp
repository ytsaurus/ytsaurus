#include "profiler.h"
#include "private.h"

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

NProfiling::TCounter TNbdProfilerCounters::GetCounter(const NProfiling::TTagSet& tagSet, const TString& name)
{
    auto key = CreateKey(tagSet, name);

    auto guard = Guard(Lock_);
    auto [it, inserted] = Counters_.emplace(key, NProfiling::TCounter());
    if (inserted) {
        it->second = NbdProfiler.WithTags(tagSet).Counter(name);
    }

    return it->second;
}

NProfiling::TGauge TNbdProfilerCounters::GetGauge(const NProfiling::TTagSet& tagSet, const TString& name)
{
    auto key = CreateKey(tagSet, name);

    auto guard = Guard(Lock_);
    auto [it, inserted] = Gauges_.emplace(key, NProfiling::TGauge());
    if (inserted) {
        it->second = NbdProfiler.WithTags(tagSet).Gauge(name);
    }

    return it->second;
}

NProfiling::TEventTimer TNbdProfilerCounters::GetTimeHistogram(const NProfiling::TTagSet& tagSet, const TString& name)
{
    auto key = CreateKey(tagSet, name);

    auto guard = Guard(Lock_);
    auto [it, inserted] = EventTimers_.emplace(key, NProfiling::TEventTimer());
    if (inserted) {
        std::vector<TDuration> bounds{
            TDuration::Zero(),
            TDuration::MilliSeconds(100),
            TDuration::MilliSeconds(500),
            TDuration::Seconds(1),
            TDuration::Seconds(5),
            TDuration::Seconds(10)};
        it->second = NbdProfiler.WithTags(tagSet).TimeHistogram(name, std::move(bounds));
    }

    return it->second;
}

NProfiling::TEventTimer TNbdProfilerCounters::GetTimer(const NProfiling::TTagSet& tagSet, const TString& name)
{
    auto key = CreateKey(tagSet, name);

    auto guard = Guard(Lock_);
    auto [it, inserted] = EventTimers_.emplace(key, NProfiling::TEventTimer());
    if (inserted) {
        it->second = NbdProfiler.WithTags(tagSet).Timer(name);
    }

    return it->second;
}

NProfiling::TTagSet TNbdProfilerCounters::MakeTagSet(const TString& filePath)
{
    return NProfiling::TTagSet({{"file_path", filePath}});
}

TNbdProfilerCounters* TNbdProfilerCounters::Get()
{
    return Singleton<TNbdProfilerCounters>();
}

TNbdProfilerCounters::TKey TNbdProfilerCounters::CreateKey(const NProfiling::TTagSet& tagSet, const TString& name)
{
    auto tagList = tagSet.Tags();
    tagList.push_back({"name", name});
    return tagList;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
