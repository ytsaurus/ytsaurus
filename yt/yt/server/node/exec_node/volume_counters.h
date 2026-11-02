#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/tagged_counters.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TVolumeProfilerCounters
{
public:
    NProfiling::TCounter GetCounter(const NProfiling::TTagSet& tagSet, const std::string& name);

    NProfiling::TGauge GetGauge(const NProfiling::TTagSet& tagSet, const std::string& name);

    NProfiling::TEventTimer GetTimeHistogram(const NProfiling::TTagSet& tagSet, const std::string& name);

    NProfiling::TEventTimer GetTimer(const NProfiling::TTagSet& tagSet, const std::string& name);

    static NProfiling::TTagSet MakeTagSet(const std::string& volumeType, const std::string& volumeFilePath);

    static TVolumeProfilerCounters* Get();

private:
    TVolumeProfilerCounters();

    using TKey = NProfiling::TTagList;

    const NProfiling::TProfiler VolumeProfiler_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    THashMap<TKey, NProfiling::TCounter> Counters_;
    THashMap<TKey, NProfiling::TGauge> Gauges_;
    THashMap<TKey, NProfiling::TEventTimer> EventTimers_;

    static TKey CreateKey(const NProfiling::TTagSet& tagSet, const std::string& name);

    Y_DECLARE_SINGLETON_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

NProfiling::TTaggedCounters<int>& VolumeCounters();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
