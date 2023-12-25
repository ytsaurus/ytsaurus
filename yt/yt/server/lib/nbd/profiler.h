#pragma once

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/tag.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/singleton.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

class TNbdProfilerCounters
{
public:
    TNbdProfilerCounters(const TNbdProfilerCounters& other) = delete;

    NProfiling::TCounter GetCounter(const NProfiling::TTagSet& tagSet, const TString& name);

    NProfiling::TGauge GetGauge(const NProfiling::TTagSet& tagSet, const TString& name);

    NProfiling::TEventTimer GetTimeHistogram(const NProfiling::TTagSet& tagSet, const TString& name);

    NProfiling::TEventTimer GetTimer(const NProfiling::TTagSet& tagSet, const TString& name);

    static NProfiling::TTagSet MakeTagSet(const TString& filePath);

    static TNbdProfilerCounters* Get();

private:
    TNbdProfilerCounters() = default;
    Y_DECLARE_SINGLETON_FRIEND();

    using TKey = NProfiling::TTagList;

    static TKey CreateKey(const NProfiling::TTagSet& tagSet, const TString& name);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TKey, NProfiling::TCounter> Counters_;
    THashMap<TKey, NProfiling::TGauge> Gauges_;
    THashMap<TKey, NProfiling::TEventTimer> EventTimers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

