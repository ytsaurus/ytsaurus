#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/client/query_tracker_client/public.h>

#include <library/cpp/yt/threading/spin_lock.h>
#include <library/cpp/yt/threading/public.h>

namespace NYT::NQueryTracker {

const TString NoneQueryTracker = "None";

inline const NProfiling::TProfiler QueryTrackerProfilerGlobal = NProfiling::TProfiler("/query_tracker").WithGlobal();
inline const NProfiling::TProfiler QueryTrackerProfiler = NProfiling::TProfiler("/query_tracker");

using namespace NQueryTrackerClient;

struct TProfilingTags
{
    EQueryState State;
    EQueryEngine Engine;
    TString AssignedTracker;

    bool operator==(const TProfilingTags& other) const = default;
    bool operator<(const TProfilingTags& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NQueryTracker::TProfilingTags>
{
    size_t operator()(const NYT::NQueryTracker::TProfilingTags& tag) const;
};

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NQueryTracker {

struct TActiveQueriesProfilingCounter
{
    NProfiling::TGauge ActiveQueries;

    explicit TActiveQueriesProfilingCounter(const NProfiling::TProfiler& profiler);
};

struct TStateTimeProfilingCounter
{
    NProfiling::TTimeGauge StateTime;

    explicit TStateTimeProfilingCounter(const NProfiling::TProfiler& profiler);
};

struct TStateTimeProfilingCountersMap
    : public TRefCounted
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    THashMap<TProfilingTags, TStateTimeProfilingCounter> Map;
};

DECLARE_REFCOUNTED_STRUCT(TStateTimeProfilingCountersMap);
DEFINE_REFCOUNTED_TYPE(TStateTimeProfilingCountersMap);

template<class TProfilingCounter>
TProfilingCounter& GetOrCreateProfilingCounter(
    const NProfiling::TProfiler& profiler,
    const TProfilingTags& profilingTags,
    THashMap<TProfilingTags, TProfilingCounter>& profilingCounterMap,
    std::optional<TGuard<NThreading::TSpinLock>>& /*guard*/)
{
    auto it = profilingCounterMap.find(profilingTags);
    if (it != profilingCounterMap.end()) {
        return it->second;
    }

    auto profilingCounter =
        TProfilingCounter(profiler
            .WithTag("state", ToString(profilingTags.State))
            .WithTag("engine", ToString(profilingTags.Engine))
            .WithTag("assigned_tracker", profilingTags.AssignedTracker));

    it = profilingCounterMap.insert({profilingTags, profilingCounter}).first;
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
