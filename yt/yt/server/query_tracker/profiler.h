#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/client/query_tracker_client/public.h>

#include <library/cpp/yt/threading/spin_lock.h>
#include <library/cpp/yt/threading/public.h>

namespace NYT::NQueryTracker {

const TString NoneQueryTracker = "None";

////////////////////////////////////////////////////////////////////////////////

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

using TStateTimeProfilingCountersMap = NConcurrency::TSyncMap<TProfilingTags, TStateTimeProfilingCounter>;
using TActiveQueriesProfilingCountersMap = NConcurrency::TSyncMap<TProfilingTags, TActiveQueriesProfilingCounter>;

template<class TProfilingCounter>
TProfilingCounter* GetOrCreateProfilingCounter(
    const NProfiling::TProfiler& profiler,
    const TProfilingTags& profilingTags);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker

#define PROFILER_INL_H_
#include "profiler-inl.h"
#undef PROFILER_INL_H_
