#pragma once

#include "public.h"

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TProfileCollector
{
public:
    TProfileCollector(const NProfiling::TProfiler* profiler);

    void Add(
        const NYPath::TYPath& path,
        NProfiling::TValue value,
        NProfiling::EMetricType metricType,
        const NProfiling::TTagIdList& tagIds = NProfiling::EmptyTagIds);

    void Publish();

private:
    const NProfiling::TProfiler* Profiler_;

    using TKey = std::pair<TString, NProfiling::TTagIdList>;
    using TValue = std::pair<NProfiling::TValue, NProfiling::EMetricType>;
    THashMap<TKey, TValue> Metrics_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
