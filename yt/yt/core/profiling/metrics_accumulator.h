#pragma once

#include "profiler.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TMetricsAccumulator
{
public:
    void Add(
        const NYPath::TYPath& path,
        NProfiling::TValue value,
        NProfiling::EMetricType metricType,
        const NProfiling::TTagIdList& tagIds = {});

    void Publish(const NProfiling::TLegacyProfiler* profiler);

private:
    using TKey = std::pair<TString, NProfiling::TTagIdList>;
    using TValue = std::pair<NProfiling::TValue, NProfiling::EMetricType>;
    THashMap<TKey, TValue> Metrics_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
