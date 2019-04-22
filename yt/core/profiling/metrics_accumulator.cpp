#include "metrics_accumulator.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TMetricsAccumulator::Add(
    const NYPath::TYPath& path,
    NProfiling::TValue value,
    EMetricType metricType,
    const TTagIdList& tagIds)
{
    Metrics_.emplace_back(std::make_pair(path, tagIds), std::make_pair(value, metricType));
}

void TMetricsAccumulator::BuildAndPublish(const TProfiler* profiler)
{
    THashMap<TKey, TValue> aggregatedMetrics;
    for (const auto& [key, value] : Metrics_) {
        auto it = aggregatedMetrics.find(key);
        if (it == aggregatedMetrics.end()) {
            aggregatedMetrics.emplace(key, value);
        } else {
            auto storedType = it->second.second;
            YCHECK(storedType == value.second);
            it->second.first += value.first;
        }
    }

    for (const auto& [key, aggregatedValue] : aggregatedMetrics) {
        const auto& [path, tags] = key;
        const auto& [value, type] = aggregatedValue;
        profiler->Enqueue(path, value, type, tags);
    }
    Metrics_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

