#include "profiler.h"

namespace NYT {
namespace NScheduler {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////
    
TProfileCollector::TProfileCollector(const TProfiler* profiler)
    : Profiler_(profiler)
{ }

void TProfileCollector::Add(
    const NYPath::TYPath& path,
    NProfiling::TValue value,
    EMetricType metricType,
    const TTagIdList& tagIds)
{
    auto key = std::make_pair(path, tagIds);
    auto it = Metrics_.find(key);
    if (it == Metrics_.end()) {
        Metrics_.emplace(key, std::make_pair(value, metricType));
    } else {
        auto storedType = it->second.second;
        YCHECK(storedType == metricType);
        it->second.first += value;
    }
}

void TProfileCollector::Publish()
{
    for (const auto& metricPair : Metrics_) {
        const auto& path = metricPair.first.first;
        const auto& tags = metricPair.first.second;
        auto value = metricPair.second.first;
        auto type = metricPair.second.second;
        Profiler_->Enqueue(path, value, type, tags);
    }
    Metrics_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
