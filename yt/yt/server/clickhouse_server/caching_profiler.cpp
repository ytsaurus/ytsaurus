#include "caching_profiler.h"

namespace NYT::NClickHouseServer {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TCachingProfilerWrapper::TCachingProfilerWrapper(const TProfiler* underlyingProfiler)
    : UnderlyingProfiler_(underlyingProfiler)
{ }

void TCachingProfilerWrapper::Enqueue(
    const NYPath::TYPath& path,
    TValue value,
    EMetricType metricType,
    const TTagIdList& tagIds) const
{
    TKey key(path, tagIds);
    auto it = PreviousValues_.find(key);
    if (it != PreviousValues_.end()) {
        if (it->second == value && value == 0) {
            return;
        }
        it->second = value;
    } else {
        PreviousValues_[key] = value;
    }
    UnderlyingProfiler_->Enqueue(path, value, metricType, tagIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
