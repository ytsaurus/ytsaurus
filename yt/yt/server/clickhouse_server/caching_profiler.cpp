#include "caching_profiler.h"

namespace NYT::NClickHouseServer {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration SameValueUpdatePeriod = TDuration::Minutes(1);

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
    auto now = TInstant::Now();
    auto it = PreviousValues_.find(key);
    if (it != PreviousValues_.end()) {
        if (it->second.Value == value && now - it->second.Instant < SameValueUpdatePeriod) {
            return;
        }
        it->second = {value, now};
    } else {
        PreviousValues_[key] = {value, now};
    }
    UnderlyingProfiler_->Enqueue(path, value, metricType, tagIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
