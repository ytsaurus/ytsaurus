#include "resource_metering.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TMeteringStatistics::TMeteringStatistics(TJobResources minShareResources, TJobResources allocatedResources, TJobMetrics jobMetrics)
    : MinShareResources_(std::move(minShareResources))
    , AllocatedResources_(std::move(allocatedResources))
    , JobMetrics_(std::move(jobMetrics))
{ }

TMeteringStatistics& TMeteringStatistics::operator+=(const TMeteringStatistics &other)
{
    MinShareResources_ += other.MinShareResources_;
    AllocatedResources_ += other.AllocatedResources_;
    JobMetrics_ += other.JobMetrics_;
    return *this;
}

TMeteringStatistics& TMeteringStatistics::operator-=(const TMeteringStatistics &other)
{
    MinShareResources_ -= other.MinShareResources_;
    AllocatedResources_ -= other.AllocatedResources_;
    JobMetrics_ -= other.JobMetrics_;
    return *this;
}

TMeteringStatistics operator+(const TMeteringStatistics& lhs, const TMeteringStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TMeteringStatistics operator-(const TMeteringStatistics& lhs, const TMeteringStatistics& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool TMeteringKey::operator==(const TMeteringKey& other) const
{
    return AbcId == other.AbcId && TreeId == other.TreeId && PoolId == other.PoolId;
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NScheduler::TMeteringKey>::operator()(const NYT::NScheduler::TMeteringKey& key) const
{
    size_t res = 0;
    NYT::HashCombine(res, key.AbcId);
    NYT::HashCombine(res, key.TreeId);
    NYT::HashCombine(res, key.PoolId);
    return res;
}