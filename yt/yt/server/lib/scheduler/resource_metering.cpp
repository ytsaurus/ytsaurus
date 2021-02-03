#include "resource_metering.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TMeteringStatistics::TMeteringStatistics(
    const TJobResources& strongGuaranteeResources,
    const TJobResources& resourceFlow,
    const TJobResources& burstGuaranteeResources,
    const TJobResources& allocatedResources)
    : StrongGuaranteeResources_(strongGuaranteeResources)
    , ResourceFlow_(resourceFlow)
    , BurstGuaranteeResources_(burstGuaranteeResources)
    , AllocatedResources_(allocatedResources)
{ }

void TMeteringStatistics::AccountChild(const TMeteringStatistics& child, bool isRoot)
{
    ResourceFlow_ += child.ResourceFlow_;
    BurstGuaranteeResources_ += child.BurstGuaranteeResources_;
    // NB: we have no specified strong guarantee resources at root and
    // therefore calculate unaccounted guarantees at root by this hack.
    if (isRoot) {
        StrongGuaranteeResources_ += child.StrongGuaranteeResources_;
    }
}

void TMeteringStatistics::DiscountChild(const TMeteringStatistics& child, bool isRoot)
{
    // See comment above.
    if (!isRoot) {
        StrongGuaranteeResources_ -= child.StrongGuaranteeResources_;
    }
}

TMeteringStatistics& TMeteringStatistics::operator+=(const TMeteringStatistics &other)
{
    StrongGuaranteeResources_ += other.StrongGuaranteeResources_;
    ResourceFlow_ += other.ResourceFlow_;
    BurstGuaranteeResources_ += other.BurstGuaranteeResources_;
    AllocatedResources_ += other.AllocatedResources_;
    return *this;
}

TMeteringStatistics& TMeteringStatistics::operator-=(const TMeteringStatistics &other)
{
    StrongGuaranteeResources_ -= other.StrongGuaranteeResources_;
    ResourceFlow_ -= other.ResourceFlow_;
    BurstGuaranteeResources_ -= other.BurstGuaranteeResources_;
    AllocatedResources_ -= other.AllocatedResources_;
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
