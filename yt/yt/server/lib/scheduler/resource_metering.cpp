#include "resource_metering.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TMeteringStatistics::TMeteringStatistics(
    const TJobResources& strongGuaranteeResources,
    const TJobResources& resourceFlow,
    const TJobResources& burstGuaranteeResources,
    const TJobResources& allocatedResources,
    const TResourceVolume& accumulatedResourceUsage)
    : StrongGuaranteeResources_(strongGuaranteeResources)
    , ResourceFlow_(resourceFlow)
    , BurstGuaranteeResources_(burstGuaranteeResources)
    , AllocatedResources_(allocatedResources)
    , AccumulatedResourceUsage_(accumulatedResourceUsage)
{ }

void TMeteringStatistics::AccountChild(const TMeteringStatistics& child)
{
    ResourceFlow_ += child.ResourceFlow_;
    BurstGuaranteeResources_ += child.BurstGuaranteeResources_;
}

void TMeteringStatistics::DiscountChild(const TMeteringStatistics& child)
{
    AllocatedResources_ -= child.AllocatedResources_;
    AccumulatedResourceUsage_ -= child.AccumulatedResourceUsage_;
    StrongGuaranteeResources_ -= child.StrongGuaranteeResources_;

    // NB(eshcherbin): Due to computation errors, allocated resource usage may be a negative epsilon. See: YT-17435.
    AccumulatedResourceUsage_ = Max(AccumulatedResourceUsage_, TResourceVolume());
}

TMeteringStatistics& TMeteringStatistics::operator+=(const TMeteringStatistics &other)
{
    StrongGuaranteeResources_ += other.StrongGuaranteeResources_;
    ResourceFlow_ += other.ResourceFlow_;
    BurstGuaranteeResources_ += other.BurstGuaranteeResources_;
    AllocatedResources_ += other.AllocatedResources_;
    AccumulatedResourceUsage_ += other.AccumulatedResourceUsage_;
    return *this;
}

TMeteringStatistics& TMeteringStatistics::operator-=(const TMeteringStatistics &other)
{
    StrongGuaranteeResources_ -= other.StrongGuaranteeResources_;
    ResourceFlow_ -= other.ResourceFlow_;
    BurstGuaranteeResources_ -= other.BurstGuaranteeResources_;
    AllocatedResources_ -= other.AllocatedResources_;
    AccumulatedResourceUsage_ -= other.AccumulatedResourceUsage_;
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
    return AbcId == other.AbcId && TreeId == other.TreeId && PoolId == other.PoolId && MeteringTags == other.MeteringTags;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NScheduler::TMeteringKey>::operator()(const NYT::NScheduler::TMeteringKey& key) const
{
    size_t res = 0;
    NYT::HashCombine(res, key.AbcId);
    NYT::HashCombine(res, key.TreeId);
    NYT::HashCombine(res, key.PoolId);

    std::vector<std::pair<TString, TString>> sortedMeteringTags;
    for (const auto& pair : key.MeteringTags) {
        sortedMeteringTags.push_back(pair);
    }
    std::sort(sortedMeteringTags.begin(), sortedMeteringTags.end());

    for (const auto& [key, value] : sortedMeteringTags) {
        NYT::HashCombine(res, key);
        NYT::HashCombine(res, value);
    }

    return res;
}
