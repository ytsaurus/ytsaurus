#pragma once

#include "job_metrics.h"

#include <yt/yt/ytlib/scheduler/job_resources.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TMeteringStatistics
{
    DEFINE_BYREF_RO_PROPERTY(TJobResources, StrongGuaranteeResources);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceFlow);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, BurstGuaranteeResources);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AllocatedResources);
    DEFINE_BYREF_RO_PROPERTY(TResourceVolume, AccumulatedResourceUsage);

public:
    TMeteringStatistics(
        const TJobResources& strongGuaranteeResources,
        const TJobResources& resourceFlow,
        const TJobResources& burstGuaranteeResources,
        const TJobResources& allocatedResources,
        const TResourceVolume& accumulatedResourceUsage);

    TMeteringStatistics& operator+=(const TMeteringStatistics& other);
    TMeteringStatistics& operator-=(const TMeteringStatistics& other);

    void AccountChild(const TMeteringStatistics& child);
    void DiscountChild(const TMeteringStatistics& child);
};

TMeteringStatistics operator+(const TMeteringStatistics& lhs, const TMeteringStatistics& rhs);
TMeteringStatistics operator-(const TMeteringStatistics& lhs, const TMeteringStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TMeteringKey
{
    // NB(mrkastep) Use negative AbcId as default in order to be able to log root pools without ABC
    // e.g. personal experimental pools.
    int AbcId;
    TString TreeId;
    TString PoolId;
    THashMap<TString, TString> MeteringTags;

    bool operator==(const TMeteringKey& other) const;
};

////////////////////////////////////////////////////////////////////////////////

using TMeteringMap = THashMap<TMeteringKey, TMeteringStatistics>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

template<>
struct THash<NYT::NScheduler::TMeteringKey>
{
    size_t operator()(const NYT::NScheduler::TMeteringKey& key) const;
};
