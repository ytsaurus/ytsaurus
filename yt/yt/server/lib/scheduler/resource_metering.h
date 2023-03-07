#pragma once

#include "job_metrics.h"

#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TMeteringStatistics
{
    DEFINE_BYREF_RO_PROPERTY(TJobResources, MinShareResources);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AllocatedResources);
    DEFINE_BYREF_RO_PROPERTY(TJobMetrics, JobMetrics);

public:
    TMeteringStatistics(TJobResources minShareResources, TJobResources allocatedResources, TJobMetrics jobMetrics);

    TMeteringStatistics& operator+=(const TMeteringStatistics& other);
    TMeteringStatistics& operator-=(const TMeteringStatistics& other);
};

TMeteringStatistics operator+(const TMeteringStatistics& lhs, const TMeteringStatistics& rhs);
TMeteringStatistics operator-(const TMeteringStatistics& lhs, const TMeteringStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TMeteringKey
{
    // NB(mrkastep) Use negative AbcId as default in order to be able to log root pools without ABC
    // e.g. p   ersonal experimental pools.
    int AbcId;
    TString TreeId;
    TString PoolId;

    bool operator==(const TMeteringKey& other) const;
};

////////////////////////////////////////////////////////////////////////////////

using TMeteringMap = THashMap<TMeteringKey, TMeteringStatistics>;

////////////////////////////////////////////////////////////////////////////////

} // NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

template<>
struct THash<NYT::NScheduler::TMeteringKey>
{
    size_t operator()(const NYT::NScheduler::TMeteringKey& key) const;
};
