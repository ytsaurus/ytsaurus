#pragma once

#include "private.h"

#include <yt/yt/server/lib/scheduler/config.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TTreeSchedulingSegmentsState
{
    ESegmentedSchedulingMode Mode = ESegmentedSchedulingMode::Disabled;
    ESchedulingSegmentModuleType ModuleType = ESchedulingSegmentModuleType::DataCenter;
    TDuration UnsatisfiedSegmentsRebalancingTimeout;
    bool ValidateInfinibandClusterTags;

    std::optional<EJobResourceType> KeyResource;
    double TotalKeyResourceAmount = 0.0;

    TSchedulingSegmentModuleList Modules;
    THashSet<TString> InfinibandClusters;
    TSegmentToResourceAmount FairResourceAmountPerSegment;
    TSegmentToFairShare FairSharePerSegment;
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationIdWithSchedulingSegmentModule
{
    TOperationId OperationId;
    TSchedulingSegmentModule Module;
};

using TOperationIdWithSchedulingSegmentModuleList = std::vector<TOperationIdWithSchedulingSegmentModule>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
