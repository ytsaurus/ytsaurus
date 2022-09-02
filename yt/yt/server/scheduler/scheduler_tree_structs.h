#pragma once

#include "private.h"

#include <yt/yt/server/lib/scheduler/config.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// NB(eshcherbin): This file consists only of segment-specific structs.
// Remove or rename it, when segments are fully inside strategy.
struct TSetNodeSchedulingSegmentOptions
{
    NNodeTrackerClient::TNodeId NodeId = NNodeTrackerClient::InvalidNodeId;
    ESchedulingSegment Segment = ESchedulingSegment::Default;
};

using TSetNodeSchedulingSegmentOptionsList = std::vector<TSetNodeSchedulingSegmentOptions>;

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
