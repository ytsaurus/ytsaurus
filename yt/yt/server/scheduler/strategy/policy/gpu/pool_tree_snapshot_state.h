#pragma once

#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

struct TAllocationSnapshotState
{
    TAllocationId AllocationId;
    TOperationId OperationId;
    NNodeTrackerClient::TNodeId NodeId = NNodeTrackerClient::InvalidNodeId;
    TJobResources ResourceUsage;
    bool Preemptible = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationSnapshotState
{
    bool Preemptible = false;
    bool Starving = false;
    bool Enabled = true;
    std::optional<std::string> SchedulingModule;
    int RealizedAssignmentCount = 0;
    int PreliminaryAssignmentCount = 0;

    std::vector<TAllocationId> AllocationIds;
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeSnapshotState
{
    std::optional<std::string> SchedulingModule;
    TJobResources AssignedResourceUsage;
    int AllocationsToPreemptCount = 0;
    int PreemptedAllocationsCount = 0;

    std::vector<TAllocationId> AllocationIds;
};

////////////////////////////////////////////////////////////////////////////////

using TOperationSnapshotStateMap = THashMap<TOperationId, TOperationSnapshotState>;
using TNodeSnapshotStateMap = THashMap<NNodeTrackerClient::TNodeId, TNodeSnapshotState>;
using TAllocationSnapshotStateMap = THashMap<TAllocationId, TAllocationSnapshotState>;

class TPoolTreeSnapshotStateImpl
    : public TPoolTreeSnapshotState
{
public:
    DEFINE_BYREF_RO_PROPERTY(TOperationSnapshotStateMap, OperationStates);
    DEFINE_BYREF_RO_PROPERTY(TNodeSnapshotStateMap, NodeStates);
    DEFINE_BYREF_RO_PROPERTY(TAllocationSnapshotStateMap, AllocationStates);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, SnapshotTime);

public:
    TPoolTreeSnapshotStateImpl(
        TOperationSnapshotStateMap operationStates,
        TNodeSnapshotStateMap nodeStates,
        TAllocationSnapshotStateMap allocationStates,
        TInstant snapshotTime);
};

////////////////////////////////////////////////////////////////////////////////

TPoolTreeSnapshotStateImpl* GetPoolTreeSnapshotState(const TPoolTreeSnapshotPtr& treeSnapshot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
