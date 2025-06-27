#include "allocation.h"

#include "private.h"
#include "exec_node.h"
#include "helpers.h"
#include "operation.h"

namespace NYT::NScheduler {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TAllocation::TAllocation(
    TAllocationId id,
    TOperationId operationId,
    TIncarnationId incarnationId,
    TControllerEpoch controllerEpoch,
    TExecNodePtr node,
    TInstant startTime,
    const TAllocationStartDescriptor& startDescriptor,
    EPreemptionMode preemptionMode,
    TString treeId,
    int schedulingIndex,
    std::optional<EAllocationSchedulingStage> schedulingStage,
    std::optional<TNetworkPriority> networkPriority,
    NNodeTrackerClient::TNodeId revivalNodeId,
    std::string revivalNodeAddress)
    : Id_(id)
    , OperationId_(operationId)
    , IncarnationId_(incarnationId)
    , ControllerEpoch_(controllerEpoch)
    , Node_(std::move(node))
    , RevivalNodeId_(revivalNodeId)
    , RevivalNodeAddress_(std::move(revivalNodeAddress))
    , StartTime_(startTime)
    , TreeId_(std::move(treeId))
    , ResourceUsage_(startDescriptor.ResourceLimits.ToJobResources())
    , ResourceLimits_(startDescriptor.ResourceLimits.ToJobResources())
    , DiskQuota_(startDescriptor.ResourceLimits.DiskQuota())
    , AllocationAttributes_(startDescriptor.AllocationAttributes)
    , PreemptionMode_(preemptionMode)
    , SchedulingIndex_(schedulingIndex)
    , SchedulingStage_(schedulingStage)
    , NetworkPriority_(networkPriority)
    , Logger_(CreateLogger())
    , Codicil_(Format("AllocationId: %v, OperationId: %v",
        Id_,
        OperationId_))
{ }

void TAllocation::SetNode(const TExecNodePtr& node)
{
    Node_ = node;
    Logger_ = CreateLogger();
}

bool TAllocation::IsRevived() const
{
    return RevivalNodeId_ != NNodeTrackerClient::InvalidNodeId;
}

NLogging::TLogger TAllocation::CreateLogger()
{
    return SchedulerLogger().WithTag("AllocationId: %v, OperationId: %v, Address: %v",
        Id_,
        OperationId_,
        Node_ ? Node_->GetDefaultAddress() : "<unknown>");
}

TDuration TAllocation::GetPreemptibleProgressDuration() const
{
    if (PreemptibleProgressStartTime_) {
        return TInstant::Now() - PreemptibleProgressStartTime_;
    }

    return TDuration::Zero();
}

TCodicilGuard TAllocation::MakeCodicilGuard() const
{
    return TCodicilGuard(MakeOwningCodicilBuilder(Codicil_));

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
