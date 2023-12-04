#include "allocation.h"

#include "private.h"
#include "exec_node.h"
#include "helpers.h"
#include "operation.h"

namespace NYT::NScheduler {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    TJobId id,
    TOperationId operationId,
    TIncarnationId incarnationId,
    TControllerEpoch controllerEpoch,
    TExecNodePtr node,
    TInstant startTime,
    const TJobResources& resourceLimits,
    const TDiskQuota& diskQuota,
    EPreemptionMode preemptionMode,
    TString treeId,
    int schedulingIndex,
    std::optional<EJobSchedulingStage> schedulingStage,
    NNodeTrackerClient::TNodeId revivalNodeId,
    TString revivalNodeAddress)
    : Id_(id)
    , OperationId_(operationId)
    , IncarnationId_(incarnationId)
    , ControllerEpoch_(controllerEpoch)
    , Node_(std::move(node))
    , RevivalNodeId_(revivalNodeId)
    , RevivalNodeAddress_(std::move(revivalNodeAddress))
    , StartTime_(startTime)
    , TreeId_(std::move(treeId))
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
    , DiskQuota_(diskQuota)
    , PreemptionMode_(preemptionMode)
    , SchedulingIndex_(schedulingIndex)
    , SchedulingStage_(schedulingStage)
    , Logger_(CreateLogger())
    , CodicilString_(Format("JobId: %v, OperationId: %v", Id_, OperationId_))
{ }

void TJob::SetNode(const TExecNodePtr& node)
{
    Node_ = node;
    Logger_ = CreateLogger();
}

bool TJob::IsRevived() const
{
    return RevivalNodeId_ != NNodeTrackerClient::InvalidNodeId;
}

NLogging::TLogger TJob::CreateLogger()
{
    return SchedulerLogger.WithTag("JobId: %v, OperationId: %v, Address: %v",
        Id_,
        OperationId_,
        Node_ ? Node_->GetDefaultAddress() : "<unknown>");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
