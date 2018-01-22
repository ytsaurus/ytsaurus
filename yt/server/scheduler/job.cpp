#include "private.h"
#include "job.h"
#include "exec_node.h"
#include "helpers.h"
#include "operation.h"

namespace NYT {
namespace NScheduler {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& id,
    EJobType type,
    const TOperationId& operationId,
    TExecNodePtr node,
    TInstant startTime,
    const TJobResources& resourceLimits,
    bool interruptible,
    const TString& treeId)
    : Id_(id)
    , Type_(type)
    , OperationId_(operationId)
    , Node_(std::move(node))
    , StartTime_(startTime)
    , Interruptible_(interruptible)
    , State_(EJobState::None)
    , TreeId_(treeId)
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
{ }

TDuration TJob::GetDuration() const
{
    return *FinishTime_ - StartTime_;
}

////////////////////////////////////////////////////////////////////////////////

TJobStatus JobStatusFromError(const TError& error)
{
    auto status = TJobStatus();
    ToProto(status.mutable_result()->mutable_error(), error);
    return status;
}

TJobId MakeJobId(NObjectClient::TCellTag tag, NNodeTrackerClient::TNodeId nodeId)
{
    return MakeId(
        EObjectType::SchedulerJob,
        tag,
        RandomNumber<ui64>(),
        nodeId);
}

NNodeTrackerClient::TNodeId NodeIdFromJobId(const TJobId& jobId)
{
    return jobId.Parts32[0];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
