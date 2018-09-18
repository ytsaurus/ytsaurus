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
    const TIncarnationId& incarnationId,
    TExecNodePtr node,
    TInstant startTime,
    const TJobResources& resourceLimits,
    bool interruptible,
    TString treeId,
    NNodeTrackerClient::TNodeId revivalNodeId,
    TString revivalNodeAddress)
    : Id_(id)
    , Type_(type)
    , OperationId_(operationId)
    , IncarnationId_(incarnationId)
    , Node_(std::move(node))
    , RevivalNodeId_(revivalNodeId)
    , RevivalNodeAddress_(std::move(revivalNodeAddress))
    , StartTime_(startTime)
    , Interruptible_(interruptible)
    , TreeId_(std::move(treeId))
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
{ }

TDuration TJob::GetDuration() const
{
    return *FinishTime_ - StartTime_;
}

bool TJob::IsRevived() const
{
    return RevivalNodeId_ != NNodeTrackerClient::InvalidNodeId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
