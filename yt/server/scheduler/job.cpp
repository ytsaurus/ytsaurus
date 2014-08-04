#include "stdafx.h"
#include "job.h"
#include "operation.h"
#include "exec_node.h"
#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& id,
    EJobType type,
    TOperationPtr operation,
    TExecNodePtr node,
    TInstant startTime,
    const TNodeResources& resourceLimits,
    TJobSpecBuilder specBuilder)
    : Id_(id)
    , Type_(type)
    , Operation_(operation.Get())
    , Node_(node)
    , StartTime_(startTime)
    , State_(EJobState::Waiting)
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
    , SpecBuilder_(std::move(specBuilder))
    , Preemptable_(false)
{ }

TDuration TJob::GetDuration() const
{
    return *FinishTime_ - StartTime_;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

