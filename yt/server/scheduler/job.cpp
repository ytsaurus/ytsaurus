#include "stdafx.h"
#include "job.h"
#include "operation.h"
#include "exec_node.h"
#include "operation_controller.h"
#include "job_resources.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& id,
    EJobType type,
    TOperationPtr operation,
    TExecNodePtr node,
    TInstant startTime,
    const NProto::TNodeResources& resourceUsage,
    TJobSpecBuilder specBuilder)
    : Id_(id)
    , Type_(type)
    , Operation_(~operation)
    , Node_(node)
    , StartTime_(startTime)
    , State_(EJobState::Waiting)
    , ResourceUsage_(resourceUsage)
    , SpecBuilder_(MoveRV(specBuilder))
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

