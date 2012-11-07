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
    TOperationPtr operation,
    TExecNodePtr node,
    TInstant startTime)
    : Id_(id)
    , Operation_(~operation)
    , Node_(node)
    , StartTime_(startTime)
    , State_(EJobState::Waiting)
    , Spec_(NULL)
    , ResourceUtilization_(ZeroNodeResources())
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

