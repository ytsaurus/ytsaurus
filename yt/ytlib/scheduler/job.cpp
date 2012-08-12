#include "stdafx.h"
#include "job.h"
#include "operation.h"
#include "exec_node.h"
#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& id,
    TOperation* operation,
    TExecNodePtr node,
    TInstant startTime)
    : Id_(id)
    , Operation_(operation)
    , Node_(node)
    , StartTime_(startTime)
    , State_(EJobState::Running)
    , Spec_(NULL)
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

