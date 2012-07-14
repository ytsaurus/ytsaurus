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
    EJobType type,
    TOperation* operation,
    TExecNodePtr node,
    TInstant startTime)
    : Id_(id)
    , Type_(type)
    , Operation_(operation)
    , Node_(node)
    , StartTime_(startTime)
    , State_(EJobState::Running)
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

