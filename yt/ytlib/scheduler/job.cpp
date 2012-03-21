#include "stdafx.h"
#include "job.h"
#include "operation.h"
#include "exec_node.h"

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
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

