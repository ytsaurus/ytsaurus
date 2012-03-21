#include "stdafx.h"
#include "job.h"
#include "operation.h"
#include "exec_node.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& jobId,
    TOperationPtr operation,
    TExecNodePtr node,
    TInstant startTime)
    : JobId_(jobId)
    , Operation_(operation)
    , Node_(node)
    , StartTime_(startTime)
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

