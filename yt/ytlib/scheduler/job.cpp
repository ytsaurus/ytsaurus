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
    const NProto::TJobSpec& spec,
    TInstant startTime)
    : Id_(id)
    , Operation_(operation)
    , Node_(node)
    , Spec_(spec)
    , StartTime_(startTime)
    , State_(EJobState::Running)
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

