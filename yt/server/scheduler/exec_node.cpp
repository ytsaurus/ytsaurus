#include "stdafx.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"
#include "operation_controller.h"
#include "job_resources.h"


namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

TExecNode::TExecNode(const Stroka& address)
    : Address_(address)
    , ResourceLimits_(ZeroNodeResources())
    , ResourceUtilization_(ZeroNodeResources())
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

