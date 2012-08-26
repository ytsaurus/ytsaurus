#include "stdafx.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"
#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

TExecNode::TExecNode(const Stroka& address)
    : Address_(address)
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

