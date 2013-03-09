#include "stdafx.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"
#include "operation_controller.h"
#include "job_resources.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

TExecNode::TExecNode(const NChunkClient::TNodeDescriptor& descriptor)
    : Descriptor_(descriptor)
    , ResourceLimits_(ZeroNodeResources())
    , ResourceUsage_(ZeroNodeResources())
{ }

bool TExecNode::HasEnoughResources(const NProto::TNodeResources& neededResources) const
{
    return Dominates(
        ResourceLimits_ + ResourceUsageDiscount_,
        ResourceUsage_ + neededResources);
}

bool TExecNode::HasSpareResources() const
{
    return HasEnoughResources(LowWatermarkNodeResources());
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

