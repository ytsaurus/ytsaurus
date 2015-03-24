#include "stdafx.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"
#include "operation_controller.h"
#include "job_resources.h"

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient;
using NNodeTrackerClient::TNodeDescriptor;
using NNodeTrackerClient::NProto::TNodeResources;

////////////////////////////////////////////////////////////////////

TExecNode::TExecNode(const TNodeDescriptor& descriptor)
    : Descriptor_(descriptor)
    , ResourceLimits_(ZeroNodeResources())
    , ResourceUsage_(ZeroNodeResources())
{ }

bool TExecNode::HasEnoughResources(const TNodeResources& neededResources) const
{
    return Dominates(
        ResourceLimits_ + ResourceUsageDiscount_,
        ResourceUsage_ + neededResources);
}

bool TExecNode::HasSpareResources() const
{
    return HasEnoughResources(MinSpareNodeResources());
}

const Stroka& TExecNode::GetAddress() const
{
    return Descriptor_.GetDefaultAddress();
}

bool TExecNode::CanSchedule(const TNullable<Stroka>& tag) const
{
    return !tag || SchedulingTags_.find(*tag) != SchedulingTags_.end();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

