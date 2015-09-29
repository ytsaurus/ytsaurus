#include "stdafx.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"
#include "operation_controller.h"
#include "job_resources.h"

#include <ytlib/node_tracker_client/helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient;
using NNodeTrackerClient::NProto::TNodeResources;

////////////////////////////////////////////////////////////////////

TExecNode::TExecNode(const TAddressMap& addresses)
    : Addresses_(addresses)
    , ResourceLimits_(ZeroNodeResources())
    , ResourceUsage_(ZeroNodeResources())
{ }

bool TExecNode::HasEnoughResources(const TNodeResources& neededResources) const
{
    return Dominates(
        ResourceLimits_,
        ResourceUsage_ + neededResources);
}

bool TExecNode::HasSpareResources(const TNodeResources& resourceDiscount) const
{
    return HasEnoughResources(MinSpareNodeResources() - resourceDiscount);
}

const Stroka& TExecNode::GetDefaultAddress()
{
    return NNodeTrackerClient::GetDefaultAddress(Addresses_);
}

bool TExecNode::CanSchedule(const TNullable<Stroka>& tag) const
{
    return !tag || SchedulingTags_.find(*tag) != SchedulingTags_.end();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

