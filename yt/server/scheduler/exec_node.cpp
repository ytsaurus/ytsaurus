#include "exec_node.h"
#include "job_resources.h"

#include <yt/ytlib/node_tracker_client/helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////

TExecNode::TExecNode(
    TNodeId id,
    const TNodeDescriptor& nodeDescriptor)
    : Id_(id)
    , MasterState_(ENodeState::Offline)
    , HasOngoingHeartbeat_(false)
    , HasPendingUnregistration_(false)
    , DefaultAddress_(nodeDescriptor.GetDefaultAddress())
{
    UpdateNodeDescriptor(nodeDescriptor);
}

bool TExecNode::HasEnoughResources(const TJobResources& neededResources) const
{
    return Dominates(
        ResourceLimits_,
        ResourceUsage_ + neededResources);
}

bool TExecNode::HasSpareResources(const TJobResources& resourceDiscount) const
{
    return HasEnoughResources(MinSpareNodeResources() - resourceDiscount);
}

const Stroka& TExecNode::GetDefaultAddress() const
{
    return DefaultAddress_;
}

const Stroka& TExecNode::GetInterconnectAddress() const
{
    return InterconnectAddress_;
}

bool TExecNode::CanSchedule(const TNullable<Stroka>& tag) const
{
    return !tag || SchedulingTags_.find(*tag) != SchedulingTags_.end();
}

TExecNodeDescriptor TExecNode::BuildExecDescriptor() const
{
    TReaderGuard guard(SpinLock_);
    return TExecNodeDescriptor{
        Id_,
        DefaultAddress_,
        IOWeight_,
        ResourceLimits_
    };
}

double TExecNode::GetIOWeight() const
{
    return IOWeight_;
}

void TExecNode::SetIOWeight(double value)
{
    TWriterGuard guard(SpinLock_);
    IOWeight_ = value;
}

const TJobResources& TExecNode::GetResourceLimits() const
{
    return ResourceLimits_;
}

void TExecNode::SetResourceLimits(const TJobResources& value)
{
    TWriterGuard guard(SpinLock_);
    ResourceLimits_ = value;
}

const TJobResources& TExecNode::GetResourceUsage() const
{
    return ResourceUsage_;
}

void TExecNode::SetResourceUsage(const TJobResources& value)
{
    // NB: No locking is needed since ResourceUsage_ is not used
    // in BuildExecDescriptor.
    ResourceUsage_ = value;
}

void TExecNode::UpdateNodeDescriptor(const NNodeTrackerClient::TNodeDescriptor& nodeDescriptor)
{
    InterconnectAddress_ = nodeDescriptor.GetInterconnectAddress();
}

////////////////////////////////////////////////////////////////////

TExecNodeDescriptor::TExecNodeDescriptor()
{ }

TExecNodeDescriptor::TExecNodeDescriptor(
    NNodeTrackerClient::TNodeId id,
    Stroka address,
    double ioWeight,
    TJobResources resourceLimits)
    : Id(id)
    , Address(address)
    , IOWeight(ioWeight)
    , ResourceLimits(resourceLimits)
{ }

void TExecNodeDescriptor::Persist(TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Id);
    Persist(context, Address);
    Persist(context, IOWeight);
    Persist(context, ResourceLimits);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

