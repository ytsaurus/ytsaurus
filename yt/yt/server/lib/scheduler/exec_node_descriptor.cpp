#include "exec_node_descriptor.h"

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/ytlib/scheduler/job_resources.h>

namespace NYT::NScheduler {

using namespace NNodeTrackerClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TExecNodeDescriptor::TExecNodeDescriptor(
    NNodeTrackerClient::TNodeId id,
    const TString& address,
    double ioWeight,
    bool online,
    const TJobResources& resourceUsage,
    const TJobResources& resourceLimits,
    const THashSet<TString>& tags)
    : Id(id)
    , Address(address)
    , IOWeight(ioWeight)
    , Online(online)
    , ResourceUsage(resourceUsage)
    , ResourceLimits(resourceLimits)
    , Tags(tags)
{ }

bool TExecNodeDescriptor::CanSchedule(const TSchedulingTagFilter& filter) const
{
    return Online && (filter.IsEmpty() || filter.CanSchedule(Tags));
}

void TExecNodeDescriptor::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Id);
    Persist(context, Address);
    Persist(context, IOWeight);
    Persist(context, Online);
    Persist(context, ResourceLimits);
    Persist(context, Tags);
}

void ToProto(NScheduler::NProto::TExecNodeDescriptor* protoDescriptor, const NScheduler::TExecNodeDescriptor& descriptor)
{
    protoDescriptor->set_node_id(descriptor.Id);
    protoDescriptor->set_address(descriptor.Address);
    protoDescriptor->set_io_weight(descriptor.IOWeight);
    protoDescriptor->set_online(descriptor.Online);
    ToProto(protoDescriptor->mutable_resource_limits(), descriptor.ResourceLimits);
    for (const auto& tag : descriptor.Tags) {
        protoDescriptor->add_tags(tag);
    }
}

void FromProto(NScheduler::TExecNodeDescriptor* descriptor, const NScheduler::NProto::TExecNodeDescriptor& protoDescriptor)
{
    descriptor->Id = protoDescriptor.node_id();
    descriptor->Address = protoDescriptor.address();
    descriptor->IOWeight = protoDescriptor.io_weight();
    descriptor->Online = protoDescriptor.online();
    FromProto(&descriptor->ResourceLimits, protoDescriptor.resource_limits());
    for (const auto& tag : protoDescriptor.tags()) {
        descriptor->Tags.insert(tag);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

