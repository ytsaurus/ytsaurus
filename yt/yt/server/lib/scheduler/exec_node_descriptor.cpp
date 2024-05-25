#include "exec_node_descriptor.h"

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/disk_resources.h>
#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NScheduler {

using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TExecNodeDescriptor::TExecNodeDescriptor(
    NNodeTrackerClient::TNodeId id,
    TString address,
    std::optional<TString> dataCenter,
    double ioWeight,
    bool online,
    const TJobResources& resourceUsage,
    const TJobResources& resourceLimits,
    const TDiskResources& diskResources,
    const TBooleanFormulaTags& tags,
    std::optional<TString> infinibandCluster,
    IAttributeDictionaryPtr schedulingOptions)
    : Id(id)
    , Address(std::move(address))
    , DataCenter(std::move(dataCenter))
    , IOWeight(ioWeight)
    , Online(online)
    , ResourceUsage(resourceUsage)
    , ResourceLimits(resourceLimits)
    , DiskResources(diskResources)
    , Tags(tags)
    , InfinibandCluster(std::move(infinibandCluster))
    , SchedulingOptions(std::move(schedulingOptions))
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
    Persist(context, DiskResources);
    Persist(context, Tags);
}

void ToProto(NScheduler::NProto::TExecNodeDescriptor* protoDescriptor, const NScheduler::TExecNodeDescriptor& descriptor)
{
    protoDescriptor->set_node_id(ToProto<ui32>(descriptor.Id));
    protoDescriptor->set_address(descriptor.Address);
    protoDescriptor->set_io_weight(descriptor.IOWeight);
    protoDescriptor->set_online(descriptor.Online);
    ToProto(protoDescriptor->mutable_resource_limits(), descriptor.ResourceLimits);
    ToProto(protoDescriptor->mutable_disk_resources(), descriptor.DiskResources);
    for (const auto& tag : descriptor.Tags.GetSourceTags()) {
        protoDescriptor->add_tags(tag);
    }
}

void FromProto(NScheduler::TExecNodeDescriptor* descriptor, const NScheduler::NProto::TExecNodeDescriptor& protoDescriptor)
{
    descriptor->Id = FromProto<NNodeTrackerClient::TNodeId>(protoDescriptor.node_id());
    descriptor->Address = protoDescriptor.address();
    descriptor->IOWeight = protoDescriptor.io_weight();
    descriptor->Online = protoDescriptor.online();
    FromProto(&descriptor->ResourceLimits, protoDescriptor.resource_limits());
    FromProto(&descriptor->DiskResources, protoDescriptor.disk_resources());
    THashSet<TString> tags;
    for (const auto& tag : protoDescriptor.tags()) {
        tags.insert(tag);
    }
    descriptor->Tags = TBooleanFormulaTags(std::move(tags));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

