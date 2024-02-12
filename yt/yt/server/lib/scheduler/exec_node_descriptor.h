#pragma once

#include "public.h"
#include "scheduling_tag.h"

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/scheduler/proto/scheduler_service.pb.h>
#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/concurrency/lease_manager.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! An immutable snapshot of TExecNode.
struct TExecNodeDescriptor
    : public TRefCounted
{
    TExecNodeDescriptor() = default;

    TExecNodeDescriptor(
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
        NYTree::IAttributeDictionaryPtr schedulingOptions);

    bool CanSchedule(const TSchedulingTagFilter& filter) const;

    NNodeTrackerClient::TNodeId Id = NNodeTrackerClient::InvalidNodeId;
    TString Address;
    std::optional<TString> DataCenter;
    double IOWeight = 0.0;
    bool Online = false;
    TJobResources ResourceUsage;
    TJobResources ResourceLimits;
    TDiskResources DiskResources;
    TBooleanFormulaTags Tags;
    std::optional<TString> InfinibandCluster;
    NYTree::IAttributeDictionaryPtr SchedulingOptions;

    void Persist(const TStreamPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TExecNodeDescriptor)

void ToProto(NScheduler::NProto::TExecNodeDescriptor* protoDescriptor, const NScheduler::TExecNodeDescriptor& descriptor);
void FromProto(NScheduler::TExecNodeDescriptor* descriptor, const NScheduler::NProto::TExecNodeDescriptor& protoDescriptor);

////////////////////////////////////////////////////////////////////////////////

//! An immutable ref-counted map of TExecNodeDescriptor-s.
struct TRefCountedExecNodeDescriptorMap final
    : public TExecNodeDescriptorMap
{ };

DEFINE_REFCOUNTED_TYPE(TRefCountedExecNodeDescriptorMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
