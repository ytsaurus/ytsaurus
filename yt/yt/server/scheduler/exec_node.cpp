#include "exec_node.h"

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

namespace NYT::NScheduler {

using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TExecNode::TExecNode(
    TNodeId id,
    TNodeDescriptor nodeDescriptor,
    ENodeState state)
    : Id_(id)
    , NodeDescriptor_(std::move(nodeDescriptor))
    , MasterState_(NNodeTrackerClient::ENodeState::Offline)
    , SchedulerState_(state)
    , HasOngoingHeartbeat_(false)
    , HasPendingUnregistration_(false)
{ }

const TString& TExecNode::GetDefaultAddress() const
{
    return NodeDescriptor_.GetDefaultAddress();
}

bool TExecNode::CanSchedule(const TSchedulingTagFilter& filter) const
{
    return filter.IsEmpty() || filter.CanSchedule(Tags_);
}

TExecNodeDescriptorPtr TExecNode::BuildExecDescriptor() const
{
    auto guard = ReaderGuard(SpinLock_);

    return New<TExecNodeDescriptor>(
        Id_,
        GetDefaultAddress(),
        NodeDescriptor_.GetDataCenter(),
        IOWeight_,
        MasterState_ == NNodeTrackerClient::ENodeState::Online && SchedulerState_ == ENodeState::Online,
        ResourceUsage_,
        ResourceLimits_,
        DiskResources_,
        Tags_,
        InfinibandCluster_,
        SchedulingOptions_);
}

void TExecNode::SetIOWeights(const THashMap<TString, double>& mediumToWeight)
{
    auto guard = WriterGuard(SpinLock_);
    // NB: surely, something smarter than this should be done with individual medium weights here.
    IOWeight_ = 0.0;
    for (const auto& [medium, weight] : mediumToWeight) {
        IOWeight_ += weight;
    }
}

const TJobResources& TExecNode::GetResourceLimits() const
{
    return ResourceLimits_;
}

void TExecNode::SetResourceLimits(const TJobResources& value)
{
    auto guard = WriterGuard(SpinLock_);
    ResourceLimits_ = value;
}

const TJobResources& TExecNode::GetResourceUsage() const
{
    return ResourceUsage_;
}


const TDiskResources& TExecNode::GetDiskResources() const
{
    return DiskResources_;
}


void TExecNode::SetResourceUsage(const TJobResources& value)
{
    // NB: No locking is needed since ResourceUsage_ is not used
    // in BuildExecDescriptor.
    ResourceUsage_ = value;
}

void TExecNode::SetDiskResources(TDiskResources value)
{
    DiskResources_ = std::move(value);
}

void TExecNode::SetTags(TBooleanFormulaTags tags)
{
    Tags_ = std::move(tags);
    MatchingTreeCookie_ = {};
}

void TExecNode::BuildAttributes(TFluentMap fluent)
{
    auto oldState = MasterState_;
    if (SchedulerState_ != ENodeState::Online) {
        oldState = NNodeTrackerClient::ENodeState::Offline;
    }

    fluent
        .Item("scheduler_state").Value(SchedulerState_)
        .Item("scheduling_heartbeat_complexity").Value(SchedulingHeartbeatComplexity_)
        .Item("master_state").Value(MasterState_)
        .Item("state").Value(oldState)
        .Item("resource_usage").Value(ResourceUsage_)
        .Item("resource_limits").Value(ResourceLimits_)
        .Item("tags").Value(Tags_)
        .Item("data_center").Value(NodeDescriptor_.GetDataCenter())
        .Item("infiniband_cluster").Value(InfinibandCluster_)
        .Item("last_non_preemptive_heartbeat_statistics").Value(LastNonPreemptiveHeartbeatStatistics_)
        .Item("last_preemptive_heartbeat_statistics").Value(LastPreemptiveHeartbeatStatistics_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

