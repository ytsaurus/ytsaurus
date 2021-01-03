#include "exec_node.h"

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/ytlib/scheduler/job_resources.h>

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

TExecNodeDescriptor TExecNode::BuildExecDescriptor() const
{
    auto guard = ReaderGuard(SpinLock_);

    return TExecNodeDescriptor(
        Id_,
        GetDefaultAddress(),
        NodeDescriptor_.GetDataCenter(),
        IOWeight_,
        MasterState_ == NNodeTrackerClient::ENodeState::Online && SchedulerState_ == ENodeState::Online,
        ResourceUsage_,
        ResourceLimits_,
        Tags_,
        RunningJobStatistics_,
        SchedulingSegment_,
        SchedulingSegmentFrozen_);
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


const NNodeTrackerClient::NProto::TDiskResources& TExecNode::GetDiskResources() const
{
    return DiskResources_;
}


void TExecNode::SetResourceUsage(const TJobResources& value)
{
    // NB: No locking is needed since ResourceUsage_ is not used
    // in BuildExecDescriptor.
    ResourceUsage_ = value;
}

void TExecNode::SetDiskResources(const NNodeTrackerClient::NProto::TDiskResources& value)
{
    DiskResources_ = value;
}

void TExecNode::BuildAttributes(TFluentMap fluent)
{
    auto oldState = MasterState_;
    if (SchedulerState_ != ENodeState::Online) {
        oldState = NNodeTrackerClient::ENodeState::Offline;
    }

    fluent
        .Item("scheduler_state").Value(SchedulerState_)
        .Item("master_state").Value(MasterState_)
        .Item("state").Value(oldState)
        .Item("resource_usage").Value(ResourceUsage_)
        .Item("resource_limits").Value(ResourceLimits_)
        .Item("tags").Value(Tags_)
        .Item("scheduling_segment").Value(SchedulingSegment_)
        .Item("data_center").Value(NodeDescriptor_.GetDataCenter())
        .Item("last_non_preemptive_heartbeat_statistics").Value(LastNonPreemptiveHeartbeatStatistics_)
        .Item("last_preemptive_heartbeat_statistics").Value(LastPreemptiveHeartbeatStatistics_)
        .Item("running_job_statistics").BeginMap()
            .Item("total_cpu_time").Value(RunningJobStatistics_.TotalCpuTime)
            .Item("total_gpu_time").Value(RunningJobStatistics_.TotalGpuTime)
        .EndMap();
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

