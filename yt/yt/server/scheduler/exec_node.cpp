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
    const TNodeDescriptor& nodeDescriptor,
    ENodeState state)
    : Id_(id)
    , NodeDescriptor_(nodeDescriptor)
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
    TReaderGuard guard(SpinLock_);

    return TExecNodeDescriptor(
        Id_,
        GetDefaultAddress(),
        IOWeight_,
        MasterState_ == NNodeTrackerClient::ENodeState::Online && SchedulerState_ == ENodeState::Online,
        ResourceUsage_,
        ResourceLimits_,
        Tags_);
}

void TExecNode::SetIOWeights(const THashMap<TString, double>& mediumToWeight)
{
    TWriterGuard guard(SpinLock_);
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
    TWriterGuard guard(SpinLock_);
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
        .Item("last_heartbeat_statistics").BeginMap()
            .Item("preemptable_job_count").Value(LastHeartbeatStatistics_.PreemptableJobCount)
            .Item("controller_scheduler_job_count").Value(LastHeartbeatStatistics_.ControllerScheduleJobCount)
            .Item("non_preemptive_scheduler_job_attempts").Value(LastHeartbeatStatistics_.NonPreemptiveScheduleJobAttempts)
            .Item("preemptive_scheduler_job_attempts").Value(LastHeartbeatStatistics_.PreemptiveScheduleJobAttempts)
            .Item("has_aggressively_starving_elements").Value(LastHeartbeatStatistics_.HasAggressivelyStarvingElements)
        .EndMap();
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

