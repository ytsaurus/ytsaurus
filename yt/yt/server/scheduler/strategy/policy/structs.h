#pragma once

#include "private.h"
#include "scheduling_heartbeat_context.h"

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>

#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>
#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

struct TRunningAllocationStatistics
{
    //! In CPU*seconds.
    double TotalCpuTime = 0.0;
    double PreemptibleCpuTime = 0.0;

    //! In GPU*seconds.
    double TotalGpuTime = 0.0;
    double PreemptibleGpuTime = 0.0;
};

void FormatValue(TStringBuilderBase* builder, const TRunningAllocationStatistics& statistics, TStringBuf /*spec*/);
TString FormatRunningAllocationStatisticsCompact(const TRunningAllocationStatistics& statistics);
void Serialize(const TRunningAllocationStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TAllocationState
    : public NYTree::TYsonStructLite
{
    TOperationId OperationId;
    TJobResources ResourceLimits;
    EAllocationPreemptionStatus PreemptionStatus = EAllocationPreemptionStatus::NonPreemptible;

    REGISTER_YSON_STRUCT_LITE(TAllocationState);

    static void Register(TRegistrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeState final
{
    // NB: Descriptor may be missing if the node has only just registered and we haven't processed any heartbeats from it.
    TExecNodeDescriptorPtr Descriptor;

    ESchedulingSegment SchedulingSegment = ESchedulingSegment::Default;
    std::optional<ESchedulingSegment> SpecifiedSchedulingSegment;

    TScheduleAllocationsStatistics LastPreemptiveHeartbeatStatistics;
    TScheduleAllocationsStatistics LastNonPreemptiveHeartbeatStatistics;

    TRunningAllocationStatistics RunningAllocationStatistics;
    std::optional<NProfiling::TCpuInstant> LastRunningAllocationStatisticsUpdateTime;
    bool ForceRunningAllocationStatisticsUpdate = false;

    THashMap<TAllocationId, TAllocationState> RunningAllocations;
};

using TNodeStatePtr = TIntrusivePtr<TNodeState>;
using TNodeStateMap = THashMap<NNodeTrackerClient::TNodeId, TNodeStatePtr>;

////////////////////////////////////////////////////////////////////////////////

struct TOperationState final
{
    const TStrategyOperationSpecPtr Spec;
    const bool IsGang;

    // Initialized after operation's materialization, but should not be modified after that.
    std::optional<TJobResources> AggregatedInitialMinNeededResources;
    bool SingleAllocationVanillaOperation = false;

    std::optional<ESchedulingSegment> SchedulingSegment;
    TSchedulingSegmentModule SchedulingSegmentModule;
    std::optional<TNetworkPriority> NetworkPriority;
    std::optional<THashSet<TString>> SpecifiedSchedulingSegmentModules;
    std::optional<TInstant> FailingToScheduleAtModuleSince;
    std::optional<TInstant> FailingToAssignToModuleSince;

    TOperationState(
        TStrategyOperationSpecPtr spec,
        bool isGang);
};

using TOperationStatePtr = TIntrusivePtr<TOperationState>;
using TOperationStateMap = THashMap<TOperationId, TOperationStatePtr>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
