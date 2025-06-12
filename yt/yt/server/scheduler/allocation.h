#pragma once

#include "public.h"
#include "exec_node.h"

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/codicil.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

#include <optional>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TAllocation
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TAllocationId, Id);

    //! The id of operation the allocation belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, OperationId);

    //! The incarnation of the controller agent responsible for this allocation.
    DEFINE_BYVAL_RO_PROPERTY(TIncarnationId, IncarnationId);

    //! The epoch of the controller of operation allocation belongs to.
    DEFINE_BYVAL_RW_PROPERTY(TControllerEpoch, ControllerEpoch);

    //! Exec node where the allocation is running.
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);

    //! Node id obtained from corresponding allocation during the revival process.
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerClient::TNodeId, RevivalNodeId, NNodeTrackerClient::InvalidNodeId);

    //! Node address obtained from corresponding allocation during the revival process.
    DEFINE_BYVAL_RO_PROPERTY(std::string, RevivalNodeAddress);

    //! The time when the allocation was created.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

    //! True if allocation was already unregistered.
    DEFINE_BYVAL_RW_PROPERTY(bool, Unregistered, false);

    //! Current state of the allocation.
    DEFINE_BYVAL_RW_PROPERTY(EAllocationState, State, EAllocationState::Scheduled);

    //! Fair-share tree this allocation belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TString, TreeId);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(TDiskQuota, DiskQuota);
    DEFINE_BYREF_RO_PROPERTY(TAllocationAttributes, AllocationAttributes);

    //! Temporary flag used during heartbeat allocations processing to mark found allocations.
    DEFINE_BYVAL_RW_PROPERTY(bool, FoundOnNode);

    //! Preemption mode which says how to preempt allocation.
    DEFINE_BYVAL_RO_PROPERTY(EPreemptionMode, PreemptionMode);

    //! Index of operation when allocation was scheduled.
    DEFINE_BYVAL_RO_PROPERTY(int, SchedulingIndex);

    //! Stage allocation was scheduled at.
    DEFINE_BYVAL_RO_PROPERTY(std::optional<EAllocationSchedulingStage>, SchedulingStage);

    //! String describing preemption reason.
    DEFINE_BYVAL_RW_PROPERTY(TString, PreemptionReason);

    //! Preemptor allocation id and operation id.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TPreemptedFor>, PreemptedFor);

    //! Preemptor operation was starvation status corresponded to the preemptive scheduling stage type.
    DEFINE_BYVAL_RW_PROPERTY(bool, PreemptedForProperlyStarvingOperation, false);

    //! Is preemption requested for allocation.
    DEFINE_BYVAL_RW_PROPERTY(bool, Preempted, false);

    //! Timeout for allocation to be preempted (considering by node).
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuDuration, PreemptionTimeout, 0);

    //! Deadline for running allocation.
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuInstant, RunningAllocationUpdateDeadline, 0);

    DEFINE_BYVAL_RO_PROPERTY(std::optional<TNetworkPriority>, NetworkPriority);

    //! Logger for this allocation.
    DEFINE_BYREF_RO_PROPERTY(NLogging::TLogger, Logger);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, PreemptibleProgressStartTime);

public:
    TAllocation(
        TAllocationId id,
        TOperationId operationId,
        TIncarnationId incarnationId,
        TControllerEpoch controllerEpoch,
        TExecNodePtr node,
        TInstant startTime,
        const TAllocationStartDescriptor& startDescriptor,
        EPreemptionMode preemptionMode,
        TString treeId,
        int schedulingIndex,
        std::optional<EAllocationSchedulingStage> schedulingStage = std::nullopt,
        std::optional<TNetworkPriority> networkPriority = std::nullopt,
        NNodeTrackerClient::TNodeId revivalNodeId = NNodeTrackerClient::InvalidNodeId,
        std::string revivalNodeAddress = {});

    //! Returns true if the job was revived.
    bool IsRevived() const;

    void SetNode(const TExecNodePtr& node);

    //! Duration of progress that will be wasted if allocation is preempted.
    TDuration GetPreemptibleProgressDuration() const;

    TCodicilGuard MakeCodicilGuard() const;

private:
    const std::string Codicil_;

    NLogging::TLogger CreateLogger();
};

DEFINE_REFCOUNTED_TYPE(TAllocation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
