#pragma once

#include "public.h"
#include "exec_node.h"

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/phoenix.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

#include <optional>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TJobId, Id);

    //! The id of operation the job belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, OperationId);

    //! The incarnation of the controller agent responsible for this job.
    DEFINE_BYVAL_RO_PROPERTY(TIncarnationId, IncarnationId);

    //! The epoch of the controller of operation job belongs to.
    DEFINE_BYVAL_RW_PROPERTY(TControllerEpoch, ControllerEpoch);

    //! Exec node where the job is running.
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);

    //! Node id obtained from corresponding joblet during the revival process.
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerClient::TNodeId, RevivalNodeId, NNodeTrackerClient::InvalidNodeId);

    //! Node address obtained from corresponding joblet during the revival process.
    DEFINE_BYVAL_RO_PROPERTY(TString, RevivalNodeAddress);

    //! The time when the job was started.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

    //! True if job can be interrupted.
    DEFINE_BYVAL_RO_PROPERTY(bool, Interruptible);

    //! True if job was already unregistered.
    DEFINE_BYVAL_RW_PROPERTY(bool, Unregistered, false);

    //! Current state of the allocation.
    DEFINE_BYVAL_RW_PROPERTY(EAllocationState, AllocationState, EAllocationState::Scheduled);

    //! Fair-share tree this job belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TString, TreeId);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(TDiskQuota, DiskQuota);

    //! Temporary flag used during heartbeat jobs processing to mark found jobs.
    DEFINE_BYVAL_RW_PROPERTY(bool, FoundOnNode);

    //! Preemption mode which says how to preempt job.
    DEFINE_BYVAL_RO_PROPERTY(EPreemptionMode, PreemptionMode);

    //! Index of operation when job was scheduled.
    DEFINE_BYVAL_RO_PROPERTY(int, SchedulingIndex);

    //! Stage job was scheduled at.
    DEFINE_BYVAL_RO_PROPERTY(std::optional<EJobSchedulingStage>, SchedulingStage);

    //! String describing preemption reason.
    DEFINE_BYVAL_RW_PROPERTY(TString, PreemptionReason);

    //! Preemptor job id and operation id.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TPreemptedFor>, PreemptedFor);

    //! Preemptor operation was starvation status corresponded to the preemptive scheduling stage type.
    DEFINE_BYVAL_RW_PROPERTY(bool, PreemptedForProperlyStarvingOperation, false);

    //! The purpose of the job interruption.
    DEFINE_BYVAL_RW_PROPERTY(EInterruptReason, InterruptionReason, EInterruptReason::None);

    //! Timeout for job to be interrupted (considering by node).
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuDuration, InterruptionTimeout, 0);

    //! Deadline for running job.
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuInstant, RunningJobUpdateDeadline, 0);

    //! Logger for this allocation.
    DEFINE_BYREF_RO_PROPERTY(NLogging::TLogger, Logger);

    DEFINE_BYREF_RO_PROPERTY(TString, CodicilString);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, PreemptibleProgressStartTime);

public:
    TJob(
        TJobId id,
        TOperationId operationId,
        TIncarnationId incarnationId,
        TControllerEpoch controllerEpoch,
        TExecNodePtr node,
        TInstant startTime,
        const TJobResources& resourceLimits,
        const TDiskQuota& diskQuota,
        bool interruptible,
        EPreemptionMode preemptionMode,
        TString treeId,
        int schedulingIndex,
        std::optional<EJobSchedulingStage> schedulingStage = std::nullopt,
        NNodeTrackerClient::TNodeId revivalNodeId = NNodeTrackerClient::InvalidNodeId,
        TString revivalNodeAddress = TString());

    //! Returns true if the job was revived.
    bool IsRevived() const;

    void SetNode(const TExecNodePtr& node);

    bool IsInterrupted() const noexcept;

    //! Time that will be wasted if allocation is preempted.
    TDuration PreemptibleProgressTime() const;

private:
    NLogging::TLogger CreateLogger();
};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
