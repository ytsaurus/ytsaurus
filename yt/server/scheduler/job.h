#pragma once

#include "public.h"
#include "exec_node.h"

#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/node_tracker_client/proto/node.pb.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/actions/callback.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/phoenix.h>

#include <yt/core/yson/consumer.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TIntrinsicRefCounted
{
    DEFINE_BYVAL_RO_PROPERTY(TJobId, Id);

    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);

    //! The id of operation the job belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, OperationId);

    //! The incarnation of the controller agent responsible for this job.
    DEFINE_BYVAL_RO_PROPERTY(TIncarnationId, IncarnationId);

    //! Exec node where the job is running.
    DEFINE_BYVAL_RW_PROPERTY(TExecNodePtr, Node);

    //! Node id obtained from corresponding joblet during the revival process.
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerClient::TNodeId, RevivalNodeId, NNodeTrackerClient::InvalidNodeId);

    //! Node address obtained from corresponding joblet during the revival process.
    DEFINE_BYVAL_RO_PROPERTY(TString, RevivalNodeAddress);

    //! The time when the job was started.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

    //! True if job can be interrupted.
    DEFINE_BYVAL_RO_PROPERTY(bool, Interruptible);

    //! The time when the job was finished.
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, FinishTime);

    //! True if job was already unregistered.
    DEFINE_BYVAL_RW_PROPERTY(bool, Unregistered, false);

    //! Current state of the job.
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State, EJobState::None);

    //! Fair-share tree this job belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TString, TreeId);

    //! Abort reason saved if job was aborted.
    DEFINE_BYVAL_RW_PROPERTY(EAbortReason, AbortReason);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);

    //! Temporary flag used during heartbeat jobs processing to mark found jobs.
    DEFINE_BYVAL_RW_PROPERTY(bool, FoundOnNode);

    //! Flag that marks job as preempted by scheduler.
    DEFINE_BYVAL_RW_PROPERTY(bool, Preempted);

    //! Job fail was requested by scheduler.
    DEFINE_BYVAL_RW_PROPERTY(bool, FailRequested, false);

    //! String describing preemption reason.
    DEFINE_BYVAL_RW_PROPERTY(TString, PreemptionReason);

    //! The purpose of the job interruption.
    DEFINE_BYVAL_RW_PROPERTY(EInterruptReason, InterruptReason, EInterruptReason::None);

    //! Deadline for job to be interrupted.
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuInstant, InterruptDeadline, 0);

    //! Deadline for running job.
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuInstant, RunningJobUpdateDeadline, 0);

    //! True for revived job that was not confirmed by a heartbeat from the corresponding node yet.
    DEFINE_BYVAL_RW_PROPERTY(bool, WaitingForConfirmation, false);

public:
    TJob(
        const TJobId& id,
        EJobType type,
        const TOperationId& operationId,
        const TIncarnationId& incarnationId,
        TExecNodePtr node,
        TInstant startTime,
        const TJobResources& resourceLimits,
        bool interruptible,
        TString treeId,
        NNodeTrackerClient::TNodeId revivalNodeId = NNodeTrackerClient::InvalidNodeId,
        TString revivalNodeAddress = TString());

    //! The difference between |FinishTime| and |StartTime|.
    TDuration GetDuration() const;

    //! Returns true if the job was revived.
    bool IsRevived() const;
};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
