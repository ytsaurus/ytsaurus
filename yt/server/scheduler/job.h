#pragma once

#include "public.h"
#include "exec_node.h"

#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <yt/ytlib/job_tracker_client/job.pb.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/node_tracker_client/node.pb.h>

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

    //! Exec node where the job is running.
    DEFINE_BYVAL_RW_PROPERTY(TExecNodePtr, Node);

    //! Flag that marks job as revived by scheduler. It is useful, for example,
    //! when we lose information about interruption reason during revival.
    DEFINE_BYVAL_RW_PROPERTY(bool, Revived, false);

    //! Node descriptor that was obtained from corresponding joblet during the revival process.
    //! It used only during the revival and is used only for filling `Node` field.
    DEFINE_BYREF_RW_PROPERTY(TJobNodeDescriptor, RevivedNodeDescriptor);

    //! The time when the job was started.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

    //! True if job can be interrupted.
    DEFINE_BYVAL_RO_PROPERTY(bool, Interruptible);

    //! The time when the job was finished.
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, FinishTime);

    //! True if job was unregistered during heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasPendingUnregistration);

    //! Current state of the job.
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);

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
        TExecNodePtr node,
        TInstant startTime,
        const TJobResources& resourceLimits,
        bool interruptible,
        const TString& treeId);

    //! The difference between |FinishTime| and |StartTime|.
    TDuration GetDuration() const;
};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to controller agent
// NB: This particular summary does not inherit from TJobSummary.
struct TStartedJobSummary
{
    explicit TStartedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    TJobId Id;
    TInstant StartTime;
};

struct TJobSummary
{
    TJobSummary() = default;
    TJobSummary(const TJobPtr& job, const TJobStatus* status);
    TJobSummary(const TJobId& id, EJobState state);
    explicit TJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);
    virtual ~TJobSummary() = default;

    void Persist(const NPhoenix::TPersistenceContext& context);

    TJobResult Result;
    TJobId Id;
    EJobState State = EJobState::None;

    TNullable<TInstant> FinishTime;
    TNullable<TDuration> PrepareDuration;
    TNullable<TDuration> DownloadDuration;
    TNullable<TDuration> ExecDuration;

    // NB: The Statistics field will be set inside the controller in ParseStatistics().
    TNullable<NJobTrackerClient::TStatistics> Statistics;
    NYson::TYsonString StatisticsYson;

    bool LogAndProfile = false;
};

using TFailedJobSummary = TJobSummary;

struct TCompletedJobSummary
    : public TJobSummary
{
    TCompletedJobSummary(const TJobPtr& job, const TJobStatus& status, bool abandoned = false);
    //! Only for testing purpose.
    TCompletedJobSummary() = default;
    explicit TCompletedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    void Persist(const NPhoenix::TPersistenceContext& context);

    bool Abandoned = false;
    EInterruptReason InterruptReason = EInterruptReason::None;

    // These fields are for controller's use only.
    std::vector<NChunkClient::TInputDataSlicePtr> UnreadInputDataSlices;
    std::vector<NChunkClient::TInputDataSlicePtr> ReadInputDataSlices;
    int SplitJobCount = 1;
};

struct TAbortedJobSummary
    : public TJobSummary
{
    TAbortedJobSummary(const TJobId& id, EAbortReason abortReason);
    TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason);
    explicit TAbortedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    EAbortReason AbortReason;
};

struct TRunningJobSummary
    : public TJobSummary
{
    explicit TRunningJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    double Progress;
    ui64 StderrSize;
};

////////////////////////////////////////////////////////////////////////////////

TJobStatus JobStatusFromError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

struct TJobStartRequest
{
    TJobStartRequest(
        const TJobId& id,
        EJobType type,
        const TJobResources& resourceLimits,
        bool interruptible);

    const TJobId Id;
    const EJobType Type;
    const TJobResources ResourceLimits;
    const bool Interruptible;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EScheduleJobFailReason,
    ((Unknown)                       ( 0))
    ((OperationNotRunning)           ( 1))
    ((NoPendingJobs)                 ( 2))
    ((NotEnoughChunkLists)           ( 3))
    ((NotEnoughResources)            ( 4))
    ((Timeout)                       ( 5))
    ((EmptyInput)                    ( 6))
    ((NoLocalJobs)                   ( 7))
    ((TaskDelayed)                   ( 8))
    ((NoCandidateTasks)              ( 9))
    ((ResourceOvercommit)            (10))
    ((TaskRefusal)                   (11))
    ((JobSpecThrottling)             (12))
    ((IntermediateChunkLimitExceeded)(13))
    ((DataBalancingViolation)        (14))
);

struct TScheduleJobResult
    : public TIntrinsicRefCounted
{
    void RecordFail(EScheduleJobFailReason reason);
    bool IsBackoffNeeded() const;
    bool IsScheduleStopNeeded() const;

    TNullable<TJobStartRequest> JobStartRequest;
    TEnumIndexedVector<int, EScheduleJobFailReason> Failed;
    TDuration Duration;
};

DEFINE_REFCOUNTED_TYPE(TScheduleJobResult)

////////////////////////////////////////////////////////////////////////////////

TJobId MakeJobId(NObjectClient::TCellTag tag, NNodeTrackerClient::TNodeId nodeId);

NNodeTrackerClient::TNodeId NodeIdFromJobId(const TJobId& jobId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
