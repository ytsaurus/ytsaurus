#pragma once

#include "public.h"

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
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);

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

    //! Abort reason saved if job was aborted.
    DEFINE_BYVAL_RW_PROPERTY(EAbortReason, AbortReason);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);

    //! Asynchronous spec builder callback.
    DEFINE_BYVAL_RW_PROPERTY(TJobSpecBuilder, SpecBuilder);

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

public:
    TJob(
        const TJobId& id,
        EJobType type,
        const TOperationId& operationId,
        TExecNodePtr node,
        TInstant startTime,
        const TJobResources& resourceLimits,
        bool interruptible,
        TJobSpecBuilder specBuilder);

    //! The difference between |FinishTime| and |StartTime|.
    TDuration GetDuration() const;
    
    void InterruptJob(EInterruptReason reason, NProfiling::TCpuInstant interruptDeadline);
};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

struct TJobSummary
{
    TJobSummary() = default;
    TJobSummary(const TJobPtr& job, TJobStatus* status);
    TJobSummary(const TJobId& id, EJobState state);
    virtual ~TJobSummary() = default;

    TJobResult Result;
    TJobId Id;
    EJobState State;

    TNullable<TInstant> FinishTime;
    TNullable<TDuration> PrepareDuration;
    TNullable<TDuration> DownloadDuration;
    TNullable<TDuration> ExecDuration;

    // NB: The Statistics field will be set inside the controller in ParseStatistics().
    TNullable<NJobTrackerClient::TStatistics> Statistics;
    NYson::TYsonString StatisticsYson;

    // TODO(ignat): rename, it is not only about logging.
    bool ShouldLog;

    void Persist(const NPhoenix::TPersistenceContext& context);
};

using TFailedJobSummary = TJobSummary;

struct TCompletedJobSummary
    : public TJobSummary
{
    TCompletedJobSummary(const TJobPtr& job, TJobStatus* status, bool abandoned = false);
    //! Only for testing purpose.
    TCompletedJobSummary() = default;

    void Persist(const NPhoenix::TPersistenceContext& context);

    bool Abandoned = false;

    std::vector<NChunkClient::TInputDataSlicePtr> UnreadInputDataSlices;
    EInterruptReason InterruptReason = EInterruptReason::None;
    int SplitJobCount = 1;
};

struct TAbortedJobSummary
    : public TJobSummary
{
    TAbortedJobSummary(const TJobPtr& job, TJobStatus* status);
    TAbortedJobSummary(const TJobId& id, EAbortReason abortReason);
    TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason);

    const EAbortReason AbortReason;
};

struct TRunningJobSummary
    : public TJobSummary
{
    TRunningJobSummary(const TJobPtr& job, TJobStatus* status);

    const double Progress;
};

////////////////////////////////////////////////////////////////////////////////

TJobStatus JobStatusFromError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

struct TJobStartRequest
{
    TJobStartRequest(
        TJobId id,
        EJobType type,
        const TJobResources& resourceLimits,
        bool interruptible,
        TJobSpecBuilder specBuilder);

    const TJobId Id;
    const EJobType Type;
    const TJobResources ResourceLimits;
    const bool Interruptible;
    const TJobSpecBuilder SpecBuilder;
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
