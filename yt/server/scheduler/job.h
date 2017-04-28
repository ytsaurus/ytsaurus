#pragma once

#include "public.h"
#include "job_resources.h"

#include <yt/ytlib/chunk_client/data_statistics.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <yt/ytlib/job_tracker_client/job.pb.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/core/actions/callback.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/phoenix.h>

#include <yt/core/yson/consumer.h>

namespace NYT {
namespace NScheduler {

using namespace NPhoenix;

////////////////////////////////////////////////////////////////////////////////

struct TBriefJobStatistics
    : public TIntrinsicRefCounted
{
    TInstant Timestamp = TInstant::Zero();

    i64 ProcessedInputRowCount = 0;
    i64 ProcessedInputUncompressedDataSize = 0;
    i64 ProcessedInputCompressedDataSize = 0;
    i64 ProcessedOutputRowCount = 0;
    i64 ProcessedOutputUncompressedDataSize = 0;
    i64 ProcessedOutputCompressedDataSize = 0;
    // Time is given in milliseconds.
    TNullable<i64> InputPipeIdleTime = Null;
    TNullable<i64> JobProxyCpuUsage = Null;
};

DEFINE_REFCOUNTED_TYPE(TBriefJobStatistics)

void Serialize(const TBriefJobStatistics& briefJobStatistics, NYson::IYsonConsumer* consumer);

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

    //! True if this is a reincarnation of a lost job.
    DEFINE_BYVAL_RO_PROPERTY(bool, Restarted);

    //! True if job can be interrupted.
    DEFINE_BYVAL_RO_PROPERTY(bool, Interruptible);

    //! The time when the job was finished.
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, FinishTime);

    //! Job status returned by node.
    DEFINE_BYREF_RO_PROPERTY(TJobStatus, Status);

    //! Yson containing statistics produced by the job.
    DEFINE_BYREF_RO_PROPERTY(NYson::TYsonString, StatisticsYson);

    //! True if job was unregistered during heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasPendingUnregistration);

    //! Current state of the job.
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);

    //! Some rough approximation that is updated with every heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(double, Progress);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);

    //! Asynchronous spec builder callback.
    DEFINE_BYVAL_RW_PROPERTY(TJobSpecBuilder, SpecBuilder);

    //! Temporary flag used during heartbeat jobs processing to mark found jobs.
    DEFINE_BYVAL_RW_PROPERTY(bool, FoundOnNode);

    //! Flag that marks job as preempted by scheduler.
    DEFINE_BYVAL_RW_PROPERTY(bool, Preempted);

    //! String describing preemption reason.
    DEFINE_BYVAL_RW_PROPERTY(Stroka, PreemptionReason);

    //! The purpose of the job interruption.
    DEFINE_BYVAL_RW_PROPERTY(EInterruptReason, InterruptReason, EInterruptReason::None);

    //! Deadline for job to be interrupted.
    DEFINE_BYVAL_RW_PROPERTY(NProfiling::TCpuInstant, InterruptDeadline, 0);

    //! Contains several important values extracted from job statistics.
    DEFINE_BYVAL_RO_PROPERTY(TBriefJobStatisticsPtr, BriefStatistics);

    //! Means that job probably hung up.
    DEFINE_BYVAL_RO_PROPERTY(bool, Suspicious);

    //! Last time when brief statistics changed in comparison to their previous values.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, LastActivityTime);

    //! Account for node in cypress.
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Account);

    //! Last time when statistics and resource usage from running job was updated.
    DEFINE_BYVAL_RW_PROPERTY(TNullable<NProfiling::TCpuInstant>, LastRunningJobUpdateTime);

public:
    TJob(
        const TJobId& id,
        EJobType type,
        const TOperationId& operationId,
        TExecNodePtr node,
        TInstant startTime,
        const TJobResources& resourceLimits,
        bool restarted,
        bool interruptible,
        TJobSpecBuilder specBuilder,
        const Stroka& account);

    //! The difference between |FinishTime| and |StartTime|.
    TDuration GetDuration() const;

    void AnalyzeBriefStatistics(
        TDuration suspiciousInactivityTimeout,
        i64 suspiciousCpuUsageThreshold,
        double suspiciousInputPipeIdleTimeFraction,
        const TErrorOr<TBriefJobStatisticsPtr>& briefStatisticsOrError);

    void SetStatus(TJobStatus* status);

    const Stroka& GetStatisticsSuffix() const;
};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

struct TJobSummary
{
    explicit TJobSummary(const TJobPtr& job);
    explicit TJobSummary(const TJobId& id);
    //! Only for testing purpose.
    TJobSummary() = default;

    void Persist(const TPersistenceContext& context);

    void ParseStatistics();

    TJobResult Result;
    TJobId Id;
    Stroka StatisticsSuffix;
    TNullable<TInstant> FinishTime;
    TNullable<TDuration> PrepareDuration;
    TNullable<TDuration> DownloadDuration;
    TNullable<TDuration> ExecDuration;

    // NB: The Statistics field will be set inside the controller in ParseStatistics().
    NJobTrackerClient::TStatistics Statistics;
    NYson::TYsonString StatisticsYson;

    bool ShouldLog;
};

using TFailedJobSummary = TJobSummary;

struct TCompletedJobSummary
    : public TJobSummary
{
    explicit TCompletedJobSummary(const TJobPtr& job, bool abandoned = false);
    //! Only for testing purpose.
    TCompletedJobSummary() = default;

    void Persist(const TPersistenceContext& context);

    bool Abandoned = false;

    std::vector<NChunkClient::TInputDataSlicePtr> UnreadInputDataSlices;
    EInterruptReason InterruptReason = EInterruptReason::None;
    int SplitJobCount = 1;
};

struct TAbortedJobSummary
    : public TJobSummary
{
    explicit TAbortedJobSummary(const TJobPtr& job);
    TAbortedJobSummary(const TJobId& id, EAbortReason abortReason);
    TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason);

    const EAbortReason AbortReason;
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
        bool restarted,
        bool interruptible,
        TJobSpecBuilder specBuilder,
        const Stroka& account);

    const TJobId Id;
    const EJobType Type;
    const TJobResources ResourceLimits;
    const bool Restarted;
    const bool Interruptible;
    const TJobSpecBuilder SpecBuilder;
    const Stroka Account;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EScheduleJobFailReason,
    ((Unknown)                    ( 0))
    ((OperationNotRunning)        ( 1))
    ((NoPendingJobs)              ( 2))
    ((NotEnoughChunkLists)        ( 3))
    ((NotEnoughResources)         ( 4))
    ((Timeout)                    ( 5))
    ((EmptyInput)                 ( 6))
    ((NoLocalJobs)                ( 7))
    ((TaskDelayed)                ( 8))
    ((NoCandidateTasks)           ( 9))
    ((ResourceOvercommit)         (10))
    ((TaskRefusal)                (11))
    ((JobSpecThrottling)          (12))
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

struct TScheduleJobStatistics
    : public TIntrinsicRefCounted
    , public NPhoenix::IPersistent
{
    void RecordJobResult(const TScheduleJobResultPtr& scheduleJobResult);

    TEnumIndexedVector<int, EScheduleJobFailReason> Failed;
    TDuration Duration;
    i64 Count = 0;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TScheduleJobStatistics)

////////////////////////////////////////////////////////////////////////////////

TJobId MakeJobId(NObjectClient::TCellTag tag, NNodeTrackerClient::TNodeId nodeId);

NNodeTrackerClient::TNodeId NodeIdFromJobId(const TJobId& jobId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
