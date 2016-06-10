#pragma once

#include "public.h"
#include "job_resources.h"

#include <yt/ytlib/chunk_client/data_statistics.h>

#include <yt/ytlib/job_tracker_client/job.pb.h>
#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/core/actions/callback.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJob
    : public TRefCounted
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

    //! The time when the job was finished.
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, FinishTime);

    //! Job status returned by node.
    DEFINE_BYREF_RO_PROPERTY(TRefCountedJobStatusPtr, Status);

    //! Yson containing statistics produced by the job.
    DEFINE_BYREF_RO_PROPERTY(TNullable<NYson::TYsonString>, StatisticsYson);

    //! True if job was unregistered during heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasPendingUnregistration);

    //! Some rough approximation that is updated with every heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);

    //! Some rough approximation that is updated with every heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(double, Progress);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);

    //! Asynchronous spec builder callback.
    DEFINE_BYVAL_RW_PROPERTY(TJobSpecBuilder, SpecBuilder);

public:
    TJob(
        const TJobId& id,
        EJobType type,
        const TOperationId& operationId,
        TExecNodePtr node,
        TInstant startTime,
        const TJobResources& resourceLimits,
        bool restarted,
        TJobSpecBuilder specBuilder);

    //! The difference between |FinishTime| and |StartTime|.
    TDuration GetDuration() const;

    void SetStatus(TRefCountedJobStatusPtr status);

    const Stroka& GetStatisticsSuffix() const;
};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

struct TJobSummary
{
    explicit TJobSummary(const TJobPtr& job);
    explicit TJobSummary(const TJobId& id);

    void ParseStatistics();

    const TRefCountedJobResultPtr Result;
    const TJobId Id;
    const Stroka StatisticsSuffix;
    const TInstant FinishTime;
    TNullable<TDuration> PrepareDuration;
    TNullable<TDuration> ExecDuration;

    // NB: The Statistics field will be set inside the controller in ParseStatistics().
    NJobTrackerClient::TStatistics Statistics;
    TNullable<NYson::TYsonString> StatisticsYson;

    bool ShouldLog;
};

using TFailedJobSummary = TJobSummary;

struct TCompletedJobSummary
    : public TJobSummary
{
    explicit TCompletedJobSummary(const TJobPtr& job, bool abandoned = false);

    const bool Abandoned = false;
};

struct TAbortedJobSummary
    : public TJobSummary
{
    explicit TAbortedJobSummary(const TJobPtr& job);
    TAbortedJobSummary(const TJobId& id, EAbortReason abortReason);

    const EAbortReason AbortReason;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobStartRequest
{
    TJobStartRequest(
        TJobId id,
        EJobType type,
        const TJobResources& resourceLimits,
        bool restarted,
        const TJobSpecBuilder& specBuilder);

    const TJobId Id;
    const EJobType Type;
    const TJobResources ResourceLimits;
    const bool Restarted;
    const TJobSpecBuilder SpecBuilder;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EScheduleJobFailReason,
    (Unknown)
    (OperationNotRunning)
    (NoPendingJobs)
    (NotEnoughChunkLists)
    (NotEnoughResources)
    (Timeout)
    (EmptyInput)
    (NoLocalJobs)
    (TaskDelayed)
    (NoCandidateTasks)
    (ResourceOvercommit)
    (TaskRefusal)
);

struct TScheduleJobResult
    : public TIntrinsicRefCounted
{
    void RecordFail(EScheduleJobFailReason reason);

    TNullable<TJobStartRequest> JobStartRequest;
    TEnumIndexedVector<int, EScheduleJobFailReason> Failed;
    TDuration Duration;
};

DEFINE_REFCOUNTED_TYPE(TScheduleJobResult)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
