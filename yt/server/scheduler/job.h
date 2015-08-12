#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/nullable.h>

#include <core/actions/callback.h>

#include <ytlib/job_tracker_client/job.pb.h>

#include <ytlib/node_tracker_client/node.pb.h>

#include <ytlib/scheduler/statistics.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

typedef TCallback<void(NJobTrackerClient::NProto::TJobSpec* jobSpec)> TJobSpecBuilder;

class TJob
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TJobId, Id);

    DEFINE_BYVAL_RO_PROPERTY(EJobType, Type);

    //! The operation the job belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TOperation*, Operation);

    //! The id of operation the job belongs to.
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, OperationId);

    //! Exec node where the job is running.
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);

    //! The time when the job was started.
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

    //! True if this is a reincarnation of a lost job.
    DEFINE_BYVAL_RO_PROPERTY(bool, Restarted);

    //! The time when the job was finished.
    DEFINE_BYVAL_RO_PROPERTY(TNullable<TInstant>, FinishTime);

    //! Sets finish time and other timing statistics.
    void FinalizeJob(const TInstant& finishTime);

    //! The difference between |FinishTime| and |StartTime|.
    TDuration GetDuration() const;

    //! Job result returned by node.
    DEFINE_BYREF_RO_PROPERTY(TRefCountedJobResultPtr, Result);

    void SetResult(NJobTrackerClient::NProto::TJobResult&& result);

    // Custom and builtin job statistics.
    DEFINE_BYREF_RO_PROPERTY(TStatistics, Statistics);

    //! Some rough approximation that is updated with every heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(EJobState, State);

    //! Some rough approximation that is updated with every heartbeat.
    DEFINE_BYVAL_RW_PROPERTY(double, Progress);

    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceLimits);

    //! Asynchronous spec builder callback.
    DEFINE_BYVAL_RW_PROPERTY(TJobSpecBuilder, SpecBuilder);


public:
    TJob(
        const TJobId& id,
        EJobType type,
        TOperationPtr operation,
        TExecNodePtr node,
        TInstant startTime,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        bool restarted,
        TJobSpecBuilder specBuilder);

};

DEFINE_REFCOUNTED_TYPE(TJob)

////////////////////////////////////////////////////////////////////////////////

struct TCompletedJobSummary
{
    explicit TCompletedJobSummary(TJobPtr job);

    const TRefCountedJobResultPtr Result;
    const TJobId Id;
    const TStatistics Statistics;
};

struct TFailedJobSummary
{
    explicit TFailedJobSummary(TJobPtr job);

    const TRefCountedJobResultPtr Result;
    const TJobId Id;
};

struct TAbortedJobSummary
{
    explicit TAbortedJobSummary(TJobPtr job);

    TAbortedJobSummary(const TJobId& id, EAbortReason abortReason);

    const TRefCountedJobResultPtr Result;
    const TJobId Id;
    const EAbortReason AbortReason;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
