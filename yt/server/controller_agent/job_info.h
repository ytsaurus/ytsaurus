#pragma once

#include "task.h"
#include "serialize.h"
#include "controller_agent.h"

#include <yt/server/scheduler/job_metrics.h>
#include <yt/server/scheduler/exec_node.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TJobInfoBase
{
    NJobTrackerClient::TJobId JobId;
    NJobTrackerClient::EJobType JobType;

    NScheduler::TJobNodeDescriptor NodeDescriptor;

    TInstant StartTime;
    TInstant FinishTime;

    TString Account;
    bool Suspicious = false;
    TInstant LastActivityTime;
    TBriefJobStatisticsPtr BriefStatistics;
    double Progress = 0.0;
    i64 StderrSize = 0;
    NYson::TYsonString StatisticsYson;

    virtual void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

struct TJobInfo
    : public TIntrinsicRefCounted
    , public TJobInfoBase
{
    TJobInfo() = default;
    TJobInfo(const TJobInfoBase& jobInfoBase);
};

DEFINE_REFCOUNTED_TYPE(TJobInfo)

////////////////////////////////////////////////////////////////////////////////

class TJoblet
    : public TJobInfo
{
public:
    //! Default constructor is for serialization only.
    TJoblet();
    TJoblet(TTask* task, int jobIndex, const TString& treeId);

    // Controller encapsulates lifetime of both, tasks and joblets.
    TTask* Task;
    int JobIndex;
    i64 StartRowIndex = -1;
    bool Restarted = false;
    bool Revived = false;

    // It is necessary to store tree id here since it is required to
    // create job metrics updater after revive.
    TString TreeId;

    TFuture<TSharedRef> JobSpecProtoFuture;

    NScheduler::TExtendedJobResources EstimatedResourceUsage;
    TNullable<double> JobProxyMemoryReserveFactor;
    TNullable<double> UserJobMemoryReserveFactor;
    TJobResources ResourceLimits;

    NChunkPools::TChunkStripeListPtr InputStripeList;
    NChunkPools::IChunkPoolOutput::TCookie OutputCookie;

    //! All chunk lists allocated for this job.
    /*!
     *  For jobs with intermediate output this list typically contains one element.
     *  For jobs with final output this list typically contains one element per each output table.
     */
    std::vector<NChunkClient::TChunkListId> ChunkListIds;

    NChunkClient::TChunkListId StderrTableChunkListId;
    NChunkClient::TChunkListId CoreTableChunkListId;

    NScheduler::TJobMetrics JobMetrics;

    virtual void Persist(const TPersistenceContext& context) override;

    NScheduler::TJobMetrics UpdateJobMetrics(const TJobSummary& jobSummary);
};

DEFINE_REFCOUNTED_TYPE(TJoblet)

////////////////////////////////////////////////////////////////////////////////

struct TFinishedJobInfo
    : public TJobInfo
{
    TFinishedJobInfo() = default;

    TFinishedJobInfo(
        const TJobletPtr& joblet,
        TJobSummary summary,
        NYson::TYsonString inputPaths);

    TJobSummary Summary;
    NYson::TYsonString InputPaths;

    virtual void Persist(const TPersistenceContext& context) override;
};

DEFINE_REFCOUNTED_TYPE(TFinishedJobInfo)

////////////////////////////////////////////////////////////////////////////////

struct TCompletedJob
    : public TIntrinsicRefCounted
{
    bool Suspended = false;

    std::set<NChunkClient::TChunkId> UnavailableChunks;

    TJobId JobId;

    TTaskPtr SourceTask;
    NChunkPools::IChunkPoolOutput::TCookie OutputCookie;
    i64 DataWeight;

    NChunkPools::IChunkPoolInput* DestinationPool = nullptr;
    NChunkPools::IChunkPoolInput::TCookie InputCookie;
    NChunkPools::TChunkStripePtr InputStripe;
    bool Restartrable;

    NScheduler::TJobNodeDescriptor NodeDescriptor;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TCompletedJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
