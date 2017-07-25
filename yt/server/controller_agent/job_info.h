#pragma once

#include "operation_controller_detail.h"
#include "public.h"
#include "serialize.h"

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

struct TJoblet
    : public TJobInfo
{
public:
    //! Default constructor is for serialization only.
    TJoblet();
    TJoblet(std::unique_ptr<TJobMetricsUpdater> jobMetricsUpdater, TOperationControllerBase::TTaskPtr task, int jobIndex);

    TOperationControllerBase::TTaskPtr Task;
    int JobIndex;
    i64 StartRowIndex;
    bool Restarted = false;

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

    virtual void Persist(const TPersistenceContext& context) override;
    void SendJobMetrics(const NJobTrackerClient::TStatistics& jobStatistics, bool flush);
private:
    std::unique_ptr<TJobMetricsUpdater> JobMetricsUpdater_;
};

DEFINE_REFCOUNTED_TYPE(TJoblet)

////////////////////////////////////////////////////////////////////////////////

struct TFinishedJobInfo
    : public TJobInfo
{
    TFinishedJobInfo() = default;

    TFinishedJobInfo(
        const TJobletPtr& joblet,
        NScheduler::TJobSummary summary,
        NYson::TYsonString inputPaths);

    NScheduler::TJobSummary Summary;
    NYson::TYsonString InputPaths;

    virtual void Persist(const TPersistenceContext& context) override;
};

DEFINE_REFCOUNTED_TYPE(TFinishedJobInfo)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT