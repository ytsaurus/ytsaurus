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

//! A reduced verison of TExecNodeDescriptor, which is associated with jobs.
struct TJobNodeDescriptor
{
    TJobNodeDescriptor() = default;
    TJobNodeDescriptor(const TJobNodeDescriptor& other) = default;
    TJobNodeDescriptor(const NScheduler::TExecNodeDescriptor& other);

    NNodeTrackerClient::TNodeId Id = NNodeTrackerClient::InvalidNodeId;
    TString Address;
    double IOWeight = 0.0;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

struct TJobInfoBase
{
    NJobTrackerClient::TJobId JobId;
    NJobTrackerClient::EJobType JobType;

    TJobNodeDescriptor NodeDescriptor;

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
    TJoblet(TTask* task, int jobIndex, int taskJobIndex, const TString& treeId, bool treeIsTentative);

    // Controller encapsulates lifetime of both, tasks and joblets.
    TTask* Task;
    int JobIndex;
    int TaskJobIndex = 0;
    i64 StartRowIndex = -1;
    bool Restarted = false;
    bool Revived = false;

    // It is necessary to store tree id here since it is required to
    // create job metrics updater after revive.
    TString TreeId;
    // Is the tree marked as tentative in the spec?
    bool TreeIsTentative = false;

    TFuture<TSharedRef> JobSpecProtoFuture;

    NScheduler::TExtendedJobResources EstimatedResourceUsage;
    std::optional<double> JobProxyMemoryReserveFactor;
    std::optional<double> UserJobMemoryReserveFactor;
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
        TJobSummary summary);

    TJobSummary Summary;

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
    bool Restartable;

    TJobNodeDescriptor NodeDescriptor;

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TCompletedJob)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
