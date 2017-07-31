#pragma once

#include "private.h"

#include "progress_counter.h"
#include "serialize.h"
#include "output_chunk_tree.h"

#include <yt/server/scheduler/job.h>

#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/ytlib/scheduler/job_resources.h>
#include <yt/ytlib/scheduler/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TTask
    : public TRefCounted
    , public IPersistent
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, DelayedTime);

public:
    //! For persistence only.
    TTask();
    explicit TTask(ITaskHostPtr taskHost);

    void Initialize();

    virtual TString GetId() const = 0;
    virtual TTaskGroupPtr GetGroup() const = 0;

    virtual int GetPendingJobCount() const;
    int GetPendingJobCountDelta();

    virtual int GetTotalJobCount() const;
    int GetTotalJobCountDelta();

    const TProgressCounter& GetJobCounter() const;

    virtual TJobResources GetTotalNeededResources() const;
    TJobResources GetTotalNeededResourcesDelta();

    virtual bool IsIntermediateOutput() const;

    bool IsStderrTableEnabled() const;

    bool IsCoreTableEnabled() const;

    virtual TDuration GetLocalityTimeout() const = 0;
    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const;
    virtual bool HasInputLocality() const;

    TJobResources GetMinNeededResources() const;

    virtual NScheduler::TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const = 0;

    void ResetCachedMinNeededResources();

    void AddInput(NChunkPools::TChunkStripePtr stripe);
    void AddInput(const std::vector<NChunkPools::TChunkStripePtr>& stripes);
    void FinishInput();

    void CheckCompleted();

    void ScheduleJob(
        NScheduler::ISchedulingContext* context,
        const TJobResources& jobLimits,
        NScheduler::TScheduleJobResult* scheduleJobResult);

    virtual void OnJobCompleted(TJobletPtr joblet, const NScheduler::TCompletedJobSummary& jobSummary);
    virtual void OnJobFailed(TJobletPtr joblet, const NScheduler::TFailedJobSummary& jobSummary);
    virtual void OnJobAborted(TJobletPtr joblet, const NScheduler::TAbortedJobSummary& jobSummary);
    virtual void OnJobLost(TCompletedJobPtr completedJob);

    // First checks against a given node, then against all nodes if needed.
    void CheckResourceDemandSanity(
        const TJobResources& nodeResourceLimits,
        const TJobResources& neededResources);

    // Checks against all available nodes.
    void CheckResourceDemandSanity(
        const TJobResources& neededResources);

    void DoCheckResourceDemandSanity(const TJobResources& neededResources);

    bool IsPending() const;
    bool IsCompleted() const;

    virtual bool IsActive() const;

    i64 GetTotalDataWeight() const;
    i64 GetCompletedDataWeight() const;
    i64 GetPendingDataWeight() const;

    TNullable<i64> GetMaximumUsedTmpfsSize() const;

    virtual NChunkPools::IChunkPoolInput* GetChunkPoolInput() const = 0;
    virtual NChunkPools::IChunkPoolOutput* GetChunkPoolOutput() const = 0;

    virtual void Persist(const TPersistenceContext& context) override;

    virtual NScheduler::TUserJobSpecPtr GetUserJobSpec() const;

    virtual EJobType GetJobType() const = 0;

protected:
    NLogging::TLogger Logger;

    virtual bool CanScheduleJob(
        NScheduler::ISchedulingContext* context,
        const TJobResources& jobLimits);

    virtual NScheduler::TExtendedJobResources GetMinNeededResourcesHeavy() const = 0;

    virtual void OnTaskCompleted();

    virtual void PrepareJoblet(TJobletPtr joblet);
    virtual void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) = 0;

    virtual void OnJobStarted(TJobletPtr joblet);

    void AddPendingHint();
    void AddLocalityHint(NNodeTrackerClient::TNodeId nodeId);

    void ReinstallJob(TJobletPtr joblet, std::function<void()> releaseOutputCookie);

    std::unique_ptr<NNodeTrackerClient::TNodeDirectoryBuilder> MakeNodeDirectoryBuilder(
        NScheduler::NProto::TSchedulerJobSpecExt* schedulerJobSpec);
    void AddSequentialInputSpec(
        NJobTrackerClient::NProto::TJobSpec* jobSpec,
        TJobletPtr joblet);
    void AddParallelInputSpec(
        NJobTrackerClient::NProto::TJobSpec* jobSpec,
        TJobletPtr joblet);
    void AddChunksToInputSpec(
        NNodeTrackerClient::TNodeDirectoryBuilder* directoryBuilder,
        NScheduler::NProto::TTableInputSpec* inputSpec,
        NChunkPools::TChunkStripePtr stripe);

    void AddFinalOutputSpecs(NJobTrackerClient::NProto::TJobSpec* jobSpec, TJobletPtr joblet);
    void AddIntermediateOutputSpec(
        NJobTrackerClient::NProto::TJobSpec* jobSpec,
        TJobletPtr joblet,
        const NTableClient::TKeyColumns& keyColumns);

    static void UpdateInputSpecTotals(
        NJobTrackerClient::NProto::TJobSpec* jobSpec,
        TJobletPtr joblet);

    void RegisterIntermediate(
        TJobletPtr joblet,
        NChunkPools::TChunkStripePtr stripe,
        TTaskPtr destinationTask,
        bool attachToLivePreview);
    void RegisterIntermediate(
        TJobletPtr joblet,
        NChunkPools::TChunkStripePtr stripe,
        NChunkPools::IChunkPoolInput* destinationPool,
        bool attachToLivePreview);

    static NChunkPools::TChunkStripePtr BuildIntermediateChunkStripe(
        google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs);

    void RegisterOutput(
        TJobletPtr joblet,
        TOutputChunkTreeKey key,
        const NScheduler::TCompletedJobSummary& jobSummary);

    void AddFootprintAndUserJobResources(NScheduler::TExtendedJobResources& jobResources) const;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TTask, 0x81ab3cd3);

    //! Raw pointer here avoids cyclic reference; task cannot live longer than its host.
    ITaskHost* TaskHost_;

    int CachedPendingJobCount_;
    int CachedTotalJobCount_;

    TNullable<i64> MaximumUsedTmfpsSize_;

    TJobResources CachedTotalNeededResources_;
    mutable TNullable<NScheduler::TExtendedJobResources> CachedMinNeededResources_;

    NProfiling::TCpuInstant DemandSanityCheckDeadline_;
    bool CompletedFired_;

    //! For each lost job currently being replayed, maps output cookie to corresponding input cookie.
    yhash<NChunkPools::IChunkPoolOutput::TCookie, NChunkPools::IChunkPoolInput::TCookie> LostJobCookieMap;

    TJobResources ApplyMemoryReserve(const NScheduler::TExtendedJobResources& jobResources) const;

    void UpdateMaximumUsedTmpfsSize(const NJobTrackerClient::TStatistics& statistics);

};

DEFINE_REFCOUNTED_TYPE(TTask)

////////////////////////////////////////////////////////////////////////////////

//! Groups provide means:
//! - to prioritize tasks
//! - to skip a vast number of tasks whose resource requirements cannot be met
struct TTaskGroup
    : public TIntrinsicRefCounted
{
    //! No task from this group is considered for scheduling unless this requirement is met.
    TJobResources MinNeededResources;

    //! All non-local tasks.
    yhash_set<TTaskPtr> NonLocalTasks;

    //! Non-local tasks that may possibly be ready (but a delayed check is still needed)
    //! keyed by min memory demand (as reported by TTask::GetMinNeededResources).
    std::multimap<i64, TTaskPtr> CandidateTasks;

    //! Non-local tasks keyed by deadline.
    std::multimap<TInstant, TTaskPtr> DelayedTasks;

    //! Local tasks keyed by node id.
    yhash<NNodeTrackerClient::TNodeId, yhash_set<TTaskPtr>> NodeIdToTasks;

    TTaskGroup();

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TTaskGroup)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
