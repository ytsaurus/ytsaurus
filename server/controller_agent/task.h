#pragma once

#include "private.h"

#include "progress_counter.h"
#include "serialize.h"
#include "data_flow_graph.h"

#include <yt/server/scheduler/job.h>

#include <yt/server/chunk_pools/chunk_stripe_key.h>
#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/ytlib/scheduler/job_resources.h>
#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/table_client/helpers.h>

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
    TTask(ITaskHostPtr taskHost, std::vector<TEdgeDescriptor> edgeDescriptors);
    explicit TTask(ITaskHostPtr taskHost);

    void Initialize();

    virtual TString GetId() const = 0;
    virtual TTaskGroupPtr GetGroup() const = 0;

    virtual int GetPendingJobCount() const;
    int GetPendingJobCountDelta();

    virtual int GetTotalJobCount() const;
    int GetTotalJobCountDelta();

    const TProgressCounterPtr& GetJobCounter() const;

    virtual TJobResources GetTotalNeededResources() const;
    TJobResources GetTotalNeededResourcesDelta();

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

    virtual bool ValidateChunkCount(int chunkCount);

    void ScheduleJob(
        NScheduler::ISchedulingContext* context,
        const TJobResources& jobLimits,
        NScheduler::TScheduleJobResult* scheduleJobResult);

    virtual void OnJobCompleted(TJobletPtr joblet, NScheduler::TCompletedJobSummary& jobSummary);
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

    bool IsCompleted() const;

    virtual bool IsActive() const;

    i64 GetTotalDataWeight() const;
    i64 GetCompletedDataWeight() const;
    i64 GetPendingDataWeight() const;

    i64 GetInputDataSliceCount() const;

    TNullable<i64> GetMaximumUsedTmpfsSize() const;

    virtual NChunkPools::IChunkPoolInput* GetChunkPoolInput() const = 0;
    virtual NChunkPools::IChunkPoolOutput* GetChunkPoolOutput() const = 0;

    virtual void Persist(const TPersistenceContext& context) override;

    virtual NScheduler::TUserJobSpecPtr GetUserJobSpec() const;

    virtual EJobType GetJobType() const = 0;

    ITaskHost* GetTaskHost();
    void AddLocalityHint(NNodeTrackerClient::TNodeId nodeId);
    void AddPendingHint();

    virtual void SetupCallbacks();

    //! This method shows if the jobs of this task have an "input_paths" attribute
    //! in Cypress. This depends on if this task gets it input directly from the
    //! input tables or from the intermediate data.
    virtual bool SupportsInputPathYson() const = 0;

protected:
    NLogging::TLogger Logger;

    //! Raw pointer here avoids cyclic reference; task cannot live longer than its host.
    ITaskHost* TaskHost_;

    virtual bool CanScheduleJob(
        NScheduler::ISchedulingContext* context,
        const TJobResources& jobLimits);

    virtual NScheduler::TExtendedJobResources GetMinNeededResourcesHeavy() const = 0;

    virtual void OnTaskCompleted();

    virtual void PrepareJoblet(TJobletPtr joblet);
    virtual void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) = 0;

    virtual void OnJobStarted(TJobletPtr joblet);

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

    void AddOutputTableSpecs(NJobTrackerClient::NProto::TJobSpec* jobSpec, TJobletPtr joblet);

    static void UpdateInputSpecTotals(
        NJobTrackerClient::NProto::TJobSpec* jobSpec,
        TJobletPtr joblet);

    // Send stripe to the next chunk pool.
    void RegisterStripe(
        NChunkPools::TChunkStripePtr chunkStripe,
        const TEdgeDescriptor& edgeDescriptor,
        TJobletPtr joblet,
        NChunkPools::TChunkStripeKey key = NChunkPools::TChunkStripeKey());

    static std::vector<NChunkPools::TChunkStripePtr> BuildChunkStripes(
        google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs,
        int tableCount);

    static NChunkPools::TChunkStripePtr BuildIntermediateChunkStripe(
        google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs);

    std::vector<NChunkPools::TChunkStripePtr> BuildOutputChunkStripes(
        NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt,
        const std::vector<NChunkClient::TChunkTreeId>& chunkTreeIds,
        google::protobuf::RepeatedPtrField<NScheduler::NProto::TOutputResult> boundaryKeys);

    void AddFootprintAndUserJobResources(NScheduler::TExtendedJobResources& jobResources) const;

    //! This method processes `chunkListIds`, forming the chunk stripes (maybe with boundary
    //! keys taken from `jobResult` if they are present) and sends them to the destination pools
    //! depending on the table index.
    //!
    //! If destination pool requires the recovery info, `joblet` should be non-null since it is used
    //! in the recovery info, otherwise it is not used.
    //!
    //! This method steals output chunk specs for `jobResult`.
    void RegisterOutput(
        NJobTrackerClient::NProto::TJobResult* jobResult,
        const std::vector<NChunkClient::TChunkListId>& chunkListIds,
        TJobletPtr joblet,
        const NChunkPools::TChunkStripeKey& key = NChunkPools::TChunkStripeKey());

protected:
    //! Outgoing edges in data flow graph.
    std::vector<TEdgeDescriptor> EdgeDescriptors_;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TTask, 0x81ab3cd3);

    int CachedPendingJobCount_;
    int CachedTotalJobCount_;

    TNullable<i64> MaximumUsedTmfpsSize_;

    TJobResources CachedTotalNeededResources_;
    mutable TNullable<NScheduler::TExtendedJobResources> CachedMinNeededResources_;

    NProfiling::TCpuInstant DemandSanityCheckDeadline_;
    bool CompletedFired_;

    using TCookieAndPool = std::pair<NChunkPools::IChunkPoolInput::TCookie, NChunkPools::IChunkPoolInput*>;
    //! For each lost job currently being replayed and destination pool, maps output cookie to corresponding input cookie.
    std::map<TCookieAndPool, NChunkPools::IChunkPoolInput::TCookie> LostJobCookieMap;

    TJobResources ApplyMemoryReserve(const NScheduler::TExtendedJobResources& jobResources) const;

    TSharedRef BuildJobSpecProto(TJobletPtr joblet);

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
    THashSet<TTaskPtr> NonLocalTasks;

    //! Non-local tasks that may possibly be ready (but a delayed check is still needed)
    //! keyed by min memory demand (as reported by TTask::GetMinNeededResources).
    std::multimap<i64, TTaskPtr> CandidateTasks;

    //! Non-local tasks keyed by deadline.
    std::multimap<TInstant, TTaskPtr> DelayedTasks;

    //! Local tasks keyed by node id.
    THashMap<NNodeTrackerClient::TNodeId, THashSet<TTaskPtr>> NodeIdToTasks;

    TTaskGroup();

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TTaskGroup)

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
