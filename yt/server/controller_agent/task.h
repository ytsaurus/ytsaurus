#pragma once

#include "private.h"

#include "tentative_tree_eligibility.h"
#include "progress_counter.h"
#include "serialize.h"
#include "data_flow_graph.h"
#include "input_chunk_mapping.h"

#include <yt/server/scheduler/job.h>

#include <yt/server/chunk_pools/chunk_stripe_key.h>
#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/ytlib/scheduler/job_resources.h>
#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/table_client/helpers.h>

#include <yt/core/misc/digest.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TTask
    : public TRefCounted
    , public IPersistent
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, DelayedTime);
    DEFINE_BYVAL_RW_PROPERTY(TDataFlowGraph::TVertexDescriptor, InputVertex, TDataFlowGraph::TVertexDescriptor());

public:
    //! For persistence only.
    TTask();
    TTask(ITaskHostPtr taskHost, std::vector<TEdgeDescriptor> edgeDescriptors);
    explicit TTask(ITaskHostPtr taskHost);

    //! This method is called on task object creation (both at clean creation and at revival).
    //! It may be used when calling virtual method is needed, but not allowed.
    virtual void Initialize();

    //! Title of a data flow graph vertex that appears in a web interface and coincides with the job type
    //! for builtin tasks. For example, "SortedReduce" or "PartitionMap".
    virtual TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const;
    //! Human-readable title of a particular task that appears in logging. For builtin tasks it coincides
    //! with the vertex descriptor and a partition index in brackets (if applicable).
    virtual TString GetTitle() const;

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

    virtual TDuration GetLocalityTimeout() const;
    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const;
    virtual bool HasInputLocality() const;

    NScheduler::TJobResourcesWithQuota GetMinNeededResources() const;

    void ResetCachedMinNeededResources();

    void AddInput(NChunkPools::TChunkStripePtr stripe);
    void AddInput(const std::vector<NChunkPools::TChunkStripePtr>& stripes);

    // NB: This works well until there is no more than one input data flow vertex for any task.
    void FinishInput(TDataFlowGraph::TVertexDescriptor inputVertex);
    virtual void FinishInput();

    void CheckCompleted();

    virtual bool ValidateChunkCount(int chunkCount);

    void ScheduleJob(
        ISchedulingContext* context,
        const TJobResourcesWithQuota& jobLimits,
        const TString& treeId,
        bool treeIsTentative,
        TScheduleJobResult* scheduleJobResult);

    virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary);
    virtual TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary);
    virtual TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary);

    virtual void OnJobLost(TCompletedJobPtr completedJob);

    virtual void OnStripeRegistrationFailed(
        TError error,
        NChunkPools::IChunkPoolInput::TCookie cookie,
        const NChunkPools::TChunkStripePtr& stripe,
        const TEdgeDescriptor& edgeDescriptor);

    // First checks against a given node, then against all nodes if needed.
    void CheckResourceDemandSanity(
        const TJobResourcesWithQuota& nodeResourceLimits,
        const TJobResourcesWithQuota& neededResources);

    void DoCheckResourceDemandSanity(const TJobResourcesWithQuota& neededResources);

    bool IsCompleted() const;

    virtual bool IsActive() const;

    i64 GetTotalDataWeight() const;
    i64 GetCompletedDataWeight() const;
    i64 GetPendingDataWeight() const;

    i64 GetInputDataSliceCount() const;

    TNullable<i64> GetMaximumUsedTmpfsSize() const;

    virtual void Persist(const TPersistenceContext& context) override;

    virtual NScheduler::TUserJobSpecPtr GetUserJobSpec() const;

    ITaskHost* GetTaskHost();
    void AddLocalityHint(NNodeTrackerClient::TNodeId nodeId);
    void AddPendingHint();

    IDigest* GetUserJobMemoryDigest() const;
    IDigest* GetJobProxyMemoryDigest() const;

    virtual void SetupCallbacks();


    virtual NScheduler::TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const = 0;

    virtual NChunkPools::IChunkPoolInput* GetChunkPoolInput() const = 0;
    virtual NChunkPools::IChunkPoolOutput* GetChunkPoolOutput() const = 0;

    virtual EJobType GetJobType() const = 0;

    //! This method shows if the jobs of this task have an "input_paths" attribute
    //! in Cypress. This depends on if this task gets it input directly from the
    //! input tables or from the intermediate data.
    virtual bool SupportsInputPathYson() const = 0;

    //! Return a chunk mapping that is used to substitute input chunks when job spec is built.
    //! Base implementation returns task's own mapping.
    virtual TInputChunkMappingPtr GetChunkMapping() const;

    TSharedRef BuildJobSpecProto(TJobletPtr joblet);

protected:
    NLogging::TLogger Logger;

    //! Raw pointer here avoids cyclic reference; task cannot live longer than its host.
    ITaskHost* TaskHost_;

    //! Outgoing edges in data flow graph.
    std::vector<TEdgeDescriptor> EdgeDescriptors_;

    //! Increments each time a new job in this task is scheduled.
    TIdGenerator TaskJobIndexGenerator_;

    TTentativeTreeEligibility TentativeTreeEligibility_;

    mutable std::unique_ptr<IDigest> JobProxyMemoryDigest_;
    mutable std::unique_ptr<IDigest> UserJobMemoryDigest_;

    virtual TNullable<EScheduleJobFailReason> GetScheduleFailReason(ISchedulingContext* context);

    virtual void OnTaskCompleted();

    virtual void PrepareJoblet(TJobletPtr joblet);

    virtual void OnJobStarted(TJobletPtr joblet);

    //! True if task supports lost jobs.
    virtual bool CanLoseJobs() const;

    void ReinstallJob(TJobletPtr joblet, std::function<void()> releaseOutputCookie, bool waitForSnapshot = false);

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

    //! A convenience method for calling task->Finish() and
    //! task->SetInputVertex(this->GetJobType());
    void FinishTaskInput(const TTaskPtr& task);

    virtual NScheduler::TExtendedJobResources GetMinNeededResourcesHeavy() const = 0;
    virtual void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) = 0;

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

    TInputChunkMappingPtr InputChunkMapping_;

    NScheduler::TJobResources ApplyMemoryReserve(const NScheduler::TExtendedJobResources& jobResources) const;

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
    TJobResourcesWithQuota MinNeededResources;

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
