#pragma once

#include "private.h"

#include "competitive_job_manager.h"
#include "data_flow_graph.h"
#include "job_splitter.h"
#include "helpers.h"
#include "extended_job_resources.h"

#include <yt/yt/server/controller_agent/tentative_tree_eligibility.h>

#include <yt/yt/server/lib/chunk_pools/chunk_stripe_key.h>
#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/input_chunk_mapping.h>

#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/server/lib/controller_agent/progress_counter.h>
#include <yt/yt/server/lib/controller_agent/serialize.h>
#include <yt/yt/server/lib/controller_agent/read_range_registry.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/key_bound.h>

#include <yt/yt/core/concurrency/spinlock.h>
#include <yt/yt/core/misc/digest.h>
#include <yt/yt/core/misc/histogram.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TTask
    : public TRefCounted
    , public IPersistent
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TInstant>, DelayedTime);
    DEFINE_BYVAL_RW_PROPERTY(TDataFlowGraph::TVertexDescriptor, InputVertex, TDataFlowGraph::TVertexDescriptor());

    // TODO(max42): this should be done somehow better.
    //! Indicates if input for this task corresponds to input tables.
    DEFINE_BYVAL_RW_PROPERTY(bool, IsInput);

public:
    //! For persistence only.
    TTask();
    TTask(ITaskHostPtr taskHost, std::vector<TStreamDescriptor> streamDescriptors);
    explicit TTask(ITaskHostPtr taskHost);

    //! This method is called on task object creation (both at clean creation and at revival).
    //! It may be used when calling virtual method is needed, but not allowed.
    virtual void Initialize();

    //! This method is called on task object creation (at clean creation only).
    //! It may be used when calling virtual method is needed, but not allowed.
    virtual void Prepare();

    //! Title of a data flow graph vertex that appears in a web interface and coincides with the job type
    //! for builtin tasks. For example, "SortedReduce" or "PartitionMap".
    virtual TDataFlowGraph::TVertexDescriptor GetVertexDescriptor() const;
    //! Human-readable title of a particular task that appears in logging. For builtin tasks it coincides
    //! with the vertex descriptor and a task level in brackets (if applicable).
    virtual TString GetTitle() const;

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

    virtual void FinishInput();

    void UpdateTask();

    // NB: This works well until there is no more than one input data flow vertex for any task.
    void RegisterInGraph();
    void RegisterInGraph(TDataFlowGraph::TVertexDescriptor inputVertex);

    void CheckCompleted();
    void ForceComplete();

    virtual bool ValidateChunkCount(int chunkCount);

    void ScheduleJob(
        ISchedulingContext* context,
        const NScheduler::TJobResources& jobLimits,
        const TString& treeId,
        bool treeIsTentative,
        NScheduler::TControllerScheduleJobResult* scheduleJobResult);

    bool TryRegisterSpeculativeJob(const TJobletPtr& joblet);
    std::optional<EAbortReason> ShouldAbortJob(const TJobletPtr& joblet);

    virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary);
    virtual TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary);
    virtual TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary);
    virtual void OnJobRunning(TJobletPtr joblet, const TRunningJobSummary& jobSummary);
    virtual void OnJobLost(TCompletedJobPtr completedJob);

    virtual void OnStripeRegistrationFailed(
        TError error,
        NChunkPools::IChunkPoolInput::TCookie cookie,
        const NChunkPools::TChunkStripePtr& stripe,
        const TStreamDescriptor& streamDescriptor);

    void CheckResourceDemandSanity(const NScheduler::TJobResources& neededResources);
    void DoCheckResourceDemandSanity(const NScheduler::TJobResources& neededResources);

    virtual bool IsCompleted() const;

    virtual bool IsActive() const;

    i64 GetTotalDataWeight() const;
    i64 GetCompletedDataWeight() const;
    i64 GetPendingDataWeight() const;

    i64 GetInputDataSliceCount() const;

    std::vector<std::optional<i64>> GetMaximumUsedTmpfsSizes() const;

    virtual void Persist(const TPersistenceContext& context) override;

    virtual NScheduler::TUserJobSpecPtr GetUserJobSpec() const;
    bool HasUserJob() const;

    // TODO(max42): eliminate necessity for this method (YT-10528).
    virtual bool IsSimpleTask() const;

    ITaskHost* GetTaskHost();

    IDigest* GetUserJobMemoryDigest() const;
    IDigest* GetJobProxyMemoryDigest() const;

    virtual void SetupCallbacks();

    virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const = 0;

    virtual NChunkPools::IChunkPoolInputPtr GetChunkPoolInput() const = 0;
    virtual NChunkPools::IChunkPoolOutputPtr GetChunkPoolOutput() const = 0;

    virtual EJobType GetJobType() const = 0;

    //! Return a chunk mapping that is used to substitute input chunks when job spec is built.
    //! Base implementation returns task's own mapping.
    virtual NChunkPools::TInputChunkMappingPtr GetChunkMapping() const;

    std::vector<TString> FindAndBanSlowTentativeTrees();

    void LogTentativeTreeStatistics() const;

    TSharedRef BuildJobSpecProto(TJobletPtr joblet, const NScheduler::NProto::TScheduleJobSpec& scheduleJobSpec);

    //! Checks if jobs can be interrupted. Subclasses should call the base method
    //! but may add extra restrictions.
    virtual bool IsJobInterruptible() const;

    void BuildTaskYson(NYTree::TFluentMap fluent) const;

    virtual void PropagatePartitions(
        const std::vector<TStreamDescriptor>& streamDescriptors,
        const NChunkPools::TChunkStripeListPtr& inputStripeList,
        std::vector<NChunkPools::TChunkStripePtr>* outputStripes);

    virtual NChunkPools::IChunkPoolOutput::TCookie ExtractCookie(NNodeTrackerClient::TNodeId nodeId);

    void UpdateMemoryDigests(const TJobletPtr& joblet, const TStatistics& statistics, bool resourceOverdraft);

    void BuildFeatureYson(NYTree::TFluentAny fluent) const;

    void FinalizeFeatures();

    void StopTiming();

protected:
    NLogging::TLogger Logger;

    //! Raw pointer here avoids cyclic reference; task cannot live longer than its host.
    ITaskHost* TaskHost_;

    //! Outgoing data stream descriptors.
    std::vector<TStreamDescriptor> StreamDescriptors_;

    //! Increments each time a new job in this task is scheduled.
    TIdGenerator TaskJobIndexGenerator_;

    TTentativeTreeEligibility TentativeTreeEligibility_;

    mutable std::unique_ptr<IDigest> JobProxyMemoryDigest_;
    mutable std::unique_ptr<IDigest> UserJobMemoryDigest_;

    std::unique_ptr<IJobSplitter> JobSplitter_;

    NChunkPools::TInputChunkMappingPtr InputChunkMapping_;

    virtual std::optional<EScheduleJobFailReason> GetScheduleFailReason(ISchedulingContext* context);

    virtual void OnTaskCompleted();

    virtual void OnJobStarted(TJobletPtr joblet);

    //! True if task supports lost jobs.
    virtual bool CanLoseJobs() const;

    virtual void OnChunkTeleported(NChunkClient::TInputChunkPtr chunk, std::any tag);

    void ReinstallJob(std::function<void()> releaseOutputCookie);

    void ReleaseJobletResources(TJobletPtr joblet, bool waitForSnapshot);

    std::unique_ptr<NNodeTrackerClient::TNodeDirectoryBuilder> MakeNodeDirectoryBuilder(
        NScheduler::NProto::TSchedulerJobSpecExt* schedulerJobSpec);
    void AddSequentialInputSpec(
        NJobTrackerClient::NProto::TJobSpec* jobSpec,
        TJobletPtr joblet,
        NTableClient::TComparator comparator = NTableClient::TComparator());
    void AddParallelInputSpec(
        NJobTrackerClient::NProto::TJobSpec* jobSpec,
        TJobletPtr joblet,
        NTableClient::TComparator comparator = NTableClient::TComparator());
    void AddChunksToInputSpec(
        NNodeTrackerClient::TNodeDirectoryBuilder* directoryBuilder,
        NScheduler::NProto::TTableInputSpec* inputSpec,
        NChunkPools::TChunkStripePtr stripe,
        NTableClient::TComparator comparator = NTableClient::TComparator());

    void AddOutputTableSpecs(NJobTrackerClient::NProto::TJobSpec* jobSpec, TJobletPtr joblet);

    static void UpdateInputSpecTotals(
        NJobTrackerClient::NProto::TJobSpec* jobSpec,
        TJobletPtr joblet);

    // Send stripe to the next chunk pool.
    void RegisterStripe(
        NChunkPools::TChunkStripePtr chunkStripe,
        const TStreamDescriptor& streamDescriptor,
        TJobletPtr joblet,
        NChunkPools::TChunkStripeKey key = NChunkPools::TChunkStripeKey(),
        bool processEmptyStripes = false);

    static std::vector<NChunkPools::TChunkStripePtr> BuildChunkStripes(
        google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs,
        int tableCount);

    static NChunkPools::TChunkStripePtr BuildIntermediateChunkStripe(
        google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs);

    std::vector<NChunkPools::TChunkStripePtr> BuildOutputChunkStripes(
        NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt,
        const std::vector<NChunkClient::TChunkTreeId>& chunkTreeIds,
        google::protobuf::RepeatedPtrField<NScheduler::NProto::TOutputResult> boundaryKeys);

    void AddFootprintAndUserJobResources(TExtendedJobResources& jobResources) const;

    //! This method is called for each input data slice before adding it to the chunk pool input.
    //! It transforms data slices into new mode, adapts them for using in corresponding chunk pool
    //! (using task-dependent AdaptInputDataSlice virtual method) and registers data slice
    //! input read limits in the InputChunkToReadBounds_ mapping.
    void AdjustInputKeyBounds(const NChunkClient::TLegacyDataSlicePtr& dataSlice);

    //! Default implementation simply drops key bounds which is perfectly OK for non-sorted chunk pools.
    virtual void AdjustDataSliceForPool(const NChunkClient::TLegacyDataSlicePtr& dataSlice) const;

    //! This method is called for each ouput data slice before serializing it to the job spec.
    //! It applies data slice input read limits from the InputChunkToReadBounds_ mapping.
    //! It is overridden only in the legacy version of sorted controller.
    void AdjustOutputKeyBounds(const NChunkClient::TLegacyDataSlicePtr& dataSlice) const;

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
        const NChunkPools::TChunkStripeKey& key = NChunkPools::TChunkStripeKey(),
        bool processEmptyStripes = false);

    //! A convenience method for calling task->Finish() and
    //! task->SetInputVertex(this->GetJobType());
    void FinishTaskInput(const TTaskPtr& task);

    virtual TExtendedJobResources GetMinNeededResourcesHeavy() const = 0;

    /*!
     *  \note Invoker affinity: JobSpecBuildInvoker.
     */
    virtual void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) = 0;

    virtual void SetStreamDescriptors(TJobletPtr joblet) const;

    virtual bool IsInputDataWeightHistogramSupported() const;

    virtual TJobSplitterConfigPtr GetJobSplitterConfig() const = 0;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TTask, 0x81ab3cd3);

    int CachedPendingJobCount_;
    int CachedTotalJobCount_;

    std::vector<std::optional<i64>> MaximumUsedTmpfsSizes_;

    TJobResources CachedTotalNeededResources_;
    mutable std::optional<TExtendedJobResources> CachedMinNeededResources_;

    bool CompletedFired_ = false;

    using TCookieAndPool = std::pair<NChunkPools::IChunkPoolInput::TCookie, NChunkPools::IChunkPoolInputPtr>;

    //! For each lost job currently being replayed and destination pool, maps output cookie to corresponding input cookie.
    std::map<TCookieAndPool, NChunkPools::IChunkPoolInput::TCookie> LostJobCookieMap;

    TCompetitiveJobManager CompetitiveJobManager_;

    //! Time of first job scheduling.
    std::optional<TInstant> StartTime_;

    //! Time of task completion.
    std::optional<TInstant> CompletionTime_;

    NProfiling::TWallTimer ReadyTimer_{false};
    NProfiling::TWallTimer ExhaustTimer_{false};

    //! Caches results of SerializeToWireProto serializations.
    // NB: This field is transient intentionally.
    // NB: this field is used in BuildJobSpecProto which is run in an non-serialized invoker,
    // so access it only under the following spinlock.
    THashMap<NTableClient::TTableSchemaPtr, TString> TableSchemaToProtobufTableSchema_;
    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, TableSchemaToProtobufTableSchemaLock_);

    std::unique_ptr<IHistogram> EstimatedInputDataWeightHistogram_;
    std::unique_ptr<IHistogram> InputDataWeightHistogram_;

    TReadRangeRegistry InputReadRangeRegistry_;
    TControllerFeatures ControllerFeatures_;

    //! If job is aborted because of resource overdraft, its output cookie is put into this set.
    //! Next incarnation of this job will be run with maximal possible memory reserve factor to
    //! prevent repeated overdraft.
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> ResourceOverdraftedOutputCookies_;

    NScheduler::TJobResources ApplyMemoryReserve(
        const TExtendedJobResources& jobResources,
        double jobProxyMemoryReserveFactor,
        std::optional<double> userJobMemoryReserveFactor) const;

    void UpdateMaximumUsedTmpfsSizes(const TStatistics& statistics);

    void AbortJobViaScheduler(TJobId jobId, EAbortReason reason);

    void OnSpeculativeJobScheduled(const TJobletPtr& joblet);

    double GetJobProxyMemoryReserveFactor() const;

    //! std::nullopt means that user jobs are not used in task (e.g. unordered merge).
    std::optional<double> GetUserJobMemoryReserveFactor() const;

    int EstimateSplitJobCount(const TCompletedJobSummary& jobSummary, const TJobletPtr& joblet);

    TString GetOrCacheSerializedSchema(const NTableClient::TTableSchemaPtr& schema);

    std::optional<TString> SelectProfiler();

    void OnPendingJobCountUpdated();

    TDuration GetWallTime() const;
    TDuration GetReadyTime() const;
    TDuration GetExhaustTime() const;
};

DEFINE_REFCOUNTED_TYPE(TTask)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
