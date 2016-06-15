#pragma once

#include "private.h"
#include "chunk_list_pool.h"
#include "chunk_pool.h"
#include "config.h"
#include "event_log.h"
#include "job_memory.h"
#include "job_resources.h"
#include "operation_controller.h"
#include "serialize.h"

#include <yt/server/chunk_server/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/value_consumer.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/id_generator.h>
#include <yt/core/misc/nullable.h>

#include <yt/core/ytree/ypath_client.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

//! Describes which part of the operation needs a particular file.
DEFINE_ENUM(EOperationStage,
    (None)
    (Map)
    (ReduceCombiner)
    (Reduce)
);

DEFINE_ENUM(EInputChunkState,
    (Active)
    (Skipped)
    (Waiting)
);

DEFINE_ENUM(EJobReinstallReason,
    (Failed)
    (Aborted)
);

DEFINE_ENUM(EControllerState,
    (Preparing)
    (Running)
    (Finished)
);


class TOperationControllerBase
    : public IOperationController
    , public TEventLogHostBase
    , public NPhoenix::IPersistent
    , public NPhoenix::TFactoryTag<NPhoenix::TNullFactory>
{
public:
    TOperationControllerBase(
        TSchedulerConfigPtr config,
        TOperationSpecBasePtr spec,
        IOperationHost* host,
        TOperation* operation);

    virtual void Initialize(bool cleanStart) override;
    virtual void Prepare() override;
    virtual void Materialize() override;
    virtual void SaveSnapshot(TOutputStream* output) override;
    virtual void Revive(const TSharedRef& snapshot) override;
    virtual void Commit() override;

    virtual void OnJobStarted(const TJobId& jobId, TInstant startTime) override;
    virtual void OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary) override;
    virtual void OnJobFailed(std::unique_ptr<TFailedJobSummary> jobSummary) override;
    virtual void OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary) override;

    virtual void Abort() override;
    virtual void Complete() override;

    virtual TScheduleJobResultPtr ScheduleJob(
        ISchedulingContextPtr context,
        const TJobResources& jobLimits) override;

    virtual void UpdateConfig(TSchedulerConfigPtr config) override;

    virtual TCancelableContextPtr GetCancelableContext() const override;
    virtual IInvokerPtr GetCancelableControlInvoker() const override;
    virtual IInvokerPtr GetCancelableInvoker() const override;
    virtual IInvokerPtr GetInvoker() const override;

    virtual TFuture<void> Suspend() override;
    virtual void Resume() override;

    virtual bool GetCleanStart() const override;
    virtual int GetPendingJobCount() const override;
    virtual int GetTotalJobCount() const override;
    virtual TJobResources GetNeededResources() const override;

    virtual bool HasProgress() const override;

    virtual void BuildProgress(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildBriefProgress(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildResult(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildBriefSpec(NYson::IYsonConsumer* consumer) const override;

    virtual void Persist(TPersistenceContext& context) override;

protected:
    // Forward declarations.
    class TTask;
    typedef TIntrusivePtr<TTask> TTaskPtr;

    struct TTaskGroup;
    typedef TIntrusivePtr<TTaskGroup> TTaskGroupPtr;

    struct TJoblet;
    typedef TIntrusivePtr<TJoblet> TJobletPtr;

    struct TCompletedJob;
    typedef TIntrusivePtr<TCompletedJob> TCompletedJobPtr;


    TSchedulerConfigPtr Config;
    IOperationHost* Host;
    TOperation* Operation;

    const TOperationId OperationId;

    NApi::IClientPtr AuthenticatedMasterClient;
    NApi::IClientPtr AuthenticatedInputMasterClient;
    NApi::IClientPtr AuthenticatedOutputMasterClient;

    mutable NLogging::TLogger Logger;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;
    IInvokerPtr Invoker;
    ISuspendableInvokerPtr SuspendableInvoker;
    IInvokerPtr CancelableInvoker;

    std::atomic<EControllerState> State = {EControllerState::Preparing};

    // These totals are approximate.
    int TotalEstimatedInputChunkCount = 0;
    i64 TotalEstimatedInputDataSize = 0;
    i64 TotalEstimatedInputRowCount = 0;
    i64 TotalEstimatedCompressedDataSize = 0;

    int ChunkLocatedCallCount = 0;
    int UnavailableInputChunkCount = 0;

    bool CleanStart = true;

    // Job counters.
    TProgressCounter JobCounter;

    // Maps node ids to descriptors for job input chunks.
    NNodeTrackerClient::TNodeDirectoryPtr InputNodeDirectory;
    // Maps node ids to descriptors for job auxiliary chunks.
    NNodeTrackerClient::TNodeDirectoryPtr AuxNodeDirectory;

    // Operation transactions ids are stored here to avoid their retrieval from
    // potentially dangling Operation pointer.
    NObjectClient::TTransactionId AsyncSchedulerTransactionId;
    NObjectClient::TTransactionId SyncSchedulerTransactionId;
    NObjectClient::TTransactionId InputTransactionId;
    NObjectClient::TTransactionId OutputTransactionId;

    struct TLivePreviewTableBase
    {
        // Live preview table id.
        NCypressClient::TNodeId LivePreviewTableId;

        void Persist(TPersistenceContext& context);
    };

    struct TInputTable
        : public NChunkClient::TUserObject
    {
        //! Number of chunks in the whole table (without range selectors).
        int ChunkCount = -1;
        std::vector<NChunkClient::TInputChunkPtr> Chunks;
        NTableClient::TKeyColumns KeyColumns;
        NTableClient::TTableSchema Schema;
        bool PreserveSchemaOnWrite;

        bool IsForeign() const
        {
            return Path.GetForeign();
        }

        bool IsPrimary() const
        {
            return !IsForeign();
        }

        void Persist(TPersistenceContext& context);
    };

    std::vector<TInputTable> InputTables;


    struct TJobBoundaryKeys
    {
        NTableClient::TOwningKey MinKey;
        NTableClient::TOwningKey MaxKey;
        NChunkClient::TChunkTreeId ChunkTreeId;

        void Persist(TPersistenceContext& context);

    };

    struct TOutputTable
        : public NChunkClient::TUserObject
        , public TLivePreviewTableBase
    {
        bool AppendRequested = false;
        NChunkClient::EUpdateMode UpdateMode = NChunkClient::EUpdateMode::Overwrite;
        NCypressClient::ELockMode LockMode = NCypressClient::ELockMode::Exclusive;
        NTableClient::TTableWriterOptionsPtr Options = New<NTableClient::TTableWriterOptions>();
        NTableClient::TTableSchema Schema;
        bool PreserveSchemaOnWrite;
        bool ChunkPropertiesUpdateNeeded = false;

        // Server-side upload transaction.
        NTransactionClient::TTransactionId UploadTransactionId;

        // Chunk list for appending the output.
        NChunkClient::TChunkListId OutputChunkListId;

        // Statistics returned by EndUpload call.
        NChunkClient::NProto::TDataStatistics DataStatistics;

        //! Chunk trees comprising the output (the order matters).
        //! Keys are used when the output is sorted (e.g. in sort operations).
        //! Trees are sorted w.r.t. key and appended to #OutputChunkListId.
        std::multimap<int, NChunkClient::TChunkTreeId> OutputChunkTreeIds;

        std::vector<TJobBoundaryKeys> BoundaryKeys;

        NYson::TYsonString EffectiveAcl;

        void Persist(TPersistenceContext& context);
    };

    std::vector<TOutputTable> OutputTables;


    struct TIntermediateTable
        : public TLivePreviewTableBase
    {
        void Persist(TPersistenceContext& context);
    };

    TIntermediateTable IntermediateTable;


    struct TUserFile
        : public NChunkClient::TUserObject
    {
        std::shared_ptr<NYTree::IAttributeDictionary> Attributes;
        EOperationStage Stage = EOperationStage::None;
        Stroka FileName;
        std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
        bool Executable = false;
        NYson::TYsonString Format;
        
        void Persist(TPersistenceContext& context);
    };

    std::vector<TUserFile> Files;

    struct TJoblet
        : public TIntrinsicRefCounted
    {
        //! For serialization only.
        TJoblet()
            : JobIndex(-1)
            , StartRowIndex(-1)
            , OutputCookie(-1)
            , MemoryReserveEnabled(true)
        { }

        TJoblet(TTaskPtr task, int jobIndex)
            : Task(task)
            , JobIndex(jobIndex)
            , StartRowIndex(-1)
            , OutputCookie(IChunkPoolOutput::NullCookie)
        { }

        TTaskPtr Task;
        int JobIndex;
        i64 StartRowIndex;

        TJobId JobId;
        EJobType JobType;

        TExecNodeDescriptor NodeDescriptor;

        TJobResources ResourceLimits;

        TChunkStripeListPtr InputStripeList;
        IChunkPoolOutput::TCookie OutputCookie;

        bool MemoryReserveEnabled;

        //! All chunk lists allocated for this job.
        /*!
         *  For jobs with intermediate output this list typically contains one element.
         *  For jobs with final output this list typically contains one element per each output table.
         */
        std::vector<NChunkClient::TChunkListId> ChunkListIds;

        TInstant StartTime;
        TInstant FinishTime;

        void Persist(TPersistenceContext& context);
    };

    struct TCompletedJob
        : public TIntrinsicRefCounted
    {
        //! For persistence only.
        TCompletedJob()
            : Lost(false)
            , DestinationPool(nullptr)
        { }

        TCompletedJob(
            const TJobId& jobId,
            TTaskPtr sourceTask,
            IChunkPoolOutput::TCookie outputCookie,
            i64 dataSize,
            IChunkPoolInput* destinationPool,
            IChunkPoolInput::TCookie inputCookie,
            const TExecNodeDescriptor& nodeDescriptor)
            : Lost(false)
            , JobId(jobId)
            , SourceTask(std::move(sourceTask))
            , OutputCookie(outputCookie)
            , DataSize(dataSize)
            , DestinationPool(destinationPool)
            , InputCookie(inputCookie)
            , NodeDescriptor(nodeDescriptor)
        { }

        bool Lost;

        TJobId JobId;

        TTaskPtr SourceTask;
        IChunkPoolOutput::TCookie OutputCookie;
        i64 DataSize;

        IChunkPoolInput* DestinationPool;
        IChunkPoolInput::TCookie InputCookie;

        TExecNodeDescriptor NodeDescriptor;

        void Persist(TPersistenceContext& context);

    };

    class TTask
        : public TRefCounted
        , public NPhoenix::IPersistent
    {
    public:
        //! For persistence only.
        TTask();
        explicit TTask(TOperationControllerBase* controller);

        void Initialize();

        virtual Stroka GetId() const = 0;
        virtual TTaskGroupPtr GetGroup() const = 0;

        virtual int GetPendingJobCount() const;
        int GetPendingJobCountDelta();

        virtual int GetTotalJobCount() const;
        int GetTotalJobCountDelta();

        virtual TJobResources GetTotalNeededResources() const;
        TJobResources GetTotalNeededResourcesDelta();

        virtual bool IsIntermediateOutput() const;

        virtual TDuration GetLocalityTimeout() const = 0;
        virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const;
        virtual bool HasInputLocality() const;

        const TJobResources& GetMinNeededResources() const;
        virtual TJobResources GetNeededResources(TJobletPtr joblet) const;

        void ResetCachedMinNeededResources();

        DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, DelayedTime);

        void AddInput(TChunkStripePtr stripe);
        void AddInput(const std::vector<TChunkStripePtr>& stripes);
        void FinishInput();

        void CheckCompleted();

        void ScheduleJob(
            ISchedulingContext* context,
            const TJobResources& jobLimits,
            TScheduleJobResult* scheduleJobResult);

        virtual void OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary);
        virtual void OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary);
        virtual void OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary);
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

        i64 GetTotalDataSize() const;
        i64 GetCompletedDataSize() const;
        i64 GetPendingDataSize() const;

        virtual IChunkPoolInput* GetChunkPoolInput() const = 0;
        virtual IChunkPoolOutput* GetChunkPoolOutput() const = 0;

        virtual void Persist(TPersistenceContext& context) override;

    private:
        TOperationControllerBase* Controller;

        int CachedPendingJobCount;
        int CachedTotalJobCount;

        TJobResources CachedTotalNeededResources;
        mutable TNullable<TJobResources> CachedMinNeededResources;

        TInstant LastDemandSanityCheckTime;
        bool CompletedFired;

        //! For each lost job currently being replayed, maps output cookie to corresponding input cookie.
        yhash_map<IChunkPoolOutput::TCookie, IChunkPoolInput::TCookie> LostJobCookieMap;

    protected:
        NLogging::TLogger Logger;

        virtual bool CanScheduleJob(
            ISchedulingContext* context,
            const TJobResources& jobLimits);

        virtual TJobResources GetMinNeededResourcesHeavy() const = 0;

        virtual void OnTaskCompleted();

        virtual EJobType GetJobType() const = 0;
        virtual void PrepareJoblet(TJobletPtr joblet);
        virtual void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) = 0;

        virtual void OnJobStarted(TJobletPtr joblet);

        virtual bool IsMemoryReserveEnabled() const = 0;
        virtual NTableClient::TTableReaderOptionsPtr GetTableReaderOptions() const = 0;

        void AddPendingHint();
        void AddLocalityHint(NNodeTrackerClient::TNodeId nodeId);

        void ReinstallJob(TJobletPtr joblet, EJobReinstallReason reason);

        void AddSequentialInputSpec(
            NJobTrackerClient::NProto::TJobSpec* jobSpec,
            TJobletPtr joblet);
        void AddParallelInputSpec(
            NJobTrackerClient::NProto::TJobSpec* jobSpec,
            TJobletPtr joblet);
        static void AddChunksToInputSpec(
            NNodeTrackerClient::TNodeDirectoryBuilder* directoryBuilder,
            NScheduler::NProto::TTableInputSpec* inputSpec,
            TChunkStripePtr stripe);

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
            TChunkStripePtr stripe,
            TTaskPtr destinationTask,
            bool attachToLivePreview);
        void RegisterIntermediate(
            TJobletPtr joblet,
            TChunkStripePtr stripe,
            IChunkPoolInput* destinationPool,
            bool attachToLivePreview);

        static TChunkStripePtr BuildIntermediateChunkStripe(
            google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs);

        void RegisterOutput(
            TJobletPtr joblet,
            int key,
            const TCompletedJobSummary& jobSummary);

    };

    //! All tasks declared by calling #RegisterTask, mostly for debugging purposes.
    std::vector<TTaskPtr> Tasks;


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
        yhash_map<NNodeTrackerClient::TNodeId, yhash_set<TTaskPtr>> NodeIdToTasks;

        TTaskGroup()
        {
            MinNeededResources.SetUserSlots(1);
        }

        void Persist(TPersistenceContext& context);

    };

    NApi::ITransactionPtr StartTransaction(
        const Stroka& transactionName,
        NApi::IClientPtr client,
        const TNullable<NTransactionClient::TTransactionId>& parentTransactionId);

    //! All task groups declared by calling #RegisterTaskGroup, in the order of decreasing priority.
    std::vector<TTaskGroupPtr> TaskGroups;

    void RegisterTask(TTaskPtr task);
    void RegisterTaskGroup(TTaskGroupPtr group);

    void UpdateTask(TTaskPtr task);

    void UpdateAllTasks();

    virtual void CustomizeJoblet(TJobletPtr joblet);
    virtual void CustomizeJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec);

    void DoAddTaskLocalityHint(TTaskPtr task, NNodeTrackerClient::TNodeId nodeId);
    void AddTaskLocalityHint(TTaskPtr task, NNodeTrackerClient::TNodeId nodeId);
    void AddTaskLocalityHint(TTaskPtr task, TChunkStripePtr stripe);
    void AddTaskPendingHint(TTaskPtr task);
    void ResetTaskLocalityDelays();

    void MoveTaskToCandidates(TTaskPtr task, std::multimap<i64, TTaskPtr>& candidateTasks);

    bool CheckJobLimits(
        TTaskPtr task,
        const TJobResources& jobLimits,
        const TJobResources& nodeResourceLimits);

    void CheckTimeLimit();

    void DoScheduleJob(
        ISchedulingContext* context,
        const TJobResources& jobLimits,
        TScheduleJobResult* scheduleJobResult);

    void DoScheduleLocalJob(
        ISchedulingContext* context,
        const TJobResources& jobLimits,
        TScheduleJobResult* scheduleJobResult);

    void DoScheduleNonLocalJob(
        ISchedulingContext* context,
        const TJobResources& jobLimits,
        TScheduleJobResult* scheduleJobResult);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);


    // Jobs in progress management.
    void RegisterJoblet(TJobletPtr joblet);
    TJobletPtr GetJoblet(const TJobId& jobId);
    void RemoveJoblet(const TJobId& jobId);


    // Initialization.
    virtual void DoInitialize();
    virtual void InitializeConnections();
    virtual void InitializeTransactions();
    void CheckTransactions();


    // Preparation.
    void FetchInputTables();
    void LockInputTables();
    void GetOutputTablesSchema();
    virtual void PrepareOutputTables();
    void BeginUploadOutputTables();
    void GetOutputTablesUploadParams();
    void FetchUserFiles(std::vector<TUserFile>* files);
    void LockUserFiles(std::vector<TUserFile>* files);
    void CreateLivePreviewTables();
    void LockLivePreviewTables();
    void CollectTotals();
    virtual void CustomPrepare();
    void AddAllTaskPendingHints();
    void InitInputChunkScraper();
    void SuspendUnavailableInputStripes();
    void InitQuerySpec(
        NProto::TSchedulerJobSpecExt* schedulerJobSpecExt,
        const Stroka& queryString,
        const NQueryClient::TTableSchema& schema);

    void PickIntermediateDataCell();
    void InitChunkListPool();

    // Initialize transactions
    void StartAsyncSchedulerTransaction();
    void StartSyncSchedulerTransaction();
    virtual void StartInputTransaction(const NObjectClient::TTransactionId& parentTransactionId);
    virtual void StartOutputTransaction(const NObjectClient::TTransactionId& parentTransactionId);

    // Completion.
    void TeleportOutputChunks();
    void AttachOutputChunks();
    void EndUploadOutputTables();
    virtual void CustomCommit();

    // Revival.
    void ReinstallLivePreview();
    void AbortAllJoblets();

    void DoSaveSnapshot(TOutputStream* output);
    void DoLoadSnapshot(TSharedRef snapshot);

    //! Called to extract input table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetInputTablePaths() const = 0;

    //! Called to extract output table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetOutputTablePaths() const = 0;

    typedef std::pair<NYPath::TRichYPath, EOperationStage> TPathWithStage;

    //! Called to extract file paths from the spec.
    virtual std::vector<TPathWithStage> GetFilePaths() const;

    //! Called when a job is unable to read a chunk.
    void OnChunkFailed(const NChunkClient::TChunkId& chunkId);

    //! Called when a job is unable to read an intermediate chunk
    //! (i.e. that is not a part of the input).
    /*!
     *  The default implementation fails the operation immediately.
     *  Those operations providing some fault tolerance for intermediate chunks
     *  must override this method.
     */
    void OnIntermediateChunkUnavailable(const NChunkClient::TChunkId& chunkId);


    struct TStripeDescriptor
    {
        TChunkStripePtr Stripe;
        IChunkPoolInput::TCookie Cookie;
        TTaskPtr Task;

        TStripeDescriptor()
            : Cookie(IChunkPoolInput::NullCookie)
        { }

        void Persist(TPersistenceContext& context);

    };

    struct TInputChunkDescriptor
    {
        SmallVector<TStripeDescriptor, 1> InputStripes;
        SmallVector<NChunkClient::TInputChunkPtr, 1> InputChunks;
        EInputChunkState State;

        TInputChunkDescriptor()
            : State(EInputChunkState::Active)
        { }

        void Persist(TPersistenceContext& context);

    };

    //! Callback called by TChunkScraper when get information on some chunk.
    void OnInputChunkLocated(
        const NChunkClient::TChunkId& chunkId,
        const NChunkClient::TChunkReplicaList& replicas);

    //! Called when a job is unable to read an input chunk or
    //! chunk scraper has encountered unavailable chunk.
    void OnInputChunkUnavailable(
        const NChunkClient::TChunkId& chunkId,
        TInputChunkDescriptor& descriptor);

    void OnInputChunkAvailable(
        const NChunkClient::TChunkId& chunkId,
        TInputChunkDescriptor& descriptor,
        const NChunkClient::TChunkReplicaList& replicas);

    virtual bool IsOutputLivePreviewSupported() const;
    virtual bool IsIntermediateLivePreviewSupported() const;

    virtual void OnOperationCompleted();
    virtual void OnOperationFailed(const TError& error);

    virtual bool IsCompleted() const = 0;

    //! Returns |true| when the controller is prepared.
    /*!
     *  Preparation happens in a controller thread.
     *  The state must not be touched from the control thread
     *  while this function returns |false|.
     */
    bool IsPrepared() const;

    //! Returns |true| as long as the operation can schedule new jobs.
    bool IsRunning() const;

    //! Returns |true| when operation completion event is scheduled to control invoker.
    bool IsFinished() const;

    // Unsorted helpers.

    //! Enables verification that the output is sorted.
    virtual bool ShouldVerifySortedOutput() const;

    //! Enables fetching all input replicas (not only data)
    virtual bool IsParityReplicasFetchEnabled() const;

    //! Enables fetching boundary keys for chunk specs.
    virtual bool IsBoundaryKeysFetchEnabled() const;

    //! If |true| then all jobs started within the operation must
    //! preserve row count. This invariant is checked for each completed job.
    //! Should a violation be discovered, the operation fails.
    virtual bool IsRowCountPreserved() const;

    typedef std::function<bool(const TInputTable& table)> TInputTableFilter;

    NTableClient::TKeyColumns CheckInputTablesSorted(
        const NTableClient::TKeyColumns& keyColumns,
        TInputTableFilter inputTableFilter = [](const TInputTable&) { return true; });

    static bool CheckKeyColumnsCompatible(
        const NTableClient::TKeyColumns& fullColumns,
        const NTableClient::TKeyColumns& prefixColumns);

    void UpdateAllTasksIfNeeded(const TProgressCounter& jobCounter);
    bool IsMemoryReserveEnabled(const TProgressCounter& jobCounter) const;
    i64 GetMemoryReserve(bool memoryReserveEnabled, TUserJobSpecPtr userJobSpec) const;

    void RegisterInputStripe(TChunkStripePtr stripe, TTaskPtr task);


    void RegisterBoundaryKeys(
        const NProto::TOutputResult& boundaryKeys,
        const NChunkClient::TChunkTreeId& chunkTreeId,
        TOutputTable* outputTable);

    void RegisterBoundaryKeys(
        const NTableClient::TBoundaryKeys& boundaryKeys,
        int key,
        TOutputTable* outputTable);

    virtual void RegisterOutput(TJobletPtr joblet, int key, const TCompletedJobSummary& jobSummary);

    void RegisterOutput(
        NChunkClient::TInputChunkPtr chunkSpec,
        int key,
        int tableIndex);

    void RegisterOutput(
        const NChunkClient::TChunkTreeId& chunkTreeId,
        int key,
        int tableIndex,
        TOutputTable& table);

    void RegisterIntermediate(
        TJobletPtr joblet,
        TCompletedJobPtr completedJob,
        TChunkStripePtr stripe,
        bool attachToLivePreview);

    bool HasEnoughChunkLists(bool intermediate);
    NChunkClient::TChunkListId ExtractChunkList(NObjectClient::TCellTag cellTag);
    void ReleaseChunkLists(const std::vector<NChunkClient::TChunkListId>& ids);

    //! Called after preparation to decrease memory footprint.
    void ClearInputChunkBoundaryKeys();

    //! Returns the list of all input chunks collected from all primary input tables.
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryInputChunks() const;

    //! Returns the list of lists of all input chunks collected from all foreign input tables.
    std::vector<std::deque<NChunkClient::TInputChunkPtr>> CollectForeignInputChunks() const;

    //! Converts a list of input chunks into a list of chunk stripes for further
    //! processing. Each stripe receives exactly one chunk (as suitable for most
    //! jobs except merge). The resulting stripes are of approximately equal
    //! size. The size per stripe is either |maxSliceDataSize| or
    //! |TotalEstimateInputDataSize / jobCount|, whichever is smaller. If the resulting
    //! list contains less than |jobCount| stripes then |jobCount| is decreased
    //! appropriately.
    std::vector<TChunkStripePtr> SliceChunks(
        const std::vector<NChunkClient::TInputChunkPtr>& chunkSpecs,
        i64 maxSliceDataSize,
        int* jobCount);

    std::vector<TChunkStripePtr> SliceInputChunks(
        i64 maxSliceDataSize,
        int* jobCount);

    int SuggestJobCount(
        i64 totalDataSize,
        i64 dataSizePerJob,
        TNullable<int> configJobCount,
        int maxJobCount) const;

    void InitUserJobSpecTemplate(
        NScheduler::NProto::TUserJobSpec* proto,
        TUserJobSpecPtr config,
        const std::vector<TUserFile>& files);

    void InitUserJobSpec(
        NScheduler::NProto::TUserJobSpec* proto,
        TJobletPtr joblet,
        i64 memoryReserve);

    // Amount of memory reserved for output table writers in job proxy.
    i64 GetFinalOutputIOMemorySize(TJobIOConfigPtr ioConfig) const;

    i64 GetFinalIOMemorySize(
        TJobIOConfigPtr ioConfig,
        const TChunkStripeStatisticsVector& stripeStatistics) const;

    static void InitIntermediateOutputConfig(TJobIOConfigPtr config);
    void InitFinalOutputConfig(TJobIOConfigPtr config);

    static NTableClient::TTableReaderOptionsPtr CreateTableReaderOptions(TJobIOConfigPtr ioConfig);
    static NTableClient::TTableReaderOptionsPtr CreateIntermediateTableReaderOptions();

    void ValidateUserFileCount(TUserJobSpecPtr spec, const Stroka& operation);

    const std::vector<TExecNodeDescriptor>& GetExecNodeDescriptors();

private:
    typedef TOperationControllerBase TThis;

    typedef yhash_map<NChunkClient::TChunkId, TInputChunkDescriptor> TInputChunkMap;

    //! Keeps information needed to maintain the liveness state of input chunks.
    TInputChunkMap InputChunkMap;

    TOperationSpecBasePtr Spec;

    NObjectClient::TCellTag IntermediateOutputCellTag = NObjectClient::InvalidCellTag;
    TChunkListPoolPtr ChunkListPool;
    yhash<NObjectClient::TCellTag, int> CellTagToOutputTableCount;

    std::atomic<int> CachedPendingJobCount = {0};

    TJobResources CachedNeededResources;
    NConcurrency::TReaderWriterSpinLock CachedNeededResourcesLock;

    //! Maps an intermediate chunk id to its originating completed job.
    yhash_map<NChunkClient::TChunkId, TCompletedJobPtr> ChunkOriginMap;

    //! Maps scheduler's job ids to controller's joblets.
    //! NB: |TJobPtr -> TJobletPtr| mapping would be faster but
    //! it cannot be serialized that easily.
    yhash_map<TJobId, TJobletPtr> JobletMap;

    //! Used to distinguish already seen InputChunks while building #InputChunkMap.
    yhash_set<NChunkClient::TInputChunkPtr> InputChunkSpecs;

    NChunkClient::TChunkScraperPtr InputChunkScraper;

    //! Increments each time a new job is scheduled.
    TIdGenerator JobIndexGenerator;

    //! Aggregates job statistics.
    NJobTrackerClient::TStatistics JobStatistics;

    //! One output table can have row count limit on operation.
    TNullable<int> RowCountLimitTableIndex;
    i64 RowCountLimit = std::numeric_limits<i64>::max();

    //! Runs periodic time limit checks that fail operation on timeout.
    NConcurrency::TPeriodicExecutorPtr CheckTimeLimitExecutor;

    //! Exec node count do not consider scheduling tag.
    //! But descriptors do.
    int ExecNodeCount_ = 0;
    std::vector<TExecNodeDescriptor> ExecNodesDescriptors_;
    TInstant LastGetExecNodesInformationTime_;

    const std::unique_ptr<NTableClient::IValueConsumer> EventLogValueConsumer_;
    const std::unique_ptr<NYson::IYsonConsumer> EventLogTableConsumer_;

    void GetExecNodesInformation();
    int GetExecNodeCount();

    void UpdateJobStatistics(const TJobSummary& jobSummary);

    NApi::IClientPtr CreateClient();

    void IncreaseNeededResources(const TJobResources& resourcesDelta);

    //! Sets finish time and other timing statistics.
    void FinalizeJoblet(
        const TJobletPtr& joblet,
        TJobSummary* jobSummary);

    TFluentLogEvent LogFinishedJobFluently(
        ELogEventType eventType,
        const TJobletPtr& joblet,
        const TJobSummary& jobSummary);

    virtual NYson::IYsonConsumer* GetEventLogConsumer() override;

    TCodicilGuard MakeCodicilGuard() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
