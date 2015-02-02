#pragma once

#include "private.h"
#include "operation_controller.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "job_resources.h"
#include "serialize.h"
#include "event_log.h"

#include <core/misc/nullable.h>
#include <core/misc/id_generator.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/periodic_executor.h>

#include <core/actions/cancelable_context.h>

#include <core/logging/log.h>

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/cypress_client/public.h>

#include <core/ytree/ypath_client.h>
#include <core/ytree/yson_string.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <ytlib/node_tracker_client/public.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/job_tracker_client/statistics.h>

#include <ytlib/scheduler/statistics.h>

#include <server/chunk_server/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

//! Describes which part of the operation needs a particular file.
DEFINE_ENUM(EOperationStage,
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

class TOperationControllerBase
    : public IOperationController
    , public NPhoenix::IPersistent
    , public NPhoenix::TFactoryTag<NPhoenix::TNullFactory>
{
public:
    TOperationControllerBase(
        TSchedulerConfigPtr config,
        TOperationSpecBasePtr spec,
        IOperationHost* host,
        TOperation* operation);

    virtual void Initialize() override;
    virtual void Essentiate() override;
    virtual TFuture<void> Prepare() override;
    virtual void SaveSnapshot(TOutputStream* output) override;
    virtual TFuture<void> Revive() override;
    virtual TFuture<void> Commit() override;

    virtual void OnJobRunning(TJobPtr job, const NJobTrackerClient::NProto::TJobStatus& status) override;
    virtual void OnJobCompleted(TJobPtr job) override;
    virtual void OnJobFailed(TJobPtr job) override;
    virtual void OnJobAborted(TJobPtr job) override;

    virtual void Abort() override;

    virtual TJobPtr ScheduleJob(
        ISchedulingContext* context,
        const NNodeTrackerClient::NProto::TNodeResources& jobLimits) override;

    virtual TCancelableContextPtr GetCancelableContext() const override;
    virtual IInvokerPtr GetCancelableControlInvoker() const override;
    virtual IInvokerPtr GetCancelableBackgroundInvoker() const override;

    virtual int GetPendingJobCount() const override;
    virtual int GetTotalJobCount() const override;
    virtual NNodeTrackerClient::NProto::TNodeResources GetNeededResources() const override;

    virtual void BuildProgress(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildBriefProgress(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildResult(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildBriefSpec(NYson::IYsonConsumer* consumer) const override;

    virtual bool NeedsAllChunkParts() const override;

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

    NApi::IClientPtr AuthenticatedMasterClient;
    NApi::IClientPtr AuthenticatedInputMasterClient;
    NApi::IClientPtr AuthenticatedOutputMasterClient;

    mutable NLog::TLogger Logger;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;
    IInvokerPtr CancelableBackgroundInvoker;


    //! Becomes |true| when the controller is prepared.
    /*!
     *  Preparation happens in a background thread.
     *  The state must not be touched from the control thread
     *  while this flag is |false|.
     */
    bool Prepared;

    //! Remains |true| as long as the operation can schedule new jobs.
    bool Running;


    // These totals are approximate.
    int TotalEstimatedInputChunkCount;
    i64 TotalEstimatedInputDataSize;
    i64 TotalEstimatedInputRowCount;
    i64 TotalEstimatedInputValueCount;

    // These totals are exact.
    NChunkClient::NProto::TDataStatistics TotalExactInputDataStatistics;

    // These totals are exact.
    NChunkClient::NProto::TDataStatistics TotalIntermediateDataStatistics;

    // These totals are exact.
    std::vector<NChunkClient::NProto::TDataStatistics> TotalOutputsDataStatistics;

    int UnavailableInputChunkCount;

    // Job counters.
    TProgressCounter JobCounter;

    // Job statistics.
    NJobTrackerClient::NProto::TJobStatistics CompletedJobStatistics;
    NJobTrackerClient::NProto::TJobStatistics FailedJobStatistics;
    NJobTrackerClient::NProto::TJobStatistics AbortedJobStatistics;

    // Maps node ids seen in fetch responses to node descriptors.
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;


    struct TUserTableBase
    {
        NYPath::TRichYPath Path;
        NObjectClient::TObjectId ObjectId;

        void Persist(TPersistenceContext& context);
    };


    struct TLivePreviewTableBase
    {
        // Live preview table id.
        NCypressClient::TNodeId LivePreviewTableId;

        // Chunk list for appending live preview results.
        NChunkClient::TChunkListId LivePreviewChunkListId;

        void Persist(TPersistenceContext& context);
    };

    struct TInputTable
        : public TUserTableBase
    {
        // Number of chunks in the whole table (without range selectors).
        int ChunkCount = -1;
        std::vector<NChunkClient::NProto::TChunkSpec> Chunks;
        TNullable< std::vector<Stroka> > KeyColumns;

        void Persist(TPersistenceContext& context);
    };

    std::vector<TInputTable> InputTables;


    struct TEndpoint
    {
        NVersionedTableClient::TOwningKey Key;
        bool Left;
        int ChunkTreeKey;

        void Persist(TPersistenceContext& context);

    };

    struct TOutputTable
        : public TUserTableBase
        , public TLivePreviewTableBase
    {
        TOutputTable()
            : Clear(false)
            , Overwrite(false)
            , LockMode(NCypressClient::ELockMode::Shared)
            , Options(New<NTableClient::TTableWriterOptions>())
        { }

        bool Clear;
        bool Overwrite;
        NCypressClient::ELockMode LockMode;
        NTableClient::TTableWriterOptionsPtr Options;

        // Chunk list for appending the output.
        NChunkClient::TChunkListId OutputChunkListId;

        //! Chunk trees comprising the output (the order matters).
        //! Keys are used when the output is sorted (e.g. in sort operations).
        //! Trees are sorted w.r.t. key and appended to #OutputChunkListId.
        std::multimap<int, NChunkClient::TChunkTreeId> OutputChunkTreeIds;

        std::vector<TEndpoint> Endpoints;

        NYTree::TYsonString EffectiveAcl;

        void Persist(TPersistenceContext& context);
    };

    std::vector<TOutputTable> OutputTables;


    struct TIntermediateTable
        : public TLivePreviewTableBase
    {
        void Persist(TPersistenceContext& context);
    };

    TIntermediateTable IntermediateTable;


    struct TUserFileBase
    {
        NYPath::TRichYPath Path;
        EOperationStage Stage;
        Stroka FileName;

        void Persist(TPersistenceContext& context);

    };

    struct TRegularUserFile
        : public TUserFileBase
    {
        NChunkClient::NProto::TRspFetch FetchResponse;
        bool Executable;

        void Persist(TPersistenceContext& context);

    };

    std::vector<TRegularUserFile> RegularFiles;


    struct TUserTableFile
        : public TUserFileBase
    {
        NChunkClient::NProto::TRspFetch FetchResponse;
        NYTree::TYsonString Format;

        void Persist(TPersistenceContext& context);

    };

    std::vector<TUserTableFile> TableFiles;


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

        TJobPtr Job;
        TChunkStripeListPtr InputStripeList;
        IChunkPoolOutput::TCookie OutputCookie;

        bool MemoryReserveEnabled;

        //! All chunk lists allocated for this job.
        /*!
         *  For jobs with intermediate output this list typically contains one element.
         *  For jobs with final output this list typically contains one element per each output table.
         */
        std::vector<NChunkClient::TChunkListId> ChunkListIds;

        void Persist(TPersistenceContext& context);
    };

    struct TCompletedJob
        : public TIntrinsicRefCounted
    {
        //! For persistence only.
        TCompletedJob()
            : IsLost(false)
            , DestinationPool(nullptr)
        { }

        TCompletedJob(
            const TJobId& jobId,
            TTaskPtr sourceTask,
            IChunkPoolOutput::TCookie outputCookie,
            IChunkPoolInput* destinationPool,
            IChunkPoolInput::TCookie inputCookie,
            const Stroka& address)
            : IsLost(false)
            , JobId(jobId)
            , SourceTask(std::move(sourceTask))
            , OutputCookie(outputCookie)
            , DestinationPool(destinationPool)
            , InputCookie(inputCookie)
            , Address(address)
        { }

        bool IsLost;

        TJobId JobId;

        TTaskPtr SourceTask;
        IChunkPoolOutput::TCookie OutputCookie;

        IChunkPoolInput* DestinationPool;
        IChunkPoolInput::TCookie InputCookie;

        Stroka Address;

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

        virtual NNodeTrackerClient::NProto::TNodeResources GetTotalNeededResources() const;
        NNodeTrackerClient::NProto::TNodeResources GetTotalNeededResourcesDelta();

        virtual int GetChunkListCountPerJob() const = 0;

        virtual TDuration GetLocalityTimeout() const = 0;
        virtual i64 GetLocality(const Stroka& address) const;
        virtual bool HasInputLocality() const;

        const NNodeTrackerClient::NProto::TNodeResources& GetMinNeededResources() const;
        virtual NNodeTrackerClient::NProto::TNodeResources GetNeededResources(TJobletPtr joblet) const;

        void ResetCachedMinNeededResources();

        DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, DelayedTime);

        void AddInput(TChunkStripePtr stripe);
        void AddInput(const std::vector<TChunkStripePtr>& stripes);
        void FinishInput();

        void CheckCompleted();

        TJobPtr ScheduleJob(ISchedulingContext* context, const NNodeTrackerClient::NProto::TNodeResources& jobLimits);

        virtual void OnJobCompleted(TJobletPtr joblet);
        virtual void OnJobFailed(TJobletPtr joblet);
        virtual void OnJobAborted(TJobletPtr joblet);
        virtual void OnJobLost(TCompletedJobPtr completedJob);

        // First checks against a given node, then against all nodes if needed.
        void CheckResourceDemandSanity(
            TExecNodePtr node,
            const NNodeTrackerClient::NProto::TNodeResources& neededResources);

        // Checks against all available nodes.
        void CheckResourceDemandSanity(
            const NNodeTrackerClient::NProto::TNodeResources& neededResources);

        void DoCheckResourceDemandSanity(const NNodeTrackerClient::NProto::TNodeResources& neededResources);

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

        NNodeTrackerClient::NProto::TNodeResources CachedTotalNeededResources;
        mutable TNullable<NNodeTrackerClient::NProto::TNodeResources> CachedMinNeededResources;

        TInstant LastDemandSanityCheckTime;
        bool CompletedFired;

        //! For each lost job currently being replayed, maps output cookie to corresponding input cookie.
        yhash_map<IChunkPoolOutput::TCookie, IChunkPoolInput::TCookie> LostJobCookieMap;

    protected:
        NLog::TLogger Logger;

        virtual NNodeTrackerClient::NProto::TNodeResources GetMinNeededResourcesHeavy() const = 0;

        virtual void OnTaskCompleted();

        virtual EJobType GetJobType() const = 0;
        virtual void PrepareJoblet(TJobletPtr joblet);
        virtual void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) = 0;

        virtual void OnJobStarted(TJobletPtr joblet);

        virtual bool IsMemoryReserveEnabled() const = 0;

        void AddPendingHint();
        void AddLocalityHint(const Stroka& address);

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
            TChunkStripePtr stripe,
            TNullable<int> partitionTag);

        void AddFinalOutputSpecs(NJobTrackerClient::NProto::TJobSpec* jobSpec, TJobletPtr joblet);
        void AddIntermediateOutputSpec(
            NJobTrackerClient::NProto::TJobSpec* jobSpec,
            TJobletPtr joblet,
            TNullable<NTableClient::TKeyColumns> keyColumns);

        static void UpdateInputSpecTotals(
            NJobTrackerClient::NProto::TJobSpec* jobSpec,
            TJobletPtr joblet);

        void RegisterIntermediate(TJobletPtr joblet, TChunkStripePtr stripe, TTaskPtr destinationTask);
        void RegisterIntermediate(TJobletPtr joblet, TChunkStripePtr stripe, IChunkPoolInput* destinationPool);

        static TChunkStripePtr BuildIntermediateChunkStripe(
            google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs);

        void RegisterInput(TJobletPtr joblet);
        void RegisterOutput(TJobletPtr joblet, int key);

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
        NNodeTrackerClient::NProto::TNodeResources MinNeededResources;

        //! All non-local tasks.
        yhash_set<TTaskPtr> NonLocalTasks;

        //! Non-local tasks that may possibly be ready (but a delayed check is still needed)
        //! keyed by min memory demand (as reported by TTask::GetMinNeededResources).
        std::multimap<i64, TTaskPtr> CandidateTasks;

        //! Non-local tasks keyed by deadline.
        std::multimap<TInstant, TTaskPtr> DelayedTasks;

        //! Local tasks keyed by address.
        yhash_map<Stroka, yhash_set<TTaskPtr>> LocalTasks;


        void Persist(TPersistenceContext& context);

    };

    //! All task groups declared by calling #RegisterTaskGroup, in the order of decreasing priority.
    std::vector<TTaskGroupPtr> TaskGroups;

    void RegisterTask(TTaskPtr task);
    void RegisterTaskGroup(TTaskGroupPtr group);

    void UpdateTask(TTaskPtr task);

    void UpdateAllTasks();

    virtual void CustomizeJoblet(TJobletPtr joblet);
    virtual void CustomizeJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec);

    void DoAddTaskLocalityHint(TTaskPtr task, const Stroka& address);
    void AddTaskLocalityHint(TTaskPtr task, const Stroka& address);
    void AddTaskLocalityHint(TTaskPtr task, TChunkStripePtr stripe);
    void AddTaskPendingHint(TTaskPtr task);
    void ResetTaskLocalityDelays();

    void MoveTaskToCandidates(TTaskPtr task, std::multimap<i64, TTaskPtr>& candidateTasks);

    bool CheckJobLimits(TExecNodePtr node, TTaskPtr task, const NNodeTrackerClient::NProto::TNodeResources& jobLimits);

    TJobPtr DoScheduleJob(ISchedulingContext* context, const NNodeTrackerClient::NProto::TNodeResources& jobLimits);
    TJobPtr DoScheduleLocalJob(ISchedulingContext* context, const NNodeTrackerClient::NProto::TNodeResources& jobLimits);
    TJobPtr DoScheduleNonLocalJob(ISchedulingContext* context, const NNodeTrackerClient::NProto::TNodeResources& jobLimits);

    void OnJobStarted(TJobPtr job);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);


    // Jobs in progress management.
    void RegisterJoblet(TJobletPtr joblet);
    TJobletPtr GetJoblet(TJobPtr job);
    void RemoveJoblet(TJobPtr job);


    // Initialization.
    virtual void DoInitialize();
    virtual void InitializeTransactions();


    // Preparation.
    void DoPrepare();
    void GetInputObjectIds();
    void GetOutputObjectIds();
    void ValidateFileTypes();
    void FetchInputTables();
    void RequestInputObjects();
    void RequestOutputObjects();
    void FetchFileObjects();
    void RequestFileObjects();
    void CreateLivePreviewTables();
    void PrepareLivePreviewTablesForUpdate();
    void CollectTotals();
    virtual void CustomPrepare();
    void AddAllTaskPendingHints();
    void InitChunkListPool();
    void InitInputChunkScratcher();
    void SuspendUnavailableInputStripes();

    // Initialize transactions.
    void StartAsyncSchedulerTransaction();
    void StartSyncSchedulerTransaction();
    void StartIOTransactions();
    virtual void StartInputTransaction(NObjectClient::TTransactionId parentTransactionId);
    virtual void StartOutputTransaction(NObjectClient::TTransactionId parentTransactionId);

    // Completion.
    void DoCommit();
    void CommitResults();


    // Revival.
    void DoRevive();
    void ReinstallLivePreview();
    void AbortAllJoblets();


    void DoSaveSnapshot(TOutputStream* output);
    void DoLoadSnapshot();


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
        SmallVector<NChunkClient::TRefCountedChunkSpecPtr, 1> ChunkSpecs;
        EInputChunkState State;

        TInputChunkDescriptor()
            : State(EInputChunkState::Active)
        { }

        void Persist(TPersistenceContext& context);

    };

    //! Called when a job is unable to read an input chunk or
    //! chunk scratcher has encountered unavailable chunk.
    void OnInputChunkUnavailable(
        const NChunkClient::TChunkId& chunkId,
        TInputChunkDescriptor& descriptor);

    void OnInputChunkAvailable(
        const NChunkClient::TChunkId& chunkId,
        TInputChunkDescriptor& descriptor,
        const NChunkClient::TChunkReplicaList& replicas);

    virtual bool IsOutputLivePreviewSupported() const;
    virtual bool IsIntermediateLivePreviewSupported() const;

    void OnOperationCompleted();
    virtual void DoOperationCompleted();

    void OnOperationFailed(const TError& error);
    virtual void DoOperationFailed(const TError& error);

    virtual bool IsCompleted() const = 0;


    // Unsorted helpers.

    //! Enables sorted output from user jobs.
    virtual bool IsSortedOutputSupported() const;

    //! Enables fetching all input replicas (not only data)
    virtual bool IsParityReplicasFetchEnabled() const;

    //! If |true| then all jobs started within the operation must
    //! preserve row count. This invariant is checked for each completed job.
    //! Should a violation be discovered, the operation fails.
    virtual bool IsRowCountPreserved() const;

    std::vector<Stroka> CheckInputTablesSorted(
        const TNullable< std::vector<Stroka> >& keyColumns);
    static bool CheckKeyColumnsCompatible(
        const std::vector<Stroka>& fullColumns,
        const std::vector<Stroka>& prefixColumns);

    static EAbortReason GetAbortReason(TJobPtr job);
    static EAbortReason GetAbortReason(TJobletPtr joblet);

    void UpdateAllTasksIfNeeded(const TProgressCounter& jobCounter);
    bool IsMemoryReserveEnabled(const TProgressCounter& jobCounter) const;
    i64 GetMemoryReserve(bool memoryReserveEnabled, TUserJobSpecPtr userJobSpec) const;

    void RegisterInputStripe(TChunkStripePtr stripe, TTaskPtr task);


    void RegisterEndpoints(
        const NTableClient::NProto::TOldBoundaryKeysExt& boundaryKeys,
        int key,
        TOutputTable* outputTable);

    void RegisterInput(TJobletPtr joblet);

    void RegisterOutput(
        NChunkClient::TRefCountedChunkSpecPtr chunkSpec,
        int key,
        int tableIndex);

    void RegisterOutput(
        TJobletPtr joblet,
        int key);

    void RegisterOutput(
        const NChunkClient::TChunkTreeId& chunkTreeId,
        int key,
        int tableIndex,
        TOutputTable& table);

    void RegisterIntermediate(
        TJobletPtr joblet,
        TCompletedJobPtr completedJob,
        TChunkStripePtr stripe);

    bool HasEnoughChunkLists(int requestedCount);
    NChunkClient::TChunkListId ExtractChunkList();

    //! Returns the list of all input chunks collected from all input tables.
    std::vector<NChunkClient::TRefCountedChunkSpecPtr> CollectInputChunks() const;

    //! Converts a list of input chunks into a list of chunk stripes for further
    //! processing. Each stripe receives exactly one chunk (as suitable for most
    //! jobs except merge). The resulting stripes are of approximately equal
    //! size. The size per stripe is either |maxSliceDataSize| or
    //! |TotalEstimateInputDataSize / jobCount|, whichever is smaller. If the resulting
    //! list contains less than |jobCount| stripes then |jobCount| is decreased
    //! appropriately.
    std::vector<TChunkStripePtr> SliceInputChunks(i64 maxSliceDataSize, int jobCount);

    int SuggestJobCount(
        i64 totalDataSize,
        i64 dataSizePerJob,
        TNullable<int> configJobCount) const;

    void InitUserJobSpecTemplate(
        NScheduler::NProto::TUserJobSpec* proto,
        TUserJobSpecPtr config,
        const std::vector<TRegularUserFile>& regularFiles,
        const std::vector<TUserTableFile>& tableFiles);

    void InitUserJobSpec(
        NScheduler::NProto::TUserJobSpec* proto,
        TJobletPtr joblet,
        i64 memoryReserve);

    // Amount of memory reserved for output table writers in job proxy.
    i64 GetFinalOutputIOMemorySize(TJobIOConfigPtr ioConfig) const;

    i64 GetFinalIOMemorySize(
        TJobIOConfigPtr ioConfig,
        const TChunkStripeStatisticsVector& stripeStatistics) const;

    static void InitIntermediateInputConfig(TJobIOConfigPtr config);

    static void InitIntermediateOutputConfig(TJobIOConfigPtr config);
    void InitFinalOutputConfig(TJobIOConfigPtr config);

    TFluentLogEvent LogEventFluently(ELogEventType eventType);
    TFluentLogEvent LogFinishedJobFluently(ELogEventType eventType, TJobPtr job);

private:
    typedef TOperationControllerBase TThis;

    typedef yhash_map<NChunkClient::TChunkId, TInputChunkDescriptor> TInputChunkMap;

    //! Keeps information needed to maintain the liveness state of input chunks.
    TInputChunkMap InputChunkMap;

    class TInputChunkScratcher
        : public virtual TRefCounted
    {
    public:
        TInputChunkScratcher(TOperationControllerBase* controller, NRpc::IChannelPtr masterChannel);

        //! Starts periodic polling.
        /*!
         *  Should be called when operation preparation is complete.
         *  Safe to call multiple times.
         */
        void Start();

    private:
        void LocateChunks();
        void OnLocateChunksResponse(const NChunkClient::TChunkServiceProxy::TErrorOrRspLocateChunksPtr& rspOrError);

        TOperationControllerBase* Controller;
        NConcurrency::TPeriodicExecutorPtr PeriodicExecutor;
        NChunkClient::TChunkServiceProxy Proxy;
        TInputChunkMap::iterator NextChunkIterator;
        bool Started;

        NLog::TLogger& Logger;

    };

    typedef TIntrusivePtr<TInputChunkScratcher> TInputChunkScratcherPtr;

    TOperationSpecBasePtr Spec;
    TChunkListPoolPtr ChunkListPool;

    int CachedPendingJobCount;

    NNodeTrackerClient::NProto::TNodeResources CachedNeededResources;

    //! Maps an intermediate chunk id to its originating completed job.
    yhash_map<NChunkClient::TChunkId, TCompletedJobPtr> ChunkOriginMap;

    //! Maps scheduler's job ids to controller's joblets.
    //! NB: |TJobPtr -> TJobletPtr| mapping would be faster but
    //! it cannot be serialized that easily.
    yhash_map<TJobId, TJobletPtr> JobletMap;

    //! Used to distinguish already seen ChunkSpecs while building #InputChunkMap.
    yhash_set<NChunkClient::TRefCountedChunkSpecPtr> InputChunkSpecs;

    TInputChunkScratcherPtr InputChunkScratcher;

    //! Increments each time a new job is scheduled.
    TIdGenerator JobIndexGenerator;


    NApi::IClientPtr CreateClient();

    static const NProto::TUserJobResult* FindUserJobResult(TJobletPtr joblet);

    NTransactionClient::TTransactionManagerPtr GetTransactionManagerForTransaction(
        const NObjectClient::TTransactionId& transactionId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
