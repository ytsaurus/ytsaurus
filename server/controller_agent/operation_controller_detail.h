#pragma once

#include "private.h"

#include "auto_merge_director.h"
#include "chunk_list_pool.h"
#include "job_memory.h"
#include "job_splitter.h"
#include "operation_controller.h"
#include "serialize.h"
#include "helpers.h"
#include "master_connector.h"
#include "task_host.h"

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/event_log.h>

#include <yt/server/chunk_pools/chunk_pool.h>
#include <yt/server/chunk_pools/public.h>
#include <yt/server/chunk_pools/chunk_stripe_key.h>

#include <yt/server/chunk_server/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/node_tracker_client/helpers.h>
#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/table_ypath_proxy.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/value_consumer.h>

#include <yt/ytlib/query_client/public.h>
#include <yt/ytlib/query_client/range_inferrer.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/id_generator.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/digest.h>
#include <yt/core/misc/histogram.h>
#include <yt/core/misc/safe_assert.h>

#include <yt/core/ytree/ypath_client.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_ENUM(EControllerState,
    (Preparing)
    (Running)
    (Failing)
    (Finished)
);

DEFINE_ENUM(ETransactionType,
    (Async)
    (Input)
    (Output)
    (Completion)
    (DebugOutput)
);

class TOperationControllerBase
    : public IOperationController
    , public NScheduler::TEventLogHostBase
    , public ITaskHost
{
    // In order to make scheduler more stable, we do not allow
    // pure YCHECK to be executed from the controller code (directly
    // or indirectly). Thus, all interface methods of IOperationController
    // are divided into two groups: those that involve YCHECKs
    // to make assertions essential for further execution, and pure ones.

    // All potentially faulty controller interface methods are
    // guarded by enclosing into an extra method. Two intermediate
    // macro below are needed due to
    // http://stackoverflow.com/questions/1489932.
    // Welcome to the beautiful world of preprocessor!
#define VERIFY_PASTER(affinity) VERIFY_ ## affinity
#define VERIFY_EVALUATOR(affinity) VERIFY_PASTER(affinity)
#define IMPLEMENT_SAFE_METHOD(returnType, method, signature, args, affinity, catchStdException, defaultValue) \
public: \
    virtual returnType method signature final \
    { \
        VERIFY_EVALUATOR(affinity); \
        TSafeAssertionsGuard guard( \
            Host->GetCoreDumper(), \
            Host->GetCoreSemaphore(), \
            {"OperationId: " + ToString(OperationId)}); \
        try { \
            return Safe ## method args; \
        } catch (const TAssertionFailedException& ex) { \
            ProcessSafeException(ex); \
            return defaultValue; \
        } catch (const std::exception& ex) { \
            if (catchStdException) { \
                ProcessSafeException(ex); \
                return defaultValue; \
            } \
            throw; \
        } \
    } \
private: \
    returnType Safe ## method signature;

#define IMPLEMENT_SAFE_VOID_METHOD(method, signature, args, affinity, catchStdException) \
    IMPLEMENT_SAFE_METHOD(void, method, signature, args, affinity, catchStdException, )

    IMPLEMENT_SAFE_VOID_METHOD(Prepare, (), (), INVOKER_AFFINITY(CancelableInvoker), false)
    IMPLEMENT_SAFE_VOID_METHOD(Materialize, (), (), INVOKER_AFFINITY(CancelableInvoker), false)

    IMPLEMENT_SAFE_VOID_METHOD(OnJobStarted, (const TJobId& jobId, TInstant startTime), (jobId, startTime), INVOKER_AFFINITY(CancelableInvoker), true)
    IMPLEMENT_SAFE_VOID_METHOD(OnJobCompleted, (std::unique_ptr<NScheduler::TCompletedJobSummary> jobSummary), (std::move(jobSummary)), INVOKER_AFFINITY(CancelableInvoker), true)
    IMPLEMENT_SAFE_VOID_METHOD(OnJobFailed, (std::unique_ptr<NScheduler::TFailedJobSummary> jobSummary), (std::move(jobSummary)), INVOKER_AFFINITY(CancelableInvoker), true)
    IMPLEMENT_SAFE_VOID_METHOD(OnJobAborted, (std::unique_ptr<NScheduler::TAbortedJobSummary> jobSummary), (std::move(jobSummary)), INVOKER_AFFINITY(CancelableInvoker), true)
    IMPLEMENT_SAFE_VOID_METHOD(OnJobRunning, (std::unique_ptr<NScheduler::TRunningJobSummary> jobSummary), (std::move(jobSummary)), INVOKER_AFFINITY(CancelableInvoker), true)

    IMPLEMENT_SAFE_VOID_METHOD(Commit, (), (), INVOKER_AFFINITY(CancelableInvoker), false)
    IMPLEMENT_SAFE_VOID_METHOD(Abort, (), (), THREAD_AFFINITY(ControlThread), false)
    IMPLEMENT_SAFE_VOID_METHOD(Forget, (), (), THREAD_AFFINITY(ControlThread), false)
    IMPLEMENT_SAFE_VOID_METHOD(Complete, (), (), THREAD_AFFINITY(ControlThread), false)

    IMPLEMENT_SAFE_METHOD(
        NScheduler::TScheduleJobResultPtr,
        ScheduleJob,
        (NScheduler::ISchedulingContextPtr context, const TJobResources& jobLimits),
        (context, jobLimits),
        INVOKER_AFFINITY(CancelableInvoker),
        true,
        New<NScheduler::TScheduleJobResult>())

    //! Callback called by TChunkScraper when get information on some chunk.
    IMPLEMENT_SAFE_VOID_METHOD(
        OnInputChunkLocated,
        (const NChunkClient::TChunkId& chunkId, const NChunkClient::TChunkReplicaList& replicas),
        (chunkId, replicas),
        THREAD_AFFINITY_ANY(),
        false)

    //! Called by #IntermediateChunkScraper.
    IMPLEMENT_SAFE_VOID_METHOD(
        OnIntermediateChunkLocated,
        (const NChunkClient::TChunkId& chunkId, const NChunkClient::TChunkReplicaList& replicas),
        (chunkId, replicas),
        THREAD_AFFINITY_ANY(),
        false)

public:
    // These are "pure" interface methods, i. e. those that do not involve YCHECKs.
    // If some of these methods still fails due to unnoticed YCHECK, consider
    // moving it to the section above.

    // NB(max42): Don't make Revive safe! It may lead to either destroying all
    // operations on a cluster, or to a scheduler crash.
    virtual void Revive() override;

    virtual void Initialize() override;
    virtual TOperationControllerInitializeResult GetInitializeResult() const override;

    virtual void InitializeReviving(TControllerTransactionsPtr operationTransactions) override;

    virtual void OnTransactionAborted(const NTransactionClient::TTransactionId& transactionId) override;

    virtual std::vector<NApi::ITransactionPtr> GetTransactions() override;

    virtual void UpdateConfig(TSchedulerConfigPtr config) override;

    virtual TCancelableContextPtr GetCancelableContext() const override;
    virtual IInvokerPtr GetInvoker() const override;

    virtual int GetTotalJobCount() const override;
    virtual int GetPendingJobCount() const override;
    virtual TJobResources GetNeededResources() const override;

    virtual std::vector<TJobResources> GetMinNeededJobResources() const override;

    virtual bool IsForgotten() const override;
    virtual bool IsRevivedFromSnapshot() const override;

    virtual void SetProgressUpdated() override;
    virtual bool ShouldUpdateProgress() const override;

    virtual bool HasProgress() const override;
    virtual bool HasJobSplitterInfo() const override;

    virtual void Resume() override;
    virtual TFuture<void> Suspend() override;

    virtual void BuildSpec(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildOperationAttributes(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildProgress(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildBriefProgress(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildMemoryDigestStatistics(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildJobSplitterInfo(NYson::IYsonConsumer* consumer) const override;

    // NB(max42, babenko): this method should not be safe. Writing a core dump or trying to fail
    // operation from a forked process is a bad idea.
    virtual void SaveSnapshot(IOutputStream* output) override;

    virtual NYson::TYsonString GetProgress() const override;
    virtual NYson::TYsonString GetBriefProgress() const override;

    virtual NYson::TYsonString BuildJobYson(const TJobId& jobId, bool outputStatistics) const override;
    virtual NYson::TYsonString BuildJobsYson() const override;

    virtual TSharedRef ExtractJobSpec(const TJobId& jobId) const override;

    virtual NYson::TYsonString BuildSuspiciousJobsYson() const override;

    virtual void Persist(const TPersistenceContext& context) override;

    TOperationControllerBase(
        TSchedulerConfigPtr config,
        TOperationSpecBasePtr spec,
        TOperationOptionsPtr options,
        IOperationHost* host,
        TOperation* operation);

    // ITaskHost implementation.

    virtual IInvokerPtr GetCancelableInvoker() const override;

    virtual TNullable<NYPath::TRichYPath> GetStderrTablePath() const override;
    virtual TNullable<NYPath::TRichYPath> GetCoreTablePath() const override;

    virtual void RegisterInputStripe(const NChunkPools::TChunkStripePtr& stripe, const TTaskPtr& task) override;
    virtual void AddTaskLocalityHint(const NChunkPools::TChunkStripePtr& stripe, const TTaskPtr& task) override;
    virtual void AddTaskLocalityHint(NNodeTrackerClient::TNodeId nodeId, const TTaskPtr& task);
    virtual void AddTaskPendingHint(const TTaskPtr& task) override;

    virtual ui64 NextJobIndex() override;
    virtual std::unique_ptr<TJobMetricsUpdater> CreateJobMetricsUpdater() const override;

    virtual void CustomizeJobSpec(const TJobletPtr& joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) override;
    virtual void CustomizeJoblet(const TJobletPtr& joblet) override;

    virtual void AddValueToEstimatedHistogram(const TJobletPtr& joblet) override;
    virtual void RemoveValueFromEstimatedHistogram(const TJobletPtr& joblet) override;

    virtual const TSchedulerConfigPtr& SchedulerConfig() const override;
    virtual const TOperationSpecBasePtr& Spec() const override;

    virtual void OnOperationFailed(const TError& error, bool flush = true) override;

    virtual bool IsRowCountPreserved() const override;
    virtual bool IsJobInterruptible() const override;
    virtual bool ShouldSkipSanityCheck() override;

    virtual NScheduler::TExtendedJobResources GetAutoMergeResources(
        const NChunkPools::TChunkStripeStatisticsVector& statistics) const override;
    virtual const NJobTrackerClient::NProto::TJobSpec& GetAutoMergeJobSpecTemplate(int tableIndex) const override;
    virtual TTaskGroupPtr GetAutoMergeTaskGroup() const override;
    virtual TAutoMergeDirector* GetAutoMergeDirector() override;

    virtual const IDigest* GetJobProxyMemoryDigest(EJobType jobType) const override;
    virtual const IDigest* GetUserJobMemoryDigest(EJobType jobType) const override;

    virtual NObjectClient::TCellTag GetIntermediateOutputCellTag() const override;

    virtual const TChunkListPoolPtr& ChunkListPool() const override;
    virtual NChunkClient::TChunkListId ExtractChunkList(NObjectClient::TCellTag cellTag) override;
    virtual void ReleaseChunkLists(const std::vector<NChunkClient::TChunkListId>& chunkListIds) override;

    virtual TOperationId GetOperationId() const override;
    virtual EOperationType GetOperationType() const override;

    const std::vector<TOutputTable>& OutputTables() const;
    virtual const TNullable<TOutputTable>& StderrTable() const override;
    virtual const TNullable<TOutputTable>& CoreTable() const override;

    virtual void RegisterStderr(const TJobletPtr& joblet, const NScheduler::TJobSummary& summary) override;
    virtual void RegisterCores(const TJobletPtr& joblet, const NScheduler::TJobSummary& summary) override;

    virtual void RegisterJoblet(const TJobletPtr& joblet) override;

    virtual IJobSplitter* JobSplitter() override;

    virtual const TNullable<TJobResources>& CachedMaxAvailableExecNodeResources() const override;

    virtual const NNodeTrackerClient::TNodeDirectoryPtr& InputNodeDirectory() const override;

    virtual void RegisterRecoveryInfo(
        const TCompletedJobPtr& completedJob,
        const NChunkPools::TChunkStripePtr& stripe);

    virtual NTableClient::TRowBufferPtr GetRowBuffer() override;

    virtual int GetRecentlyCompletedJobCount() const override;

    virtual TFuture<void> ReleaseJobs(int jobCount) override;

    virtual std::vector<NScheduler::TJobPtr> BuildJobsFromJoblets() const override;

protected:
    TSchedulerConfigPtr Config;
    IOperationHost* Host;
    TMasterConnectorPtr MasterConnector;

    const TOperationId OperationId;

    const EOperationType OperationType;
    const TInstant StartTime;
    const TString AuthenticatedUser;

    // Usually these clients are all the same (and connected to master of the current cluster).
    // But `remote copy' operation connects AuthenticatedInputMasterClient to remote cluster master server.
    // AuthenticatedOutputMasterClient is created for the sake of symmetry with Input,
    // i.e. AuthenticatedMasterClient and AuthenticatedOutputMasterClient are always connected to the same master.
    NApi::INativeClientPtr AuthenticatedMasterClient;
    NApi::INativeClientPtr AuthenticatedInputMasterClient;
    NApi::INativeClientPtr AuthenticatedOutputMasterClient;

    mutable NLogging::TLogger Logger;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr Invoker;
    ISuspendableInvokerPtr SuspendableInvoker;
    IInvokerPtr CancelableInvoker;

    std::atomic<EControllerState> State = {EControllerState::Preparing};
    std::atomic<bool> Forgotten = {false};
    std::atomic<bool> RevivedFromSnapshot = {false};

    // These totals are approximate.
    int TotalEstimatedInputChunkCount = 0;
    i64 TotalEstimatedInputDataWeight = 0;
    i64 TotalEstimatedInputRowCount = 0;
    i64 TotalEstimatedInputCompressedDataSize = 0;
    i64 TotalEstimatedInputUncompressedDataSize = 0;

    // Only used during materialization, not persisted.
    double InputCompressionRatio = 0.0;

    // Ratio DataWeight/UncomprssedDataSize for input data.
    // Only used during materialization, not persisted.
    double DataWeightRatio = 0.0;

    // Total uncompressed data size for input tables.
    // Used only during preparation, not persisted.
    i64 PrimaryInputDataWeight = 0;
    i64 ForeignInputDataWeight = 0;

    int ChunkLocatedCallCount = 0;
    int UnavailableInputChunkCount = 0;

    // Job counters.
    TProgressCounterPtr JobCounter = New<TProgressCounter>();

    // Maps node ids to descriptors for job input chunks.
    NNodeTrackerClient::TNodeDirectoryPtr InputNodeDirectory_;

    NApi::ITransactionPtr AsyncSchedulerTransaction;
    NApi::ITransactionPtr InputTransaction;
    NApi::ITransactionPtr OutputTransaction;
    NApi::ITransactionPtr CompletionTransaction;
    NApi::ITransactionPtr DebugOutputTransaction;
    NApi::ITransactionPtr UserTransaction;

    NTransactionClient::TTransactionId UserTransactionId;

    std::atomic<bool> AreTransactionsActive = {false};

    bool CommitFinished = false;

    TOperationSnapshot Snapshot;
    struct TRowBufferTag { };
    NTableClient::TRowBufferPtr RowBuffer;

    const NYTree::IMapNodePtr SecureVault;

    const std::vector<TString> Owners;

    std::vector<TInputTable> InputTables;
    std::vector<TOutputTable> OutputTables_;
    TNullable<TOutputTable> StderrTable_;
    TNullable<TOutputTable> CoreTable_;

    // All output tables plus stderr and core tables (if present).
    std::vector<TOutputTable*> UpdatingTables;

    TIntermediateTable IntermediateTable;

    struct TUserFile
        : public TLockedUserObject
    {
        std::shared_ptr<NYTree::IAttributeDictionary> Attributes;
        EOperationStage Stage = EOperationStage::None;
        TString FileName;
        std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
        i64 ChunkCount = -1;
        bool Executable = false;
        NYson::TYsonString Format;
        NTableClient::TTableSchema Schema;
        bool IsDynamic = false;

        void Persist(const TPersistenceContext& context);
    };

    std::vector<TUserFile> Files;

    struct TInputQuery
    {
        NQueryClient::TQueryPtr Query;
        NQueryClient::TExternalCGInfoPtr ExternalCGInfo;
        NQueryClient::TRangeInferrer RangeInferrer;
    };

    TNullable<TInputQuery> InputQuery;

    //! All tasks declared by calling #RegisterTask, mostly for debugging purposes.
    std::vector<TTaskPtr> Tasks;

    //! All task groups declared by calling #RegisterTaskGroup, in the order of decreasing priority.
    std::vector<TTaskGroupPtr> TaskGroups;

    //! Auto merge task for each of the output tables.
    std::vector<TAutoMergeTaskPtr> AutoMergeTasks;
    TTaskGroupPtr AutoMergeTaskGroup;

    TDataFlowGraph DataFlowGraph_;

    TFuture<NApi::ITransactionPtr> StartTransaction(
        ETransactionType type,
        NApi::INativeClientPtr client,
        const NTransactionClient::TTransactionId& parentTransactionId = NTransactionClient::NullTransactionId);

    void RegisterTask(TTaskPtr task);
    void RegisterTaskGroup(TTaskGroupPtr group);

    void UpdateTask(TTaskPtr task);
    void UpdateDynamicNeededResources(TTaskPtr task);

    void UpdateAllTasks();

    void DoAddTaskLocalityHint(TTaskPtr task, NNodeTrackerClient::TNodeId nodeId);
    void ResetTaskLocalityDelays();

    void MoveTaskToCandidates(TTaskPtr task, std::multimap<i64, TTaskPtr>& candidateTasks);

    bool CheckJobLimits(
        TTaskPtr task,
        const TJobResources& jobLimits,
        const TJobResources& nodeResourceLimits);

    void CheckTimeLimit();

    void CheckAvailableExecNodes();

    virtual TFuture<void> AnalyzePartitionHistogram() const;
    TFuture<void> AnalyzeTmpfsUsage() const;
    TFuture<void> AnalyzeIntermediateJobsStatistics() const;
    TFuture<void> AnalyzeInputStatistics() const;
    TFuture<void> AnalyzeAbortedJobs() const;
    TFuture<void> AnalyzeJobsIOUsage() const;
    TFuture<void> AnalyzeJobsDuration() const;
    TFuture<void> AnalyzeScheduleJobStatistics() const;

    void AnalyzeOperationProgess() const;
    TFuture<void> DoAnalyzeOperationProgress() const;

    void FlushOperationNode(bool checkFlushResult);

    void CheckMinNeededResourcesSanity();
    void UpdateCachedMaxAvailableExecNodeResources();

    void DoScheduleJob(
        NScheduler::ISchedulingContext* context,
        const TJobResources& jobLimits,
        NScheduler::TScheduleJobResult* scheduleJobResult);

    void DoScheduleLocalJob(
        NScheduler::ISchedulingContext* context,
        const TJobResources& jobLimits,
        NScheduler::TScheduleJobResult* scheduleJobResult);

    void DoScheduleNonLocalJob(
        NScheduler::ISchedulingContext* context,
        const TJobResources& jobLimits,
        NScheduler::TScheduleJobResult* scheduleJobResult);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    TJobletPtr FindJoblet(const TJobId& jobId) const;
    TJobletPtr GetJoblet(const TJobId& jobId) const;
    TJobletPtr GetJobletOrThrow(const TJobId& jobId) const;
    void RemoveJoblet(const TJobletPtr& joblet);

    // Initialization.
    virtual void DoInitialize();
    virtual void InitializeConnections();
    virtual void InitializeTransactions();
    virtual void InitializeStructures();
    virtual void SyncPrepare();
    void InitUpdatingTables();


    // Preparation.
    void FetchInputTables();
    void RegisterInputChunk(const NChunkClient::TInputChunkPtr& inputChunk);
    void LockInputTables();
    void GetInputTablesAttributes();
    void GetOutputTablesSchema();
    virtual void PrepareInputTables();
    virtual void PrepareOutputTables();
    void LockOutputTablesAndGetAttributes();
    void FetchUserFiles();
    void LockUserFiles();
    void GetUserFilesAttributes();
    void CreateLivePreviewTables();
    void CollectTotals();
    virtual void CustomPrepare();
    void AddAllTaskPendingHints();
    void InitInputChunkScraper();
    void InitIntermediateChunkScraper();
    void InitAutoMerge(int outputChunkCountEstimate, double dataWeightRatio);

    void ParseInputQuery(
        const TString& queryString,
        const TNullable<NQueryClient::TTableSchema>& schema);
    void WriteInputQueryToJobSpec(
        NScheduler::NProto::TSchedulerJobSpecExt* schedulerJobSpecExt);
    virtual void PrepareInputQuery();

    void PickIntermediateDataCell();
    void InitChunkListPool();

    // Initialize transactions
    TFuture<void> StartAsyncSchedulerTransaction();
    TFuture<void> StartInputTransaction(const NObjectClient::TTransactionId& parentTransactionId);
    TFuture<void> StartOutputTransaction(const NObjectClient::TTransactionId& parentTransactionId);
    TFuture<void> StartDebugOutputTransaction();

    // Completion.
    void TeleportOutputChunks();
    void BeginUploadOutputTables(const std::vector<TOutputTable*>& updatingTables);
    void AttachOutputChunks(const std::vector<TOutputTable*>& tableList);
    void EndUploadOutputTables(const std::vector<TOutputTable*>& tableList);
    void CommitTransactions();
    virtual void CustomCommit();

    void StartCompletionTransaction();
    void CommitCompletionTransaction();

    void SetPartSize(const TNullable<TOutputTable>& table, size_t partSize);

    // Revival.
    void ReinstallLivePreview();

    void DoLoadSnapshot(const TOperationSnapshot& snapshot);

    bool InputHasDynamicTables() const;
    bool InputHasVersionedTables() const;
    bool InputHasReadLimits() const;

    //! Called to extract input table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetInputTablePaths() const = 0;

    //! Called to extract output table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetOutputTablePaths() const = 0;

    //! Called in jobs duration analyzer to get proper data weight parameter name in spec.
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const = 0;

    //! Called in jobs duration analyzer to get interesting for analysis jobs set.
    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const = 0;

    //! Called to extract stderr table writer config from the spec.
    virtual NTableClient::TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const;

    //! Called to extract core table writer config from the spec.
    virtual NTableClient::TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const;

    typedef std::pair<NYPath::TRichYPath, EOperationStage> TPathWithStage;

    //! Called to extract file paths from the spec.
    virtual std::vector<TPathWithStage> GetFilePaths() const;

    //! Called when a job is unable to read a chunk.
    void OnChunkFailed(const NChunkClient::TChunkId& chunkId);

    //! Gets the list of all intermediate chunks that are not lost.
    THashSet<NChunkClient::TChunkId> GetAliveIntermediateChunks() const;

    //! Called when a job is unable to read an intermediate chunk
    //! (i.e. that is not a part of the input).
    //! Returns false if the chunk was already considered lost.
    bool OnIntermediateChunkUnavailable(const NChunkClient::TChunkId& chunkId);

    int EstimateSplitJobCount(const NScheduler::TCompletedJobSummary& jobSummary, const TJobletPtr& joblet);
    void ExtractInterruptDescriptor(NScheduler::TCompletedJobSummary& jobSummary) const;
    virtual void ReinstallUnreadInputDataSlices(const std::vector<NChunkClient::TInputDataSlicePtr>& inputDataSlices);

    struct TStripeDescriptor
    {
        NChunkPools::TChunkStripePtr Stripe;
        NChunkPools::IChunkPoolInput::TCookie Cookie;
        TTaskPtr Task;

        TStripeDescriptor()
            : Cookie(NChunkPools::IChunkPoolInput::NullCookie)
        { }

        void Persist(const TPersistenceContext& context);

    };

    struct TInputChunkDescriptor
        : public TRefTracked<TInputChunkDescriptor>
    {
        SmallVector<TStripeDescriptor, 1> InputStripes;
        SmallVector<NChunkClient::TInputChunkPtr, 1> InputChunks;
        EInputChunkState State;

        TInputChunkDescriptor()
            : State(EInputChunkState::Active)
        { }

        void Persist(const TPersistenceContext& context);

    };

    //! Called when a job is unable to read an input chunk or
    //! chunk scraper has encountered unavailable chunk.
    void OnInputChunkUnavailable(
        const NChunkClient::TChunkId& chunkId,
        TInputChunkDescriptor* descriptor);

    void OnInputChunkAvailable(
        const NChunkClient::TChunkId& chunkId,
        const NChunkClient::TChunkReplicaList& replicas,
        TInputChunkDescriptor* descriptor);

    virtual bool IsOutputLivePreviewSupported() const;
    virtual bool IsIntermediateLivePreviewSupported() const;
    virtual bool IsInputDataSizeHistogramSupported() const;
    virtual bool AreForeignTablesSupported() const;

    //! Successfully terminates and finalizes the operation.
    /*!
     *  #interrupted flag indicates premature completion and disables standard validations.
     */
    virtual void OnOperationCompleted(bool interrupted);

    virtual void OnOperationTimeLimitExceeded();

    virtual bool IsCompleted() const;

    //! Returns |true| when the controller is prepared.
    /*!
     *  Preparation happens in a controller thread.
     *  The state must not be touched from the control thread
     *  while this function returns |false|.
     */
    bool IsPrepared() const;

    //! Returns |true| as long as the operation can schedule new jobs.
    bool IsRunning() const;

    //! Returns |true| as long as the operation is waiting for jobs abort events.
    bool IsFailing() const;

    //! Returns |true| when operation completion event is scheduled to control invoker.
    bool IsFinished() const;

    // Unsorted helpers.

    //! Enables verification that the output is sorted.
    virtual bool ShouldVerifySortedOutput() const;

    virtual NChunkPools::TOutputOrderPtr GetOutputOrder() const;

    //! Enables fetching all input replicas (not only data)
    virtual bool CheckParityReplicas() const;

    //! Enables fetching boundary keys for chunk specs.
    virtual bool IsBoundaryKeysFetchEnabled() const;

    //! Number of currently unavailable input chunks. In case of Sort or Sorted controller, shows
    //! number of unavailable chunks during materialization (fetching samples or chunk slices).
    //! Used for diagnostics only (exported into orchid).
    virtual i64 GetUnavailableInputChunkCount() const;

    i64 GetDataSliceCount() const;

    typedef std::function<bool(const TInputTable& table)> TInputTableFilter;

    NTableClient::TKeyColumns CheckInputTablesSorted(
        const NTableClient::TKeyColumns& keyColumns,
        TInputTableFilter inputTableFilter = [](const TInputTable&) { return true; });

    static bool CheckKeyColumnsCompatible(
        const NTableClient::TKeyColumns& fullColumns,
        const NTableClient::TKeyColumns& prefixColumns);

    const NObjectClient::TTransactionId& GetTransactionIdForOutputTable(const TOutputTable& table);

    virtual void AttachToIntermediateLivePreview(NChunkClient::TChunkId chunkId) override;

    void AttachToLivePreview(NChunkClient::TChunkTreeId chunkTreeId, NCypressClient::TNodeId& tableId);

    virtual void RegisterTeleportChunk(
        NChunkClient::TInputChunkPtr chunkSpec,
        NChunkPools::TChunkStripeKey key,
        int tableIndex) override;

    bool HasEnoughChunkLists(bool isWritingStderrTable, bool isWritingCoreTable);

    //! Called after preparation to decrease memory footprint.
    void ClearInputChunkBoundaryKeys();

    //! Returns the list of all input chunks collected from all primary input tables.
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryChunks(bool versioned) const;
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryUnversionedChunks() const;
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryVersionedChunks() const;
    std::pair<i64, i64> CalculatePrimaryVersionedChunksStatistics() const;
    std::vector<NChunkClient::TInputDataSlicePtr> CollectPrimaryVersionedDataSlices(i64 sliceSize);

    //! Returns the list of all input data slices collected from all primary input tables.
    std::vector<NChunkClient::TInputDataSlicePtr> CollectPrimaryInputDataSlices(i64 versionedSliceSize);

    //! Returns the list of lists of all input chunks collected from all foreign input tables.
    std::vector<std::deque<NChunkClient::TInputDataSlicePtr>> CollectForeignInputDataSlices(int foreignKeyColumnCount) const;

    //! Converts a list of input chunks into a list of chunk stripes for further
    //! processing. Each stripe receives exactly one chunk (as suitable for most
    //! jobs except merge). The resulting stripes are of approximately equal
    //! size. The size per stripe is either |maxSliceDataSize| or
    //! |TotalEstimateInputDataSize / jobCount|, whichever is smaller. If the resulting
    //! list contains less than |jobCount| stripes then |jobCount| is decreased
    //! appropriately.
    void SliceUnversionedChunks(
        const std::vector<NChunkClient::TInputChunkPtr>& unversionedChunks,
        const IJobSizeConstraintsPtr& jobSizeConstraints,
        std::vector<NChunkPools::TChunkStripePtr>* result) const;
    void SlicePrimaryUnversionedChunks(
        const IJobSizeConstraintsPtr& jobSizeConstraints,
        std::vector<NChunkPools::TChunkStripePtr>* result) const;
    void SlicePrimaryVersionedChunks(
        const IJobSizeConstraintsPtr& jobSizeConstraints,
        std::vector<NChunkPools::TChunkStripePtr>* result);

    void InitUserJobSpecTemplate(
        NScheduler::NProto::TUserJobSpec* proto,
        NScheduler::TUserJobSpecPtr config,
        const std::vector<TUserFile>& files,
        const TString& fileAccount);

    void InitUserJobSpec(
        NScheduler::NProto::TUserJobSpec* proto,
        TJobletPtr joblet);

    void AddStderrOutputSpecs(
        NScheduler::NProto::TUserJobSpec* jobSpec,
        TJobletPtr joblet);

    void AddCoreOutputSpecs(
        NScheduler::NProto::TUserJobSpec* jobSpec,
        TJobletPtr joblet);

    NChunkClient::TDataSourceDirectoryPtr MakeInputDataSources() const;
    NChunkClient::TDataSourceDirectoryPtr CreateIntermediateDataSource() const;

    // Amount of memory reserved for output table writers in job proxy.
    i64 GetFinalOutputIOMemorySize(NScheduler::TJobIOConfigPtr ioConfig) const;

    i64 GetFinalIOMemorySize(
        NScheduler::TJobIOConfigPtr ioConfig,
        const NChunkPools::TChunkStripeStatisticsVector& stripeStatistics) const;

    void InitIntermediateOutputConfig(NScheduler::TJobIOConfigPtr config);
    void InitFinalOutputConfig(NScheduler::TJobIOConfigPtr config);

    static NTableClient::TTableReaderOptionsPtr CreateTableReaderOptions(NScheduler::TJobIOConfigPtr ioConfig);

    void ValidateUserFileCount(NScheduler::TUserJobSpecPtr spec, const TString& operation);

    const std::vector<NScheduler::TExecNodeDescriptor>& GetExecNodeDescriptors();

    virtual void RegisterUserJobMemoryDigest(EJobType jobType, double memoryReserveFactor, double minMemoryReserveFactor);
    IDigest* GetUserJobMemoryDigest(EJobType jobType);

    virtual void RegisterJobProxyMemoryDigest(EJobType jobType, const TLogDigestConfigPtr& config);
    IDigest* GetJobProxyMemoryDigest(EJobType jobType);

    i64 ComputeUserJobMemoryReserve(EJobType jobType, NScheduler::TUserJobSpecPtr jobSpec) const;

    void InferSchemaFromInput(const NTableClient::TKeyColumns& keyColumns = NTableClient::TKeyColumns());
    void InferSchemaFromInputOrdered();
    void FilterOutputSchemaByInputColumnSelectors();
    void ValidateOutputSchemaOrdered() const;
    void ValidateOutputSchemaCompatibility(bool ignoreSortOrder, bool validateComputedColumns = false) const;

    virtual void BuildBriefSpec(NYson::IYsonConsumer* consumer) const;

    virtual NScheduler::TJobSplitterConfigPtr GetJobSplitterConfig() const;

    void CheckFailedJobsStatusReceived();

    virtual const std::vector<TEdgeDescriptor>& GetStandardEdgeDescriptors() override;

    NTableClient::TTableWriterOptionsPtr GetIntermediateTableWriterOptions() const;
    TEdgeDescriptor GetIntermediateEdgeDescriptorTemplate() const;

    virtual void UnstageChunkTreesNonRecursively(std::vector<NChunkClient::TChunkTreeId> chunkTreeIds) override;

    virtual TDataFlowGraph& DataFlowGraph() override;

private:
    typedef TOperationControllerBase TThis;

    typedef THashMap<NChunkClient::TChunkId, TInputChunkDescriptor> TInputChunkMap;

    //! Keeps information needed to maintain the liveness state of input chunks.
    TInputChunkMap InputChunkMap;

    TOperationSpecBasePtr Spec_;
    TOperationOptionsPtr Options;

    NObjectClient::TCellTag IntermediateOutputCellTag = NObjectClient::InvalidCellTag;
    TChunkListPoolPtr ChunkListPool_;
    THashMap<NObjectClient::TCellTag, int> CellTagToRequiredChunkLists;

    std::atomic<int> CachedPendingJobCount = {0};

    TJobResources CachedNeededResources;
    NConcurrency::TReaderWriterSpinLock CachedNeededResourcesLock;

    //! Maps an intermediate chunk id to its originating completed job.
    THashMap<NChunkClient::TChunkId, TCompletedJobPtr> ChunkOriginMap;

    TIntermediateChunkScraperPtr IntermediateChunkScraper;

    //! Maps scheduler's job ids to controller's joblets.
    THashMap<TJobId, TJobletPtr> JobletMap;

    //! List of job ids that were completed after the latest snapshot was built.
    //! This list is transient.
    std::deque<TJobId> RecentlyCompletedJobIds;

    NChunkClient::TChunkScraperPtr InputChunkScraper;

    //! Scrapes chunks of dynamic tables during data slice fetching.
    NChunkClient::IFetcherChunkScraperPtr DataSliceFetcherChunkScraper;

    NProfiling::TCpuInstant TaskUpdateDeadline_ = 0;

    //! Increments each time a new job is scheduled.
    TIdGenerator JobIndexGenerator;

    //! Aggregates job statistics.
    NJobTrackerClient::TStatistics JobStatistics;

    //! Aggregated schedule job statistics.
    TScheduleJobStatisticsPtr ScheduleJobStatistics_;

    //! Deadline after which schedule job statistics can be logged.
    NProfiling::TCpuInstant ScheduleJobStatisticsLogDeadline_ = 0;

    //! One output table can have row count limit on operation.
    TNullable<int> RowCountLimitTableIndex;
    i64 RowCountLimit = std::numeric_limits<i64>::max();

    //! Runs periodic time limit checks that fail operation on timeout.
    NConcurrency::TPeriodicExecutorPtr CheckTimeLimitExecutor;

    //! Runs periodic checks to verify that compatible nodes are present in the cluster.
    NConcurrency::TPeriodicExecutorPtr ExecNodesCheckExecutor;

    //! Periodically checks operation progress and registers operation alerts if necessary.
    NConcurrency::TPeriodicExecutorPtr AnalyzeOperationProgressExecutor;

    //! Periodically checks min needed resources of tasks for sanity.
    NConcurrency::TPeriodicExecutorPtr MinNeededResourcesSanityCheckExecutor;

    //! Periodically updates cached max available exec node resources.
    NConcurrency::TPeriodicExecutorPtr MaxAvailableExecNodeResourcesUpdateExecutor;

    //! Exec node count do not consider scheduling tag.
    //! But descriptors do.
    int ExecNodeCount_ = 0;
    TExecNodeDescriptorListPtr ExecNodesDescriptors_ = New<NScheduler::TExecNodeDescriptorList>();

    NProfiling::TCpuInstant GetExecNodesInformationDeadline_ = 0;
    NProfiling::TCpuInstant AvaialableNodesLastSeenTime_ = 0;

    TNullable<TJobResources> CachedMaxAvailableExecNodeResources_;

    const std::unique_ptr<NTableClient::IValueConsumer> EventLogValueConsumer_;
    const std::unique_ptr<NYson::IYsonConsumer> EventLogTableConsumer_;

    typedef THashMap<EJobType, std::unique_ptr<IDigest>> TMemoryDigestMap;
    TMemoryDigestMap JobProxyMemoryDigests_;
    TMemoryDigestMap UserJobMemoryDigests_;

    const TString CodicilData_;

    std::unique_ptr<IHistogram> EstimatedInputDataSizeHistogram_;
    std::unique_ptr<IHistogram> InputDataSizeHistogram_;

    const NProfiling::TCpuDuration LogProgressBackoff;
    NProfiling::TCpuInstant NextLogProgressDeadline = 0;

    std::atomic<bool> ShouldUpdateProgressInCypress_ = {true};
    NYson::TYsonString ProgressString_;
    NYson::TYsonString BriefProgressString_;

    std::vector<TEdgeDescriptor> StandardEdgeDescriptors_;

    TSpinLock ProgressLock_;
    const NConcurrency::TPeriodicExecutorPtr ProgressBuildExecutor_;

    i64 CurrentInputDataSliceTag_ = 0;

    int StderrCount_ = 0;
    int JobNodeCount_ = 0;

    THashMap<TJobId, TFinishedJobInfoPtr> FinishedJobs_;

    class TSink;
    std::vector<std::unique_ptr<TSink>> Sinks_;

    std::vector<NJobTrackerClient::NProto::TJobSpec> AutoMergeJobSpecTemplates_;

    std::unique_ptr<TAutoMergeDirector> AutoMergeDirector_;

    void BuildAndSaveProgress();

    void UpdateMemoryDigests(TJobletPtr joblet, const NJobTrackerClient::TStatistics& statistics, bool resourceOverdraft = false);

    void InitializeHistograms();
    void AddValueToEstimatedHistogram(TJobletPtr joblet);
    void RemoveValueFromEstimatedHistogram(TJobletPtr joblet);
    void UpdateActualHistogram(const NJobTrackerClient::TStatistics& statistics);

    void GetExecNodesInformation();
    int GetExecNodeCount();

    void UpdateJobStatistics(const TJobletPtr& joblet, const NScheduler::TJobSummary& jobSummary);

    void LogProgress(bool force = false);

    std::unique_ptr<IJobSplitter> JobSplitter_;

    NApi::INativeClientPtr CreateClient();
    void UpdateAllTasksIfNeeded();

    void IncreaseNeededResources(const TJobResources& resourcesDelta);

    void InitializeStandardEdgeDescriptors();

    TNullable<TDuration> GetTimeLimit() const;
    TError GetTimeLimitError() const;

    //! Sets finish time and other timing statistics.
    void FinalizeJoblet(
        const TJobletPtr& joblet,
        NScheduler::TJobSummary* jobSummary);

    NScheduler::TFluentLogEvent LogFinishedJobFluently(
        NScheduler::ELogEventType eventType,
        const TJobletPtr& joblet,
        const NScheduler::TJobSummary& jobSummary);

    virtual NYson::IYsonConsumer* GetEventLogConsumer() override;

    TCodicilGuard MakeCodicilGuard() const;

    void SleepInStage(NScheduler::EDelayInsideOperationCommitStage desiredStage);

    //! An internal helper for invoking OnOperationFailed with an error
    //! built by data from `ex`.
    void ProcessSafeException(const TAssertionFailedException& ex);
    void ProcessSafeException(const std::exception& ex);

    static EJobState GetStatisticsJobState(const TJobletPtr& joblet, const EJobState& state);

    NYson::TYsonString BuildInputPathYson(const TJobletPtr& joblet) const;

    void ProcessFinishedJobResult(std::unique_ptr<NScheduler::TJobSummary> summary, bool suggestCreateJobNodeByStatus);

    void InitAutoMergeJobSpecTemplates();

    void BuildJobAttributes(
        const TJobInfoPtr& job,
        EJobState state,
        bool outputStatistics,
        NYson::IYsonConsumer* consumer) const;

    void BuildFinishedJobAttributes(
        const TFinishedJobInfoPtr& job,
        bool outputStatistics,
        NYson::IYsonConsumer* consumer) const;

    void AnalyzeBriefStatistics(
        const TJobletPtr& job,
        TDuration suspiciousInactivityTimeout,
        i64 suspiciousCpuUsageThreshold,
        double suspiciousInputPipeIdleTimeFraction,
        const TErrorOr<TBriefJobStatisticsPtr>& briefStatisticsOrError);

    NScheduler::TJobPtr BuildJobFromJoblet(const TJobletPtr& joblet) const;

    void AbortAllJoblets();

    //! Helper class that implements IChunkPoolInput interface for output tables.
    class TSink
        : public NChunkPools::IChunkPoolInput
        , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    {
    public:
        //! Used only for persistense.
        TSink() = default;

        TSink(TThis* controller, int outputTableIndex);

        virtual TCookie AddWithKey(NChunkPools::TChunkStripePtr stripe, NChunkPools::TChunkStripeKey key) override;

        virtual TCookie Add(NChunkPools::TChunkStripePtr stripe) override;

        virtual void Suspend(TCookie cookie) override;
        virtual void Resume(TCookie cookie, NChunkPools::TChunkStripePtr stripe) override;
        virtual void Finish() override;

        void Persist(const TPersistenceContext& context);

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TSink, 0x7fb74a90);

        TThis* Controller_;
        int OutputTableIndex_ = -1;
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
