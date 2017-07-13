#pragma once

#include "private.h"
#include "chunk_list_pool.h"
#include "chunk_pool.h"
#include "config.h"
#include "event_log.h"
#include "job_memory.h"
#include "job_resources.h"
#include "job_splitter.h"
#include "operation_controller.h"
#include "serialize.h"
#include "helpers.h"
#include "master_connector.h"

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
namespace NScheduler {

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

DEFINE_ENUM(EJobReinstallReason,
    (Failed)
    (Aborted)
);

DEFINE_ENUM(EControllerState,
    (Preparing)
    (Running)
    (Finished)
);

DEFINE_ENUM(EOutputTableType,
    (Output)
    (Stderr)
    (Core)
);

DEFINE_ENUM(ETransactionType,
    (Sync)
    (Async)
    (Input)
    (Output)
    (DebugOutput)
);

class TOperationControllerBase
    : public IOperationController
    , public TEventLogHostBase
    , public IPersistent
    , public NPhoenix::TFactoryTag<NPhoenix::TNullFactory>
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
        TSafeAssertionsGuard guard(Host->GetCoreDumper(), Host->GetCoreSemaphore()); \
        try { \
            return Safe ## method args; \
        } catch (const TAssertionFailedException& ex) { \
            FailOperation(ex); \
            return defaultValue; \
        } catch (const std::exception& ex) { \
            if (catchStdException) { \
                FailOperation(ex); \
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
    IMPLEMENT_SAFE_VOID_METHOD(Revive, (), (), INVOKER_AFFINITY(CancelableInvoker), false)

    IMPLEMENT_SAFE_VOID_METHOD(OnJobStarted, (const TJobId& jobId, TInstant startTime), (jobId, startTime), INVOKER_AFFINITY(CancelableInvoker), true)
    IMPLEMENT_SAFE_VOID_METHOD(OnJobCompleted, (std::unique_ptr<TCompletedJobSummary> jobSummary), (std::move(jobSummary)), INVOKER_AFFINITY(CancelableInvoker), true)
    IMPLEMENT_SAFE_VOID_METHOD(OnJobFailed, (std::unique_ptr<TFailedJobSummary> jobSummary), (std::move(jobSummary)), INVOKER_AFFINITY(CancelableInvoker), true)
    IMPLEMENT_SAFE_VOID_METHOD(OnJobAborted, (std::unique_ptr<TAbortedJobSummary> jobSummary), (std::move(jobSummary)), INVOKER_AFFINITY(CancelableInvoker), true)
    IMPLEMENT_SAFE_VOID_METHOD(OnJobRunning, (std::unique_ptr<TJobSummary> jobSummary), (std::move(jobSummary)), INVOKER_AFFINITY(CancelableInvoker), true)

    IMPLEMENT_SAFE_VOID_METHOD(Commit, (), (), INVOKER_AFFINITY(CancelableInvoker), false)
    IMPLEMENT_SAFE_VOID_METHOD(Abort, (), (), THREAD_AFFINITY(ControlThread), false)
    IMPLEMENT_SAFE_VOID_METHOD(Forget, (), (), THREAD_AFFINITY(ControlThread), false)
    IMPLEMENT_SAFE_VOID_METHOD(Complete, (), (), THREAD_AFFINITY(ControlThread), false)

    IMPLEMENT_SAFE_METHOD(
        TScheduleJobResultPtr,
        ScheduleJob,
        (ISchedulingContextPtr context, const TJobResources& jobLimits),
        (context, jobLimits),
        INVOKER_AFFINITY(CancelableInvoker),
        true,
        New<TScheduleJobResult>())

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

    virtual void Initialize() override;

    void InitializeReviving(TControllerTransactionsPtr operationTransactions);

    virtual std::vector<NApi::ITransactionPtr> GetTransactions() override;

    virtual void UpdateConfig(TSchedulerConfigPtr config) override;

    virtual TCancelableContextPtr GetCancelableContext() const override;
    virtual IInvokerPtr GetCancelableControlInvoker() const override;
    virtual IInvokerPtr GetCancelableInvoker() const override;
    virtual IInvokerPtr GetInvoker() const override;

    virtual int GetTotalJobCount() const override;
    virtual int GetPendingJobCount() const override;
    virtual TJobResources GetNeededResources() const override;

    virtual bool IsForgotten() const override;

    virtual bool HasProgress() const override;
    virtual bool HasJobSplitterInfo() const override;

    virtual void Resume() override;
    virtual TFuture<void> Suspend() override;

    virtual void BuildOperationAttributes(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildProgress(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildBriefProgress(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildBriefSpec(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildMemoryDigestStatistics(NYson::IYsonConsumer* consumer) const override;
    virtual void BuildJobSplitterInfo(NYson::IYsonConsumer* consumer) const override;

    // NB(max42, babenko): this method should not be safe. Writing a core dump or trying to fail
    // operation from a forked process is a bad idea.
    virtual void SaveSnapshot(TOutputStream* output) override;

    virtual NYson::TYsonString GetProgress() const override;
    virtual NYson::TYsonString GetBriefProgress() const override;

    NYson::TYsonString BuildInputPathYson(const TJobId& jobId) const override;

    virtual void Persist(const TPersistenceContext& context) override;

    TOperationControllerBase(
        TSchedulerConfigPtr config,
        TOperationSpecBasePtr spec,
        TOperationOptionsPtr options,
        IOperationHost* host,
        TOperation* operation);

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
    IInvokerPtr CancelableControlInvoker;
    IInvokerPtr Invoker;
    ISuspendableInvokerPtr SuspendableInvoker;
    IInvokerPtr CancelableInvoker;

    std::atomic<EControllerState> State = {EControllerState::Preparing};
    std::atomic<bool> Forgotten = {false};

    bool RevivedFromSnapshot = false;

    // These totals are approximate.
    int TotalEstimatedInputChunkCount = 0;
    i64 TotalEstimatedInputDataWeight = 0;
    i64 TotalEstimatedInputRowCount = 0;
    i64 TotalEstimatedCompressedDataSize = 0;
    i64 TotalEstimatedInputDataSize = 0;

    // Total uncompressed data size for input tables.
    // Used only during preparation, not persisted.
    i64 PrimaryInputDataSize = 0;
    i64 ForeignInputDataSize = 0;

    int ChunkLocatedCallCount = 0;
    int UnavailableInputChunkCount = 0;

    // Job counters.
    TProgressCounter JobCounter;

    // Maps node ids to descriptors for job input chunks.
    NNodeTrackerClient::TNodeDirectoryPtr InputNodeDirectory;

    const NObjectClient::TTransactionId UserTransactionId;

    NApi::ITransactionPtr SyncSchedulerTransaction;
    NApi::ITransactionPtr AsyncSchedulerTransaction;
    NApi::ITransactionPtr InputTransaction;
    NApi::ITransactionPtr OutputTransaction;
    NApi::ITransactionPtr DebugOutputTransaction;

    TOperationSnapshot Snapshot;

    struct TRowBufferTag { };
    NTableClient::TRowBufferPtr RowBuffer;

    const NYTree::IMapNodePtr SecureVault;

    const std::vector<TString> Owners;


    struct TLivePreviewTableBase
    {
        // Live preview table id.
        NCypressClient::TNodeId LivePreviewTableId;

        void Persist(const TPersistenceContext& context);
    };

    std::vector<TInputTable> InputTables;

    struct TJobBoundaryKeys
    {
        NTableClient::TKey MinKey;
        NTableClient::TKey MaxKey;
        NChunkClient::TChunkTreeId ChunkTreeId;

        void Persist(const TPersistenceContext& context);
    };

    struct TOutputTable
        : public NChunkClient::TUserObject
        , public TLivePreviewTableBase
    {
        NTableClient::TTableWriterOptionsPtr Options = New<NTableClient::TTableWriterOptions>();
        NTableClient::TTableUploadOptions TableUploadOptions;
        bool ChunkPropertiesUpdateNeeded = false;
        EOutputTableType OutputType = EOutputTableType::Output;

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

        NYson::TYsonString WriterConfig;

        NTransactionClient::TTimestamp Timestamp;

        bool IsBeginUploadCompleted() const;
        void Persist(const TPersistenceContext& context);
    };

    std::vector<TOutputTable> OutputTables;
    TNullable<TOutputTable> StderrTable;
    TNullable<TOutputTable> CoreTable;

    // All output tables and stderr table (if present).
    std::vector<TOutputTable*> UpdatingTables;

    struct TIntermediateTable
        : public TLivePreviewTableBase
    {
        void Persist(const TPersistenceContext& context);
    };

    TIntermediateTable IntermediateTable;


    struct TUserFile
        : public TLockedUserObject
    {
        std::shared_ptr<NYTree::IAttributeDictionary> Attributes;
        EOperationStage Stage = EOperationStage::None;
        TString FileName;
        std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
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
    };

    TNullable<TInputQuery> InputQuery;

    struct TJoblet
        : public TIntrinsicRefCounted
    {
        //! For serialization only.
        TJoblet()
            : JobIndex(-1)
            , StartRowIndex(-1)
            , OutputCookie(-1)
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

        TJobNodeDescriptor NodeDescriptor;

        TExtendedJobResources EstimatedResourceUsage;
        double JobProxyMemoryReserveFactor = -1;
        double UserJobMemoryReserveFactor = -1;
        TJobResources ResourceLimits;

        TChunkStripeListPtr InputStripeList;
        IChunkPoolOutput::TCookie OutputCookie;

        //! All chunk lists allocated for this job.
        /*!
         *  For jobs with intermediate output this list typically contains one element.
         *  For jobs with final output this list typically contains one element per each output table.
         */
        std::vector<NChunkClient::TChunkListId> ChunkListIds;

        NChunkClient::TChunkListId StderrTableChunkListId;
        NChunkClient::TChunkListId CoreTableChunkListId;

        TInstant StartTime;
        TInstant FinishTime;

        void Persist(const TPersistenceContext& context);
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
            const TJobNodeDescriptor& nodeDescriptor)
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

        TJobNodeDescriptor NodeDescriptor;

        void Persist(const TPersistenceContext& context);

    };

    class TTask
        : public TRefCounted
        , public IPersistent
    {
    public:
        DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, DelayedTime);

    public:
        //! For persistence only.
        TTask();
        explicit TTask(TOperationControllerBase* controller);

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

        virtual TExtendedJobResources GetNeededResources(TJobletPtr joblet) const = 0;

        void ResetCachedMinNeededResources();

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

        virtual void Persist(const TPersistenceContext& context) override;

    private:
        TOperationControllerBase* Controller;

        int CachedPendingJobCount;
        int CachedTotalJobCount;

        TJobResources CachedTotalNeededResources;
        mutable TNullable<TExtendedJobResources> CachedMinNeededResources;

        NProfiling::TCpuInstant DemandSanityCheckDeadline;
        bool CompletedFired;

        //! For each lost job currently being replayed, maps output cookie to corresponding input cookie.
        yhash<IChunkPoolOutput::TCookie, IChunkPoolInput::TCookie> LostJobCookieMap;

        TJobResources ApplyMemoryReserve(const TExtendedJobResources& jobResources) const;

    protected:
        NLogging::TLogger Logger;

        virtual bool CanScheduleJob(
            ISchedulingContext* context,
            const TJobResources& jobLimits);

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const = 0;

        virtual void OnTaskCompleted();

        virtual EJobType GetJobType() const = 0;
        virtual TUserJobSpecPtr GetUserJobSpec() const;
        virtual void PrepareJoblet(TJobletPtr joblet);
        virtual void BuildJobSpec(TJobletPtr joblet, NJobTrackerClient::NProto::TJobSpec* jobSpec) = 0;

        virtual void OnJobStarted(TJobletPtr joblet);

        void AddPendingHint();
        void AddLocalityHint(NNodeTrackerClient::TNodeId nodeId);

        void ReinstallJob(TJobletPtr joblet, EJobReinstallReason reason);

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

        void AddFootprintAndUserJobResources(TExtendedJobResources& jobResources) const;
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
        yhash<NNodeTrackerClient::TNodeId, yhash_set<TTaskPtr>> NodeIdToTasks;

        TTaskGroup()
        {
            MinNeededResources.SetUserSlots(1);
        }

        void Persist(const TPersistenceContext& context);

    };

    NApi::ITransactionPtr StartTransaction(
        ETransactionType type,
        NApi::INativeClientPtr client,
        const NTransactionClient::TTransactionId& parentTransactionId = NTransactionClient::NullTransactionId);

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

    void CheckAvailableExecNodes();

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
    TJobletPtr FindJoblet(const TJobId& jobId) const;
    TJobletPtr GetJoblet(const TJobId& jobId) const;
    TJobletPtr GetJobletOrThrow(const TJobId& jobId) const;
    void RemoveJoblet(const TJobId& jobId);


    // Initialization.
    virtual void DoInitialize();
    virtual void InitializeConnections();
    virtual void InitializeTransactions();
    virtual void InitializeStructures();
    void InitUpdatingTables();


    // Preparation.
    void FetchInputTables();
    void RegisterInputChunk(const NChunkClient::TInputChunkPtr& inputChunk);
    void LockInputTables();
    void GetInputTablesAttributes();
    void GetOutputTablesSchema();
    virtual void PrepareInputTables();
    virtual void PrepareOutputTables();
    void BeginUploadOutputTables();
    void GetOutputTablesUploadParams();
    void FetchUserFiles();
    void LockUserFiles();
    void GetUserFilesAttributes();
    void CreateLivePreviewTables();
    void LockLivePreviewTables();
    void CollectTotals();
    virtual void CustomPrepare();
    void AddAllTaskPendingHints();
    void InitInputChunkScraper();
    void InitIntermediateChunkScraper();
    void SuspendUnavailableInputStripes();

    void ParseInputQuery(
        const TString& queryString,
        const TNullable<NQueryClient::TTableSchema>& schema);
    void WriteInputQueryToJobSpec(
        NProto::TSchedulerJobSpecExt* schedulerJobSpecExt);
    virtual void PrepareInputQuery();

    void PickIntermediateDataCell();
    void InitChunkListPool();

    // Initialize transactions
    void StartAsyncSchedulerTransaction();
    void StartSyncSchedulerTransaction();
    void StartInputTransaction(const NObjectClient::TTransactionId& parentTransactionId);
    void StartOutputTransaction(const NObjectClient::TTransactionId& parentTransactionId);
    void StartDebugOutputTransaction();

    // Completion.
    void TeleportOutputChunks();
    void AttachOutputChunks(const std::vector<TOutputTable*>& tableList);
    void EndUploadOutputTables(const std::vector<TOutputTable*>& tableList);
    void CommitTransactions();
    virtual void CustomCommit();

    bool GetCommitting();
    void SetCommitting();
    void SetCommitted();

    // Revival.
    void ReinstallLivePreview();
    void AbortAllJoblets();

    void DoLoadSnapshot(const TOperationSnapshot& snapshot);

    bool InputHasDynamicTables() const;
    bool InputHasVersionedTables() const;
    bool InputHasReadLimits() const;

    //! Called to extract input table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetInputTablePaths() const = 0;

    //! Called to extract output table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetOutputTablePaths() const = 0;

    //! Called to extract stderr table path from the spec.
    virtual TNullable<NYPath::TRichYPath> GetStderrTablePath() const;
    //! Called to extract stderr table writer config from the spec.
    virtual NTableClient::TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const;

    //! Called to extract core table path from the spec.
    virtual TNullable<NYPath::TRichYPath> GetCoreTablePath() const;
    //! Called to extract core table writer config from the spec.
    virtual NTableClient::TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const;

    typedef std::pair<NYPath::TRichYPath, EOperationStage> TPathWithStage;

    //! Called to extract file paths from the spec.
    virtual std::vector<TPathWithStage> GetFilePaths() const;

    //! Called when a job is unable to read a chunk.
    void OnChunkFailed(const NChunkClient::TChunkId& chunkId);

    //! Gets the list of all intermediate chunks that are not lost.
    yhash_set<NChunkClient::TChunkId> GetAliveIntermediateChunks() const;

    //! Called when a job is unable to read an intermediate chunk
    //! (i.e. that is not a part of the input).
    //! Returns false if the chunk was already considered lost.
    bool OnIntermediateChunkUnavailable(const NChunkClient::TChunkId& chunkId);

    virtual bool IsJobInterruptible() const;
    int EstimateSplitJobCount(const TCompletedJobSummary& jobSummary, const TJobletPtr& joblet);
    std::vector<NChunkClient::TInputDataSlicePtr> ExtractInputDataSlices(const TCompletedJobSummary& jobSummary) const;
    virtual void ReinstallUnreadInputDataSlices(const std::vector<NChunkClient::TInputDataSlicePtr>& inputDataSlices);

    struct TStripeDescriptor
    {
        TChunkStripePtr Stripe;
        IChunkPoolInput::TCookie Cookie;
        TTaskPtr Task;

        TStripeDescriptor()
            : Cookie(IChunkPoolInput::NullCookie)
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

    void RegisterInputStripe(TChunkStripePtr stripe, TTaskPtr task);


    void RegisterBoundaryKeys(
        const NProto::TOutputResult& boundaryKeys,
        const NChunkClient::TChunkTreeId& chunkTreeId,
        TOutputTable* outputTable);

    virtual void RegisterOutput(TJobletPtr joblet, int key, const TCompletedJobSummary& jobSummary);

    virtual void RegisterOutput(
        const std::vector<NChunkClient::TChunkListId>& chunkListIds,
        int key,
        const TCompletedJobSummary& jobSummary);

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

    void RegisterStderr(TJobletPtr joblet, const TJobSummary& jobSummary);
    void RegisterCores(TJobletPtr joblet, const TJobSummary& jobSummary);

    bool HasEnoughChunkLists(bool intermediate, bool isWritingStderrTable, bool isWritingCoreTable);
    NChunkClient::TChunkListId ExtractChunkList(NObjectClient::TCellTag cellTag);
    void ReleaseChunkLists(const std::vector<NChunkClient::TChunkListId>& ids);

    //! Called after preparation to decrease memory footprint.
    void ClearInputChunkBoundaryKeys();

    //! Returns the list of all input chunks collected from all primary input tables.
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryChunks(bool versioned) const;
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryUnversionedChunks() const;
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryVersionedChunks() const;
    std::pair<i64, i64> CalculatePrimaryVersionedChunksStatistics() const;
    std::vector<NChunkClient::TInputDataSlicePtr> CollectPrimaryVersionedDataSlices(i64 sliceSize) const;

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
        std::vector<TChunkStripePtr>* result) const;
    void SlicePrimaryUnversionedChunks(
        const IJobSizeConstraintsPtr& jobSizeConstraints,
        std::vector<TChunkStripePtr>* result) const;
    void SlicePrimaryVersionedChunks(
        const IJobSizeConstraintsPtr& jobSizeConstraints,
        std::vector<TChunkStripePtr>* result) const;

    void InitUserJobSpecTemplate(
        NScheduler::NProto::TUserJobSpec* proto,
        TUserJobSpecPtr config,
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
    i64 GetFinalOutputIOMemorySize(TJobIOConfigPtr ioConfig) const;

    i64 GetFinalIOMemorySize(
        TJobIOConfigPtr ioConfig,
        const TChunkStripeStatisticsVector& stripeStatistics) const;

    void InitIntermediateOutputConfig(TJobIOConfigPtr config);
    void InitFinalOutputConfig(TJobIOConfigPtr config);

    static NTableClient::TTableReaderOptionsPtr CreateTableReaderOptions(TJobIOConfigPtr ioConfig);

    void ValidateUserFileCount(TUserJobSpecPtr spec, const TString& operation);

    const std::vector<TExecNodeDescriptor>& GetExecNodeDescriptors();

    virtual void RegisterUserJobMemoryDigest(EJobType jobType, double memoryReserveFactor);
    IDigest* GetUserJobMemoryDigest(EJobType jobType);
    const IDigest* GetUserJobMemoryDigest(EJobType jobType) const;

    virtual void RegisterJobProxyMemoryDigest(EJobType jobType, const TLogDigestConfigPtr& config);
    IDigest* GetJobProxyMemoryDigest(EJobType jobType);
    const IDigest* GetJobProxyMemoryDigest(EJobType jobType) const;

    i64 ComputeUserJobMemoryReserve(EJobType jobType, TUserJobSpecPtr jobSpec) const;

    void InferSchemaFromInput(const NTableClient::TKeyColumns& keyColumns = NTableClient::TKeyColumns());
    void InferSchemaFromInputOrdered();
    void ValidateOutputSchemaOrdered() const;

    virtual TJobSplitterConfigPtr GetJobSplitterConfig() const;

private:
    typedef TOperationControllerBase TThis;

    typedef yhash<NChunkClient::TChunkId, TInputChunkDescriptor> TInputChunkMap;

    //! Keeps information needed to maintain the liveness state of input chunks.
    TInputChunkMap InputChunkMap;

    TOperationSpecBasePtr Spec;
    TOperationOptionsPtr Options;

    NObjectClient::TCellTag IntermediateOutputCellTag = NObjectClient::InvalidCellTag;
    TChunkListPoolPtr ChunkListPool;
    yhash<NObjectClient::TCellTag, int> CellTagToOutputRequiredChunkList;
    yhash<NObjectClient::TCellTag, int> CellTagToIntermediateRequiredChunkList;

    std::atomic<int> CachedPendingJobCount = {0};

    TJobResources CachedNeededResources;
    NConcurrency::TReaderWriterSpinLock CachedNeededResourcesLock;

    //! Maps an intermediate chunk id to its originating completed job.
    yhash<NChunkClient::TChunkId, TCompletedJobPtr> ChunkOriginMap;

    TIntermediateChunkScraperPtr IntermediateChunkScraper;

    //! Maps scheduler's job ids to controller's joblets.
    //! NB: |TJobPtr -> TJobletPtr| mapping would be faster but
    //! it cannot be serialized that easily.
    yhash<TJobId, TJobletPtr> JobletMap;

    NChunkClient::TChunkScraperPtr InputChunkScraper;

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

    //! Exec node count do not consider scheduling tag.
    //! But descriptors do.
    int ExecNodeCount_ = 0;
    TExecNodeDescriptorListPtr ExecNodesDescriptors_ = New<TExecNodeDescriptorList>();

    NProfiling::TCpuInstant GetExecNodesInformationDeadline_ = 0;
    NProfiling::TCpuInstant AvaialableNodesLastSeenTime_ = 0;

    const std::unique_ptr<NTableClient::IValueConsumer> EventLogValueConsumer_;
    const std::unique_ptr<NYson::IYsonConsumer> EventLogTableConsumer_;

    typedef yhash<EJobType, std::unique_ptr<IDigest>> TMemoryDigestMap;
    TMemoryDigestMap JobProxyMemoryDigests_;
    TMemoryDigestMap UserJobMemoryDigests_;

    const TString CodicilData_;

    std::atomic<bool> AreTransactionsActive = {false};

    std::unique_ptr<IHistogram> EstimatedInputDataSizeHistogram_;
    std::unique_ptr<IHistogram> InputDataSizeHistogram_;

    const NProfiling::TCpuDuration LogProgressBackoff;
    NProfiling::TCpuInstant NextLogProgressDeadline = 0;

    NYson::TYsonString ProgressString_;
    NYson::TYsonString BriefProgressString_;

    TSpinLock ProgressLock_;
    const NConcurrency::TPeriodicExecutorPtr ProgressBuildExecutor_;

    i64 CurrentInputDataSliceTag_ = 0;

    void BuildAndSaveProgress();

    void UpdateMemoryDigests(TJobletPtr joblet, const NJobTrackerClient::TStatistics& statistics, bool resourceOverdraft = false);

    void InitializeHistograms();
    void UpdateEstimatedHistogram(TJobletPtr joblet);
    void UpdateEstimatedHistogram(TJobletPtr joblet, EJobReinstallReason reason);
    void UpdateActualHistogram(const NJobTrackerClient::TStatistics& statistics);

    void GetExecNodesInformation();
    int GetExecNodeCount();

    bool ShouldSkipSanityCheck();

    void UpdateJobStatistics(const TJobSummary& jobSummary);

    void LogProgress(bool force = false);

    std::unique_ptr<IJobSplitter> JobSplitter_;

    NApi::INativeClientPtr CreateClient();
    void UpdateAllTasksIfNeeded();

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

    void ValidateDynamicTableTimestamp(
        const NYPath::TRichYPath& path,
        bool dynamic,
        const NTableClient::TTableSchema& schema,
        const NYTree::IAttributeDictionary& attributes) const;

    //! An internal helper for invoking OnOperationFailed with an error
    //! built by data from `ex`.
    void FailOperation(const std::exception& ex);
    void FailOperation(const TAssertionFailedException& ex);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
