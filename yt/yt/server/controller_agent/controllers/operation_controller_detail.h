#pragma once

#include "private.h"

#include "aggregated_job_statistics.h"
#include "alert_manager.h"
#include "auto_merge_director.h"
#include "extended_job_resources.h"
#include "helpers.h"
#include "input_manager.h"
#include "job_info.h"
#include "job_memory.h"
#include "task.h"
#include "task_host.h"

#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/server/lib/chunk_pools/input_stream.h>
#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/server/lib/controller_agent/job_report.h>

#include <yt/yt/server/lib/scheduler/event_log.h>
#include <yt/yt/server/lib/scheduler/exec_node_descriptor.h>
#include <yt/yt/server/lib/scheduler/transactions.h>

#include <yt/yt/server/lib/misc/release_queue.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe_key.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/library/safe_assert/safe_assert.h>

#include <yt/yt/core/misc/histogram.h>
#include <yt/yt/core/misc/id_generator.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

#include <optional>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionType,
    (Async)
    (Input)
    (Output)
    (Debug)
    (OutputCompletion)
    (DebugCompletion)
);

DEFINE_ENUM(EIntermediateChunkUnstageMode,
    // Unstage chunks when job is completed.
    (OnJobCompleted)
    // Keep a release queue of chunks and unstage then when snapshot is built.
    (OnSnapshotCompleted)
);

class TOperationControllerBase
    : public IOperationController
    , public NScheduler::TEventLogHostBase
    , public virtual ITaskHost
    , public IAlertManagerHost
    , public IInputManagerHost
{
    // In order to make scheduler more stable, we do not allow
    // pure YT_VERIFY to be executed from the controller code (directly
    // or indirectly). Thus, all interface methods of IOperationController
    // are divided into two groups: those that involve YT_VERIFYs
    // to make assertions essential for further execution, and pure ones.

    // All potentially faulty controller interface methods are
    // guarded by enclosing into an extra method.
#define IMPLEMENT_SAFE_METHOD_WITH_RETURN_VALUE(accessSpecifier, returnType, method, signature, args, catchStdException, returnValue) \
accessSpecifier: \
    virtual returnType method signature final \
    { \
        auto safeAssertionsGuard = CreateSafeAssertionGuard( \
            Host->GetCoreDumper(), \
            Host->GetCoreSemaphore(), \
            CoreNotes_); \
        try { \
            return Safe ## method args; \
        } catch (const TAssertionFailedException& ex) { \
            ProcessSafeException(ex); \
            return returnValue; \
        } catch (const std::exception& ex) { \
            if (catchStdException) { \
                ProcessSafeException(ex); \
                return returnValue; \
            } \
            throw; \
        } \
    } \
private: \
    returnType Safe ## method signature;

#define IMPLEMENT_SAFE_METHOD(accessSpecifier, returnType, method, signature, args, catchStdException) \
    IMPLEMENT_SAFE_METHOD_WITH_RETURN_VALUE(accessSpecifier, returnType, method, signature, args, catchStdException, returnType())

    IMPLEMENT_SAFE_METHOD_WITH_RETURN_VALUE(
        public,
        TOperationControllerPrepareResult,
        Prepare,
        (),
        (),
        false,
        (Error_.ThrowOnError(), TOperationControllerPrepareResult()))
    IMPLEMENT_SAFE_METHOD(public, TOperationControllerMaterializeResult, Materialize, (), (), false)

    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        OnJobStarted,
        (const TJobletPtr& joblet),
        (joblet),
        true)
    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        OnAllocationAborted,
        (TAbortedAllocationSummary&& abortedAllocationSummary),
        (std::move(abortedAllocationSummary)),
        true)
    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        OnAllocationFinished,
        (TFinishedAllocationSummary&& finishedAllocationSummary),
        (std::move(finishedAllocationSummary)),
        true);

    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        AbandonJob,
        (TJobId jobId),
        (jobId),
        false)

    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        InterruptJobByUserRequest,
        (TJobId jobId, TDuration timeout),
        (jobId, timeout),
        false)

    IMPLEMENT_SAFE_METHOD(public, void, UpdateGroupedNeededResources, (), (), true)

    IMPLEMENT_SAFE_METHOD(public, void, Commit, (), (), false)
    IMPLEMENT_SAFE_METHOD(public, void, Terminate, (EControllerState finalState), (finalState), false)

    IMPLEMENT_SAFE_METHOD(public, void, Complete, (), (), false)

    IMPLEMENT_SAFE_METHOD(
        public,
        NScheduler::TControllerScheduleAllocationResultPtr,
        ScheduleAllocation,
        (const TAllocationSchedulingContext& context, const TString& treeId),
        (context, treeId),
        true)

    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        InvokeSafely,
        (std::function<void()> closure),
        (std::move(closure)),
        true)

    //! Called by #IntermediateChunkScraper.
    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        OnIntermediateChunkLocated,
        (NChunkClient::TChunkId chunkId, const NChunkClient::TChunkReplicaWithMediumList& replicas, bool missing),
        (chunkId, replicas, missing),
        false)

    //! Called by `TSnapshotBuilder` when snapshot is built.
    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        OnSnapshotCompleted,
        (const TSnapshotCookie& cookie),
        (cookie),
        false)

    /*!
     *  \note Thread affinity: JobSpecBuildPool
     */
    IMPLEMENT_SAFE_METHOD(
        public,
        TSharedRef,
        BuildJobSpecProto,
        (const TJobletPtr& joblet, const std::optional<NScheduler::NProto::TScheduleAllocationSpec>& scheduleAllocationSpec),
        (joblet, scheduleAllocationSpec),
        false)

    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        OnJobInfoReceivedFromNode,
        (std::unique_ptr<TJobSummary> jobSummary),
        (std::move(jobSummary)),
        true)

    IMPLEMENT_SAFE_METHOD(
        public,
        void,
        AbortJobByJobTracker,
        (TJobId jobId, EAbortReason abortReason),
        (jobId, abortReason),
        true);

    IMPLEMENT_SAFE_METHOD(private, void, BuildAndSaveProgress, (), (), true);

    IMPLEMENT_SAFE_METHOD_WITH_RETURN_VALUE(
        public,
        TJobStartInfo,
        SettleJob,
        (TAllocationId allocationId, std::optional<TJobId> lastJobId),
        (allocationId, lastJobId),
        false,
        TJobStartInfo());

#undef IMPLEMENT_SAFE_METHOD

public:
    // These are "pure" interface methods, i. e. those that do not involve YT_VERIFYs.
    // If some of these methods still fails due to unnoticed YT_VERIFY, consider
    // moving it to the section above.

    // NB(max42): Don't make Revive safe! It may lead to either destroying all
    // operations on a cluster, or to a scheduler crash.
    TOperationControllerReviveResult Revive() override;

    TOperationControllerInitializeResult InitializeClean() override;
    TOperationControllerInitializeResult InitializeReviving(const NScheduler::TControllerTransactionIds& transactions) override;

    bool ShouldSkipScheduleAllocationRequest() const noexcept override;

    bool ShouldSkipRunningJobEvents() const noexcept override;

    void RecordScheduleAllocationFailure(EScheduleFailReason reason) noexcept override;

    void OnTransactionsAborted(const std::vector<NTransactionClient::TTransactionId>& transactionIds) override;

    void UpdateConfig(const TControllerAgentConfigPtr& config) override;

    TCancelableContextPtr GetCancelableContext() const override;
    IInvokerPtr GetInvoker(EOperationControllerQueue queue = EOperationControllerQueue::Default) const override;
    IInvokerPoolPtr GetCancelableInvokerPool() const override;

    TCompositePendingJobCount GetPendingJobCount() const override;
    i64 GetFailedJobCount() const override;
    NScheduler::TCompositeNeededResources GetNeededResources() const override;

    bool ShouldUpdateLightOperationAttributes() const override;
    void SetLightOperationAttributesUpdated() override;

    NScheduler::TAllocationGroupResourcesMap GetGroupedNeededResources() const override;

    bool IsRunning() const override;

    std::vector<NServer::TTestAllocationGuard> TestHeap() const override;

    void SetProgressAttributesUpdated() override;
    bool ShouldUpdateProgressAttributes() const override;

    bool HasProgress() const override;

    void Resume() override;
    TFuture<void> Suspend() override;

    void Cancel() override;

    // BuildProgress can set alerts, so it can't be const.
    virtual void BuildProgress(NYTree::TFluentMap fluent);

    virtual void BuildBriefProgress(NYTree::TFluentMap fluent) const;
    virtual void BuildJobsYson(NYTree::TFluentMap fluent) const;
    virtual void BuildRetainedFinishedJobsYson(NYTree::TFluentMap fluent) const;

    // NB(max42, babenko): this method should not be safe. Writing a core dump or trying to fail
    // operation from a forked process is a bad idea.
    void SaveSnapshot(IZeroCopyOutput* output) override;

    NYson::TYsonString GetProgress() const override;
    NYson::TYsonString GetBriefProgress() const override;

    NYson::TYsonString GetSuspiciousJobsYson() const override;

    TOperationControllerBase(
        TOperationSpecBasePtr spec,
        TControllerAgentConfigPtr config,
        TOperationOptionsPtr options,
        IOperationControllerHostPtr host,
        TOperation* operation);

    // ITaskHost implementation.

    IInvokerPtr GetChunkScraperInvoker() const override;
    IInvokerPtr GetCancelableInvoker(EOperationControllerQueue queue = EOperationControllerQueue::Default) const override;
    IInvokerPtr GetJobSpecBuildInvoker() const override;
    TDiagnosableInvokerPool::TInvokerStatistics GetInvokerStatistics(
        EOperationControllerQueue queue = EOperationControllerQueue::Default) const override;

    std::optional<NYPath::TRichYPath> GetStderrTablePath() const override;
    std::optional<NYPath::TRichYPath> GetCoreTablePath() const override;
    bool GetEnableCudaGpuCoreDump() const override;

    void AccountBuildingJobSpecDelta(int countDelta, i64 sliceCountDelta) noexcept override;

    ui64 NextJobIndex();
    void InitUserJobSpecTemplate(
        NControllerAgent::NProto::TUserJobSpec* jobSpec,
        const NScheduler::TUserJobSpecPtr& jobSpecConfig,
        const std::vector<TUserFile>& files,
        const TString& debugArtifactsAccount) override;
    const std::vector<TUserFile>& GetUserFiles(const NScheduler::TUserJobSpecPtr& userJobSpec) const override;

    void CustomizeJobSpec(const TJobletPtr& joblet, NControllerAgent::NProto::TJobSpec* jobSpec) const override;
    void CustomizeJoblet(const TJobletPtr& joblet) override;

    void AddValueToEstimatedHistogram(const TJobletPtr& joblet) override;
    void RemoveValueFromEstimatedHistogram(const TJobletPtr& joblet) override;

    const TControllerAgentConfigPtr& GetConfig() const override;
    const TOperationSpecBasePtr& GetSpec() const override;
    const TOperationOptionsPtr& GetOptions() const override;

    //! Unsuccessfully terminates and finalizes the operation.
    /*!
     *  Does it asynchronously to avoid context switches inside #OnJobCompleted, #OnJobFailed, ...
     *  In some contexts, eg. in periodics, doing it asynchronously makes no sense.
     *  Use #DoFailOperation there instead.
     *  See also YT-19936.
     */
    void OnOperationFailed(const TError& error, bool flush = true, bool abortAllJoblets = true) override;

    void OnOperationAborted(const TError& error);

    bool IsRowCountPreserved() const override;
    bool ShouldSkipSanityCheck() override;

    TExtendedJobResources GetAutoMergeResources(
        const NTableClient::TChunkStripeStatisticsVector& statistics) const override;
    TAutoMergeDirector* GetAutoMergeDirector() override;

    const TChunkListPoolPtr& GetOutputChunkListPool() const override;
    NChunkClient::TChunkListId ExtractOutputChunkList(NObjectClient::TCellTag cellTag) override;
    NChunkClient::TChunkListId ExtractDebugChunkList(NObjectClient::TCellTag cellTag) override;
    void ReleaseChunkTrees(
        const std::vector<NChunkClient::TChunkListId>& chunkListIds,
        bool unstageRecursively,
        bool waitForSnapshot) override;
    void ReleaseIntermediateStripeList(const NChunkPools::TChunkStripeListPtr& stripeList) override;

    TOperationId GetOperationId() const override;
    EOperationType GetOperationType() const override;
    TInstant GetStartTime() const override;

    const std::string& GetAuthenticatedUser() const override;

    const TOutputTablePtr& StderrTable() const override;
    const TOutputTablePtr& CoreTable() const override;

    void RegisterStderr(const TJobletPtr& joblet, const TJobSummary& summary) override;
    void RegisterCores(const TJobletPtr& joblet, const TJobSummary& summary) override;

    void RegisterJoblet(const TJobletPtr& joblet) override;

    std::optional<TJobMonitoringDescriptor> RegisterJobForMonitoring(TJobId jobId) override;
    void UnregisterJobForMonitoring(const TJobletPtr& joblet);
    std::optional<TJobMonitoringDescriptor> RegisterNewMonitoringDescriptor();

    int GetMonitoredUserJobCount() const override;
    int GetRegisteredMonitoringDescriptorCount() const;

    const std::optional<TJobResources>& CachedMaxAvailableExecNodeResources() const override;

    TInputManagerPtr GetInputManager() const override;

    void RegisterRecoveryInfo(
        const TCompletedJobPtr& completedJob,
        const NChunkPools::TChunkStripePtr& stripe) override;

    NTableClient::TRowBufferPtr GetRowBuffer() override;

    TSnapshotCookie OnSnapshotStarted() override;

    bool HasSnapshot() const override;

    void Dispose() override;

    void UpdateRuntimeParameters(const NScheduler::TOperationRuntimeParametersUpdatePtr& update) override;

    //! Returns the aggregated delta of job metrics and resets it to zero.
    //! When `force` is true, the delta is returned unconditionally, otherwise a zero delta is
    //! returned within a certain period since last call.
    NScheduler::TOperationJobMetrics PullJobMetricsDelta(bool force = false) override;

    TOperationAlertMap GetAlerts() override;

    TOperationInfo BuildOperationInfo() override;

    NYTree::IYPathServicePtr GetOrchid() const override;

    void ZombifyOrchid() final;

    // Job shell options should never be changed in operation spec.
    const std::vector<NScheduler::TJobShellPtr>& GetJobShells() const override;

    TString WriteCoreDump() const override;

    //! Needed for row_count_limit.
    void RegisterOutputRows(i64 count, int tableIndex) override;

    std::optional<int> GetRowCountLimitTableIndex() override;

    void LoadSnapshot(const TOperationSnapshot& snapshot) override;

    void RegisterOutputTables(const std::vector<NYPath::TRichYPath>& outputTablePaths) override;

    void AsyncAbortJob(TJobId jobId, EAbortReason abortReason) override;
    void AbortJob(TJobId jobId, EAbortReason abortReason) override;

    bool CanInterruptJobs() const override;
    void InterruptJob(TJobId jobId, EInterruptionReason reason) override;

    void OnCompetitiveJobScheduled(const TJobletPtr& joblet, EJobCompetitionType competitionType) override;

    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() const override;

    TJobSplitterConfigPtr GetJobSplitterConfigTemplate() const override;

    const TInputTablePtr& GetInputTable(int tableIndex) const override;
    const TOutputTablePtr& GetOutputTable(int tableIndex) const override;
    int GetOutputTableCount() const override;

    const NLogging::TLogger& GetLogger() const override;

    const std::vector<TTaskPtr>& GetTasks() const override;

    void SetOperationAlert(EOperationAlertType type, const TError& alert) override;

    void OnMemoryLimitExceeded(const TError& error) override;

    bool IsMemoryLimitExceeded() const override;

    i64 GetMemoryUsage() const override;

    //! Returns |true| when operation completion event is scheduled to control invoker.
    bool IsFinished() const override;

    std::pair<NApi::ITransactionPtr, TString> GetIntermediateMediumTransaction() override;
    void UpdateIntermediateMediumUsage(i64 usage) override;

    const std::vector<TString>& GetOffloadingPoolTrees() override;
    void InitializeJobExperiment();
    TJobExperimentBasePtr GetJobExperiment() override;

    void UpdateWriteBufferMemoryAlert(TJobId jobId, i64 currentMemory, i64 previousMemory) override;

    std::expected<TJobId, EScheduleFailReason> GenerateJobId(NScheduler::TAllocationId allocationId, TJobId previousJobId) override;

    TJobletPtr CreateJoblet(
        TTask* task,
        TJobId jobId,
        TString treeId,
        int taskJobIndex,
        std::optional<TString> poolPath,
        bool treeIsTentative) override;

    virtual std::shared_ptr<const THashMap<NScheduler::TClusterName, bool>> GetClusterToNetworkBandwidthAvailability() const override;

    virtual bool IsNetworkBandwidthAvailable(const NScheduler::TClusterName& clusterName) const override;

    virtual void SubscribeToClusterNetworkBandwidthAvailabilityUpdated(
        const NScheduler::TClusterName& clusterName,
        const TCallback<void()>& callback) const override;

    virtual void UnsubscribeFromClusterNetworkBandwidthAvailabilityUpdated(
        const NScheduler::TClusterName& clusterName,
        const TCallback<void()>& callback) const override;

protected:
    const IOperationControllerHostPtr Host;
    TControllerAgentConfigPtr Config;

    const TOperationId OperationId;

    const EOperationType OperationType;
    const TInstant StartTime_;
    const std::string AuthenticatedUser;
    const NYTree::IMapNodePtr SecureVault;
    const NTransactionClient::TTransactionId UserTransactionId;

    const NLogging::TLogger Logger;
    const std::vector<TString> CoreNotes_;

    NSecurityClient::TSerializableAccessControlList Acl;

    std::optional<TString> AcoName;

    // Intentionally transient.
    NScheduler::TControllerEpoch ControllerEpoch;

    THashMap<TAllocationId, TAllocation> AllocationMap_;
    int RunningJobCount_ = 0;

    // Usually these clients are all the same (and connected to the current cluster).
    // But `remote copy' operation connects InputClient to remote cluster.
    // OutputClient is created for the sake of symmetry with Input;
    // i.e. Client and OutputClient are always connected to the same cluster.
    NApi::NNative::IClientPtr Client;
    NApi::NNative::IClientPtr InputClient;
    NApi::NNative::IClientPtr OutputClient;

    // These clients are identical to the above, but uses scheduler user.
    NApi::NNative::IClientPtr SchedulerClient;
    NApi::NNative::IClientPtr SchedulerInputClient;
    NApi::NNative::IClientPtr SchedulerOutputClient;

    TCancelableContextPtr CancelableContext;
    const IInvokerPtr ChunkScraperInvoker_;
    TDiagnosableInvokerPoolPtr DiagnosableInvokerPool_;
    IInvokerPoolPtr InvokerPool;
    ISuspendableInvokerPoolPtr SuspendableInvokerPool;
    IInvokerPoolPtr CancelableInvokerPool;

    IInvokerPtr JobSpecBuildInvoker_;

    NChunkPools::TInputStreamDirectory InputStreamDirectory_;

    std::atomic<EControllerState> State = {EControllerState::Preparing};

    // These totals are approximate.
    int TotalEstimatedInputChunkCount = 0;
    i64 TotalEstimatedInputDataWeight = 0;
    i64 TotalEstimatedInputRowCount = 0;
    i64 TotalEstimatedInputValueCount = 0;
    i64 TotalEstimatedInputCompressedDataSize = 0;
    i64 TotalEstimatedInputUncompressedDataSize = 0;

    i64 TeleportedOutputRowCount = 0;

    // Only used during materialization, not persisted.
    double InputCompressionRatio = 0.0;

    // Ratio DataWeight/UncomprssedDataSize for input data.
    // Only used during materialization, not persisted.
    double DataWeightRatio = 0.0;

    // Total uncompressed data size for input tables.
    // Used only during preparation, not persisted.
    i64 PrimaryInputDataWeight = 0;
    i64 ForeignInputDataWeight = 0;

    int UnavailableIntermediateChunkCount = 0;

    // NB: Transaction objects are ephemeral and should not be saved to snapshot.
    TInputTransactionManagerPtr InputTransactions;
    NApi::ITransactionPtr AsyncTransaction;
    NApi::ITransactionPtr OutputTransaction;
    NApi::ITransactionPtr DebugTransaction;
    NApi::NNative::ITransactionPtr OutputCompletionTransaction;
    NApi::ITransactionPtr DebugCompletionTransaction;
    NApi::ITransactionPtr UserTransaction;

    bool CommitFinished = false;

    //! If this flag is set, operation clean start is done instead of revive.
    bool CleanStart = false;

    TOperationSnapshot Snapshot;

    struct TRowBufferTag { };
    NTableClient::TRowBufferPtr RowBuffer;

    TInputManagerPtr InputManager;

    THashMap<NYPath::TYPath, TOutputTablePtr> PathToOutputTable_;
    std::vector<TOutputTablePtr> OutputTables_;
    TOutputTablePtr StderrTable_;
    TOutputTablePtr CoreTable_;

    NNodeTrackerClient::TNodeDirectoryPtr OutputNodeDirectory_ = New<NNodeTrackerClient::TNodeDirectory>();

    // All output tables plus stderr and core tables (if present).
    std::vector<TOutputTablePtr> UpdatingTables_;

    TIntermediateTablePtr IntermediateTable = New<TIntermediateTable>();

    THashMap<NScheduler::TUserJobSpecPtr, std::vector<TUserFile>> UserJobFiles_;

    struct TInputQuery
    {
        NQueryClient::TQueryPtr Query;
        NQueryClient::TExternalCGInfoPtr ExternalCGInfo;
        NScheduler::TQueryFilterOptionsPtr QueryFilterOptions;
    };

    std::optional<TInputQuery> InputQuery;

    //! All tasks declared by calling #RegisterTask, in the order of decreasing priority.
    std::vector<TTaskPtr> Tasks;

    TAutoMergeTaskPtr AutoMergeTask_;

    //! Whether auto-merge is enabled for particular output table.
    std::vector<bool> AutoMergeEnabled_;

    TDataFlowGraphPtr DataFlowGraph_ = New<TDataFlowGraph>();

    using TLivePreviewMap = THashMap<TString, TLivePreviewPtr>;
    std::shared_ptr<TLivePreviewMap> LivePreviews_;
    NYTree::IYPathServicePtr LivePreviewService_;

    NYTree::IMapNodePtr UnrecognizedSpec_;

    TAtomicIntrusivePtr<NYTree::IYPathService> Orchid_;

    std::vector<std::vector<char>> TestingAllocationVector_;

    // NB: These values are accessed from BuildJobSpecProto invoker queue, ScheduleAllocation invoker queue and from control invoker.
    // Slight discrepancy in their values due to concurrent modification and access is OK.
    // These values are transient.
    std::atomic<int> BuildingJobSpecCount_ = {0};
    std::atomic<i64> TotalBuildingJobSpecSliceCount_ = {0};

    int RegisteredMonitoringDescriptorCount_ = 0;
    std::atomic<int> MonitoredUserJobCount_ = 0;
    int MonitoredUserJobAttemptCount_ = 0;

    // These values are intentionally transient.
    THashSet<int> MonitoringDescriptorIndexPool_;
    THashMap<TJobId, TJobMonitoringDescriptor> JobIdToMonitoringDescriptor_;

    std::optional<TUserFile> BaseLayer_;

    TJobExperimentBasePtr JobExperiment_;

    //! One output table can have row_count_limit attribute in operation.
    std::optional<int> RowCountLimitTableIndex;
    i64 RowCountLimit = std::numeric_limits<i64>::max() / 4;

    // Current row count in table with attribute row_count_limit.
    i64 CompletedRowCount_ = 0;

    virtual bool IsTransactionNeeded(ETransactionType type) const;

    TFuture<NApi::NNative::ITransactionPtr> StartTransaction(
        ETransactionType type,
        const NApi::NNative::IClientPtr& client,
        NTransactionClient::TTransactionId parentTransactionId = {});

    TFuture<void> AbortInputTransactions() const;

    void RegisterTask(TTaskPtr task);

    void UpdateTask(TTask* task) override;

    void UpdateAllTasks();

    void ResetTaskLocalityDelays();

    void CheckTimeLimit();

    void CheckAvailableExecNodes();

    bool CheckUserTransactionAlive();

    void FlushOperationNode(bool checkFlushResult);

    void CheckMinNeededResourcesSanity();

    void DoScheduleAllocation(
        TAllocation& allocation,
        const TAllocationSchedulingContext& context,
        const TString& treeId,
        NScheduler::TControllerScheduleAllocationResult* scheduleJobResult);

    void TryScheduleFirstJob(
        TAllocation& allocation,
        const TAllocationSchedulingContext& context,
        NScheduler::TControllerScheduleAllocationResult* scheduleJobResult,
        bool scheduleLocalJob);

    std::optional<EScheduleFailReason> TryScheduleNextJob(TAllocation& allocation, TJobId lastJobId);

    std::optional<EScheduleFailReason> TryScheduleJob(
        TAllocation& allocation,
        TTask& task,
        const TSchedulingContext& context,
        bool scheduleLocalJob,
        std::optional<TJobId> previousJobId);

    TAllocation* FindAllocation(TAllocationId allocationId);
    TJobletPtr FindJoblet(TAllocationId allocationId) const;
    TJobletPtr FindJoblet(TJobId jobId) const;
    TJobletPtr GetJoblet(TJobId jobId) const;
    TJobletPtr GetJoblet(TAllocationId allocationId) const;
    TJobletPtr GetJobletOrThrow(TJobId jobId) const;

    void UnregisterJoblet(const TJobletPtr& joblet);

    std::vector<TAllocationId> GetAllocationIdsByTreeId(const TString& treeId);

    // Initialization.
    virtual void DoInitialize();
    virtual void InitializeClients();
    void InitializeInputTransactions();
    NYTree::IAttributeDictionaryPtr CreateTransactionAttributes(ETransactionType transactionType) const;
    void StartTransactions();
    virtual NTransactionClient::TTransactionId GetInputTransactionParentId();
    virtual NTransactionClient::TTransactionId GetOutputTransactionParentId();
    virtual void InitializeStructures();
    virtual void LockInputs();
    void InitUnrecognizedSpec();
    void FillInitializeResult(TOperationControllerInitializeResult* result);
    void ValidateIntermediateDataAccess(const std::string& user, NYTree::EPermission permission) const;
    void InitUpdatingTables();
    virtual void PrepareInputTables();
    bool HasDiskRequestsWithSpecifiedAccount() const;
    void InitAccountResourceUsageLeases();
    void ValidateSecureVault();

    // Preparation.
    void ValidateInputTablesTypes() const override;
    virtual void ValidateUpdatingTablesTypes() const;
    virtual NObjectClient::EObjectType GetOutputTableDesiredType() const;
    void ValidateOutputDynamicTablesAllowed() const;
    void GetOutputTablesSchema();
    virtual void PrepareOutputTables();
    void PatchTableWriteBuffer(
        NTableClient::TTableWriterOptionsPtr& writerOptions,
        NTableClient::ETableSchemaMode schemaMode,
        const NTableClient::TEpochSchema& schema) const;
    void LockOutputTablesAndGetAttributes();
    void LockUserFiles();
    void GetUserFilesAttributes();
    virtual void CustomPrepare();
    void InferInputRanges();

    // Materialization.
    void FetchUserFiles();
    void DoFetchUserFiles(const NScheduler::TUserJobSpecPtr& userJobSpec, std::vector<TUserFile>& files);
    void ValidateUserFileSizes();
    void PickIntermediateDataCells();
    void InitChunkListPools();
    void SuppressLivePreviewIfNeeded();
    void CreateLivePreviewTables();
    void CollectTotals();
    virtual void CustomMaterialize();
    void InitializeHistograms();
    void InitializeSecurityTags();
    void InitIntermediateChunkScraper();

    //! If auto-merge is not possible for operation, returns error with a reason.
    virtual TError GetAutoMergeError() const;

    //! If auto-merge is needed, init auto-merge tasks and auto-merge director and return true, otherwise return false.
    bool TryInitAutoMerge(int outputChunkCountEstimate);

    //! If fetching chunk slice statistics is not possible for the operation, returns an error with a reason.
    TError GetUseChunkSliceStatisticsError() const override;

    //! Return stream descriptors adjusted according to existing auto-merge tasks.
    std::vector<TOutputStreamDescriptorPtr> GetAutoMergeStreamDescriptors();

    void FillPrepareResult(TOperationControllerPrepareResult* result);

    void ParseInputQuery(
        const TString& queryString,
        const std::optional<NQueryClient::TTableSchema>& schema,
        NScheduler::TQueryFilterOptionsPtr queryFilterOptions);
    void WriteInputQueryToJobSpec(
        NControllerAgent::NProto::TJobSpecExt* jobSpecExt);
    virtual void PrepareInputQuery();

    // Completion.
    void TeleportOutputChunks();
    void BeginUploadOutputTables(const std::vector<TOutputTablePtr>& tables);
    void AttachOutputChunks(const std::vector<TOutputTablePtr>& tableList);
    void EndUploadOutputTables(const std::vector<TOutputTablePtr>& tables);
    void LockOutputDynamicTables();
    void CommitTransactions();
    virtual void CustomCommit();
    void VerifySortedOutput(TOutputTablePtr table);

    void StartOutputCompletionTransaction();
    void CommitOutputCompletionTransaction();
    void ManuallyMergeBranchedCypressNode(
        NCypressClient::TNodeId nodeId,
        NTransactionClient::TTransactionId transactionId);

    void StartDebugCompletionTransaction();
    void CommitDebugCompletionTransaction();

    i64 GetPartSize(EOutputTableType tableType);

    void CommitFeatures();
    void FinalizeFeatures();

    void FinalizeSubscriptions();

    // Revival.
    void ReinstallLivePreview();

    void DoLoadSnapshot(const TOperationSnapshot& snapshot);

    bool InputHasVersionedTables() const;
    bool InputHasReadLimits() const;
    bool InputHasDynamicStores() const;

    bool HasUserJobFilesOrLayers() const;

    bool IsLocalityEnabled() const;

    virtual TString GetLoggingProgress() const;

    //! Called to extract output table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetOutputTablePaths() const = 0;

    const TProgressCounterPtr& GetTotalJobCounter() const override;

    const TScheduleAllocationStatisticsPtr& GetScheduleAllocationStatistics() const override;
    const TAggregatedJobStatistics& GetAggregatedFinishedJobStatistics() const override;
    const TAggregatedJobStatistics& GetAggregatedRunningJobStatistics() const override;

    std::unique_ptr<IHistogram> ComputeFinalPartitionSizeHistogram() const override;

    //! Called before snapshot downloading to check if revival is allowed
    //! (by default checks that fail_on_job_restart is not set).
    virtual void ValidateRevivalAllowed() const;

    //! Called after snapshot downloading to check if revival is allowed
    //! (by default revival is always permitted).
    virtual void ValidateSnapshot() const;

    //! Is called by controller on stage of structure initialization.
    virtual std::vector<NScheduler::TUserJobSpecPtr> GetUserJobSpecs() const;

    //! What to do with intermediate chunks that are not useful any more.
    virtual EIntermediateChunkUnstageMode GetIntermediateChunkUnstageMode() const;

    //! Called to extract stderr table writer config from the spec.
    virtual NTableClient::TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const;

    //! Called to extract core table writer config from the spec.
    virtual NTableClient::TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const;

    //! Is called by controller when chunks are passed to master connector for unstaging.
    virtual void OnChunksReleased(int chunkCount);

    //! Called when a job is unable to read a chunk.
    void OnChunkFailed(NChunkClient::TChunkId chunkId, TJobId jobId);

    //! Gets the list of all intermediate chunks that are not lost.
    THashSet<NChunkClient::TChunkId> GetAliveIntermediateChunks() const;

    //! Called when a job is unable to read an intermediate chunk
    //! (i.e. that is not a part of the input).
    //! Returns false if the chunk was already considered lost.
    bool OnIntermediateChunkUnavailable(NChunkClient::TChunkId chunkId);

    void OnIntermediateChunkAvailable(
        NChunkClient::TChunkId chunkId,
        const NChunkClient::TChunkReplicaWithMediumList& replicas);

    //! Return a pointer to `YsonSerializable` object that represents
    //! the fully typed operation spec which know more than a simple
    //! `TOperationSpecBase::Spec`.
    virtual NYTree::TYsonStructPtr GetTypedSpec() const = 0;

    void ExtractInterruptDescriptor(TCompletedJobSummary& jobSummary, const TJobletPtr& joblet) const;

    bool IsLegacyOutputLivePreviewSupported() const;
    bool IsOutputLivePreviewSupported() const;
    bool IsLegacyIntermediateLivePreviewSupported() const;
    virtual bool IsIntermediateLivePreviewSupported() const;
    virtual TDataFlowGraph::TVertexDescriptor GetOutputLivePreviewVertexDescriptor() const;

    //! Accumulate information about legacy live preview depending on operation type and user intent.
    virtual ELegacyLivePreviewMode GetLegacyOutputLivePreviewMode() const;
    virtual ELegacyLivePreviewMode GetLegacyIntermediateLivePreviewMode() const;
    virtual bool IsInputDataSizeHistogramSupported() const;
    virtual bool AreForeignTablesSupported() const;

    //! Successfully terminates and finalizes the operation.
    /*!
     *  Does it asynchronously to avoid context switches inside #OnJobCompleted, #OnJobFailed, ...
     *  See also YT-19936.
     *  #interrupted flag indicates premature completion and disables standard validations.
     */
    void OnOperationCompleted(bool interrupted) override;

    virtual void OnOperationTimeLimitExceeded();

    bool HasJobUniquenessRequirements() const;
    bool IsJobUniquenessRequired(const TJobletPtr& joblet) const;
    virtual void OnJobUniquenessViolated(TError error);

    void GracefullyFailOperation(TError error);

    bool IsCompleted() const override;

    //! Returns |true| when the controller is prepared.
    /*!
     *  Preparation happens in a controller thread.
     *  The state must not be touched from the control thread
     *  while this function returns |false|.
     */
    bool IsPrepared() const;

    //! Returns |true| as long as the operation is waiting for jobs abort events.
    bool IsFailing() const;

    bool IsFailingByTimeout() const;

    // Unsorted helpers.

    //! Enables verification that the output is sorted.
    virtual bool ShouldVerifySortedOutput() const;

    virtual NChunkPools::TOutputOrderPtr GetOutputOrder() const;

    NChunkClient::EChunkAvailabilityPolicy GetChunkAvailabilityPolicy() const override;

    //! Enables fetching boundary keys for chunk specs.
    bool IsBoundaryKeysFetchEnabled() const override;

    //! Number of currently unavailable input chunks. In case of Sort or Sorted controller, shows
    //! number of unavailable chunks during materialization (fetching samples or chunk slices).
    //! Used for diagnostics only (exported into orchid).
    i64 GetUnavailableInputChunkCount() const override;

    int GetTotalJobCount() const override;

    i64 GetDataSliceCount() const;

    using TInputTableFilter = std::function<bool(const TInputTablePtr& table)>;

    NTableClient::TSortColumns CheckInputTablesSorted(
        const NTableClient::TSortColumns& sortColumns,
        TInputTableFilter inputTableFilter = [](const TInputTablePtr& /*table*/) { return true; });

    static bool CheckKeyColumnsCompatible(
        const NTableClient::TKeyColumns& fullColumns,
        const NTableClient::TKeyColumns& prefixColumns);

    static bool CheckSortColumnsCompatible(
        const NTableClient::TSortColumns& fullColumns,
        const NTableClient::TSortColumns& prefixColumns);

    NApi::ITransactionPtr AttachTransaction(
        NTransactionClient::TTransactionId transactionId,
        const NApi::NNative::IClientPtr& client,
        bool ping = false);

    const NApi::ITransactionPtr GetTransactionForOutputTable(const TOutputTablePtr& table) const;

    void RegisterLivePreviewTable(TString name, const TOutputTablePtr& table);

    void AttachToIntermediateLivePreview(NChunkClient::TInputChunkPtr chunk) override;

    void AttachToLivePreview(
        NChunkClient::TChunkTreeId chunkTreeId,
        NCypressClient::TNodeId tableId);

    void AttachToLivePreview(
        TStringBuf tableName,
        NChunkClient::TInputChunkPtr chunk);

    void AttachToLivePreview(
        TStringBuf tableName,
        const NChunkPools::TChunkStripePtr& stripe);

    void RegisterTeleportChunk(
        NChunkClient::TInputChunkPtr chunk,
        NChunkPools::TChunkStripeKey key,
        int tableIndex) override;

    bool HasEnoughChunkLists(bool isWritingStderrTable, bool isWritingCoreTable);

    std::vector<NChunkClient::TLegacyDataSlicePtr> CollectPrimaryVersionedDataSlices(i64 sliceSize);

    //! Returns the list of all input data slices collected from all primary input tables.
    std::vector<NChunkClient::TLegacyDataSlicePtr> CollectPrimaryInputDataSlices(i64 versionedSliceSize);

    //! Returns the list of lists of all input chunks collected from all foreign input tables.
    std::vector<std::deque<NChunkClient::TLegacyDataSlicePtr>> CollectForeignInputDataSlices(int foreignKeyColumnCount) const;

    virtual void InitUserJobSpec(
        NControllerAgent::NProto::TUserJobSpec* proto,
        const TJobletPtr& joblet) const;

    void AddStderrOutputSpecs(
        NControllerAgent::NProto::TUserJobSpec* jobSpec,
        TJobletPtr joblet) const;

    void AddCoreOutputSpecs(
        NControllerAgent::NProto::TUserJobSpec* jobSpec,
        TJobletPtr joblet) const;

    // Amount of memory reserved for output table writers in job proxy.
    i64 GetFinalOutputIOMemorySize(
        NScheduler::TJobIOConfigPtr ioConfig,
        bool useEstimatedBufferSize) const;

    i64 GetFinalIOMemorySize(
        NScheduler::TJobIOConfigPtr ioConfig,
        bool useEstimatedBufferSize,
        const NTableClient::TChunkStripeStatisticsVector& stripeStatistics) const;

    void ValidateUserFileCount(NScheduler::TUserJobSpecPtr spec, const TString& operation);

    const TExecNodeDescriptorMap& GetExecNodeDescriptors();
    const TExecNodeDescriptorMap& GetOnlineExecNodeDescriptors();

    void UpdateExecNodes();

    void InferSchemaFromInput(const NTableClient::TSortColumns& sortColumns = NTableClient::TSortColumns());
    void InferSchemaFromInputOrdered();
    void FilterOutputSchemaByInputColumnSelectors(const NTableClient::TSortColumns& sortColumns);
    void ValidateOutputSchemaOrdered() const;
    void ValidateOutputSchemaCompatibility(NTableClient::TTableSchemaCompatibilityOptions options) const;
    // Validate that ESchemaInferenceMode::Auto is used when output table is dynamic.
    void ValidateSchemaInferenceMode(NScheduler::ESchemaInferenceMode schemaInferenceMode) const;
    void ValidateOutputSchemaComputedColumnsCompatibility() const;

    virtual void BuildPrepareAttributes(NYTree::TFluentMap fluent) const;
    virtual void BuildBriefSpec(NYTree::TFluentMap fluent) const;

    // |true| iff operation was failed.
    bool CheckGracefullyAbortedJobsStatusReceived();

    const std::vector<TOutputStreamDescriptorPtr>& GetStandardStreamDescriptors() const override;

    NTableClient::TTableWriterOptionsPtr GetIntermediateTableWriterOptions() const;
    TOutputStreamDescriptorPtr GetIntermediateStreamDescriptorTemplate() const;

    const TDataFlowGraphPtr& GetDataFlowGraph() const override;

    void RegisterLivePreviewChunk(
        const TDataFlowGraph::TVertexDescriptor& vertexDescriptor,
        int index,
        const NChunkClient::TInputChunkPtr& chunk) override;

    const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() const override;

    void FinishTaskInput(const TTaskPtr& task);

    void AbortAllJoblets(EAbortReason abortReason, bool honestly);

    void InitInputStreamDirectory();
    const NChunkPools::TInputStreamDirectory& GetInputStreamDirectory() const;

    int GetPrimaryInputTableCount() const;

    virtual void InitOutputTables();

    const NChunkPools::IPersistentChunkPoolInputPtr& GetSink();

    void ValidateAccountPermission(const TString& account, NYTree::EPermission permission) const;

    int GetYsonNestingLevelLimit() const;

    template <typename T>
    NYson::TYsonString ConvertToYsonStringNestingLimited(const T& value) const;

    i64 GetFastIntermediateMediumLimit() const;

    virtual void DoFailOperation(const TError& error, bool flush = true, bool abortAllJoblets = true);

    virtual bool OnJobCompleted(TJobletPtr joblet, std::unique_ptr<TCompletedJobSummary> jobSummary);
    virtual bool OnJobFailed(TJobletPtr joblet, std::unique_ptr<TFailedJobSummary> jobSummary);
    virtual bool OnJobAborted(TJobletPtr joblet, std::unique_ptr<TAbortedJobSummary> jobSummary);

    virtual void OnOperationRevived();

    virtual void BuildControllerInfoYson(NYTree::TFluentMap fluent) const;

private:
    NScheduler::TPoolTreeControllerSettingsMap PoolTreeControllerSettingsMap_;
    std::optional<std::vector<TString>> OffloadingPoolTrees_;

    THashSet<TString> BannedTreeIds_;

    TOperationSpecBasePtr Spec_;
    TOperationOptionsPtr Options;

    NObjectClient::TCellTagList IntermediateOutputCellTagList;
    TChunkListPoolPtr OutputChunkListPool_;
    TChunkListPoolPtr DebugChunkListPool_;
    THashMap<NObjectClient::TCellTag, int> CellTagToRequiredOutputChunkListCount_;
    THashMap<NObjectClient::TCellTag, int> CellTagToRequiredDebugChunkListCount_;

    NThreading::TAtomicObject<TCompositePendingJobCount> CachedPendingJobCount = {};
    int CachedTotalJobCount = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, OverrunWriteBufferMemoryPerJobLock);
    std::set<TOverrunTableWriteBufferMemoryInfo> OverrunWriteBufferMemoryPerJob;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CachedNeededResourcesLock);
    NScheduler::TCompositeNeededResources CachedNeededResources;

    NThreading::TAtomicObject<NScheduler::TAllocationGroupResourcesMap> CachedGroupedNeededResources;

    NScheduler::TAllocationGroupResourcesMap InitialGroupedNeededResources_;

    class TCachedYsonCallback
    {
    public:
        using TCallback = TCallback<NYson::TYsonString()>;

        DEFINE_BYVAL_RW_PROPERTY(TDuration, UpdatePeriod);

        TCachedYsonCallback(TDuration period, TCallback callback);

        const NYson::TYsonString& GetValue();

    private:
        const TCallback Callback_;

        TInstant UpdateTime_ = TInstant::Zero();
        NYson::TYsonString Value_;
    };

    mutable TCachedYsonCallback CachedRunningJobs_;

    NYson::TYsonString DoBuildJobsYson();

    NYson::TYsonString CachedSuspiciousJobsYson_ = NYson::TYsonString(TStringBuf(), NYson::EYsonType::MapFragment);
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CachedSuspiciousJobsYsonLock_);
    NConcurrency::TPeriodicExecutorPtr SuspiciousJobsYsonUpdater_;

    //! Maps an intermediate chunk id to its originating completed job.
    THashMap<NChunkClient::TChunkId, TCompletedJobPtr> ChunkOriginMap;

    TIntermediateChunkScraperPtr IntermediateChunkScraper;

    //! Scrapes chunks of dynamic tables during data slice fetching.
    std::vector<NChunkClient::IFetcherChunkScraperPtr> DataSliceFetcherChunkScrapers;

    NProfiling::TCpuInstant TaskUpdateDeadline_ = 0;

    //! Increments each time a new job is scheduled.
    TIdGenerator JobIndexGenerator;

    TAggregatedJobStatistics AggregatedRunningJobStatistics_;
    NConcurrency::TPeriodicExecutorPtr RunningJobStatisticsUpdateExecutor_;

    TAggregatedJobStatistics AggregatedFinishedJobStatistics_;

    //! Records peak memory usage.
    i64 PeakMemoryUsage_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, JobMetricsDeltaPerTreeLock_);
    //! Delta of job metrics that was not reported to scheduler.
    THashMap<TString, NScheduler::TJobMetrics> JobMetricsDeltaPerTree_;
    // NB(eshcherbin): this is very ad-hoc and hopefully temporary. We need to get the total time
    // per tree in the end of the operation, however, (1) job metrics are sent as deltas and
    // are not accumulated, and (2) job statistics don't provide per tree granularity.
    //! Aggregated total time of jobs per tree.
    THashMap<TString, i64> TotalTimePerTree_;
    THashMap<TString, i64> MainResourceConsumptionPerTree_;
    NProfiling::TCpuInstant LastJobMetricsDeltaReportTime_ = 0;

    //! Aggregated schedule job statistics.
    mutable TScheduleAllocationStatisticsPtr ScheduleAllocationStatistics_;

    //! Deadline after which schedule job statistics can be logged.
    NProfiling::TCpuInstant ScheduleAllocationStatisticsLogDeadline_ = 0;

    //! Runs periodic time limit checks that fail operation on timeout.
    NConcurrency::TPeriodicExecutorPtr CheckTimeLimitExecutor;

    //! Runs periodic checks to verify that compatible nodes are present in the cluster.
    NConcurrency::TPeriodicExecutorPtr ExecNodesCheckExecutor;

    //! Periodically checks operation progress and registers operation alerts if necessary.
    IAlertManagerPtr AlertManager_;

    //! Periodically checks min needed resources of tasks for sanity.
    NConcurrency::TPeriodicExecutorPtr MinNeededResourcesSanityCheckExecutor;

    //! Periodically checks operation controller memory usage.
    //! If memory usage exceeds the limit, operation fails.
    NConcurrency::TPeriodicExecutorPtr PeakMemoryUsageUpdateExecutor;

    //! Periodically updates various info about exec nodes.
    NConcurrency::TPeriodicExecutorPtr ExecNodesUpdateExecutor;

    //! Exec node count do not consider schedufling tag.
    //! But descriptors do.
    int AvailableExecNodeCount_ = 0;
    TRefCountedExecNodeDescriptorMapPtr ExecNodesDescriptors_ = New<NScheduler::TRefCountedExecNodeDescriptorMap>();
    TRefCountedExecNodeDescriptorMapPtr OnlineExecNodesDescriptors_ = New<NScheduler::TRefCountedExecNodeDescriptorMap>();

    std::optional<TJobResources> CachedMaxAvailableExecNodeResources_;

    const std::unique_ptr<NYson::IYsonConsumer> EventLogConsumer_;

    std::unique_ptr<IHistogram> EstimatedInputDataSizeHistogram_;
    std::unique_ptr<IHistogram> InputDataSizeHistogram_;

    const NProfiling::TCpuDuration LogProgressBackoff;
    NProfiling::TCpuInstant NextLogProgressDeadline = 0;

    std::atomic<bool> ShouldUpdateProgressAttributesInCypress_ = true;
    NYson::TYsonString ProgressString_;
    NYson::TYsonString BriefProgressString_;

    std::vector<TOutputStreamDescriptorPtr> StandardStreamDescriptors_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ProgressLock_);
    const NConcurrency::TPeriodicExecutorPtr ProgressBuildExecutor_;

    const NConcurrency::TPeriodicExecutorPtr CheckTentativeTreeEligibilityExecutor_;

    const NChunkClient::TMediumDirectoryPtr MediumDirectory_;

    std::atomic<int> RetainedJobWithStderrCount_ = 0;
    int RetainedJobsCoreInfoCount_ = 0;
    int RetainedJobCount_ = 0;
    int JobSpecCompletedArchiveCount_ = 0;

    std::atomic<int> FailedJobCount_ = 0;
    std::atomic<bool> ShouldUpdateLightOperationAttributes_ = false;

    int UnknownExitCodeFailCount_ = 0;
    int NoExitCodeFailCount_ = 0;
    THashMap<int, int> FailCountsPerKnownExitCode_;

    // Release job flags to be sent to scheduler in EAgentToSchedulerJobEventType::Released.
    THashMap<TJobId, TReleaseJobFlags> JobIdToReleaseFlags_;
    std::vector<std::pair<TJobId, NYson::TYsonString>> RetainedFinishedJobs_;

    NChunkPools::IPersistentChunkPoolInputPtr Sink_;

    std::unique_ptr<TAutoMergeDirector> AutoMergeDirector_;

    //! Release queue of job ids that were completed after the latest snapshot was built.
    //! It is a transient field.
    NServer::TReleaseQueue<TJobId> CompletedJobIdsReleaseQueue_;

    //! Cookie corresponding to a state of the completed job ids release queue
    //! by the moment the most recent snapshot started to be built.
    NServer::TReleaseQueue<TJobId>::TCookie CompletedJobIdsSnapshotCookie_ = 0;

    //! Release queue of chunk stripe lists that are no longer needed by a controller.
    //! Similar to the previous field.
    NServer::TReleaseQueue<NChunkPools::TChunkStripeListPtr> IntermediateStripeListReleaseQueue_;
    NServer::TReleaseQueue<NChunkPools::TChunkStripeListPtr>::TCookie IntermediateStripeListSnapshotCookie_ = 0;

    //! Release queue of chunk trees that should be released, but the corresponding
    //! node does not know yet about their invalidation.
    /* It may happen (presumably) in two situations:
     *  - Abandoned completed jobs.
     *  - Jobs aborted by confirmation timeout during the revival.
     */
    NServer::TReleaseQueue<NChunkClient::TChunkTreeId> ChunkTreeReleaseQueue_;
    NServer::TReleaseQueue<NChunkClient::TChunkTreeId>::TCookie ChunkTreeSnapshotCookie_ = 0;

    //! Number of times `OnSnapshotStarted()` was called up to this moment.
    std::atomic<int> SnapshotIndex_ = 0;
    //! Index of a snapshot that is building right now.
    std::optional<int> RecentSnapshotIndex_ = std::nullopt;
    //! Timestamp of last successful uploaded snapshot.
    TInstant LastSuccessfulSnapshotTime_ = TInstant::Zero();

    bool AvailableExecNodesObserved_ = false;
    TInstant LastAvailableExecNodesCheckTime_;

    mutable std::atomic<TInstant> LastControllerJobSchedulingThrottlingLogTime_ = TInstant::Zero();
    mutable std::atomic<TInstant> LastControllerJobEventThrottlingLogTime_ = TInstant::Zero();

    THashSet<NNodeTrackerClient::TNodeId> BannedNodeIds_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, AlertsLock_);
    TOperationAlertMap Alerts_;

    bool IsLegacyLivePreviewSuppressed = false;

    std::atomic<bool> MemoryLimitExceeded_ = false;

    //! Error that lead to operation failure.
    TError Error_;

    bool OperationTimedOut_ = false;

    // Used for testing purposes.
    bool CommitSleepStarted_ = false;

    //! Schedule job failures that happened outside of controller.
    //! These values are added to corresponding values in ScheduleAllocationStatistics_
    //! on each access in thread-safe manner.
    mutable TEnumIndexedArray<EScheduleFailReason, std::atomic<int>> ExternalScheduleAllocationFailureCounts_;

    TInstant FinishTime_;
    std::vector<NScheduler::TExperimentAssignmentPtr> ExperimentAssignments_;

    struct TLivePreviewChunkDescriptor
    {
        TDataFlowGraph::TVertexDescriptor VertexDescriptor;
        int LivePreviewIndex = -1;

        PHOENIX_DECLARE_TYPE(TLivePreviewChunkDescriptor, 0x9c27bce6);
    };

    THashMap<NChunkClient::TInputChunkPtr, TLivePreviewChunkDescriptor> LivePreviewChunks_;

    bool EnableMasterResourceUsageAccounting_ = false;
    struct TResourceUsageLeaseInfo
    {
        NSecurityClient::TAccountResourceUsageLeaseId LeaseId;
        NScheduler::TDiskQuota DiskQuota;

        PHOENIX_DECLARE_TYPE(TResourceUsageLeaseInfo, 0xcbbf64d9);
    };
    THashMap<TString, TResourceUsageLeaseInfo> AccountResourceUsageLeaseMap_;

    THashMap<TString, TResourceUsageLeaseInfo> LastUpdatedAccountResourceUsageLeaseMap_;

    const NConcurrency::TPeriodicExecutorPtr UpdateAccountResourceUsageLeasesExecutor_;

    TProgressCounterPtr TotalJobCounter_;

    //! Size of allocation will produced by method TestHeap().
    mutable std::atomic<i64> TestingAllocationSize_;
    //! Duration of storing allocations on heap.
    std::optional<TDuration> AllocationReleaseDelay_;

    //! Per transaction intermediate data weight limit for the fast medium (SSD)
    //! in the public intermediate account.
    i64 FastIntermediateMediumLimit_ = 0;

    THashMap<TAllocationId, TInstant> RunningAllocationPreemptibleProgressStartTimes_;

    const NConcurrency::TPeriodicExecutorPtr SendRunningAllocationTimeStatisticsUpdatesExecutor_;

    //! How many initial successive jobs need to abort until we fail operation.
    THashMap<EAbortReason, int> JobAbortsUntilOperationFailure_;

    //! GracefulAbort sends requests all jobs for graceful abort
    //! transitioning operation state to Failing.
    //! Each job (whether it is reported as aborted or failed)
    //! will be observed again as finished one
    //! where it has to report itself so that once this
    //! set is empty, we cancel timeout and actually fail
    //! the operation.
    TError OperationFailError_;
    NConcurrency::TDelayedExecutorCookie GracefulAbortTimeoutFailureCookie_;

    void AccountExternalScheduleAllocationFailures() const;

    void InitializeOrchid();

    void UpdateActualHistogram(const TCompletedJobSummary& jobSummary);

    void CreateOutputTables(
        const NApi::NNative::IClientPtr& client,
        const std::vector<NChunkClient::TUserObject*>& tables,
        NTransactionClient::TTransactionId defaultTransactionId,
        EOutputTableType outputTableType,
        NCypressClient::EObjectType desiredType);

    virtual void OnExecNodesUpdated();

    int GetAvailableExecNodeCount();

    void UpdateAggregatedFinishedJobStatistics(const TJobletPtr& joblet, const TJobSummary& jobSummary);
    void UpdateJobMetrics(const TJobletPtr& joblet, const TJobSummary& jobSummary, bool isJobFinished);

    void LogProgress(bool force = false);

    void UpdateAllTasksIfNeeded();

    void IncreaseNeededResources(const NScheduler::TCompositeNeededResources& resourcesDelta);

    void IncreaseAccountResourceUsageLease(const std::optional<TString>& account, const NScheduler::TDiskQuota& quota);

    void UpdateAccountResourceUsageLeases();

    void InitializeStandardStreamDescriptors();

    void AddChunksToUnstageList(std::vector<NChunkClient::TInputChunkPtr> chunks);

    NScheduler::TControllerTransactionIds GetTransactionIds();

    std::optional<TDuration> GetTimeLimit() const;
    TError GetTimeLimitError() const;

    NEventLog::TFluentLogEvent LogFinishedJobFluently(
        NScheduler::ELogEventType eventType,
        const TJobletPtr& joblet);

    NYson::IYsonConsumer* GetEventLogConsumer() override;
    const NLogging::TLogger* GetEventLogger() override;

    void SleepInCommitStage(NScheduler::EDelayInsideOperationCommitStage desiredStage);
    void SleepInRevive();
    void SleepInPrepare();
    void SleepInInitialize();

    //! An internal helper for invoking OnOperationFailed with an error
    //! built by data from `ex`.
    void ProcessSafeException(const TAssertionFailedException& ex);
    void ProcessSafeException(const std::exception& ex);

    void BuildMemoryUsageYson(NYTree::TFluentAny fluent) const;
    void BuildStateYson(NYTree::TFluentAny fluent) const;

    void BuildTestingState(NYTree::TFluentAny fluent) const;

    void OnJobFinished(std::unique_ptr<TJobSummary> summary, bool suggestCreateJobNodeByStatus);

    void ProcessJobFinishedResult(const TJobFinishedResult& result);

    void BuildJobAttributes(
        const TJobletPtr& joblet,
        EJobState state,
        i64 stderrSize,
        NYTree::TFluentMap fluent) const;

    void BuildFinishedJobAttributes(
        const TJobletPtr& joblet,
        TJobSummary* jobSummary,
        bool hasStderr,
        bool hasFailContext,
        NYTree::TFluentMap fluent) const;

    void AnalyzeBriefStatistics(
        const TJobletPtr& joblet,
        const TSuspiciousJobsOptionsPtr& options,
        const TErrorOr<TBriefJobStatisticsPtr>& briefStatisticsOrError);

    void UpdateSuspiciousJobsYson();

    void UpdateAggregatedRunningJobStatistics();

    void CheckTentativeTreeEligibility();

    void ReleaseJobs(const std::vector<TJobId>& jobIds);

    bool IsIdleCpuPolicyAllowedInTree(const TString& treeId) const override;
    bool IsTreeTentative(const TString& treeId) const;
    bool IsTreeProbing(const TString& treeId) const override;
    void MaybeBanInTentativeTree(const TString& treeId);

    void RegisterTestingSpeculativeJobIfNeeded(TTask& task, TAllocationId allocationId);

    std::vector<NYPath::TRichYPath> GetLayerPaths(const NScheduler::TUserJobSpecPtr& userJobSpec) const;

    void MaybeCancel(NScheduler::ECancelationStage cancelationStage) override;
    const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() const override;

    void HandleJobReport(const TJobletPtr& joblet, TControllerJobReport&& jobReport) const;

    void ReportJobHasCompetitors(const TJobletPtr& joblet, EJobCompetitionType competitionType);

    //! Returns list of operation tasks that have a vertex in data flow graph,
    //! ordered according to topological order of data flow graph.
    std::vector<TTaskPtr> GetTopologicallyOrderedTasks() const;

    void UpdatePeakMemoryUsage();

    void BuildFeatureYson(NYTree::TFluentAny fluent) const;

    void UpdateRunningJobStatistics(TJobletPtr joblet, std::unique_ptr<TRunningJobSummary> jobStatus);

    NYTree::IYPathServicePtr BuildZombieOrchid();

    void OnJobRunning(const TJobletPtr& joblet, std::unique_ptr<TRunningJobSummary> jobSummary);
    bool WasJobGracefullyAborted(const std::unique_ptr<TAbortedJobSummary>& jobSummary);
    void OnJobStartTimeReceived(const TJobletPtr& joblet, const std::unique_ptr<TRunningJobSummary>& jobSummary);

    void ReportJobCookieToArchive(const TJobletPtr& joblet) const;
    void ReportControllerStateToArchive(const TJobletPtr& joblet, EJobState state) const;
    void ReportOperationIncarnationToArchive(const TJobletPtr& joblet) const;

    std::unique_ptr<TAbortedJobSummary> RegisterOutputChunkReplicas(
        const TJobSummary& jobSummary,
        const NChunkClient::NProto::TChunkSpec& chunkSpec);

    friend class TSink;

    TControllerFeatures ControllerFeatures_;

    bool NeedEraseOffloadingTrees() const;

    void SendRunningAllocationTimeStatisticsUpdates();

    void RemoveRemainingJobsOnOperationFinished();

    void DoAbortJob(
        TJobletPtr joblet,
        EAbortReason abortReason,
        bool requestJobTrackerJobAbortion);

    void OnOperationReady() const;

    bool ShouldProcessJobEvents() const;

    void InterruptJob(TJobId jobId, EInterruptionReason interruptionReason, TDuration timeout);

    void ClearEmptyAllocationsInRevive();

    bool IsJobIdEarlier(TJobId lhs, TJobId rhs) const noexcept;
    TJobId GetLaterJobId(TJobId lhs, TJobId rhs) const noexcept;

    template <class TAllocationEvent>
    void ProcessAllocationEvent(TAllocationEvent&& eventSummary, TStringBuf eventType);

    // Returns nullptr if corresponding feature-flag is |false|.
    NScheduler::TJobFailsTolerancePtr GetJobFailsTolerance() const;
    bool IsExitCodeKnown(int exitCode) const;
    int GetMaxJobFailCountForExitCode(std::optional<int> maybeExitCode);
    bool IsJobsFailToleranceExceeded(std::optional<int> maybeExitCode);
    void UpdateFailedJobsExitCodeCounters(std::optional<int> maybeExitCode);

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TOperationControllerBase, 0x6715254c);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
