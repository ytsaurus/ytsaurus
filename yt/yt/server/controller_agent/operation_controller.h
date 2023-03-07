#pragma once

#include "public.h"

#include <yt/server/lib/job_agent/public.h>

#include <yt/server/lib/scheduler/structs.h>
#include <yt/server/lib/scheduler/job_metrics.h>

#include <yt/server/lib/controller_agent/structs.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/event_log/public.h>

#include <yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/actions/future.h>
#include <yt/core/actions/invoker_pool.h>

#include <yt/core/misc/error.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TControllerTransactionIds
{
    NTransactionClient::TTransactionId AsyncId;
    NTransactionClient::TTransactionId InputId;
    NTransactionClient::TTransactionId OutputId;
    NTransactionClient::TTransactionId DebugId;
    NTransactionClient::TTransactionId OutputCompletionId;
    NTransactionClient::TTransactionId DebugCompletionId;
    std::vector<NTransactionClient::TTransactionId> NestedInputIds;
};

void ToProto(NProto::TControllerTransactionIds* transactionIdsProto, const NControllerAgent::TControllerTransactionIds& transactionIds);
void FromProto(NControllerAgent::TControllerTransactionIds* transactionIds, const NProto::TControllerTransactionIds& transactionIdsProto);

////////////////////////////////////////////////////////////////////////////////

using TOperationAlertMap = THashMap<EOperationAlertType, TError>;

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerInitializeResult
{
    TControllerTransactionIds TransactionIds;
    NScheduler::TOperationControllerInitializeAttributes Attributes;
};

void ToProto(NProto::TInitializeOperationResult* resultProto, const TOperationControllerInitializeResult& result);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerPrepareResult
{
    NYson::TYsonString Attributes;
};

void ToProto(NProto::TPrepareOperationResult* resultProto, const TOperationControllerPrepareResult& result);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerMaterializeResult
{
    bool Suspend = false;
    TJobResources InitialNeededResources;
};

void ToProto(NProto::TMaterializeOperationResult* resultProto, const TOperationControllerMaterializeResult& result);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerReviveResult
    : public TOperationControllerPrepareResult
{
    struct TRevivedJob
    {
        TJobId JobId;
        EJobType JobType;
        TInstant StartTime;
        TJobResources ResourceLimits;
        bool Interruptible;
        TString TreeId;
        NNodeTrackerClient::TNodeId NodeId;
        TString NodeAddress;
    };

    bool RevivedFromSnapshot = false;
    std::vector<TRevivedJob> RevivedJobs;
    THashSet<TString> RevivedBannedTreeIds;
    TJobResources NeededResources;
};

void ToProto(NProto::TReviveOperationResult* resultProto, const TOperationControllerReviveResult& result);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerCommitResult
{
};

void ToProto(NProto::TCommitOperationResult* resultProto, const TOperationControllerCommitResult& result);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerUnregisterResult
{
    NScheduler::TOperationJobMetrics ResidualJobMetrics;
};

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotCookie
{
    int SnapshotIndex = -1;
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationSnapshot
{
    int Version = -1;
    std::vector<TSharedRef> Blocks;
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateJobNodeRequest
{
    TJobId JobId;
    NYson::TYsonString Attributes;
    NChunkClient::TChunkId StderrChunkId;
    NChunkClient::TChunkId FailContextChunkId;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: Cancelable controller invoker
 */
struct IOperationControllerHost
    : public virtual TRefCounted
{
    virtual void InterruptJob(TJobId jobId, EInterruptReason reason) = 0;
    virtual void AbortJob(TJobId jobId, const TError& error) = 0;
    virtual void FailJob(TJobId jobId) = 0;
    virtual void ReleaseJobs(const std::vector<NJobTrackerClient::TJobToRelease>& jobsToRelease) = 0;

    virtual TFuture<TOperationSnapshot> DownloadSnapshot() = 0;
    virtual TFuture<void> RemoveSnapshot() = 0;

    virtual TFuture<void> FlushOperationNode() = 0;
    virtual TFuture<void> UpdateInitializedOperationNode() = 0;
    virtual void CreateJobNode(const TCreateJobNodeRequest& request) = 0;

    virtual TFuture<void> AttachChunkTreesToLivePreview(
        NTransactionClient::TTransactionId transactionId,
        NCypressClient::TNodeId tableId,
        const std::vector<NChunkClient::TChunkTreeId>& childIds) = 0;
    virtual void AddChunkTreesToUnstageList(
        const std::vector<NChunkClient::TChunkId>& chunkTreeIds,
        bool recursive) = 0;

    virtual const NApi::NNative::IClientPtr& GetClient() = 0;
    virtual const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() = 0;
    virtual const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() = 0;
    virtual const IInvokerPtr& GetControllerThreadPoolInvoker() = 0;
    virtual const NEventLog::IEventLogWriterPtr& GetEventLogWriter() = 0;
    virtual const ICoreDumperPtr& GetCoreDumper() = 0;
    virtual const NConcurrency::TAsyncSemaphorePtr& GetCoreSemaphore() = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() = 0;
    virtual const NJobAgent::TJobReporterPtr& GetJobReporter() = 0;
    virtual const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() = 0;
    virtual TMemoryTagQueue* GetMemoryTagQueue() = 0;

    virtual int GetOnlineExecNodeCount() = 0;
    virtual TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors(const NScheduler::TSchedulingTagFilter& filter, bool onlineOnly = false) = 0;
    virtual TInstant GetConnectionTime() = 0;
    virtual NScheduler::TIncarnationId GetIncarnationId() = 0;

    virtual void OnOperationCompleted() = 0;
    virtual void OnOperationAborted(const TError& error) = 0;
    virtual void OnOperationFailed(const TError& error) = 0;
    virtual void OnOperationSuspended(const TError& error) = 0;
    virtual void OnOperationBannedInTentativeTree(const TString& treeId, const std::vector<TJobId>& jobIds) = 0;

    virtual void ValidateOperationAccess(
        const TString& user,
        NYTree::EPermission permission) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerHost)

////////////////////////////////////////////////////////////////////////////////

struct IOperationControllerSchedulerHost
    : public virtual TRefCounted
{
    //! Performs controller internal state initialization. Starts all controller transactions.
    /*
     *  If an exception is thrown then the operation fails immediately.
     *  The diagnostics is returned to the client, no Cypress node is created.
     *
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual TOperationControllerInitializeResult InitializeClean() = 0;

    //! Performs controller inner state initialization for reviving operation.
    /*
     *  If an exception is thrown then the operation fails immediately.
     *
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual TOperationControllerInitializeResult InitializeReviving(const TControllerTransactionIds& transactions) = 0;

    //! Performs a lightweight initial preparation.
    /*!
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual TOperationControllerPrepareResult Prepare() = 0;

    //! Performs a possibly lengthy materialization.
    /*!
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual TOperationControllerMaterializeResult Materialize() = 0;

    //! Reactivates an already running operation, possibly restoring its progress.
    /*!
     *  This method is called during scheduler state recovery for each existing operation.
     *  Must be called after InitializeReviving().
     *
     *  \note Invoker affinity: cancelable Controller invoker
     *
     */
    virtual TOperationControllerReviveResult Revive() = 0;

    //! Called by a scheduler in operation complete pipeline.
    /*!
     *  The controller must commit the transactions related to the operation.
     *
     *  \note Invoker affinity: cancelable Controller invoker
     *
     */
    virtual void Commit() = 0;

    //! Notifies the controller that the operation has been terminated (i.e. it failed or was aborted).
    /*!
     *  All jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     *
     *  \note Invoker affinity: Controller invoker
     *
     */
    virtual void Terminate(EControllerState finalState) = 0;

    //! Notifies the controller that the operation has been completed.
    /*!
     *  All running jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     *
     *  \note Invoker affinity: cancelable Controller invoker
     *
     */
    virtual void Complete() = 0;

    /*!
     *  Returns the operation controller invoker with index #queue.
     *  Most of const controller methods are expected to be run in the provided invokers.
     */
    virtual IInvokerPtr GetInvoker(EOperationControllerQueue queue = EOperationControllerQueue::Default) const = 0;

    //! Called in the end of heartbeat when scheduler agrees to run operation job.
    /*!
     *  \note Invoker affinity: cancellable Controller invoker
     */
    virtual void OnJobStarted(std::unique_ptr<TStartedJobSummary> jobSummary) = 0;

    //! Called during heartbeat processing to notify the controller that a job has completed.
    /*!
     *  \note Invoker affinity: cancellable Controller invoker
     */
    virtual void OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary) = 0;

    //! Called during heartbeat processing to notify the controller that a job has failed.
    /*!
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual void OnJobFailed(std::unique_ptr<TFailedJobSummary> jobSummary) = 0;

    //! Called during preemption to notify the controller that a job has been aborted.
    /*!
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual void OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary, bool byScheduler) = 0;

    //! Called during heartbeat processing to notify the controller that a job is still running.
    /*!
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual void OnJobRunning(std::unique_ptr<TRunningJobSummary> jobSummary) = 0;

    //! Method that is called after operation results are committed and before
    //! controller is disposed.
    /*!
     *  \note Invoker affinity: Controller invoker.
     */
    virtual void Dispose() = 0;

    //! Updates runtime parameters.
    virtual void UpdateRuntimeParameters(const NScheduler::TOperationRuntimeParametersUpdatePtr& runtimeParameters) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerSchedulerHost)

////////////////////////////////////////////////////////////////////////////////

struct IOperationControllerSnapshotBuilderHost
    : public virtual TRefCounted
{
    //! Returns |true| as long as the operation can schedule new jobs.
    /*!
     *  \note Invoker affinity: any.
     */
    virtual bool IsRunning() const = 0;

    //! Returns the context that gets invalidated by #Abort and #Cancel.
    virtual TCancelableContextPtr GetCancelableContext() const = 0;

    /*!
     *  Returns the operation controller invoker with index #queue.
     *  Most of const controller methods are expected to be run in the provided invokers.
     */
    virtual IInvokerPtr GetInvoker(EOperationControllerQueue queue = EOperationControllerQueue::Default) const = 0;

    /*!
     *  Returns the operation controller invoker with index #queue wrapped by the context provided by #GetCancelableContext.
     *  Most of non-const controller methods are expected to be run in the provided invokers.
     */
    virtual IInvokerPtr GetCancelableInvoker(EOperationControllerQueue queue = EOperationControllerQueue::Default) const = 0;

    //! Called right before the controller is suspended and snapshot builder forks.
    //! Returns a certain opaque cookie.
    //! This method should not throw.
    /*!
     *  \note Invoker affinity: Controller invoker.
     */
    virtual TSnapshotCookie OnSnapshotStarted() = 0;

    //! Method that is called right after each snapshot is uploaded.
    //! #cookie must be equal to the result of the last #OnSnapshotStarted call.
    /*!
     *  \note Invoker affinity: Cancellable controller invoker.
     */
    virtual void OnSnapshotCompleted(const TSnapshotCookie& cookie) = 0;

    //! Suspends controller invoker and returns future that is set after last action in invoker is executed.
    /*!
     *  \note Invoker affinity: Control invoker
     */
    virtual TFuture<void> Suspend() = 0;

    //! Resumes execution in controller invoker.
    /*!
     *  \note Invoker affinity: Control invoker
     */
    virtual void Resume() = 0;

    //! Called from a forked copy of the scheduler to make a snapshot of operation's progress.
    /*!
     *  \note Invoker affinity: Control invoker in forked state.
     */
    virtual void SaveSnapshot(IOutputStream* stream) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerSnapshotBuilderHost)

////////////////////////////////////////////////////////////////////////////////

struct TOperationInfo
{
    NYson::TYsonString Progress;
    NYson::TYsonString BriefProgress;
    NYson::TYsonString Alerts;
    NYson::TYsonString RunningJobs;
    NYson::TYsonString JobSplitter;
    ssize_t MemoryUsage;
    EControllerState ControllerState;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Invoker affinity: Controller invoker
 */
struct IOperationController
    : public IOperationControllerSchedulerHost
    , public IOperationControllerSnapshotBuilderHost
{
    virtual IInvokerPtr GetInvoker(EOperationControllerQueue queue = EOperationControllerQueue::Default) const = 0;
    virtual IInvokerPtr GetCancelableInvoker(EOperationControllerQueue queue = EOperationControllerQueue::Default) const = 0;
    virtual IDiagnosableInvokerPool::TInvokerStatistics GetInvokerStatistics(
        EOperationControllerQueue queue = EOperationControllerQueue::Default) const = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to request actions the node must perform.
    virtual NScheduler::TControllerScheduleJobResultPtr ScheduleJob(
        ISchedulingContext* context,
        const NScheduler::TJobResourcesWithQuota& jobLimits,
        const TString& treeId) = 0;

    //! Returns the total resources that are additionally needed.
    /*!
     *  \note Thread affinity: any
     */
    virtual TJobResources GetNeededResources() const = 0;

    //! Initiates updating min needed resources estimates.
    //! Note that the actual update may happen in background.
    /*!
     *  \note Thread affinity: any
     */
    virtual void UpdateMinNeededJobResources() = 0;

    //! Returns the cached min needed resources estimate.
    /*!
     *  \note Thread affinity: any
     */
    virtual NScheduler::TJobResourcesWithQuotaList GetMinNeededJobResources() const = 0;

    //! Returns the number of jobs the controller is able to start right away.
    /*!
     *  \note Thread affinity: any
     */
    virtual int GetPendingJobCount() const = 0;

    //! Invokes controller finalization due to aborted or expired transaction.
    virtual void OnTransactionsAborted(const std::vector<NTransactionClient::TTransactionId>& transactionIds) = 0;

    //! Cancels the controller context
    /*!
     *  \note Invoker affinity: any
     */
    virtual void Cancel() = 0;

    //! Marks that progress was dumped to Cypress.
    /*!
     *  \note Invoker affinity: any.
     */
    virtual void SetProgressUpdated() = 0;

    //! Check that progress has changed and should be dumped to the Cypress.
    /*!
     *  \note Invoker affinity: any.
     */
    virtual bool ShouldUpdateProgress() const = 0;

    //! Provides a string describing operation status and statistics.
    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //virtual TString GetLoggingProgress() const = 0;

    //! Called to get a cached YSON string representing the current progress.
    /*!
     *  \note Invoker affinity: any.
     */
    virtual NYson::TYsonString GetProgress() const = 0;

    //! Called to get a cached YSON string representing the current brief progress.
    /*!
     *  \note Invoker affinity: any.
     */
    virtual NYson::TYsonString GetBriefProgress() const = 0;

    //! Called to get a YSON string representing suspicious jobs of operation.
    /*!
     *  \note Invoker affinity: any.
     */
    virtual NYson::TYsonString GetSuspiciousJobsYson() const = 0;

    //! Returns metrics delta since the last call and resets the state.
    //! When `force` is true, the delta is returned unconditionally, otherwise the method has
    //! no effect in case too little time has passed since the last call.
    /*!
     * \note Invoker affinity: any.
     */
    virtual NScheduler::TOperationJobMetrics PullJobMetricsDelta(bool force = false) = 0;

    //! Extracts the job spec proto blob, which is being built at background.
    //! After this call, the reference to this blob is released.
    /*!
     *  \note Invoker affinity: cancelable Controller invoker with EOperationControllerQueue::GetJobSpec index.
     */
    virtual TSharedRef ExtractJobSpec(TJobId jobId) const = 0;

    //! Builds operation alerts.
    /*!
     * \note Invoker affinity: any.
     */
    virtual TOperationAlertMap GetAlerts() = 0;

    //! Updates internal copy of scheduler config used by controller.
    /*!
     *  \note Invoker affinity: Controller invoker.
     */
    virtual void UpdateConfig(const TControllerAgentConfigPtr& config) = 0;

    // TODO(ignat): remake it to method that returns attributes that should be updated in Cypress.
    //! Returns |true| when controller can build its progress.
    /*!
     *  \note Invoker affinity: any.
     */
    virtual bool HasProgress() const = 0;

    //! Builds operation info, used for orchid.
    /*!
     *  \note Invoker affinity: Controller invoker.
     */
    virtual TOperationInfo BuildOperationInfo() = 0;

    //! Builds job info, used for orchid.
    /*!
     *  \note Invoker affinity: Controller invoker.
     */
    virtual NYson::TYsonString BuildJobYson(TJobId jobId, bool outputStatistics) const = 0;

    //! Return a YPath service representing this controller in controller agent Orchid.
    /*!
     *  \note Invoker affinity: Controller invoker.
     */
    virtual NYTree::IYPathServicePtr GetOrchid() const = 0;

    virtual TString WriteCoreDump() const = 0;

    virtual void RegisterOutputRows(i64 count, int tableIndex) = 0;

    virtual std::optional<int> GetRowCountLimitTableIndex() = 0;

    virtual void LoadSnapshot(const TOperationSnapshot& snapshot) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateControllerForOperation(
    TControllerAgentConfigPtr config,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
