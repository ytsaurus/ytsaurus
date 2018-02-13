#pragma once

#include "public.h"

#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/job_metrics.h>
#include <yt/server/scheduler/operation_controller.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/job_tracker_client/job.pb.h>

#include <yt/ytlib/event_log/public.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/error.h>

#include <yt/core/yson/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TControllerTransactions
    : public TIntrinsicRefCounted
{
    NApi::ITransactionPtr Async;
    NApi::ITransactionPtr Input;
    NApi::ITransactionPtr Output;
    NApi::ITransactionPtr Debug;
    NApi::ITransactionPtr OutputCompletion;
    NApi::ITransactionPtr DebugCompletion;
};

DEFINE_REFCOUNTED_TYPE(TControllerTransactions)

////////////////////////////////////////////////////////////////////////////////

using TOperationAlertMap = THashMap<EOperationAlertType, TError>;

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerInitializationAttributes
{
    NYson::TYsonString Immutable;
    NYson::TYsonString Mutable;
    NYson::TYsonString BriefSpec;
    NYson::TYsonString UnrecognizedSpec;
};

struct TOperationControllerInitializationResult
{
    std::vector<NApi::ITransactionPtr> Transactions;
    TOperationControllerInitializationAttributes InitializationAttributes;
};

struct TOperationControllerReviveResult
{
    bool IsRevivedFromSnapshot = false;
    std::vector<NScheduler::TJobPtr> Jobs;
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
    TSharedRef Data;
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

// NB: This particular summary does not inherit from TJobSummary.
struct TStartedJobSummary
{
    explicit TStartedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    TJobId Id;
    TInstant StartTime;
};

struct TJobSummary
{
    TJobSummary() = default;
    TJobSummary(const TJobId& id, EJobState state);
    explicit TJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);
    virtual ~TJobSummary() = default;

    void Persist(const NPhoenix::TPersistenceContext& context);

    NJobTrackerClient::NProto::TJobResult Result;
    TJobId Id;
    EJobState State = EJobState::None;

    TNullable<TInstant> FinishTime;
    TNullable<TDuration> PrepareDuration;
    TNullable<TDuration> DownloadDuration;
    TNullable<TDuration> ExecDuration;

    // NB: The Statistics field will be set inside the controller in ParseStatistics().
    TNullable<NJobTrackerClient::TStatistics> Statistics;
    NYson::TYsonString StatisticsYson;

    bool LogAndProfile = false;
};

struct TCompletedJobSummary
    : public TJobSummary
{
    TCompletedJobSummary() = default;
    explicit TCompletedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    void Persist(const NPhoenix::TPersistenceContext& context);

    bool Abandoned = false;
    EInterruptReason InterruptReason = EInterruptReason::None;

    // These fields are for controller's use only.
    std::vector<NChunkClient::TInputDataSlicePtr> UnreadInputDataSlices;
    std::vector<NChunkClient::TInputDataSlicePtr> ReadInputDataSlices;
    int SplitJobCount = 1;
};

struct TAbortedJobSummary
    : public TJobSummary
{
    TAbortedJobSummary(const TJobId& id, EAbortReason abortReason);
    TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason);
    explicit TAbortedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    EAbortReason AbortReason = EAbortReason::None;
};

struct TRunningJobSummary
    : public TJobSummary
{
    explicit TRunningJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event);

    double Progress = 0;
    i64 StderrSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: Cancelable controller invoker
 */
struct IOperationControllerHost
    : public virtual TRefCounted
{
    virtual void InterruptJob(const TJobId& jobId, EInterruptReason reason) = 0;
    virtual void AbortJob(const TJobId& jobId, const TError& error) = 0;
    virtual void FailJob(const TJobId& jobId) = 0;
    virtual void ReleaseJobs(const std::vector<TJobId>& jobIds) = 0;

    virtual TFuture<TOperationSnapshot> DownloadSnapshot() = 0;
    virtual TFuture<void> RemoveSnapshot() = 0;

    virtual TFuture<void> FlushOperationNode() = 0;
    virtual void CreateJobNode(const TCreateJobNodeRequest& request) = 0;

    virtual TFuture<void> AttachChunkTreesToLivePreview(
        const NTransactionClient::TTransactionId& transactionId,
        const std::vector<NCypressClient::TNodeId>& tableIds,
        const std::vector<NChunkClient::TChunkTreeId>& childIds) = 0;
    virtual void AddChunkTreesToUnstageList(
        const std::vector<NChunkClient::TChunkId>& chunkTreeIds,
        bool recursive) = 0;

    virtual const NApi::INativeClientPtr& GetClient() = 0;
    virtual const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() = 0;
    virtual const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() = 0;
    virtual const IInvokerPtr& GetControllerThreadPoolInvoker() = 0;
    virtual const NEventLog::TEventLogWriterPtr& GetEventLogWriter() = 0;
    virtual const TCoreDumperPtr& GetCoreDumper() = 0;
    virtual const NConcurrency::TAsyncSemaphorePtr& GetCoreSemaphore() = 0;
    virtual const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() = 0;

    virtual int GetExecNodeCount() = 0;
    virtual TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors(const NScheduler::TSchedulingTagFilter& filter) = 0;
    virtual TInstant GetConnectionTime() = 0;

    virtual void OnOperationCompleted() = 0;
    virtual void OnOperationAborted(const TError& error) = 0;
    virtual void OnOperationFailed(const TError& error) = 0;
    virtual void OnOperationSuspended(const TError& error) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerHost)

////////////////////////////////////////////////////////////////////////////////

struct TJobStartDescriptor
{
    TJobStartDescriptor(
        const TJobId& id,
        EJobType type,
        const TJobResources& resourceLimits,
        bool interruptible);

    const TJobId Id;
    const EJobType Type;
    const TJobResources ResourceLimits;
    const bool Interruptible;
};

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobResult
    : public TIntrinsicRefCounted
{
    void RecordFail(EScheduleJobFailReason reason);
    bool IsBackoffNeeded() const;
    bool IsScheduleStopNeeded() const;

    TNullable<TJobStartDescriptor> StartDescriptor;
    TEnumIndexedVector<int, EScheduleJobFailReason> Failed;
    TDuration Duration;
};

DEFINE_REFCOUNTED_TYPE(TScheduleJobResult)

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): merge into NScheduler::IOperationController
struct IOperationControllerSchedulerHost
    : public virtual TRefCounted
{
    //! Performs controller inner state initialization. Starts all controller transactions.
    /*
     *  If an exception is thrown then the operation fails immediately.
     *  The diagnostics is returned to the client, no Cypress node is created.
     *
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual void Initialize() = 0;

    //! Performs controller inner state initialization for reviving operation.
    /*
     *  If an exception is thrown then the operation fails immediately.
     *
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual void InitializeReviving(TControllerTransactionsPtr operationTransactions) = 0;

    //! Performs a lightweight initial preparation.
    /*!
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual void Prepare() = 0;

    //! Performs a possibly lengthy materialization.
    /*!
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual void Materialize() = 0;

    //! Reactivates an already running operation, possibly restoring its progress.
    /*!
     *  This method is called during scheduler state recovery for each existing operation.
     *  Must be called after InitializeReviving().
     *
     *  \note Invoker affinity: cancelable Controller invoker
     *
     */
    virtual void Revive() = 0;

    //! Called by a scheduler in operation complete pipeline.
    /*!
     *  The controller must commit the transactions related to the operation.
     *
     *  \note Invoker affinity: cancelable Controller invoker
     *
     */
    virtual void Commit() = 0;

    //! Notifies the controller that the operation has been aborted.
    /*!
     *  All jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     *
     *  \note Invoker affinity: Control invoker
     *
     */
    virtual void Abort() = 0;

    //! Notifies the controller that the operation has been completed.
    /*!
     *  All running jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     *
     *  \note Invoker affinity: Control invoker
     *
     */
    virtual void Complete() = 0;

    //! Returns controller attributes and transactions that determined during initialization.
    //! Must be called once after initialization since result is moved to caller.
    /*!
     *  \note Invoker affinity: Control invoker
     */
    virtual TOperationControllerInitializationResult GetInitializationResult() = 0;

    //! Returns result of revive process.
    //! Must be called once after initialization since result is moved to caller.
    /*!
     *  \note Invoker affinity: Control invoker
     */
    virtual TOperationControllerReviveResult GetReviveResult() = 0;

    //! Returns controller attributes that determined after operation is prepared.
    /*!
     *  \note Invoker affinity: Control invoker
     */
    virtual NYson::TYsonString GetAttributes() const = 0;

    /*!
     *  Returns the operation controller invoker.
     *  Most of const controller methods are expected to be run in this invoker.
     */
    virtual IInvokerPtr GetInvoker() const = 0;

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
    virtual void OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary) = 0;

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
    virtual void OnBeforeDisposal() = 0;

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
     *  Returns the operation controller invoker.
     *  Most of const controller methods are expected to be run in this invoker.
     */
    virtual IInvokerPtr GetInvoker() const = 0;

    /*!
     *  Returns the operation controller invoker wrapped by the context provided by #GetCancelableContext.
     *  Most of non-const controller methods are expected to be run in this invoker.
     */
    virtual IInvokerPtr GetCancelableInvoker() const = 0;

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
    NYson::TYsonString RunningJobs;
    NYson::TYsonString JobSplitter;
    size_t MemoryUsage;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Invoker affinity: Controller invoker
 */
struct IOperationController
    : public IOperationControllerSchedulerHost
    , public IOperationControllerSnapshotBuilderHost
{
    virtual IInvokerPtr GetInvoker() const = 0;
    virtual IInvokerPtr GetCancelableInvoker() const = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to request actions the node must perform.
    virtual NControllerAgent::TScheduleJobResultPtr ScheduleJob(
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
    virtual void OnTransactionAborted(const NTransactionClient::TTransactionId& transactionId) = 0;

    //! Cancels the context returned by #GetCancelableContext.
    /*!
     *  \note Invoker affinity: Control invoker
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
    /*!
     * \note Invoker affinity: any.
     */
    virtual NScheduler::TOperationJobMetrics PullJobMetricsDelta() = 0;

    //! Extracts the job spec proto blob, which is being built at background.
    //! After this call, the reference to this blob is released.
    /*!
     *  \note Invoker affinity: cancelable Controller invoker
     */
    virtual TSharedRef ExtractJobSpec(const TJobId& jobId) const = 0;

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
    virtual NYson::TYsonString BuildJobYson(const TJobId& jobId, bool outputStatistics) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateControllerForOperation(
    TControllerAgentConfigPtr config,
    TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
