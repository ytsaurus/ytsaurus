#pragma once

#include "public.h"

#include "private.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/server/scheduler/scheduling_context.h>
#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/job_metrics.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/job_tracker_client/job.pb.h>

#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/ytlib/scheduler/job.pb.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/actions/cancelable_context.h>
#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/error.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/server/table_server/public.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

struct TControllerTransactions
    : public TIntrinsicRefCounted
{
    NApi::ITransactionPtr Async;
    NApi::ITransactionPtr Input;
    NApi::ITransactionPtr Output;
    NApi::ITransactionPtr Completion;
    NApi::ITransactionPtr DebugOutput;
};

DEFINE_REFCOUNTED_TYPE(TControllerTransactions)

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerInitializeResult
{
    NYson::TYsonString BriefSpec;
};

////////////////////////////////////////////////////////////////////////////////

struct IOperationHost
{
    virtual ~IOperationHost() = default;

    /*!
     *  \note Thread affinity: any
     */
    virtual TControllerAgent* GetControllerAgent() = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() = 0;

    //! Returns the control invoker of the scheduler.
    /*!
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr GetControlInvoker(NCellScheduler::EControlQueue queue = NCellScheduler::EControlQueue::Default) const = 0;

    //! Returns the total number of online exec nodes.
    virtual int GetExecNodeCount() const = 0;

    //! Returns the descriptors of online exec nodes that can handle operations
    //! marked with a given #tag.
    /*!
     *  \note Thread affinity: any
     */
    virtual TExecNodeDescriptorListPtr GetExecNodeDescriptors(const NScheduler::TSchedulingTagFilter& filter) const = 0;

    //! Called by a controller to notify the host that the operation has
    //! finished successfully.
    /*!
     *  Must be called exactly once.
     *
     *  \note Thread affinity: any
     */
    virtual void OnOperationCompleted(const TOperationId& operation) = 0;

    //! Called by a controller to notify the host that the operation has failed.
    /*!
     *  Safe to call multiple times (only the first call counts).
     *
     *  \note Thread affinity: any
     */
    virtual void OnOperationFailed(
        const TOperationId& operationId,
        const TError& error) = 0;

    //! Called by a controller to notify the host that the operation should be suspended.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnOperationSuspended(
        const TOperationId& operationId,
        const TError& error) = 0;

    //! Called by a controller to notify the host that the operation has aborted.
    /*!
     *  Safe to call multiple times (only the first call counts).
     *
     *  \note Thread affinity: any
     */
    virtual void OnOperationAborted(
        const TOperationId& operationId,
        const TError& error) = 0;

    //! Called by a controller to notify the host that the user transaction of operations is expired or aborted.
    /*!
     *  Safe to call multiple times (only the first call counts).
     *
     *  \note Thread affinity: any
     */
    virtual void OnUserTransactionAborted(
        const TOperationId& operationId) = 0;

    //! Creates new value consumer that can be used for logging.
    /*!
     *  \note Thread affinity: any
     */
    virtual std::unique_ptr<NTableClient::IValueConsumer> CreateLogConsumer() = 0;

    //! Returns a TCoreDumper for current process.
    /*!
     *  \note Thread affinity: any
     */
    virtual const TCoreDumperPtr& GetCoreDumper() const = 0;

    //! Returns an AsyncSemaphore limiting concurrency for core dumps being written by failed safe assertions.
    /*!
     *  \note Thread affinity: any
     */
    virtual const NConcurrency::TAsyncSemaphorePtr& GetCoreSemaphore() const = 0;

    //! Sets operation alert.
    /*!
     *  \note Thread affinity: any
     */
    virtual TFuture<void> SetOperationAlert(
        const TOperationId& operationId,
        EOperationAlertType alertType,
        const TError& alert) = 0;

    //! Return IJobHost - access object to TJob
    /*!
     *  \note Thread affinity: any
     */
    virtual NScheduler::IJobHostPtr GetJobHost(const TJobId& jobId) const = 0;

    //! Tell scheduler the list of jobs that may be safely removed from their containing nodes as their
    //! results were saved to the corresponding controller snapshot.
    /*!
     *  \note Thread affinity: any
     */
    virtual TFuture<void> ReleaseJobs(const std::vector<NJobTrackerClient::TJobId>& jobIds) = 0;

    virtual void SendJobMetricsToStrategy(const TOperationId& operationdId, const NScheduler::TJobMetrics& jobMetrics) = 0;
};

struct IOperationControllerStrategyHost
    : public virtual TRefCounted
{
    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to request actions the node must perform.
    virtual NScheduler::TScheduleJobResultPtr ScheduleJob(
        NScheduler::ISchedulingContextPtr context,
        const TJobResources& jobLimits) = 0;

    /*!
     *  Returns the operation controller invoker wrapped by the context provided by #GetCancelableContext.
     *  Most of non-const controller methods are expected to be run in this invoker.
     */
    virtual IInvokerPtr GetCancelableInvoker() const = 0;


    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during preemption to notify the controller that a job has been aborted.
    virtual void OnJobAborted(std::unique_ptr<NScheduler::TAbortedJobSummary> jobSummary) = 0;

    /*!
     *  \note Thread affinity: any
     */
    //! Returns the total resources that are additionally needed.
    virtual TJobResources GetNeededResources() const = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called periodically during heartbeat to obtain min needed resources to schedule any operation job.
    virtual std::vector<TJobResources> GetMinNeededJobResources() const = 0;

    /*!
     *  \note Thread affinity: any
     */
    //! Returns the number of jobs the controller still needs to start right away.
    virtual int GetPendingJobCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerStrategyHost)


/*!
 *  \note Invoker affinity: OperationControllerInvoker
 */
struct IOperationController
    : public virtual TRefCounted
    , public IOperationControllerStrategyHost
{
    //! Performs controller inner state initialization. Starts all controller transactions.
    /*
     *  If an exception is thrown then the operation fails immediately.
     *  The diagnostics is returned to the client, no Cypress node is created.
     *
     *  \note Invoker affinity: Control invoker
     */
    virtual void Initialize() = 0;

    //! Returns controller initialization result. Scheduler uses it to build operation attributes in Cypress.
    virtual TOperationControllerInitializeResult GetInitializeResult() const = 0;

    //! Performs controller inner state initialization for reviving operation.
    /*
     *  If an exception is thrown then the operation fails immediately.
     *  The diagnostics is returned to the client, no Cypress node is created.
     *
     *  \note Invoker affinity: Control invoker
     */
    virtual void InitializeReviving(TControllerTransactionsPtr operationTransactions) = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Performs a lightweight initial preparation.
    virtual void Prepare() = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Performs a possibly lengthy materialization.
    virtual void Materialize() = 0;

    //! Reactivates an already running operation, possibly restoring its progress.
    /*!
     *  This method is called during scheduler state recovery for each existing operation.
     *  Must be called after InitializeReviving().
     */
    virtual void Revive() = 0;

    //! Called by a scheduler in response to IOperationHost::OnOperationCompleted.
    /*!
     *  The controller must commit the transactions related to the operation.
     */
    virtual void Commit() = 0;

    //! Notifies the controller that the operation has been aborted.
    /*!
     *  All jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     */
    virtual void Abort() = 0;

    //! Notifies the controller that its current scheduling decisions should be discarded.
    /*!
     *  Happens when current scheduler gets disconnected from master and
     *  the negotiation between scheduler and controller becomes no longer valid.
     */
    virtual void Forget() = 0;

    //! Notifies the controller that the operation has been completed.
    /*!
     *  All running jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     */
    virtual void Complete() = 0;

    //! Invokes controller finalization due to aborted or expired transaction.
    virtual void OnTransactionAborted(const NTransactionClient::TTransactionId& transactionId) = 0;

    //! Called from a forked copy of the scheduler to make a snapshot of operation's progress.
    virtual void SaveSnapshot(IOutputStream* stream) = 0;

    //! Returns the list of all active controller transactions.
    virtual std::vector<NApi::ITransactionPtr> GetTransactions() = 0;

    //! Returns the context that gets invalidated by #Abort.
    virtual TCancelableContextPtr GetCancelableContext() const = 0;

    /*!
     *  Returns the operation controller invoker.
     *  Most of const controller methods are expected to be run in this invoker.
     */
    virtual IInvokerPtr GetInvoker() const = 0;

    /*!
     *  Suspends controller invoker and returns future that is set after last action in invoker is executed.
     *
     *  \note Invoker affinity: Control invoker
     */
    virtual TFuture<void> Suspend() = 0;

    /*!
     *  Resumes execution in controller invoker.
     *
     *  \note Invoker affinity: Control invoker
     */
    virtual void Resume() = 0;

    //! Returns the total number of jobs to be run during the operation.
    virtual int GetTotalJobCount() const = 0;

    //! Returns whether controller was forgotten or not.
    virtual bool IsForgotten() const = 0;

    //! Returns whether controller was revived from snapshot.
    virtual bool IsRevivedFromSnapshot() const = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called in the end of heartbeat when scheduler agrees to run operation job.
    virtual void OnJobStarted(const TJobId& jobId, TInstant startTime) = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to notify the controller that a job has completed.
    virtual void OnJobCompleted(std::unique_ptr<NScheduler::TCompletedJobSummary> jobSummary) = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to notify the controller that a job has failed.
    virtual void OnJobFailed(std::unique_ptr<NScheduler::TFailedJobSummary> jobSummary) = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to notify the controller that a job is still running.
    virtual void OnJobRunning(std::unique_ptr<NScheduler::TRunningJobSummary> jobSummary) = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Updates internal copy of scheduler config used by controller.
    virtual void UpdateConfig(TSchedulerConfigPtr config) = 0;

    //! Called to construct a YSON representing the controller part of operation attributes.
    virtual void BuildOperationAttributes(NYson::IYsonConsumer* consumer) const = 0;

    /*!
     *  \note Invoker affinity: any;
     */
    //! Called to construct a YSON representing the current progress.
    virtual void BuildSpec(NYson::IYsonConsumer* consumer) const = 0;

    /*!
     *  \note Invoker affinity: any.
     */
    //! Marks that progress was dumped to cypress.
    virtual void SetProgressUpdated() = 0;

    /*!
     *  \note Invoker affinity: any.
     */
    //! Check that progress has changed and should be dumped to the cypress.
    virtual bool ShouldUpdateProgress() const = 0;

    /*!
     *  \note Invoker affinity: any.
     */
    //! Returns |true| when controller can build it's progress.
    virtual bool HasProgress() const = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Called to construct a YSON representing the current progress.
    virtual void BuildProgress(NYson::IYsonConsumer* consumer) const = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Similar to #BuildProgress but constructs a reduced version to be used by UI.
    virtual void BuildBriefProgress(NYson::IYsonConsumer* consumer) const = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Provides a string describing operation status and statistics.
    virtual TString GetLoggingProgress() const = 0;

    //! Called to construct a YSON representing the current state of memory digests for jobs of each type.
    virtual void BuildMemoryDigestStatistics(NYson::IYsonConsumer* consumer) const = 0;

    //! Called to get a cached YSON string representing the current progress.
    virtual NYson::TYsonString GetProgress() const = 0;

    //! Called to get a cached YSON string representing the current brief progress.
    virtual NYson::TYsonString GetBriefProgress() const = 0;

    //! Returns |true| when controller can build job splitter info.
    virtual bool HasJobSplitterInfo() const = 0;

    //! Called to construct a YSON representing job splitter state.
    virtual void BuildJobSplitterInfo(NYson::IYsonConsumer* consumer) const = 0;

    //! Called to get a YSON string representing current job(s) state.
    virtual NYson::TYsonString BuildJobYson(const TJobId& jobId, bool outputStatistics) const = 0;
    virtual NYson::TYsonString BuildJobsYson() const = 0;

    //! Builds job spec proto blob.
    virtual TSharedRef ExtractJobSpec(const TJobId& jobId) const = 0;

    //! Called to get a YSON string representing suspicious jobs of operation.
    virtual NYson::TYsonString BuildSuspiciousJobsYson() const = 0;

    /*!
     *  \note Invoker affinity: Controller invoker or scheduler control thread when controller is suspended.
     */
    //! Return the number of jobs that become completed after the moment of last snapshot save.
    virtual int GetRecentlyCompletedJobCount() const = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Remove `jobCount` oldest jobs from the list of recent jobs waiting their removal inside controller
    //! and submit them to the scheduler for the removal.
    //!
    //!                  (`jobCount` first)           v current moment of time
    //! completed jobs | x  x xxx xx xx xxx | ** * ** |    <- all these guys are stored in `recentCompletedJobs`
    //!                |                    |                 inside the controller.
    //!                ^ snapshot           ^ newly created snapshot
    virtual TFuture<void> ReleaseJobs(int jobCount) = 0;

    //! Build scheduler jobs from the joblets. Used during revival pipeline.
    virtual std::vector<NScheduler::TJobPtr> BuildJobsFromJoblets() const = 0;

};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateControllerForOperation(
    IOperationHost* host,
    NScheduler::TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
