#pragma once

#include "public.h"

#include "private.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/server/scheduler/scheduling_context.h>
#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/job_metrics.h>

#include <yt/server/table_server/public.h>

#include <yt/server/misc/release_queue.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/job_tracker_client/job.pb.h>

#include <yt/ytlib/node_tracker_client/node.pb.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/actions/cancelable_context.h>
#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/error.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/fluent.h>

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

using TOperationAlertsMap = yhash<EOperationAlertType, TError>;

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerInitializationAttributes
{
    NYson::TYsonString Immutable;
    NYson::TYsonString Mutable;
    NYson::TYsonString BriefSpec;
    NYson::TYsonString UnrecognizedSpec;
};

////////////////////////////////////////////////////////////////////////////////

struct IOperationControllerStrategyHost
    : public virtual TRefCounted
{
    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to request actions the node must perform.
    virtual NScheduler::TScheduleJobResultPtr ScheduleJob(
        NScheduler::ISchedulingContextPtr context,
        const NScheduler::TJobResourcesWithQuota& jobLimits,
        const TString& treeId) = 0;

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
    virtual std::vector<NScheduler::TJobResourcesWithQuota> GetMinNeededJobResources() const = 0;

    /*!
     *  \note Thread affinity: any
     */
    //! Returns the number of jobs the controller still needs to start right away.
    virtual int GetPendingJobCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerStrategyHost)

struct IOperationControllerSchedulerHost
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

    //! Called by a scheduler in operation complete pipeline.
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

    //! Notifies the controller that the operation has been completed.
    /*!
     *  All running jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     */
    virtual void Complete() = 0;

    //! Returns controller attributes that determined during initialization.
    virtual TOperationControllerInitializationAttributes GetInitializationAttributes() const = 0;

    //! Returns controller attributes that determined after operation is prepared.
    virtual NYson::TYsonString GetAttributes() const = 0;

    /*!
     *  Returns the operation controller invoker.
     *  Most of const controller methods are expected to be run in this invoker.
     */
    virtual IInvokerPtr GetInvoker() const = 0;

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

    //! Build scheduler jobs from the joblets. Used during revival pipeline.
    virtual std::vector<NScheduler::TJobPtr> BuildJobsFromJoblets() const = 0;

    /*!
     *  \note Invoker affinity: controller invoker.
     */
    //! Method that is called after operation results are commited and before
    //! controller is disposed.
    virtual void OnBeforeDisposal() = 0;

    virtual void SetPoolTreeSchedulingTagFilters(const std::vector<NScheduler::TSchedulingTagFilter>& filters) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerSchedulerHost)

/*!
 *  \note Invoker affinity: OperationControllerInvoker
 */
struct IOperationController
    : public virtual TRefCounted
    , public IOperationControllerSchedulerHost
{
    //! Invokes controller finalization due to aborted or expired transaction.
    virtual void OnTransactionAborted(const NTransactionClient::TTransactionId& transactionId) = 0;

    //! Called from a forked copy of the scheduler to make a snapshot of operation's progress.
    virtual void SaveSnapshot(IOutputStream* stream) = 0;

    //! Returns the list of all active controller transactions.
    virtual std::vector<NApi::ITransactionPtr> GetTransactions() = 0;

    //! Returns the context that gets invalidated by #Abort.
    virtual TCancelableContextPtr GetCancelableContext() const = 0;

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

    //! Notifies the controller that its current scheduling decisions should be discarded.
    /*!
     *  Happens when current scheduler gets disconnected from master and
     *  the negotiation between scheduler and controller becomes no longer valid.
     */
    virtual void Forget() = 0;

    //! Returns the total number of jobs to be run during the operation.
    virtual int GetTotalJobCount() const = 0;

    //! Returns whether controller is running or not.
    virtual bool IsRunning() const = 0;

    /*!
     *  \note Invoker affinity: any.
     */
    //! Marks that progress was dumped to Cypress.
    virtual void SetProgressUpdated() = 0;

    /*!
     *  \note Invoker affinity: any.
     */
    //! Check that progress has changed and should be dumped to the Cypress.
    virtual bool ShouldUpdateProgress() const = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Provides a string describing operation status and statistics.
    virtual TString GetLoggingProgress() const = 0;

    //! Called to get a cached YSON string representing the current progress.
    virtual NYson::TYsonString GetProgress() const = 0;

    //! Called to get a cached YSON string representing the current brief progress.
    virtual NYson::TYsonString GetBriefProgress() const = 0;

    //! Builds job spec proto blob.
    virtual TSharedRef ExtractJobSpec(const TJobId& jobId) const = 0;

    /*!
     *  \note Invoker affinity: controller invoker.
     */
    //! Method that is called right before the controller is suspended and snapshot builder forks.
    //! Return value is an index of snapshot upload attempt starting from zero.
    //! This method should not throw.
    virtual int OnSnapshotStarted() = 0;

    /*!
     *  \note Invoker affinity: cancellable controller invoker.
     */
    //! Method that is called right after each snapshot is uploaded.
    //! `snapshotIndex` should be equal to a last `OnSnapshotStarted()` return value,
    //! otherwise controller crashes.
    virtual void OnSnapshotCompleted(int snapshotIndex) = 0;

    /*!
     * \note Invoker affinity: any.
     */
    //! Returns metrics delta since last call.
    virtual NScheduler::TOperationJobMetrics ExtractJobMetricsDelta() = 0;

    /*!
     * \note Invoker affinity: any.
     */
    //! Build operation alerts.
    virtual TOperationAlertsMap GetAlerts() = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Updates internal copy of scheduler config used by controller.
    virtual void UpdateConfig(TSchedulerConfigPtr config) = 0;

    // TODO(ignat): remake it to method that returns attributes that should be updated in Cypress.
    /*!
     *  \note Invoker affinity: any.
     */
    //! Returns |true| when controller can build it's progress.
    virtual bool HasProgress() const = 0;

    /*!
     * \note Invoker affinity: controller.
     */
    //! Builds operation info, used for orchid.
    virtual void BuildOperationInfo(NScheduler::NProto::TRspGetOperationInfo* response) = 0;

    /*!
     * \note Invoker affinity: controller.
     */
    //! Builds job info, used for orchid.
    virtual NYson::TYsonString BuildJobYson(const TJobId& jobId, bool outputStatistics) const = 0;

    //! Called to get a YSON string representing suspicious jobs of operation.
    virtual NYson::TYsonString GetSuspiciousJobsYson() const = 0;

    //! Called to check that operation fully completed.
    virtual bool IsCompleteFinished() const = 0;

    //! Returns non-trivial error if operation should be suspended.
    virtual TError GetSuspensionError() const = 0;
    virtual void ResetSuspensionError() = 0;

    //! Returns non-trivial error if operation should be aborted.
    virtual TError GetAbortError() const = 0;

    //! Returns non-trivial error if operation should be failed.
    virtual TError GetFailureError() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateControllerForOperation(
    TControllerAgentPtr controllerAgent,
    NScheduler::TOperation* operation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
