#pragma once

#include "public.h"
#include "event_log.h"
#include "job.h"
#include "scheduling_context.h"

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

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IOperationHost
{
    virtual ~IOperationHost()
    { }


    /*!
     *  \note Thread affinity: any
     */
    virtual NApi::IClientPtr GetMasterClient() = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual TMasterConnector* GetMasterConnector() = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual NHiveClient::TClusterDirectoryPtr GetClusterDirectory() = 0;

    //! Returns the control invoker of the scheduler.
    /*!
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr GetControlInvoker() = 0;

    //! Returns invoker for operation controller.
    /*!
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr CreateOperationControllerInvoker() = 0;

    //! Returns the manager of the throttlers to limit #LocateChunk requests from chunk scraper.
    virtual NChunkClient::TThrottlerManagerPtr GetChunkLocationThrottlerManager() const = 0;

    //! Returns the total number of online exec nodes.
    virtual int GetExecNodeCount() const = 0;

    //! Returns the descriptors of online exec nodes that can handle operations
    //! marked with a given #tag.
    /*!
     *  \note Thread affinity: any
     */
    virtual std::vector<TExecNodeDescriptor> GetExecNodeDescriptors(const TNullable<Stroka>& tag) const = 0;

    //! Called by a controller to notify the host that the operation has
    //! finished successfully.
    /*!
     *  Must be called exactly once.
     *
     *  \note Thread affinity: any
     */
    virtual void OnOperationCompleted(
        TOperationPtr operation) = 0;

    //! Called by a controller to notify the host that the operation has failed.
    /*!
     *  Safe to call multiple times (only the first call counts).
     *
     *  \note Thread affinity: any
     */
    virtual void OnOperationFailed(
        TOperationPtr operation,
        const TError& error) = 0;

    //! Creates new value consumer that can be used for logging.
    /*!
     *  \note Thread affinity: any
     */
    virtual std::unique_ptr<NTableClient::IValueConsumer> CreateLogConsumer() = 0;
};

/*!
 *  \note Invoker affinity: OperationControllerInvoker
 */
struct IOperationController
    : public virtual TRefCounted
{
    //! Performs controller inner state initialization. Check and start all operation transactions.
    /*
     *  If an exception is thrown then the operation fails immediately.
     *  The diagnostics is returned to the client, no Cypress node is created.
     *
     *  \note Invoker affinity: Control invoker
     */
    virtual void Initialize(bool cleanStart) = 0;

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

    //! Called by a scheduler in response to IOperationHost::OnOperationCompleted.
    /*!
     *  The controller must commit the transactions related to the operation.
     */
    virtual void Commit() = 0;

    //! Called from a forked copy of the scheduler to make a snapshot of operation's progress.
    virtual void SaveSnapshot(TOutputStream* stream) = 0;

    //! Reactivates an already running operation, possibly restoring its progress.
    /*!
     *  This method is called during scheduler state recovery for each existing operation.
     *  The controller may try to recover its state from the snapshot, if any
     *  (see TOperation::Snapshot).
     */
    virtual void Revive(const TSharedRef& snapshot) = 0;

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


    //! Returns the context that gets invalidated by #Abort.
    virtual TCancelableContextPtr GetCancelableContext() const = 0;

    //! Returns the control invoker wrapped by the context provided by #GetCancelableContext.
    virtual IInvokerPtr GetCancelableControlInvoker() const = 0;

    /*!
     *  Returns the operation controller invoker wrapped by the context provided by #GetCancelableContext.
     *  Most of non-const controller methods are expected to be run in this invoker.
     */
    virtual IInvokerPtr GetCancelableInvoker() const = 0;

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

    /*!
     *  Indicates that controller should be initialized from scratch.
     *
     *  \note Invoker affinity: Control invoker
     */
    virtual bool GetCleanStart() const = 0;

    /*!
     *  \note Thread affinity: any
     */
    //! Returns the number of jobs the controller still needs to start right away.
    virtual int GetPendingJobCount() const = 0;

    //! Returns the total number of jobs to be run during the operation.
    virtual int GetTotalJobCount() const = 0;

    /*!
     *  \note Thread affinity: any
     */
    //! Returns the total resources that are additionally needed.
    virtual TJobResources GetNeededResources() const = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called in the end of heartbeat when scheduler agrees to run operation job.
    virtual void OnJobStarted(const TJobId& jobId, TInstant startTime) = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to notify the controller that a job has completed.
    virtual void OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary) = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to notify the controller that a job has failed.
    virtual void OnJobFailed(std::unique_ptr<TFailedJobSummary> jobSummary) = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during preemption to notify the controller that a job has been aborted.
    virtual void OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary) = 0;

    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to request actions the node must perform.
    virtual TScheduleJobResultPtr ScheduleJob(
        ISchedulingContextPtr context,
        const TJobResources& jobLimits) = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Updates internal copy of scheduler config used by controller.
    virtual void UpdateConfig(TSchedulerConfigPtr config) = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Returns |true| when controller can build it's progress.
    virtual bool HasProgress() const = 0;

    //! Called to construct a YSON representing the current progress.
    virtual void BuildProgress(NYson::IYsonConsumer* consumer) const = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Similar to #BuildProgress but constructs a reduced version to used by UI.
    virtual void BuildBriefProgress(NYson::IYsonConsumer* consumer) const = 0;

    /*!
     *  \note Invoker affinity: Controller invoker
     */
    //! Provides a string describing operation status and statistics.
    virtual Stroka GetLoggingProgress() const = 0;

    /*!
     *  \note Invoker affinity: Control invoker
     */
    //! Called for finished operations to construct a YSON representing the result.
    virtual void BuildResult(NYson::IYsonConsumer* consumer) const = 0;

    /*!
     *  \note Thread affinity: any
     */
    //! Called for a just initialized operation to construct its brief spec
    //! to be used by UI.
    virtual void BuildBriefSpec(NYson::IYsonConsumer* consumer) const = 0;

};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
