#pragma once

#include "public.h"
#include "job.h"
#include "event_log.h"

#include <core/misc/error.h>

#include <core/actions/future.h>
#include <core/actions/cancelable_context.h>

#include <core/concurrency/public.h>

#include <core/yson/public.h>

#include <core/ytree/public.h>

#include <ytlib/scheduler/job.pb.h>

#include <ytlib/api/public.h>

#include <ytlib/scheduler/job.pb.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/node_tracker_client/node.pb.h>

#include <ytlib/job_tracker_client/job.pb.h>

#include <ytlib/hive/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IOperationHost
    : public virtual IEventLogHost
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
    virtual NHive::TClusterDirectoryPtr GetClusterDirectory() = 0;

    //! Returns the control invoker of the scheduler.
    /*!
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr GetControlInvoker() = 0;

    //! Returns the invoker for heavy background activities.
    /*!
     *  This invoker is typically used by controllers for preparing operations
     *  (e.g. sorting samples keys, constructing partitions etc).
     *  There are no affinity guarantees whatsoever.
     *  This could easily be a thread pool.
     *
     *  \note Thread affinity: any
     */
    virtual IInvokerPtr GetBackgroundInvoker() = 0;

    //! Returns the throttler to limit #LocateChunk requests from chunk scratcher.
    virtual NConcurrency::IThroughputThrottlerPtr GetChunkLocationThrottler() = 0;

    //! Returns the list of currently active exec nodes.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual std::vector<TExecNodePtr> GetExecNodes() const = 0;

    //! Returns the number of currently active exec nodes.
    virtual int GetExecNodeCount() const = 0;

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
};

struct ISchedulingContext
{
    virtual ~ISchedulingContext()
    { }


    virtual TExecNodePtr GetNode() const = 0;

    virtual const std::vector<TJobPtr>& StartedJobs() const = 0;
    virtual const std::vector<TJobPtr>& PreemptedJobs() const = 0;
    virtual const std::vector<TJobPtr>& RunningJobs() const = 0;

    virtual bool CanStartMoreJobs() const = 0;

    virtual TJobPtr StartJob(
        TOperationPtr operation,
        EJobType type,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        TJobSpecBuilder specBuilder) = 0;

    virtual void PreemptJob(TJobPtr job) = 0;
};

/*!
 *  \note Thread affinity: ControlThread
 */
struct IOperationController
    : public virtual TRefCounted
{
    //! Performs a fast synchronous initialization.
    /*
     *  If an exception is thrown then the operation fails immediately.
     *  The diagnostics is returned to the client, no Cypress node is created.
     */
    virtual void Initialize() = 0;
    
    //! TODO(ignat): make reasonable comment
    virtual void Essentiate() = 0;

    //! Performs a possibly lengthy initial preparation.
    virtual TFuture<void> Prepare() = 0;

    //! Called by a scheduler in response to IOperationHost::OnOperationCompleted.
    /*!
     *  The controller must commit the transactions related to the operation.
     */
    virtual TFuture<void> Commit() = 0;

    //! Called from a forked copy of the scheduler to make a snapshot of operation's progress.
    virtual void SaveSnapshot(TOutputStream* stream) = 0;

    //! Reactivates an already running operation, possibly restoring its progress.
    /*!
     *  This method is called during scheduler state recovery for each existing operation.
     *  The controller may try to recover its state from the snapshot, if any
     *  (see TOperation::Snapshot).
     */
    virtual TFuture<void> Revive() = 0;

    //! Notifies the controller that the operation has been aborted.
    /*!
     *  All jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     */
    virtual void Abort() = 0;



    //! Returns the context that gets invalidated by #Abort.
    virtual TCancelableContextPtr GetCancelableContext() const = 0;

    //! Returns the control invoker wrapped by the context provided by #GetCancelableContext.
    virtual IInvokerPtr GetCancelableControlInvoker() const = 0;

    //! Returns the background invoker wrapped by the context provided by #GetCancelableContext.
    virtual IInvokerPtr GetCancelableBackgroundInvoker() const = 0;


    //! Returns the number of jobs the controller still needs to start right away.
    virtual int GetPendingJobCount() const = 0;

    //! Returns the total number of jobs to be run during the operation.
    virtual int GetTotalJobCount() const = 0;

    //! Returns the total resources that are additionally needed.
    virtual NNodeTrackerClient::NProto::TNodeResources GetNeededResources() const = 0;


    //! Called during heartbeat processing to notify the controller that a job is running.
    virtual void OnJobRunning(TJobPtr job, const NJobTrackerClient::NProto::TJobStatus& status) = 0;

    //! Called during heartbeat processing to notify the controller that a job has completed.
    virtual void OnJobCompleted(TJobPtr job) = 0;

    //! Called during heartbeat processing to notify the controller that a job has failed.
    virtual void OnJobFailed(TJobPtr job) = 0;

    //! Called during preemption to notify the controller that a job has been aborted.
    virtual void OnJobAborted(TJobPtr job) = 0;

    //! Called during heartbeat processing to request actions the node must perform.
    virtual TJobPtr ScheduleJob(
        ISchedulingContext* context,
        const NNodeTrackerClient::NProto::TNodeResources& jobLimits) = 0;

    //! Called to construct a YSON representing the current progress.
    virtual void BuildProgress(NYson::IYsonConsumer* consumer) const = 0;

    //! Similar to #BuildProgress but constructs a reduced version to used by UI.
    virtual void BuildBriefProgress(NYson::IYsonConsumer* consumer) const = 0;

    //! Provides a string describing operation status and statistics.
    virtual Stroka GetLoggingProgress() const = 0;

    //! Called for finished operations to construct a YSON representing the result.
    virtual void BuildResult(NYson::IYsonConsumer* consumer) const = 0;

    //! Called for a just initialized operation to construct its brief spec
    //! to be used by UI.
    virtual void BuildBriefSpec(NYson::IYsonConsumer* consumer) const = 0;

    //! Checks if the operation requires all chunk parts to be available.
    //! Used by remote copy operations which depend on all parts, including parity ones.
    virtual bool NeedsAllChunkParts() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
