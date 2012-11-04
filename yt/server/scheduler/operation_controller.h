#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/actions/invoker.h>
#include <ytlib/actions/cancelable_context.h>

#include <ytlib/scheduler/job.pb.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/ytree/public.h>

#include <ytlib/rpc/public.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

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
    virtual NRpc::IChannelPtr GetMasterChannel() = 0;

    /*!
     *  \note Thread affinity: any
     */
    virtual NTransactionClient::TTransactionManagerPtr GetTransactionManager() = 0;

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

    //! Returns the list of currently active exec nodes.
    /*!
     *  \note Thread affinity: ControlThread
     */
    virtual std::vector<TExecNodePtr> GetExecNodes() = 0;

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


    virtual TExecNodePtr GetNode() = 0;

    virtual bool HasSpareResources() const = 0;

    virtual TJobPtr BeginScheduleJob(TOperationPtr operation) = 0;
    virtual void EndScheduleJob(TJobPtr job) = 0;
};

/*!
 *  \note Thread affinity: ControlThread
 */
struct IOperationController
    : public TRefCounted
{
    //! Performs a fast synchronous initialization.
    /*
     *  If an exception is thrown then the operation fails immediately.
     *  The diagnostics is returned to the client, no Cypress node is created.
     */
    virtual void Initialize() = 0;

    //! Performs a possibly lengthy initial preparation.
    /*!
     *  The controller must set the promise when the preparation is finished successfully.
     *  IOperationHost::OnOperationFailed can be called during preparation to indicate an error.
     */
    virtual TFuture<void> Prepare() = 0;

    //! Called by a scheduler in response to IOperationHost::OnOperationCompleted.
    /*!
     *  The controller must the transactions related to the operation.
     */
    virtual TFuture<void> Commit() = 0;

    //! Reactivates an already running operation.
    /*!
     *  This method is called during scheduler state recovery for each existing operation.
     *  The controller must set the promise when the revival is finished successfully.
     *  IOperationHost::OnOperationFailed can be called during revival to indicate an error.
     */
    virtual TFuture<void> Revive() = 0;

    //! Called by the scheduler notify the controller that the operation has been aborted.
    /*!
     *  All jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     */
    virtual void Abort() = 0;


    //! Returns the context that gets invalidated by #Abort.
    virtual TCancelableContextPtr GetCancelableContext() = 0;

    //! Returns the control invoker wrapped by the context provided by #GetCancelableContext.
    virtual IInvokerPtr GetCancelableControlInvoker() = 0;

    //! Returns the background invoker wrapped by the context provided by #GetCancelableContext.
    virtual IInvokerPtr GetCancelableBackgroundInvoker() = 0;


    //! Returns the number of jobs the controller still needs to start right away.
    virtual int GetPendingJobCount() = 0;

    //! Returns the total resources that are currently used.
    virtual NProto::TNodeResources GetUsedResources() = 0;

    //! Returns the total resources that are additionally needed.
    virtual NProto::TNodeResources GetNeededResources() = 0;

    //! Returns the minimum resources (per node) enabling to start a job.
    virtual NProto::TNodeResources GetMinNeededResources() = 0;
    
    //! Called during heartbeat processing to notify the controller that a job is running.
    virtual void OnJobRunning(TJobPtr job, const NProto::TJobStatus& status) = 0;

    //! Called during heartbeat processing to notify the controller that a job has completed.
    virtual void OnJobCompleted(TJobPtr job) = 0;

    //! Called during heartbeat processing to notify the controller that a job has failed.
    virtual void OnJobFailed(TJobPtr job) = 0;

    //! Called during preemption to notify the controller that a job has been aborted.
    virtual void OnJobAborted(TJobPtr job) = 0;

    //! Called during heartbeat processing to request actions the node must perform.
    virtual TJobPtr ScheduleJob(
        ISchedulingContext* context,
        bool isStarving) = 0;

    //! Called to construct a YSON representing the current progress.
    virtual void BuildProgressYson(NYTree::IYsonConsumer* consumer) = 0;

    //! Called for finished operations to construct a YSON representing the result.
    virtual void BuildResultYson(NYTree::IYsonConsumer* consumer) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
