#pragma once

#include "public.h"

#include <ytlib/scheduler/jobs.pb.h>
#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IOperationHost
{
    virtual ~IOperationHost()
    { }


    virtual NRpc::IChannel::TPtr GetMasterChannel() = 0;

    //! Returns the control invoker of the scheduler.
    virtual IInvoker::TPtr GetControlInvoker() = 0;
    
    //! Returns the invoker for heavy background activities.
    /*!
     *  This invoker is typically used by controllers for preparing operations
     *  (e.g. sorting samples keys, constructing partitions etc).
     *  There are no affinity guarantees whatsoever.
     *  This could easily be a thread pool.
     */
    virtual IInvoker::TPtr GetBackgroundInvoker() = 0;

    //! A factory method for creating new jobs.
    /*!
     *  The controller must not instantiate jobs by itself since
     *  this needs some internal bookkeeping from the scheduler
     *  (e.g. id and start time assignment).
     *  
     *  \note Thread affinity: any
     */
    virtual TJobPtr CreateJob(
        TOperationPtr operation,
        TExecNodePtr node,
        const NProto::TJobSpec& spec) = 0;

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

/*!
 *  \note Thread affinity: ControlThread
 */
struct IOperationController
{
    virtual ~IOperationController()
    { }

    //! Performs a fast synchronous initialization.
    /*
     *  If an error is returned the operation is aborted immediately.
     *  The diagnostics is returned to the client, no Cypress node is created.
     */
    virtual TError Initialize() = 0;

    //! Performs a possibly lengthy initial preparation.
    /*!
     *  If preparation succeeds then the operation is considered running.
     *  Otherwise the operation is marked as failed.
     */
    virtual TAsyncError Prepare() = 0;
    
    //! Called during heartbeat processing to notify the controller that a job is running.
    virtual void OnJobRunning(TJobPtr job) = 0;

    //! Called during heartbeat processing to notify the controller that a job has completed.
    virtual void OnJobCompleted(TJobPtr job) = 0;

    //! Called during heartbeat processing to notify the controller that a job has failed.
    virtual void OnJobFailed(TJobPtr job) = 0;

    //! Called by the scheduler notify the controller that the operation has been aborted.
    /*!
     *  All jobs are aborted automatically.
     *  The operation, however, may carry out any additional cleanup it finds necessary.
     */
    virtual void OnOperationAborted(TOperationPtr operation) = 0;

    //! Called during heartbeat processing to request actions the node must perform.
    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
