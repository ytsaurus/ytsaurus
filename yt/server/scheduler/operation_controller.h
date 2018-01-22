#pragma once

#include "job.h"

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IOperationControllerStrategyHost
    : public virtual TRefCounted
{
    /*!
     *  \note Invoker affinity: Cancellable controller invoker
     */
    //! Called during heartbeat processing to request actions the node must perform.
    // TODO(babenko): fix context type
    virtual NControllerAgent::TScheduleJobResultPtr ScheduleJob(
        NControllerAgent::ISchedulingContext* context,
        const NScheduler::TJobResourcesWithQuota& jobLimits,
        const TString& treeId) = 0;

    /*!
     *  Returns the operation controller invoker wrapped by the context provided by #GetCancelableContext.
     *  Most of non-const controller methods are expected to be run in this invoker.
     */
    virtual IInvokerPtr GetCancelableInvoker() const = 0;

    //! Called during scheduling to notify the controller that a (nonscheduled) job has been aborted.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnNonscheduledJobAborted(
        const TJobId& jobId,
        EAbortReason abortReason) = 0;

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
    virtual TJobResourcesWithQuotaList GetMinNeededJobResources() const = 0;

    //! Returns the number of jobs the controller is able to start right away.
    /*!
     *  \note Thread affinity: any
     */
    virtual int GetPendingJobCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerStrategyHost)

////////////////////////////////////////////////////////////////////////////////

struct IOperationController
    : public IOperationControllerStrategyHost
{
    //! Called in the end of heartbeat when scheduler agrees to run operation job.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobStarted(const TJobPtr& job) = 0;

    //! Called during heartbeat processing to notify the controller that a job has completed.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobCompleted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool abandoned) = 0;

    //! Called during heartbeat processing to notify the controller that a job has failed.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobFailed(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) = 0;

    //! Called during heartbeat processing to notify the controller that a job has been aborted.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobAborted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) = 0;

    //! Called during heartbeat processing to notify the controller that a job is still running.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobRunning(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) = 0;

    // XXX(ignat): it is temporary methods.
    virtual NControllerAgent::IOperationControllerPtr GetAgentController() const = 0;
    virtual void SetAgentController(const NControllerAgent::IOperationControllerPtr& controller) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
