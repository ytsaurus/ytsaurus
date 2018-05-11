#pragma once

#include "job.h"

#include <yt/server/controller_agent/operation_controller.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct IOperationControllerStrategyHost
    : public virtual TRefCounted
{
    //! Called during heartbeat processing to request actions the node must perform.
    virtual TFuture<NControllerAgent::TScheduleJobResultPtr> ScheduleJob(
        const ISchedulingContextPtr& context,
        const TJobResourcesWithQuota& jobLimits,
        const TString& treeId) = 0;

    //! Called during scheduling to notify the controller that a (nonscheduled) job has been aborted.
    virtual void OnNonscheduledJobAborted(
        const TJobId& jobId,
        EAbortReason abortReason) = 0;

    //! Returns the total resources that are additionally needed.
    virtual TJobResources GetNeededResources() const = 0;

    //! Initiates updating min needed resources estimates.
    //! Note that the actual update may happen in background.
    virtual void UpdateMinNeededJobResources() = 0;

    //! Returns the cached min needed resources estimate.
    virtual TJobResourcesWithQuotaList GetMinNeededJobResources() const = 0;

    //! Returns the number of jobs the controller is able to start right away.
    virtual int GetPendingJobCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerStrategyHost)

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerInitializationResult
{
    NControllerAgent::TOperationControllerInitializationAttributes Attributes;
};

struct TOperationControllerPrepareResult
{
    NYson::TYsonString Attributes;
};

struct TOperationControllerReviveResult
    : public TOperationControllerPrepareResult
{
    bool RevivedFromSnapshot = false;
    std::vector<TJobPtr> RevivedJobs;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: Control unless noted otherwise
 */
struct IOperationController
    : public IOperationControllerStrategyHost
{
    //! Assigns the agent to the operation controller.
    virtual void AssignAgent(const TControllerAgentPtr& agent) = 0;

    //! Revokes the agent; safe to call multiple times.
    virtual void RevokeAgent() = 0;

    //! Returns the agent is operation runs at.
    //! May return null is the agent is no longer available.
    /*!
     *  \note Thread affinity: any
     */
    virtual TControllerAgentPtr FindAgent() const = 0;

    // These methods can only be called when an agent is assigned.

    //! Invokes IOperationControllerSchedulerHost::InitializeReviving or InitializeClean asynchronously.
    virtual TFuture<TOperationControllerInitializationResult> Initialize(
        const TNullable<TOperationRevivalDescriptor>& descriptor) = 0;

    //! Invokes IOperationControllerSchedulerHost::Prepare asynchronously.
    virtual TFuture<TOperationControllerPrepareResult> Prepare() = 0;

    //! Invokes IOperationControllerSchedulerHost::Materialize asynchronously.
    virtual TFuture<void> Materialize() = 0;

    //! Invokes IOperationControllerSchedulerHost::Revive asynchronously.
    virtual TFuture<TOperationControllerReviveResult> Revive() = 0;

    //! Invokes IOperationControllerSchedulerHost::Commit asynchronously.
    virtual TFuture<void> Commit() = 0;

    //! Invokes IOperationControllerSchedulerHost::Abort asynchronously.
    virtual TFuture<void> Abort() = 0;

    //! Invokes IOperationControllerSchedulerHost::Complete asynchronously.
    virtual TFuture<void> Complete() = 0;

    //! Invokes IOperationControllerSchedulerHost::Dispose asynchronously.
    virtual TFuture<void> Dispose() = 0;


    // These methods can be called even without agent being assigned.

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
        NJobTrackerClient::NProto::TJobStatus* status,
        bool byScheduler) = 0;

    //! Called during heartbeat processing to notify the controller that a job is still running.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnJobRunning(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool shouldLogJob) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
