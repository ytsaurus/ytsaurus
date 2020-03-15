#pragma once

#include "job.h"

#include <yt/server/lib/scheduler/job_metrics.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct IOperationControllerStrategyHost
    : public virtual TRefCounted
{
    //! Called during heartbeat processing to request actions the node must perform.
    virtual TFuture<TControllerScheduleJobResultPtr> ScheduleJob(
        const ISchedulingContextPtr& context,
        const TJobResourcesWithQuota& availableResources,
        const TString& treeId) = 0;

    //! Called during scheduling to notify the controller that a (nonscheduled) job has been aborted.
    virtual void OnNonscheduledJobAborted(
        TJobId jobId,
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

    //! Returns the mode which says how to preempt jobs of this operation.
    virtual EPreemptionMode GetPreemptionMode() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerStrategyHost)

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerInitializeResult
{
    TOperationControllerInitializeAttributes Attributes;
    TOperationTransactions Transactions;
};

void FromProto(
    TOperationControllerInitializeResult* result,
    const NControllerAgent::NProto::TInitializeOperationResult& resultProto,
    TOperationId operationId,
    TBootstrap* bootstrap,
    TDuration operationTransactionPingPeriod);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerPrepareResult
{
    NYson::TYsonString Attributes;
};

void FromProto(TOperationControllerPrepareResult* result, const NControllerAgent::NProto::TPrepareOperationResult& resultProto);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerMaterializeResult
{
    bool Suspend = false;
    TJobResources InitialNeededResources;
};

void FromProto(TOperationControllerMaterializeResult* result, const NControllerAgent::NProto::TMaterializeOperationResult& resultProto);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerReviveResult
    : public TOperationControllerPrepareResult
{
    bool RevivedFromSnapshot = false;
    std::vector<TJobPtr> RevivedJobs;
    THashSet<TString> RevivedBannedTreeIds;
    TJobResources NeededResources;
};

void FromProto(
    TOperationControllerReviveResult* result,
    const NControllerAgent::NProto::TReviveOperationResult& resultProto,
    TOperationId operationId,
    TIncarnationId incarnationId,
    EPreemptionMode preemptionMode);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerCommitResult
{
};

void FromProto(TOperationControllerCommitResult* result, const NControllerAgent::NProto::TCommitOperationResult& resultProto);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerUnregisterResult
{
    TOperationJobMetrics ResidualJobMetrics;
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

    //! Returns the agent the operation runs at.
    //! May return null if the agent is no longer available.
    /*!
     *  \note Thread affinity: any
     */
    virtual TControllerAgentPtr FindAgent() const = 0;

    // These methods can only be called when an agent is assigned.

    //! Invokes IOperationControllerSchedulerHost::InitializeReviving or InitializeClean asynchronously.
    virtual TFuture<TOperationControllerInitializeResult> Initialize(
        const std::optional<TOperationTransactions>& transactions) = 0;

    //! Invokes IOperationControllerSchedulerHost::Prepare asynchronously.
    virtual TFuture<TOperationControllerPrepareResult> Prepare() = 0;

    //! Invokes IOperationControllerSchedulerHost::Materialize asynchronously.
    virtual TFuture<TOperationControllerMaterializeResult> Materialize() = 0;

    //! Invokes IOperationControllerSchedulerHost::Revive asynchronously.
    virtual TFuture<TOperationControllerReviveResult> Revive() = 0;

    //! Invokes IOperationControllerSchedulerHost::Commit asynchronously.
    virtual TFuture<TOperationControllerCommitResult> Commit() = 0;

    //! Invokes IOperationControllerSchedulerHost::Terminate asynchronously.
    virtual TFuture<void> Terminate(EOperationState finalState) = 0;

    //! Invokes IOperationControllerSchedulerHost::Complete asynchronously.
    virtual TFuture<void> Complete() = 0;

    //! Invokes IOperationControllerSchedulerHost::Dispose asynchronously.
    virtual TFuture<TOperationControllerUnregisterResult> Unregister() = 0;

    //! Invokes IOperationControllerSchedulerHost::UpdateRuntimeParameters asynchronously.
    virtual TFuture<void> UpdateRuntimeParameters(TOperationRuntimeParametersUpdatePtr update) = 0;


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

    //! Called to notify the controller that the operation initialization has finished.
    virtual void OnInitializationFinished(const TErrorOr<TOperationControllerInitializeResult>& resultOrError) = 0;

    //! Called to notify the controller that the operation preparation has finished.
    virtual void OnPreparationFinished(const TErrorOr<TOperationControllerPrepareResult>& resultOrError) = 0;

    //! Called to notify the controller that the operation materialization has finished.
    virtual void OnMaterializationFinished(const TErrorOr<TOperationControllerMaterializeResult>& resultOrError) = 0;

    //! Called to notify the controller that the operation revival has finished.
    virtual void OnRevivalFinished(const TErrorOr<TOperationControllerReviveResult>& resultOrError) = 0;

    //! Called to notify the controller that the operation commit has finished.
    virtual void OnCommitFinished(const TErrorOr<TOperationControllerCommitResult>& resultOrError) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
