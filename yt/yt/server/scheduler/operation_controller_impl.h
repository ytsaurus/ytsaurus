#pragma once

#include "operation_controller.h"
#include "controller_agent.h"

#include <yt/ytlib/controller_agent/controller_agent_service_proxy.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TOperationControllerImpl
    : public IOperationController
{
public:
    TOperationControllerImpl(
        TBootstrap* bootstrap,
        TSchedulerConfigPtr config,
        const TOperationPtr& operation);
    
    virtual void AssignAgent(const TControllerAgentPtr& agent) override;
    
    virtual void RevokeAgent() override;
    
    virtual TControllerAgentPtr FindAgent() const override;
    
    virtual TFuture<TOperationControllerInitializeResult> Initialize(const std::optional<TOperationTransactions>& transactions) override;
    virtual TFuture<TOperationControllerPrepareResult> Prepare() override;
    virtual TFuture<TOperationControllerMaterializeResult> Materialize() override;
    virtual TFuture<TOperationControllerReviveResult> Revive() override;
    virtual TFuture<TOperationControllerCommitResult> Commit() override;
    virtual TFuture<void> Terminate(EOperationState finalState) override;
    virtual TFuture<void> Complete() override;
    virtual TFuture<void> Register(const TOperationPtr& operation) override;
    virtual TFuture<TOperationControllerUnregisterResult> Unregister() override;
    virtual TFuture<void> UpdateRuntimeParameters(TOperationRuntimeParametersUpdatePtr update) override;
    
    virtual void OnJobStarted(const TJobPtr& job) override;
    virtual void OnJobCompleted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool abandoned) override;
    virtual void OnJobFailed(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override;
    virtual void OnJobAborted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool byScheduler) override;
    virtual void OnNonscheduledJobAborted(
        TJobId jobId,
        EAbortReason abortReason) override;
    virtual void OnJobRunning(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool shouldLogJob) override;
    
    virtual void OnInitializationFinished(const TErrorOr<TOperationControllerInitializeResult>& resultOrError) override;
    virtual void OnPreparationFinished(const TErrorOr<TOperationControllerPrepareResult>& resultOrError) override;
    virtual void OnMaterializationFinished(const TErrorOr<TOperationControllerMaterializeResult>& resultOrError) override;
    virtual void OnRevivalFinished(const TErrorOr<TOperationControllerReviveResult>& resultOrError) override;
    virtual void OnCommitFinished(const TErrorOr<TOperationControllerCommitResult>& resultOrError) override;
    
    virtual void SetControllerRuntimeData(const TControllerRuntimeDataPtr& controllerData) override;
    
    virtual TFuture<TControllerScheduleJobResultPtr> ScheduleJob(
        const ISchedulingContextPtr& context,
        const TJobResourcesWithQuota& jobLimits,
        const TString& treeId,
        const TFairShareStrategyTreeConfigPtr& treeConfig) override;
    
    virtual void UpdateMinNeededJobResources() override;
    
    virtual TJobResources GetNeededResources() const override;
    virtual TJobResourcesWithQuotaList GetMinNeededJobResources() const override;
    virtual int GetPendingJobCount() const override;
    virtual EPreemptionMode GetPreemptionMode() const override;

private:
    TBootstrap* const Bootstrap_;
    TSchedulerConfigPtr Config_;
    const TOperationId OperationId_;
    const EPreemptionMode PreemptionMode_;
    const NLogging::TLogger Logger;

    TControllerRuntimeDataPtr ControllerRuntimeData_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);

    TIncarnationId IncarnationId_;
    TWeakPtr<TControllerAgent> Agent_;
    std::unique_ptr<NControllerAgent::TControllerAgentServiceProxy> AgentProxy_;

    TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>> JobEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>> OperationEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>> ScheduleJobRequestsOutbox_;

    TPromise<TOperationControllerInitializeResult> PendingInitializeResult_;
    TPromise<TOperationControllerPrepareResult> PendingPrepareResult_;
    TPromise<TOperationControllerMaterializeResult> PendingMaterializeResult_;
    TPromise<TOperationControllerReviveResult> PendingReviveResult_;
    TPromise<TOperationControllerCommitResult> PendingCommitResult_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    
    bool EnqueueJobEvent(TSchedulerToAgentJobEvent&& event);
    void EnqueueOperationEvent(TSchedulerToAgentOperationEvent&& event);
    void EnqueueScheduleJobRequest(TScheduleJobRequestPtr&& event);
    
    TSchedulerToAgentJobEvent BuildEvent(
        ESchedulerToAgentJobEventType eventType,
        const TJobPtr& job,
        bool logAndProfile,
        NJobTrackerClient::NProto::TJobStatus* status);
    
    // TODO(ignat): move to inl
    template <class TResponse, class TRequest>
    TFuture<TIntrusivePtr<TResponse>> InvokeAgent(
        const TIntrusivePtr<TRequest>& request);
    
    void ProcessControllerAgentError(const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
