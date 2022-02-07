#pragma once

#include "operation_controller.h"
#include "controller_agent.h"

#include <yt/yt/ytlib/controller_agent/controller_agent_service_proxy.h>

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

    void AssignAgent(const TControllerAgentPtr& agent) override;

    bool RevokeAgent() override;

    TControllerAgentPtr FindAgent() const override;

    TFuture<TOperationControllerInitializeResult> Initialize(const std::optional<TOperationTransactions>& transactions) override;
    TFuture<TOperationControllerPrepareResult> Prepare() override;
    TFuture<TOperationControllerMaterializeResult> Materialize() override;
    TFuture<TOperationControllerReviveResult> Revive() override;
    TFuture<TOperationControllerCommitResult> Commit() override;
    TFuture<void> Terminate(EOperationState finalState) override;
    TFuture<void> Complete() override;
    TFuture<void> Register(const TOperationPtr& operation) override;
    TFuture<TOperationControllerUnregisterResult> Unregister() override;
    TFuture<void> UpdateRuntimeParameters(TOperationRuntimeParametersUpdatePtr update) override;

    void OnJobStarted(const TJobPtr& job) override;
    void OnJobCompleted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool abandoned) override;
    void OnJobFailed(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override;
    void OnJobAborted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool byScheduler) override;
    void OnNonscheduledJobAborted(
        TJobId jobId,
        EAbortReason abortReason,
        const TString& treeId) override;

    void OnInitializationFinished(const TErrorOr<TOperationControllerInitializeResult>& resultOrError) override;
    void OnPreparationFinished(const TErrorOr<TOperationControllerPrepareResult>& resultOrError) override;
    void OnMaterializationFinished(const TErrorOr<TOperationControllerMaterializeResult>& resultOrError) override;
    void OnRevivalFinished(const TErrorOr<TOperationControllerReviveResult>& resultOrError) override;
    void OnCommitFinished(const TErrorOr<TOperationControllerCommitResult>& resultOrError) override;

    void SetControllerRuntimeData(const TControllerRuntimeDataPtr& controllerData) override;

    TFuture<void> GetFullHeartbeatProcessed() override;

    TFuture<TControllerScheduleJobResultPtr> ScheduleJob(
        const ISchedulingContextPtr& context,
        const TJobResources& jobLimits,
        const TString& treeId,
        const TFairShareStrategyTreeConfigPtr& treeConfig) override;

    void UpdateMinNeededJobResources() override;

    TCompositeNeededResources GetNeededResources() const override;
    TJobResourcesWithQuotaList GetMinNeededJobResources() const override;
    EPreemptionMode GetPreemptionMode() const override;

private:
    TBootstrap* const Bootstrap_;
    TSchedulerConfigPtr Config_;
    const TOperationId OperationId_;
    const EPreemptionMode PreemptionMode_;
    const NLogging::TLogger Logger;

    TControllerRuntimeDataPtr ControllerRuntimeData_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

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
