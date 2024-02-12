#pragma once

#include "operation_controller.h"
#include "controller_agent.h"

#include <yt/yt/ytlib/controller_agent/controller_agent_service_proxy.h>
#include <yt/yt/ytlib/controller_agent/job_prober_service_proxy.h>

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

    void AssignAgent(const TControllerAgentPtr& agent, TControllerEpoch epoch) override;

    bool RevokeAgent() override;

    TControllerAgentPtr FindAgent() const override;

    TControllerEpoch GetEpoch() const override;

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

    void OnNonscheduledAllocationAborted(
        TAllocationId allocationId,
        EAbortReason abortReason,
        TControllerEpoch allocationEpoch) override;
    void OnAllocationAborted(
        const TAllocationPtr& allocation,
        const TError& error,
        bool scheduled,
        EAbortReason abortReason) override;

    void OnInitializationFinished(const TErrorOr<TOperationControllerInitializeResult>& resultOrError) override;
    void OnPreparationFinished(const TErrorOr<TOperationControllerPrepareResult>& resultOrError) override;
    void OnMaterializationFinished(const TErrorOr<TOperationControllerMaterializeResult>& resultOrError) override;
    void OnRevivalFinished(const TErrorOr<TOperationControllerReviveResult>& resultOrError) override;
    void OnCommitFinished(const TErrorOr<TOperationControllerCommitResult>& resultOrError) override;

    void SetControllerRuntimeData(const TControllerRuntimeDataPtr& controllerData) override;

    TFuture<void> GetFullHeartbeatProcessed() override;

    TFuture<TControllerScheduleAllocationResultPtr> ScheduleAllocation(
        const ISchedulingContextPtr& context,
        const TJobResources& allocationLimits,
        const TDiskResources& diskResourceLimits,
        const TString& treeId,
        const TString& poolPath,
        const TFairShareStrategyTreeConfigPtr& treeConfig) override;

    void UpdateMinNeededAllocationResources() override;

    TCompositeNeededResources GetNeededResources() const override;
    TJobResourcesWithQuotaList GetMinNeededAllocationResources() const override;
    TJobResourcesWithQuotaList GetInitialMinNeededAllocationResources() const override;
    EPreemptionMode GetPreemptionMode() const override;

    std::pair<NApi::ITransactionPtr, TString> GetIntermediateMediumTransaction();
    void UpdateIntermediateMediumUsage(i64 usage);

private:
    TBootstrap* const Bootstrap_;
    TSchedulerConfigPtr Config_;
    const TOperationId OperationId_;
    const EPreemptionMode PreemptionMode_;
    const NLogging::TLogger Logger;

    TControllerRuntimeDataPtr ControllerRuntimeData_;
    TJobResourcesWithQuotaList InitialMinNeededResources_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    TIncarnationId IncarnationId_;
    TWeakPtr<TControllerAgent> Agent_;
    std::unique_ptr<NControllerAgent::TControllerAgentServiceProxy> ControllerAgentTrackerProxy_;

    std::atomic<TControllerEpoch> Epoch_;

    TSchedulerToAgentAbortedAllocationEventOutboxPtr AbortedAllocationEventsOutbox_;
    TSchedulerToAgentOperationEventOutboxPtr OperationEventsOutbox_;
    TScheduleAllocationRequestOutboxPtr ScheduleAllocationRequestsOutbox_;

    TPromise<TOperationControllerInitializeResult> PendingInitializeResult_;
    TPromise<TOperationControllerPrepareResult> PendingPrepareResult_;
    TPromise<TOperationControllerMaterializeResult> PendingMaterializeResult_;
    TPromise<TOperationControllerReviveResult> PendingReviveResult_;
    TPromise<TOperationControllerCommitResult> PendingCommitResult_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    bool ShouldSkipAllocationAbortEvent(TAllocationId allocationId, TControllerEpoch allocationEpoch) const;

    bool EnqueueAbortedAllocationEvent(TAbortedAllocationSummary&& summary);
    void EnqueueOperationEvent(TSchedulerToAgentOperationEvent&& event);
    void EnqueueScheduleAllocationRequest(TScheduleAllocationRequestPtr&& event);

    // TODO(ignat): move to inl
    template <class TResponse, class TRequest>
    TFuture<TIntrusivePtr<TResponse>> InvokeAgent(
        const TIntrusivePtr<TRequest>& request);

    void ProcessControllerAgentError(const TError& error);

    void OnAllocationAborted(
        TAllocationId allocationId,
        const TError& error,
        const bool scheduled,
        EAbortReason abortReason,
        TControllerEpoch allocationEpoch);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
