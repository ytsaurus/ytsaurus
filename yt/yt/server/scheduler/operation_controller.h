#pragma once

#include "allocation.h"
#include "operation.h"

#include <yt/yt/server/lib/scheduler/job_metrics.h>

#include <yt/yt/client/job_tracker_client/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TControllerRuntimeData
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TCompositeNeededResources, NeededResources);
    DEFINE_BYREF_RW_PROPERTY(TJobResourcesWithQuotaList, MinNeededResources);
};

DEFINE_REFCOUNTED_TYPE(TControllerRuntimeData)

TError CheckControllerRuntimeData(const TControllerRuntimeDataPtr& runtimeData);

////////////////////////////////////////////////////////////////////////////////

/*!
 *  \note Thread affinity: any
 */
struct IOperationControllerStrategyHost
    : public virtual TRefCounted
{
    //! Returns epoch of the controller.
    virtual TControllerEpoch GetEpoch() const = 0;

    //! Called during heartbeat processing to request actions the node must perform.
    virtual TFuture<TControllerScheduleAllocationResultPtr> ScheduleAllocation(
        const ISchedulingContextPtr& context,
        const TJobResources& availableResources,
        const TDiskResources& availableDiskResources,
        const TString& treeId,
        const TString& poolPath,
        const TFairShareStrategyTreeConfigPtr& treeConfig) = 0;

    //! Called during scheduling to notify the controller that a (nonscheduled) allocation has been aborted.
    virtual void OnNonscheduledAllocationAborted(
        TAllocationId allocationId,
        EAbortReason abortReason,
        TControllerEpoch allocationEpoch) = 0;

    //! Returns the total resources that are additionally needed.
    virtual TCompositeNeededResources GetNeededResources() const = 0;

    //! Initiates updating min needed resources estimates.
    //! Note that the actual update may happen in background.
    virtual void UpdateMinNeededAllocationResources() = 0;

    //! Returns the cached min needed resources estimate.
    virtual TJobResourcesWithQuotaList GetMinNeededAllocationResources() const = 0;

    //! Returns initial min needed resources estimate (right after materialization).
    virtual TJobResourcesWithQuotaList GetInitialMinNeededAllocationResources() const = 0;

    //! Returns the mode which says how to preempt allocations of this operation.
    virtual EPreemptionMode GetPreemptionMode() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationControllerStrategyHost)

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerInitializeResult
{
    TOperationControllerInitializeAttributes Attributes;
    TOperationTransactions Transactions;
    bool EraseOffloadingTrees;
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
    TCompositeNeededResources InitialNeededResources;
    TJobResourcesWithQuotaList InitialMinNeededResources;
};

void FromProto(TOperationControllerMaterializeResult* result, const NControllerAgent::NProto::TMaterializeOperationResult& resultProto);

////////////////////////////////////////////////////////////////////////////////

struct TOperationControllerReviveResult
    : public TOperationControllerPrepareResult
{
    bool RevivedFromSnapshot = false;
    std::vector<TAllocationPtr> RevivedAllocations;
    THashSet<TString> RevivedBannedTreeIds;
    TCompositeNeededResources NeededResources;
    TJobResourcesWithQuotaList MinNeededResources;
    TJobResourcesWithQuotaList InitialMinNeededResources;
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
    virtual void AssignAgent(const TControllerAgentPtr& agent, TControllerEpoch epoch) = 0;

    //! Revokes the agent; safe to call multiple times.
    virtual bool RevokeAgent() = 0;

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

    //! Invokes IOperationControllerSchedulerHost::Register asynchronously.
    //! At registration we use a lot of information from #TOperationPtr.
    virtual TFuture<void> Register(const TOperationPtr& operation) = 0;

    //! Invokes IOperationControllerSchedulerHost::Dispose asynchronously.
    virtual TFuture<TOperationControllerUnregisterResult> Unregister() = 0;

    //! Invokes IOperationControllerSchedulerHost::UpdateRuntimeParameters asynchronously.
    virtual TFuture<void> UpdateRuntimeParameters(TOperationRuntimeParametersUpdatePtr update) = 0;


    // These methods can be called even without agent being assigned.

    //! Called to notify the controller that an allocation has been aborted by scheduler.
    /*!
     *  \note Thread affinity: any
     */
    virtual void OnAllocationAborted(
        const TAllocationPtr& allocation,
        const TError& error,
        bool scheduled,
        EAbortReason abortReason) = 0;

    // These methods should be called only by controller agent tracker.

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

    //! Called to update operation controller data at controller agent heartbeat.
    virtual void SetControllerRuntimeData(const TControllerRuntimeDataPtr& controllerData) = 0;

    //! Wait for current events has been sent in heartbeat and new heartbeat has been received.
    virtual TFuture<void> GetFullHeartbeatProcessed() = 0;
};

DEFINE_REFCOUNTED_TYPE(IOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
