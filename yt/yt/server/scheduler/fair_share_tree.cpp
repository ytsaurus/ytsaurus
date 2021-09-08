#include "fair_share_tree.h"
#include "fair_share_tree_element.h"
#include "fair_share_tree_snapshot_impl.h"
#include "persistent_scheduler_state.h"
#include "public.h"
#include "pools_config_parser.h"
#include "resource_tree.h"
#include "scheduler_strategy.h"
#include "scheduler_tree.h"
#include "scheduling_context.h"
#include "scheduling_segment_manager.h"
#include "serialize.h"
#include "fair_share_strategy_operation_controller.h"
#include "fair_share_tree_profiling.h"
#include "fair_share_update.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>
#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/algorithm_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/profiling/profile_manager.h>
#include <yt/yt/core/profiling/timing.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyOperationState::TFairShareStrategyOperationState(
    IOperationStrategyHost* host,
    const TFairShareStrategyOperationControllerConfigPtr& config)
    : Host_(host)
    , Controller_(New<TFairShareStrategyOperationController>(host, config))
{ }

TPoolName TFairShareStrategyOperationState::GetPoolNameByTreeId(const TString& treeId) const
{
    return GetOrCrash(TreeIdToPoolNameMap_, treeId);
}

void TFairShareStrategyOperationState::UpdateConfig(const TFairShareStrategyOperationControllerConfigPtr& config)
{
    Controller_->UpdateConfig(config);
}

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TPoolName> GetOperationPools(const TOperationRuntimeParametersPtr& runtimeParameters)
{
    THashMap<TString, TPoolName> pools;
    for (const auto& [treeId, options] : runtimeParameters->SchedulingOptionsPerPoolTree) {
        pools.emplace(treeId, options->Pool);
    }
    return pools;
}

////////////////////////////////////////////////////////////////////////////////

//! This class represents fair share tree.
//!
//! We maintain following entities:
//!
//!   * Actual tree, it contains the latest and consistent stucture of pools and operations.
//!     This tree represented by fields #RootElement_, #OperationIdToElement_, #Pools_.
//!     Update of this tree performed in sequentual manner from #Control thread.
//!
//!   * Snapshot of the tree with scheduling attributes (fair share ratios, best leaf descendants et. c).
//!     It is built repeatedly from actual tree by taking snapshot and calculating scheduling attributes.
//!     Clones of this tree are used in heartbeats for scheduling. Also, element attributes from this tree
//!     are used in orchid, for logging and for profiling.
//!     This tree represented by #TreeSnapshotImpl_.
//!     NB: elements of this tree may be invalidated by #Alive flag in resource tree. In this case element cannot be safely used
//!     (corresponding operation or pool can be already deleted from all other scheduler structures).
//!
//!   * Resource tree, it is thread safe tree that maintain shared attributes of tree elements.
//!     More details can be find at #TResourceTree.
class TFairShareTree
    : public ISchedulerTree
    , public IFairShareTreeHost
{
public:
    using TFairShareTreePtr = TIntrusivePtr<TFairShareTree>;

    struct TJobWithPreemptionInfo
    {
        TJobPtr Job;
        bool IsPreemptable = false;
        TSchedulerOperationElementPtr OperationElement;
    };

public:
    TFairShareTree(
        TFairShareStrategyTreeConfigPtr config,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        ISchedulerStrategyHost* strategyHost,
        const std::vector<IInvokerPtr>& feasibleInvokers,
        TString treeId)
        : Config_(std::move(config))
        , ControllerConfig_(std::move(controllerConfig))
        , ResourceTree_(New<TResourceTree>(Config_, feasibleInvokers))
        , TreeProfiler_(New<TFairShareTreeProfiler>(
            treeId,
            strategyHost->GetFairShareProfilingInvoker()))
        , StrategyHost_(strategyHost)
        , FeasibleInvokers_(feasibleInvokers)
        , TreeId_(std::move(treeId))
        , Logger(StrategyLogger.WithTag("TreeId: %v", TreeId_))
        , NonPreemptiveSchedulingStage_(
            /* nameInLogs */ "Non preemptive",
            TreeProfiler_->GetRegistry().WithTag("schedule_jobs_stage", "no_preemption"))
        , AggressivelyPreemptiveSchedulingStage_(
            /* nameInLogs */ "Aggressively preemptive",
            TreeProfiler_->GetRegistry().WithTag("schedule_jobs_stage", "aggressive_preemption"))
        , PreemptiveSchedulingStage_(
            /* nameInLogs */ "Preemptive",
            TreeProfiler_->GetRegistry().WithTag("schedule_jobs_stage", "preemption"))
        , PackingFallbackSchedulingStage_(
            /* nameInLogs */ "Packing fallback",
            TreeProfiler_->GetRegistry().WithTag("schedule_jobs_stage", "packing_fallback"))
        , FairSharePreUpdateTimer_(TreeProfiler_->GetRegistry().Timer("/fair_share_preupdate_time"))
        , FairShareUpdateTimer_(TreeProfiler_->GetRegistry().Timer("/fair_share_update_time"))
        , FairShareFluentLogTimer_(TreeProfiler_->GetRegistry().Timer("/fair_share_fluent_log_time"))
        , FairShareTextLogTimer_(TreeProfiler_->GetRegistry().Timer("/fair_share_text_log_time"))
    {
        RootElement_ = New<TSchedulerRootElement>(StrategyHost_, this, Config_, TreeId_, Logger);

        TreeProfiler_->RegisterPool(RootElement_);

        YT_LOG_INFO("Fair share tree created");
    }

    TFairShareStrategyTreeConfigPtr GetConfig() const override
    {
        return Config_;
    }

    void UpdateConfig(const TFairShareStrategyTreeConfigPtr& config) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (AreNodesEqual(ConvertToNode(config), ConvertToNode(Config_))) {
            return;
        }

        Config_ = config;
        RootElement_->UpdateTreeConfig(Config_);
        ResourceTree_->UpdateConfig(Config_);

        if (!FindPool(Config_->DefaultParentPool) && Config_->DefaultParentPool != RootPoolName) {
            auto error = TError("Default parent pool %Qv in tree %Qv is not registered", Config_->DefaultParentPool, TreeId_);
            StrategyHost_->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
        }

        YT_LOG_INFO("Tree has updated with new config");
    }

    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        ControllerConfig_ = config;

        for (const auto& [operationId, element] : OperationIdToElement_) {
            element->UpdateControllerConfig(config);
        }
    }

    const TSchedulingTagFilter& GetNodesFilter() const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return Config_->NodesFilter;
    }

    // NB: This function is public for scheduler simulator.
    TFuture<std::pair<IFairShareTreeSnapshotPtr, TError>> OnFairShareUpdateAt(TInstant now) override
    {
        return BIND(&TFairShareTree::DoFairShareUpdateAt, MakeStrong(this), now)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    void FinishFairShareUpdate() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_VERIFY(TreeSnapshotImplPrecommit_);
        TreeSnapshotImpl_ = std::move(TreeSnapshotImplPrecommit_);
        TreeSnapshotImplPrecommit_.Reset();
    }

    bool HasOperation(TOperationId operationId) const override
    {
        return static_cast<bool>(FindOperationElement(operationId));
    }

    bool HasRunningOperation(TOperationId operationId) const override
    {
        if (auto element = FindOperationElement(operationId)) {
            return element->IsOperationRunningInPool();
        }
        return false;
    }

    int GetOperationCount() const override
    {
        return OperationIdToElement_.size();
    }

    void RegisterOperation(
        const TFairShareStrategyOperationStatePtr& state,
        const TStrategyOperationSpecPtr& spec,
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParameters) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        TForbidContextSwitchGuard contextSwitchGuard;

        auto operationId = state->GetHost()->GetId();

        auto operationElement = New<TSchedulerOperationElement>(
            Config_,
            spec,
            runtimeParameters,
            state->GetController(),
            ControllerConfig_,
            StrategyHost_,
            this,
            state->GetHost(),
            TreeId_,
            Logger);

        int index = RegisterSchedulingTagFilter(TSchedulingTagFilter(spec->SchedulingTagFilter));
        operationElement->SetSchedulingTagFilterIndex(index);

        YT_VERIFY(OperationIdToElement_.emplace(operationId, operationElement).second);

        auto poolName = state->GetPoolNameByTreeId(TreeId_);
        auto pool = GetOrCreatePool(poolName, state->GetHost()->GetAuthenticatedUser());

        int slotIndex = AllocateOperationSlotIndex(state, pool->GetId());
        state->GetHost()->SetSlotIndex(TreeId_, slotIndex);

        operationElement->AttachParent(pool.Get(), slotIndex);

        bool isRunningInPool = OnOperationAddedToPool(state, operationElement);
        if (isRunningInPool) {
            OperationRunning_.Fire(operationId);
        }

        if (auto dataCenter = runtimeParameters->SchedulingSegmentDataCenter) {
            YT_LOG_DEBUG(
                "Recovering operation's scheduling segment data center assignment from runtime parameters "
                "(OperationId: %v, DataCenter: %v)",
                operationId,
                dataCenter);

            operationElement->PersistentAttributes().SchedulingSegmentDataCenter = dataCenter;
        }

        YT_LOG_INFO("Operation element registered in tree (OperationId: %v, Pool: %v, MarkedAsRunning: %v)",
            operationId,
            poolName.ToString(),
            isRunningInPool);
    }

    void UnregisterOperation(const TFairShareStrategyOperationStatePtr& state) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationId = state->GetHost()->GetId();
        auto operationElement = GetOperationElement(operationId);

        auto* pool = operationElement->GetMutableParent();

        // Profile finished operation.
        TreeProfiler_->ProfileOperationUnregistration(pool, state->GetHost()->GetState());

        operationElement->Disable(/* markAsNonAlive */ true);
        operationElement->DetachParent();

        OnOperationRemovedFromPool(state, operationElement, pool);

        UnregisterSchedulingTagFilter(operationElement->GetSchedulingTagFilterIndex());

        EraseOrCrash(OperationIdToElement_, operationId);

        // Operation can be missing in these maps.
        OperationIdToActivationTime_.erase(operationId);
        OperationIdToFirstFoundLimitingAncestorTime_.erase(operationId);
    }

    void EnableOperation(const TFairShareStrategyOperationStatePtr& state) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationId = state->GetHost()->GetId();
        auto operationElement = GetOperationElement(operationId);

        operationElement->GetMutableParent()->EnableChild(operationElement);

        operationElement->Enable();
    }

    void DisableOperation(const TFairShareStrategyOperationStatePtr& state) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationElement = GetOperationElement(state->GetHost()->GetId());
        operationElement->Disable(/* markAsNonAlive */ false);
        operationElement->GetMutableParent()->DisableChild(operationElement);
    }

    void ChangeOperationPool(
        TOperationId operationId,
        const TFairShareStrategyOperationStatePtr& state,
        const TPoolName& newPool) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = FindOperationElement(operationId);
        if (!element) {
            THROW_ERROR_EXCEPTION("Operation element for operation %Qv not found", operationId);
        }
        bool operationWasRunning = element->IsOperationRunningInPool();

        auto oldParent = element->GetMutableParent();
        auto newParent = GetOrCreatePool(newPool, state->GetHost()->GetAuthenticatedUser());

        OnOperationRemovedFromPool(state, element, oldParent);

        int newSlotIndex = AllocateOperationSlotIndex(state, newParent->GetId());
        element->ChangeParent(newParent.Get(), newSlotIndex);
        state->GetHost()->SetSlotIndex(TreeId_, newSlotIndex);

        YT_VERIFY(OnOperationAddedToPool(state, element));

        if (!operationWasRunning) {
            OperationRunning_.Fire(operationId);
        }
    }

    void UpdateOperationRuntimeParameters(
        TOperationId operationId,
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParameters) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (const auto& element = FindOperationElement(operationId)) {
            element->SetRuntimeParameters(runtimeParameters);
        }
    }

    void RegisterJobsFromRevivedOperation(TOperationId operationId, const std::vector<TJobPtr>& jobs) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& element = FindOperationElement(operationId);
        for (const auto& job : jobs) {
            element->OnJobStarted(
                job->GetId(),
                job->ResourceUsage(),
                /* precommittedResources */ {},
                /* force */ true);
        }
    }

    TError CheckOperationIsHung(
        TOperationId operationId,
        TDuration safeTimeout,
        int minScheduleJobCallAttempts,
        const THashSet<EDeactivationReason>& deactivationReasons,
        TDuration limitingAncestorSafeTimeout,
        const TJobResourcesWithQuotaList& minNeededResources) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = FindOperationElementInSnapshot(operationId);
        if (!element) {
            return TError();
        }

        auto now = TInstant::Now();
        TInstant activationTime;
        {
            auto it = OperationIdToActivationTime_.find(operationId);
            if (!element->Attributes().Alive) {
                if (it != OperationIdToActivationTime_.end()) {
                    it->second = TInstant::Max();
                }
                return TError();
            } else {
                if (it == OperationIdToActivationTime_.end()) {
                    activationTime = now;
                    OperationIdToActivationTime_.emplace(operationId, now);
                } else {
                    it->second = std::min(it->second, now);
                    activationTime = it->second;
                }
            }
        }

        // We only want to find the operations that are hanged due to poorly configured resource limits or a custom
        // scheduling tag filter. Node shortage, e.g. due to a bulk restart, shouldn't fail the operation. See: YT-13329.
        bool hasMinNeededResources = !minNeededResources.empty();
        auto aggregatedMinNeededResources = TJobResources::Infinite();
        for (const auto& jobResources : minNeededResources) {
            aggregatedMinNeededResources = Min(aggregatedMinNeededResources, jobResources.ToJobResources());
        }

        bool canFitIntoTotalResources = TreeSnapshotImpl_ &&
            Dominates(TreeSnapshotImpl_->RootElement()->GetTotalResourceLimits(), aggregatedMinNeededResources);
        bool shouldCheckLimitingAncestor = hasMinNeededResources &&
            canFitIntoTotalResources &&
            Config_->EnableLimitingAncestorCheck &&
            element->IsLimitingAncestorCheckEnabled();
        if (shouldCheckLimitingAncestor) {
            auto it = OperationIdToFirstFoundLimitingAncestorTime_.find(operationId);

            // NB(eshcherbin): Here we rely on the fact that |element->ResourceLimits_| is infinite
            // if the element is not in the fair share tree snapshot yet.
            if (auto* limitingAncestor = FindAncestorWithInsufficientResourceLimits(element, aggregatedMinNeededResources)) {
                TInstant firstFoundLimitingAncestorTime;
                if (it == OperationIdToFirstFoundLimitingAncestorTime_.end()) {
                    firstFoundLimitingAncestorTime = now;
                    OperationIdToFirstFoundLimitingAncestorTime_.emplace(operationId, now);
                } else {
                    it->second = std::min(it->second, now);
                    firstFoundLimitingAncestorTime = it->second;
                }

                if (activationTime + limitingAncestorSafeTimeout < now &&
                    firstFoundLimitingAncestorTime + limitingAncestorSafeTimeout < now)
                {
                    return TError("Operation has an ancestor whose resource limits are too small to satisfy operation's minimum job resource demand")
                        << TErrorAttribute("safe_timeout", limitingAncestorSafeTimeout)
                        << TErrorAttribute("limiting_ancestor", limitingAncestor->GetId())
                        << TErrorAttribute("resource_limits", limitingAncestor->GetResourceLimits())
                        << TErrorAttribute("min_needed_resources", minNeededResources);
                }
            } else if (it != OperationIdToFirstFoundLimitingAncestorTime_.end()) {
                it->second = TInstant::Max();
            }
        }

        int deactivationCount = 0;
        auto deactivationReasonToCount = element->GetDeactivationReasonsFromLastNonStarvingTime();
        for (auto reason : deactivationReasons) {
            deactivationCount += deactivationReasonToCount[reason];
        }

        if (activationTime + safeTimeout < now &&
            element->GetLastScheduleJobSuccessTime() + safeTimeout < now &&
            element->GetLastNonStarvingTime() + safeTimeout < now &&
            element->GetRunningJobCount() == 0 &&
            deactivationCount > minScheduleJobCallAttempts)
        {
            return TError("Operation has no successful scheduled jobs for a long period")
                << TErrorAttribute("period", safeTimeout)
                << TErrorAttribute("deactivation_count", deactivationCount)
                << TErrorAttribute("last_schedule_job_success_time", element->GetLastScheduleJobSuccessTime())
                << TErrorAttribute("last_non_starving_time", element->GetLastNonStarvingTime());
        }

        // NB(eshcherbin): See YT-14393.
        {
            const auto& segment = element->SchedulingSegment();
            const auto& dataCenter = element->PersistentAttributes().SchedulingSegmentDataCenter;
            if (segment && IsDataCenterAwareSchedulingSegment(*segment) && dataCenter && !element->GetSchedulingTagFilter().IsEmpty()) {
                auto tagFilter = element->GetSchedulingTagFilter().GetBooleanFormula().GetFormula();
                bool isDataCenterFilter = false;
                for (const auto& possibleDataCenter : Config_->SchedulingSegments->DataCenters) {
                    // NB(eshcherbin): This doesn't cover all the cases, only the most usual.
                    // Don't really want to check boolean formula satisfiability here.
                    if (tagFilter == possibleDataCenter) {
                        isDataCenterFilter = true;
                        break;
                    }
                }

                if (isDataCenterFilter && tagFilter != *dataCenter) {
                    return TError(
                        "Operation has a data center specified in the scheduling tag filter, which causes scheduling problems; "
                        "use \"scheduling_segment_data_centers\" spec option instead")
                        << TErrorAttribute("scheduling_tag_filter", tagFilter)
                        << TErrorAttribute("available_data_centers", Config_->SchedulingSegments->DataCenters);
                }
            }
        }

        return TError();
    }

    void ProcessActivatableOperations() override
    {
        while (!ActivatableOperationIds_.empty()) {
            auto operationId = ActivatableOperationIds_.back();
            ActivatableOperationIds_.pop_back();
            OperationRunning_.Fire(operationId);
        }
    }

    void TryRunAllPendingOperations() override
    {
        std::vector<TOperationId> readyOperationIds;
        std::vector<std::pair<TSchedulerOperationElementPtr, TSchedulerCompositeElement*>> stillPending;
        for (const auto& [_, pool] : Pools_) {
            for (auto pendingOperationId : pool->PendingOperationIds()) {
                if (auto element = FindOperationElement(pendingOperationId)) {
                    YT_VERIFY(!element->IsOperationRunningInPool());
                    if (auto violatingPool = FindPoolViolatingMaxRunningOperationCount(element->GetMutableParent())) {
                        stillPending.emplace_back(std::move(element), violatingPool);
                    } else {
                        element->MarkOperationRunningInPool();
                        readyOperationIds.push_back(pendingOperationId);
                    }
                }
            }
            pool->PendingOperationIds().clear();
        }

        for (const auto& [operation, pool] : stillPending) {
            operation->MarkPendingBy(pool);
        }

        for (auto operationId : readyOperationIds) {
            OperationRunning_.Fire(operationId);
        }
    }

    TPoolName CreatePoolName(const std::optional<TString>& poolFromSpec, const TString& user) const override
    {
        auto poolName = poolFromSpec.value_or(user);

        auto pool = FindPool(poolName);
        if (pool && pool->GetConfig()->CreateEphemeralSubpools) {
            return TPoolName(user, poolName);
        }
        return TPoolName(poolName, std::nullopt);
    }

    TPoolsUpdateResult UpdatePools(const INodePtr& poolsNode) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (LastPoolsNodeUpdate_ && AreNodesEqual(LastPoolsNodeUpdate_, poolsNode)) {
            YT_LOG_INFO("Pools are not changed, skipping update");
            return {LastPoolsNodeUpdateError_, false};
        }

        LastPoolsNodeUpdate_ = poolsNode;

        THashMap<TString, TString> poolToParentMap;
        THashSet<TString> ephemeralPools;
        for (const auto& [poolId, pool] : Pools_) {
            poolToParentMap[poolId] = pool->GetParent()->GetId();
            if (pool->IsDefaultConfigured()) {
                ephemeralPools.insert(poolId);
            }
        }

        TPoolsConfigParser poolsConfigParser(
            std::move(poolToParentMap),
            std::move(ephemeralPools),
            Config_->PoolConfigPresets);

        TError parseResult = poolsConfigParser.TryParse(poolsNode);
        if (!parseResult.IsOK()) {
            auto wrappedError = TError("Found pool configuration issues in tree %Qv; update skipped", TreeId_)
                << parseResult;
            LastPoolsNodeUpdateError_ = wrappedError;
            return {wrappedError, false};
        }

        // Parsing is succeeded. Applying new structure.
        for (const auto& updatePoolAction : poolsConfigParser.GetOrderedUpdatePoolActions()) {
            switch (updatePoolAction.Type) {
                case EUpdatePoolActionType::Create: {
                    auto pool = New<TSchedulerPoolElement>(
                        StrategyHost_,
                        this,
                        updatePoolAction.Name,
                        updatePoolAction.PoolConfig,
                        /* defaultConfigured */ false,
                        Config_,
                        TreeId_,
                        Logger);
                    const auto& parent = updatePoolAction.ParentName == RootPoolName
                        ? static_cast<TSchedulerCompositeElementPtr>(RootElement_)
                        : GetPool(updatePoolAction.ParentName);

                    RegisterPool(pool, parent);
                    break;
                }
                case EUpdatePoolActionType::Erase: {
                    auto pool = GetPool(updatePoolAction.Name);
                    if (pool->IsEmpty()) {
                        UnregisterPool(pool);
                    } else {
                        pool->SetDefaultConfig();

                        auto defaultParent = GetDefaultParentPoolForUser(updatePoolAction.Name);
                        if (pool->GetId() == defaultParent->GetId()) {  // Someone is deleting default pool.
                            defaultParent = RootElement_;
                        }
                        if (pool->GetParent()->GetId() != defaultParent->GetId()) {
                            pool->ChangeParent(defaultParent.Get());
                        }
                    }
                    break;
                }
                case EUpdatePoolActionType::Move:
                case EUpdatePoolActionType::Keep: {
                    auto pool = GetPool(updatePoolAction.Name);
                    if (pool->GetUserName()) {
                        const auto& userName = pool->GetUserName().value();
                        if (pool->IsEphemeralInDefaultParentPool()) {
                            EraseOrCrash(UserToEphemeralPoolsInDefaultPool_[userName], pool->GetId());
                        }
                        pool->SetUserName(std::nullopt);
                    }
                    ReconfigurePool(pool, updatePoolAction.PoolConfig);
                    if (updatePoolAction.Type == EUpdatePoolActionType::Move) {
                        const auto& parent = updatePoolAction.ParentName == RootPoolName
                            ? static_cast<TSchedulerCompositeElementPtr>(RootElement_)
                            : GetPool(updatePoolAction.ParentName);
                        pool->ChangeParent(parent.Get());
                    }
                    break;
                }
            }
        }

        LastPoolsNodeUpdateError_ = TError();

        return {LastPoolsNodeUpdateError_, true};
    }
    
    virtual TError UpdateUserToDefaultPoolMap(const THashMap<TString, TString>& userToDefaultPoolMap) override
    {
        if (!Config_->UseUserDefaultParentPoolMap) {
            UserToDefaultPoolMap_.clear();
            return TError();
        }

        THashSet<TString> uniquePoolNames;
        for (const auto& [userName, poolName] : userToDefaultPoolMap) {
            uniquePoolNames.insert(poolName);
        }

        for (const auto& poolName : uniquePoolNames) {
            if (!FindPool(poolName)) {
                return TError("User default parent pool is missing in pool tree")
                    << TErrorAttribute("pool", poolName)
                    << TErrorAttribute("pool_tree", TreeId_);
            }
        }

        UserToDefaultPoolMap_ = userToDefaultPoolMap;
        return TError();
    }

    void ValidatePoolLimits(const IOperationStrategyHost* operation, const TPoolName& poolName) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        ValidateOperationCountLimit(poolName, operation->GetAuthenticatedUser());
        ValidateEphemeralPoolLimit(operation, poolName);
    }

    void ValidatePoolLimitsOnPoolChange(const IOperationStrategyHost* operation, const TPoolName& newPoolName) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        ValidateEphemeralPoolLimit(operation, newPoolName);
        ValidateAllOperationsCountsOnPoolChange(operation->GetId(), newPoolName);
    }

    TFuture<void> ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return BIND(&TFairShareTree::DoValidateOperationPoolsCanBeUsed, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operation, poolName);
    }

    TPersistentTreeStatePtr BuildPersistentTreeState() const override
    {
        auto result = New<TPersistentTreeState>();
        for (const auto& [poolId, pool] : Pools_) {
            if (pool->GetIntegralGuaranteeType() != EIntegralGuaranteeType::None) {
                auto state = New<TPersistentPoolState>();
                state->AccumulatedResourceVolume = pool->IntegralResourcesState().AccumulatedVolume;
                result->PoolStates.emplace(poolId, std::move(state));
            }
        }
        return result;
    }

    void InitPersistentTreeState(const TPersistentTreeStatePtr& persistentTreeState) override
    {
        for (const auto& [poolName, poolState] : persistentTreeState->PoolStates) {
            auto poolIt = Pools_.find(poolName);
            if (poolIt != Pools_.end()) {
                if (poolIt->second->GetIntegralGuaranteeType() != EIntegralGuaranteeType::None) {
                    poolIt->second->InitAccumulatedResourceVolume(poolState->AccumulatedResourceVolume);
                } else {
                    YT_LOG_INFO("Pool is not integral and cannot accept integral resource volume (Pool: %v, Volume: %v)",
                        poolName,
                        poolState->AccumulatedResourceVolume);
                }
            } else {
                YT_LOG_INFO("Unknown pool in tree; dropping its integral resource volume (Pool: %v, Volume: %v)",
                    poolName,
                    poolState->AccumulatedResourceVolume);
            }
        }
    }

    ESchedulingSegment InitOperationSchedulingSegment(TOperationId operationId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);
        element->InitOrUpdateSchedulingSegment(Config_->SchedulingSegments->Mode);

        YT_VERIFY(element->SchedulingSegment());
        return *element->SchedulingSegment();
    }

    TTreeSchedulingSegmentsState GetSchedulingSegmentsState() const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return TreeSnapshotImpl_
            ? TreeSnapshotImpl_->SchedulingSegmentsState()
            : TTreeSchedulingSegmentsState{};
    }

    TOperationIdWithDataCenterList GetOperationSchedulingSegmentDataCenterUpdates() const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        TOperationIdWithDataCenterList result;
        for (const auto& [operationId, element] : OperationIdToElement_) {
            auto params = element->GetRuntimeParameters();
            const auto& dataCenter = element->PersistentAttributes().SchedulingSegmentDataCenter;
            if (params->SchedulingSegmentDataCenter != dataCenter) {
                result.push_back({operationId, dataCenter});
            }
        }

        return result;
    }

    void BuildOperationAttributes(TOperationId operationId, TFluentMap fluent) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);
        auto serializedParams = ConvertToAttributes(element->GetRuntimeParameters());
        fluent
            .Items(*serializedParams)
            .Item("pool").Value(element->GetParent()->GetId());
    }

    void BuildOperationProgress(TOperationId operationId, TFluentMap fluent) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        TSchedulerOperationElement* element = nullptr;
        if (TreeSnapshotImpl_) {
            if (auto elementFromSnapshot = TreeSnapshotImpl_->FindEnabledOperationElement(operationId)) {
                element = elementFromSnapshot;
            }
        }

        if (!element) {
            return;
        }

        DoBuildOperationProgress(element, fluent);
    }

    void BuildBriefOperationProgress(TOperationId operationId, TFluentMap fluent) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto* parent = element->GetParent();
        const auto& attributes = element->Attributes();
        fluent
            .Item("pool").Value(parent->GetId())
            .Item("weight").Value(element->GetWeight())
            .Item("fair_share_ratio").Value(MaxComponent(attributes.FairShare.Total))
            .Item("dominant_fair_share").Value(MaxComponent(attributes.FairShare.Total));
    }


    void BuildUserToEphemeralPoolsInDefaultPool(TFluentAny fluent) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        fluent
            .DoMapFor(UserToEphemeralPoolsInDefaultPool_, [] (TFluentMap fluent, const auto& pair) {
                const auto& [userName, ephemeralPools] = pair;
                fluent
                    .Item(userName).Value(ephemeralPools);
            });
    }

    void BuildStaticPoolsInformation(TFluentAny fluent) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        fluent
            .DoMapFor(Pools_, [&] (TFluentMap fluent, const auto& pair) {
                const auto& [poolName, pool] = pair;
                fluent
                    .Item(poolName).Value(pool->GetConfig());
            });
    }

    void BuildFairShareInfo(TFluentMap fluent) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        Y_UNUSED(WaitFor(BIND(&TFairShareTree::DoBuildFairShareInfo, MakeWeak(this), TreeSnapshotImpl_, fluent)
            .AsyncVia(StrategyHost_->GetOrchidWorkerInvoker())
            .Run()));
    }

    TResourceTree* GetResourceTree() override
    {
        return ResourceTree_.Get();
    }

    TFairShareTreeProfiler* GetProfiler()
    {
        return TreeProfiler_.Get();
    }

private:
    TFairShareStrategyTreeConfigPtr Config_;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig_;

    TResourceTreePtr ResourceTree_;
    TFairShareTreeProfilerPtr TreeProfiler_;

    ISchedulerStrategyHost* const StrategyHost_;

    std::vector<IInvokerPtr> FeasibleInvokers_;

    INodePtr LastPoolsNodeUpdate_;
    TError LastPoolsNodeUpdateError_;

    const TString TreeId_;

    const NLogging::TLogger Logger;

    TPoolElementMap Pools_;

    THashMap<TString, TString> UserToDefaultPoolMap_;

    std::optional<TInstant> LastFairShareUpdateTime_;

    THashMap<TString, THashSet<TString>> UserToEphemeralPoolsInDefaultPool_;

    THashMap<TString, THashSet<int>> PoolToSpareSlotIndices_;
    THashMap<TString, int> PoolToMinUnusedSlotIndex_;

    TOperationElementMap OperationIdToElement_;

    THashMap<TOperationId, TInstant> OperationIdToActivationTime_;
    THashMap<TOperationId, TInstant> OperationIdToFirstFoundLimitingAncestorTime_;

    std::vector<TOperationId> ActivatableOperationIds_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, NodeIdToLastPreemptiveSchedulingTimeLock_);
    THashMap<TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, RegisteredSchedulingTagFiltersLock_);
    std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilters_;
    std::vector<int> FreeSchedulingTagFilterIndexes_;

    struct TSchedulingTagFilterEntry
    {
        int Index;
        int Count;
    };
    THashMap<TSchedulingTagFilter, TSchedulingTagFilterEntry> SchedulingTagFilterToIndexAndCount_;

    TCachedJobPreemptionStatuses PersistentCachedJobPreemptionStatuses_;

    TSchedulerRootElementPtr RootElement_;

    class TFairShareTreeSnapshot
        : public IFairShareTreeSnapshot
    {
    public:
        TFairShareTreeSnapshot(
            TFairShareTreePtr tree,
            TFairShareTreeSnapshotImplPtr treeSnapshotImpl,
            TSchedulingTagFilter nodesFilter,
            const TJobResources& totalResourceLimits,
            const NLogging::TLogger& logger)
            : Tree_(std::move(tree))
            , TreeSnapshotImpl_(std::move(treeSnapshotImpl))
            , NodesFilter_(std::move(nodesFilter))
            , TotalResourceLimits_(totalResourceLimits)
            , Logger(logger)
        { }

        TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
        {
            return BIND(&TFairShareTree::DoScheduleJobs,
                Tree_,
                schedulingContext,
                TreeSnapshotImpl_)
                .AsyncVia(GetCurrentInvoker())
                .Run();
        }

        void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext) override
        {
            Tree_->DoPreemptJobsGracefully(schedulingContext, TreeSnapshotImpl_);
        }

        void ProcessUpdatedJob(
            TOperationId operationId,
            TJobId jobId,
            const TJobResources& jobResources,
            const TDataCenter& jobDataCenter,
            bool* shouldAbortJob) override
        {
            // NB: Should be filtered out on large clusters.
            YT_LOG_DEBUG("Processing updated job (OperationId: %v, JobId: %v, Resources: %v)", operationId, jobId, jobResources);

            *shouldAbortJob = false;

            auto* operationElement = TreeSnapshotImpl_->FindEnabledOperationElement(operationId);
            if (operationElement) {
                operationElement->SetJobResourceUsage(jobId, jobResources);

                const auto& operationSchedulingSegment = operationElement->SchedulingSegment();
                if (operationSchedulingSegment && IsDataCenterAwareSchedulingSegment(*operationSchedulingSegment)) {
                    const auto& operationDataCenter = operationElement->PersistentAttributes().SchedulingSegmentDataCenter;
                    bool jobIsRunningInTheRightDataCenter = operationDataCenter && operationDataCenter == jobDataCenter;
                    if (!jobIsRunningInTheRightDataCenter) {
                        *shouldAbortJob = true;

                        YT_LOG_DEBUG(
                            "Requested to abort job because it is running in a wrong data center "
                            "(OperationId: %v, JobId: %v, OperationDataCenter: %v, JobDataCenter: %v)",
                            operationId,
                            jobId,
                            operationDataCenter,
                            jobDataCenter);
                    }
                }
            }
        }

        void ProcessFinishedJob(TOperationId operationId, TJobId jobId) override
        {
            // NB: Should be filtered out on large clusters.
            YT_LOG_DEBUG("Processing finished job (OperationId: %v, JobId: %v)", operationId, jobId);
            auto* operationElement = TreeSnapshotImpl_->FindEnabledOperationElement(operationId);
            if (operationElement) {
                operationElement->OnJobFinished(jobId);
            }
        }

        bool HasOperation(TOperationId operationId) const override
        {
            return HasEnabledOperation(operationId) || HasDisabledOperation(operationId);
        }

        bool HasEnabledOperation(TOperationId operationId) const override
        {
            return TreeSnapshotImpl_->EnabledOperationMap().contains(operationId);
        }

        bool HasDisabledOperation(TOperationId operationId) const override
        {
            return TreeSnapshotImpl_->DisabledOperationMap().contains(operationId);
        }

        bool IsOperationRunningInTree(TOperationId operationId) const override
        {
            if (auto* element = TreeSnapshotImpl_->FindEnabledOperationElement(operationId)) {
                return element->IsOperationRunningInPool();
            }

            if (auto* element = TreeSnapshotImpl_->FindDisabledOperationElement(operationId)) {
                return element->IsOperationRunningInPool();
            }

            return false;
        }

        void ApplyJobMetricsDelta(const THashMap<TOperationId, TJobMetrics>& jobMetricsPerOperation) override
        {
            Tree_->GetProfiler()->ApplyJobMetricsDelta(TreeSnapshotImpl_, jobMetricsPerOperation);
        }

        const TFairShareStrategyTreeConfigPtr& GetConfig() const override
        {
            return TreeSnapshotImpl_->TreeConfig();
        }

        const TSchedulingTagFilter& GetNodesFilter() const override
        {
            return NodesFilter_;
        }

        TJobResources GetTotalResourceLimits() const override
        {
            return TotalResourceLimits_;
        }

        std::optional<TSchedulerElementStateSnapshot> GetMaybeStateSnapshotForPool(const TString& poolId) const override
        {
            if (auto* element = TreeSnapshotImpl_->FindPool(poolId)) {
                return TSchedulerElementStateSnapshot{
                    element->Attributes().DemandShare,
                    element->Attributes().PromisedFairShare};
            }

            return std::nullopt;
        }

        void BuildResourceMetering(TMeteringMap* meteringMap) const override
        {
            auto rootElement = TreeSnapshotImpl_->RootElement();
            rootElement->BuildResourceMetering(/* parentKey */ std::nullopt, meteringMap);
        }

        TCachedJobPreemptionStatuses GetCachedJobPreemptionStatuses() const override
        {
            return TreeSnapshotImpl_->CachedJobPreemptionStatuses();
        }

        void ProfileFairShare() const override
        {
            Tree_->DoProfileFairShare(TreeSnapshotImpl_);
        }

        void LogFairShare(NEventLog::TFluentLogEvent fluent) const override
        {
            Tree_->DoLogFairShare(TreeSnapshotImpl_, std::move(fluent));
        }

        void EssentialLogFairShare(NEventLog::TFluentLogEvent fluent) const override
        {
            Tree_->DoEssentialLogFairShare(TreeSnapshotImpl_, std::move(fluent));
        }

    private:
        const TIntrusivePtr<TFairShareTree> Tree_;
        const TFairShareTreeSnapshotImplPtr TreeSnapshotImpl_;
        const TSchedulingTagFilter NodesFilter_;
        const TJobResources TotalResourceLimits_;
        const NLogging::TLogger Logger;
    };

    TFairShareTreeSnapshotImplPtr TreeSnapshotImpl_;
    TFairShareTreeSnapshotImplPtr TreeSnapshotImplPrecommit_;

    TScheduleJobsStage NonPreemptiveSchedulingStage_;
    TScheduleJobsStage AggressivelyPreemptiveSchedulingStage_;
    TScheduleJobsStage PreemptiveSchedulingStage_;
    TScheduleJobsStage PackingFallbackSchedulingStage_;

    TEventTimer FairSharePreUpdateTimer_;
    TEventTimer FairShareUpdateTimer_;
    TEventTimer FairShareFluentLogTimer_;
    TEventTimer FairShareTextLogTimer_;

    std::atomic<TCpuInstant> LastSchedulingInformationLoggedTime_ = 0;

    std::pair<IFairShareTreeSnapshotPtr, TError> DoFairShareUpdateAt(TInstant now)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        ResourceTree_->PerformPostponedActions();

        NFairShare::TFairShareUpdateContext updateContext(
            /* totalResourceLimits */ StrategyHost_->GetResourceLimits(Config_->NodesFilter),
            Config_->MainResource,
            Config_->IntegralGuarantees->PoolCapacitySaturationPeriod,
            Config_->IntegralGuarantees->SmoothPeriod,
            now,
            LastFairShareUpdateTime_);

        THashMap<TDataCenter, TJobResources> resourceLimitsPerDataCenter;
        if (Config_->SchedulingSegments->Mode != ESegmentedSchedulingMode::Disabled) {
            for (const auto& dataCenter : Config_->SchedulingSegments->DataCenters) {
                auto tagFilter = GetNodesFilter() & TSchedulingTagFilter(MakeBooleanFormula(dataCenter));
                resourceLimitsPerDataCenter[dataCenter] = StrategyHost_->GetResourceLimits(tagFilter);
            }
        }

        TManageTreeSchedulingSegmentsContext manageSegmentsContext{
            .TreeConfig = Config_,
            .TotalResourceLimits = updateContext.TotalResourceLimits,
            .ResourceLimitsPerDataCenter = std::move(resourceLimitsPerDataCenter),
        };

        TFairSharePostUpdateContext fairSharePostUpdateContext;
        fairSharePostUpdateContext.Now = updateContext.Now;
        fairSharePostUpdateContext.CachedJobPreemptionStatuses = PersistentCachedJobPreemptionStatuses_;

        auto rootElement = RootElement_->Clone();
        {
            TEventTimerGuard timer(FairSharePreUpdateTimer_);
            rootElement->PreUpdate(&updateContext);
        }

        auto asyncUpdate = BIND([&]
            {
                TForbidContextSwitchGuard contextSwitchGuard;
                {
                    TEventTimerGuard timer(FairShareUpdateTimer_);

                    NFairShare::TFairShareUpdateExecutor updateExecutor(rootElement, &updateContext);
                    updateExecutor.Run();

                    rootElement->PostUpdate(&fairSharePostUpdateContext, &manageSegmentsContext);
                }
            })
            .AsyncVia(StrategyHost_->GetFairShareUpdateInvoker())
            .Run();
        WaitFor(asyncUpdate)
            .ThrowOnError();

        YT_LOG_DEBUG(
            "Fair share tree update finished "
            "(TreeSize: %v, SchedulableElementCount: %v, UnschedulableReasons: %v)",
            rootElement->GetTreeSize(),
            rootElement->GetSchedulableElementCount(),
            fairSharePostUpdateContext.UnschedulableReasons);

        TError error;
        if (!updateContext.Errors.empty()) {
            error = TError("Found pool configuration issues during fair share update in tree %Qv", TreeId_)
                << TErrorAttribute("pool_tree", TreeId_)
                << std::move(updateContext.Errors);
        }

        // Update starvation flags for operations and pools.
        rootElement->UpdateStarvationAttributes(now, Config_->EnablePoolStarvation);

        // Copy persistent attributes back to the original tree.
        for (const auto& [operationId, element] : fairSharePostUpdateContext.EnabledOperationIdToElement) {
            if (auto originalElement = FindOperationElement(operationId)) {
                originalElement->PersistentAttributes() = element->PersistentAttributes();
            }
        }
        for (const auto& [poolName, element] : fairSharePostUpdateContext.PoolNameToElement) {
            if (auto originalElement = FindPool(poolName)) {
                originalElement->PersistentAttributes() = element->PersistentAttributes();
            }
        }
        RootElement_->PersistentAttributes() = rootElement->PersistentAttributes();
        PersistentCachedJobPreemptionStatuses_ = fairSharePostUpdateContext.CachedJobPreemptionStatuses;

        rootElement->MarkImmutable();

        auto treeSnapshotImpl = New<TFairShareTreeSnapshotImpl>(
            std::move(rootElement),
            std::move(fairSharePostUpdateContext.EnabledOperationIdToElement),
            std::move(fairSharePostUpdateContext.DisabledOperationIdToElement),
            std::move(fairSharePostUpdateContext.PoolNameToElement),
            fairSharePostUpdateContext.CachedJobPreemptionStatuses,
            Config_,
            ControllerConfig_,
            std::move(manageSegmentsContext.SchedulingSegmentsState));

        TreeSnapshotImplPrecommit_ = treeSnapshotImpl;
        LastFairShareUpdateTime_ = now;

        auto treeSnapshot = New<TFairShareTreeSnapshot>(
            this,
            std::move(treeSnapshotImpl),
            GetNodesFilter(),
            StrategyHost_->GetResourceLimits(GetNodesFilter()),
            Logger);
        return std::make_pair(treeSnapshot, error);
    }

    void DoScheduleJobs(
        const ISchedulingContextPtr& schedulingContext,
        const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl)
    {
        bool enableSchedulingInfoLogging = false;
        auto now = schedulingContext->GetNow();
        const auto& config = treeSnapshotImpl->TreeConfig();
        if (LastSchedulingInformationLoggedTime_ + DurationToCpuDuration(config->HeartbeatTreeSchedulingInfoLogBackoff) < now) {
            enableSchedulingInfoLogging = true;
            LastSchedulingInformationLoggedTime_ = now;
        }

        std::vector<TSchedulingTagFilter> registeredSchedulingTagFilters;
        {
            auto guard = ReaderGuard(RegisteredSchedulingTagFiltersLock_);
            registeredSchedulingTagFilters = RegisteredSchedulingTagFilters_;
        }

        TScheduleJobsContext context(
            schedulingContext,
            treeSnapshotImpl->RootElement()->GetSchedulableElementCount(),
            std::move(registeredSchedulingTagFilters),
            enableSchedulingInfoLogging,
            Logger);

        context.SchedulingStatistics().ResourceUsage = schedulingContext->ResourceUsage();
        context.SchedulingStatistics().ResourceLimits = schedulingContext->ResourceLimits();

        bool needPackingFallback;
        {
            context.StartStage(&NonPreemptiveSchedulingStage_, "non_preemptive");
            DoScheduleJobsWithoutPreemption(treeSnapshotImpl, &context, now);
            context.SchedulingStatistics().NonPreemptiveScheduleJobAttempts = context.StageState()->ScheduleJobAttemptCount;
            needPackingFallback = schedulingContext->StartedJobs().empty() && !context.BadPackingOperations().empty();
            ReactivateBadPackingOperations(&context);
            context.FinishStage();
        }

        auto nodeId = schedulingContext->GetNodeDescriptor().Id;

        bool scheduleJobsWithPreemption = false;
        {
            bool nodeIsMissing = false;
            {
                auto guard = ReaderGuard(NodeIdToLastPreemptiveSchedulingTimeLock_);
                auto it = NodeIdToLastPreemptiveSchedulingTime_.find(nodeId);
                if (it == NodeIdToLastPreemptiveSchedulingTime_.end()) {
                    nodeIsMissing = true;
                    scheduleJobsWithPreemption = true;
                } else if (it->second + DurationToCpuDuration(config->PreemptiveSchedulingBackoff) <= now) {
                    scheduleJobsWithPreemption = true;
                    it->second = now;
                }
            }
            if (nodeIsMissing) {
                auto guard = WriterGuard(NodeIdToLastPreemptiveSchedulingTimeLock_);
                NodeIdToLastPreemptiveSchedulingTime_[nodeId] = now;
            }
        }

        context.SchedulingStatistics().ScheduleWithPreemption = scheduleJobsWithPreemption;
        if (scheduleJobsWithPreemption) {
            // First try to schedule a job with aggressive preemption for aggressively starving operations only.
            {
                context.StartStage(&AggressivelyPreemptiveSchedulingStage_, "aggressively_preemptive");
                DoScheduleJobsWithAggressivePreemption(treeSnapshotImpl, &context, now);
                context.SchedulingStatistics().AggressivelyPreemptiveScheduleJobAttempts = context.StageState()->ScheduleJobAttemptCount;
                context.FinishStage();
            }

            // If no jobs were scheduled in the previous stage, try to schedule a job with regular preemption.
            if (context.SchedulingStatistics().ScheduledDuringPreemption == 0) {
                context.StartStage(&PreemptiveSchedulingStage_, "preemptive");
                DoScheduleJobsWithPreemption(treeSnapshotImpl, &context, now);
                context.SchedulingStatistics().PreemptiveScheduleJobAttempts = context.StageState()->ScheduleJobAttemptCount;
                context.FinishStage();
            }
        } else {
            YT_LOG_DEBUG("Skip preemptive scheduling");
        }

        if (needPackingFallback) {
            context.StartStage(&PackingFallbackSchedulingStage_, "packing_fallback");
            DoScheduleJobsPackingFallback(treeSnapshotImpl, &context, now);
            context.SchedulingStatistics().PackingFallbackScheduleJobAttempts = context.StageState()->ScheduleJobAttemptCount;
            context.FinishStage();
        }

        // Interrupt some jobs if usage is greater that limit.
        if (schedulingContext->ShouldAbortJobsSinceResourcesOvercommit()) {
            YT_LOG_DEBUG("Interrupting jobs on node since resources are overcommitted (NodeId: %v, Address: %v)",
                schedulingContext->GetNodeDescriptor().Id,
                schedulingContext->GetNodeDescriptor().Address);

            std::vector<TJobWithPreemptionInfo> jobInfos;
            for (const auto& job : schedulingContext->RunningJobs()) {
                auto* operationElement = treeSnapshotImpl->FindEnabledOperationElement(job->GetOperationId());
                if (!operationElement || !operationElement->IsJobKnown(job->GetId())) {
                    YT_LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                        job->GetId(),
                        job->GetOperationId());
                    continue;
                }
                jobInfos.push_back(TJobWithPreemptionInfo{
                    .Job = job,
                    .IsPreemptable = operationElement->IsJobPreemptable(job->GetId(), /* aggressivePreemptionEnabled */ false),
                    .OperationElement = operationElement,
                });
            }

            auto hasCpuGap = [] (const TJobWithPreemptionInfo& jobWithPreemptionInfo)
            {
                return jobWithPreemptionInfo.Job->ResourceUsage().GetCpu() < jobWithPreemptionInfo.Job->ResourceLimits().GetCpu();
            };

            std::sort(
                jobInfos.begin(),
                jobInfos.end(),
                [&] (const TJobWithPreemptionInfo& lhs, const TJobWithPreemptionInfo& rhs) {
                    if (lhs.IsPreemptable != rhs.IsPreemptable) {
                        return lhs.IsPreemptable < rhs.IsPreemptable;
                    }

                    if (!lhs.IsPreemptable) {
                        // Save jobs without cpu gap.
                        bool lhsHasCpuGap = hasCpuGap(lhs);
                        bool rhsHasCpuGap = hasCpuGap(rhs);
                        if (lhsHasCpuGap != rhsHasCpuGap) {
                            return lhsHasCpuGap > rhsHasCpuGap;
                        }
                    }

                    return lhs.Job->GetStartTime() < rhs.Job->GetStartTime();
                }
            );

            auto currentResources = TJobResources();
            for (const auto& jobInfo : jobInfos) {
                if (!Dominates(schedulingContext->ResourceLimits(), currentResources + jobInfo.Job->ResourceUsage())) {
                    YT_LOG_DEBUG("Interrupt job since node resources are overcommitted (JobId: %v, OperationId: %v)",
                        jobInfo.Job->GetId(),
                        jobInfo.OperationElement->GetId());
                    PreemptJob(jobInfo.Job, jobInfo.OperationElement, treeSnapshotImpl, schedulingContext);
                } else {
                    currentResources += jobInfo.Job->ResourceUsage();
                }
            }
        }

        schedulingContext->SetSchedulingStatistics(context.SchedulingStatistics());
    }

    void DoScheduleJobsWithoutPreemption(
        const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl,
        TScheduleJobsContext* context,
        TCpuInstant startTime)
    {
        YT_LOG_TRACE("Scheduling new jobs");

        DoScheduleJobsWithoutPreemptionImpl(
            treeSnapshotImpl,
            context,
            startTime,
            /* ignorePacking */ false,
            /* oneJobOnly */ false);
    }

    void DoScheduleJobsPackingFallback(
        const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl,
        TScheduleJobsContext* context,
        TCpuInstant startTime)
    {
        YT_LOG_TRACE("Scheduling jobs with packing ignored");

        // Schedule at most one job with packing ignored in case all operations have rejected the heartbeat.
        DoScheduleJobsWithoutPreemptionImpl(
            treeSnapshotImpl,
            context,
            startTime,
            /* ignorePacking */ true,
            /* oneJobOnly */ true);
    }

    void DoScheduleJobsWithoutPreemptionImpl(
        const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl,
        TScheduleJobsContext* context,
        TCpuInstant startTime,
        bool ignorePacking,
        bool oneJobOnly)
    {
        auto& rootElement = treeSnapshotImpl->RootElement();
        const auto& controllerConfig = treeSnapshotImpl->ControllerConfig();

        {
            TCpuInstant schedulingDeadline = startTime + DurationToCpuDuration(controllerConfig->ScheduleJobsTimeout);

            TWallTimer scheduleTimer;
            while (context->SchedulingContext()->CanStartMoreJobs() && context->SchedulingContext()->GetNow() < schedulingDeadline)
            {
                if (!context->StageState()->PrescheduleExecuted) {
                    TWallTimer prescheduleTimer;
                    context->PrepareForScheduling(treeSnapshotImpl->RootElement());
                    rootElement->PrescheduleJob(context, EPrescheduleJobOperationCriterion::All);
                    context->StageState()->PrescheduleDuration = prescheduleTimer.GetElapsedTime();
                    context->StageState()->PrescheduleExecuted = true;
                }
                ++context->StageState()->ScheduleJobAttemptCount;
                auto scheduleJobResult = rootElement->ScheduleJob(context, ignorePacking);
                if (scheduleJobResult.Scheduled) {
                    ReactivateBadPackingOperations(context);
                }
                if (scheduleJobResult.Finished || (oneJobOnly && scheduleJobResult.Scheduled)) {
                    break;
                }
            }

            context->StageState()->TotalDuration = scheduleTimer.GetElapsedTime();
            context->ProfileStageTimingsAndLogStatistics();
        }
    }

    void DoScheduleJobsWithAggressivePreemption(
        const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl,
        TScheduleJobsContext* context,
        TCpuInstant startTime)
    {
        DoScheduleJobsWithPreemptionImpl(
            treeSnapshotImpl,
            context,
            startTime,
            /* isAggressive */ true);
    }

    void DoScheduleJobsWithPreemption(
        const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl,
        TScheduleJobsContext* context,
        TCpuInstant startTime)
    {
        DoScheduleJobsWithPreemptionImpl(
            treeSnapshotImpl,
            context,
            startTime,
            /* isAggressive */ false);
    }

    void DoScheduleJobsWithPreemptionImpl(
        const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl,
        TScheduleJobsContext* context,
        TCpuInstant startTime,
        bool isAggressive)
    {
        auto& rootElement = treeSnapshotImpl->RootElement();
        const auto& treeConfig = treeSnapshotImpl->TreeConfig();
        const auto& controllerConfig = treeSnapshotImpl->ControllerConfig();

        // NB: this method aims 2 goals relevant for scheduling with preemption
        // 1. Resets 'Active' attribute after scheduling without preemption (that is necessary for PrescheduleJob correctness).
        // 2. Initialize dynamic attributes and calculate local resource usages if scheduling without preemption was skipped.
        context->PrepareForScheduling(treeSnapshotImpl->RootElement());

        // TODO(ignat): move this logic inside TScheduleJobsContext.
        if (!context->GetHasAggressivelyStarvingElements()) {
            context->SetHasAggressivelyStarvingElements(rootElement->HasAggressivelyStarvingElements(context));
        }

        bool hasAggressivelyStarvingElements = *context->GetHasAggressivelyStarvingElements();

        context->SchedulingStatistics().HasAggressivelyStarvingElements = hasAggressivelyStarvingElements;
        if (isAggressive && !hasAggressivelyStarvingElements) {
            return;
        }

        // Compute discount to node usage.
        YT_LOG_TRACE("Looking for %v jobs",
            isAggressive ? "aggressively preemptable" : "preemptable");
        std::vector<TJobPtr> unconditionallyPreemptableJobs;
        TNonOwningJobSet forcefullyPreemptableJobs;
        int totalConditionallyPreemptableJobCount = 0;
        int maxConditionallyPreemptableJobCountInPool = 0;
        {
            NProfiling::TWallTimer timer;

            for (const auto& job : context->SchedulingContext()->RunningJobs()) {
                auto* operationElement = treeSnapshotImpl->FindEnabledOperationElement(job->GetOperationId());
                if (!operationElement || !operationElement->IsJobKnown(job->GetId())) {
                    YT_LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                        job->GetId(),
                        job->GetOperationId());
                    continue;
                }

                bool isJobForcefullyPreemptable = !operationElement->IsSchedulingSegmentCompatibleWithNode(
                    context->SchedulingContext()->GetSchedulingSegment(),
                    context->SchedulingContext()->GetNodeDescriptor().DataCenter);
                if (isJobForcefullyPreemptable) {
                    YT_ELEMENT_LOG_DETAILED(operationElement,
                        "Job is forcefully preemptable because it is running on a node in a different scheduling segment or data center "
                        "(JobId: %v, OperationId: %v, OperationSegment: %v, NodeSegment: %v, Address: %v, DataCenter: %v)",
                        job->GetId(),
                        operationElement->GetId(),
                        operationElement->SchedulingSegment(),
                        context->SchedulingContext()->GetSchedulingSegment(),
                        context->SchedulingContext()->GetNodeDescriptor().Address,
                        context->SchedulingContext()->GetNodeDescriptor().DataCenter);

                    forcefullyPreemptableJobs.insert(job.Get());
                }

                bool isAggressivePreemptionEnabled = isAggressive &&
                    operationElement->GetEffectiveAggressivePreemptionAllowed();
                bool isJobPreemptable = isJobForcefullyPreemptable ||
                    operationElement->IsJobPreemptable(job->GetId(), isAggressivePreemptionEnabled);
                if (!isJobPreemptable) {
                    continue;
                }

                auto preemptionBlockingAncestor = operationElement->FindPreemptionBlockingAncestor(
                    isAggressive,
                    context->DynamicAttributesList(),
                    treeConfig);
                bool isUnconditionalPreemptionAllowed = isJobForcefullyPreemptable ||
                    preemptionBlockingAncestor == nullptr;
                bool isConditionalPreemptionAllowed = Config_->EnableConditionalPreemption &&
                    !isUnconditionalPreemptionAllowed &&
                    preemptionBlockingAncestor != operationElement;

                if (isUnconditionalPreemptionAllowed) {
                    const auto* parent = operationElement->GetParent();
                    while (parent) {
                        context->LocalUnconditionalUsageDiscountMap()[parent->GetTreeIndex()] += job->ResourceUsage();
                        parent = parent->GetParent();
                    }
                    context->SchedulingContext()->UnconditionalResourceUsageDiscount() += job->ResourceUsage();
                    unconditionallyPreemptableJobs.push_back(job);
                } else if (isConditionalPreemptionAllowed) {
                    context->ConditionallyPreemptableJobSetMap()[preemptionBlockingAncestor->GetTreeIndex()].insert(job.Get());
                    ++totalConditionallyPreemptableJobCount;
                }
            }

            context->PrepareConditionalUsageDiscounts(treeSnapshotImpl->RootElement(), isAggressive);
            for (const auto& [_, jobSet] : context->ConditionallyPreemptableJobSetMap()) {
                maxConditionallyPreemptableJobCountInPool = std::max(
                    maxConditionallyPreemptableJobCountInPool,
                    static_cast<int>(jobSet.size()));
            }

            context->StageState()->AnalyzeJobsDuration += timer.GetElapsedTime();
        }

        context->SchedulingStatistics().UnconditionallyPreemptableJobCount = unconditionallyPreemptableJobs.size();
        context->SchedulingStatistics().UnconditionalResourceUsageDiscount = context->SchedulingContext()->UnconditionalResourceUsageDiscount();
        context->SchedulingStatistics().MaxConditionalResourceUsageDiscount = context->SchedulingContext()->GetMaxConditionalUsageDiscount();
        context->SchedulingStatistics().TotalConditionallyPreemptableJobCount = totalConditionallyPreemptableJobCount;
        context->SchedulingStatistics().MaxConditionallyPreemptableJobCountInPool = maxConditionallyPreemptableJobCountInPool;

        int startedBeforePreemption = context->SchedulingContext()->StartedJobs().size();

        // NB: Schedule at most one job with preemption.
        TJobPtr jobStartedUsingPreemption;
        {
            YT_LOG_TRACE(
                "Scheduling new jobs with preemption (UnconditionallyPreemptableJobs: %v, UnconditionalResourceUsageDiscount: %v, IsAggressive: %v)",
                unconditionallyPreemptableJobs.size(),
                FormatResources(context->SchedulingContext()->UnconditionalResourceUsageDiscount()),
                isAggressive);

            TCpuInstant schedulingDeadline = startTime + DurationToCpuDuration(controllerConfig->ScheduleJobsTimeout);

            TWallTimer timer;
            while (context->SchedulingContext()->CanStartMoreJobs() && context->SchedulingContext()->GetNow() < schedulingDeadline)
            {
                if (!context->StageState()->PrescheduleExecuted) {
                    TWallTimer prescheduleTimer;
                    rootElement->PrescheduleJob(
                        context,
                        isAggressive
                            ? EPrescheduleJobOperationCriterion::EligibleForAggressivelyPreemptiveSchedulingOnly
                            : EPrescheduleJobOperationCriterion::EligibleForPreemptiveSchedulingOnly);

                    context->StageState()->PrescheduleDuration = prescheduleTimer.GetElapsedTime();
                    context->StageState()->PrescheduleExecuted = true;
                }

                ++context->StageState()->ScheduleJobAttemptCount;
                auto scheduleJobResult = rootElement->ScheduleJob(context, /* ignorePacking */ true);
                if (scheduleJobResult.Scheduled) {
                    jobStartedUsingPreemption = context->SchedulingContext()->StartedJobs().back();
                    break;
                }
                if (scheduleJobResult.Finished) {
                    break;
                }
            }

            context->StageState()->TotalDuration = timer.GetElapsedTime();
            context->ProfileStageTimingsAndLogStatistics();
        }

        int startedAfterPreemption = context->SchedulingContext()->StartedJobs().size();

        context->SchedulingStatistics().ScheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

        // Collect conditionally preemptable jobs.
        TNonOwningJobSet conditionallyPreemptableJobs;
        if (jobStartedUsingPreemption) {
            auto* operationElement = treeSnapshotImpl->FindEnabledOperationElement(jobStartedUsingPreemption->GetOperationId());
            YT_VERIFY(operationElement);

            auto* parent = operationElement->GetParent();
            while (parent) {
                const auto& parentConditionallyPreemptableJobs = context->GetConditionallyPreemptableJobsInPool(parent);
                conditionallyPreemptableJobs.insert(
                    parentConditionallyPreemptableJobs.begin(),
                    parentConditionallyPreemptableJobs.end());

                parent = parent->GetParent();
            }
        }

        std::vector<TJobPtr> preemptableJobs = std::move(unconditionallyPreemptableJobs);
        preemptableJobs.insert(preemptableJobs.end(), conditionallyPreemptableJobs.begin(), conditionallyPreemptableJobs.end());

        // Reset discounts.
        context->SchedulingContext()->ResetUsageDiscounts();
        context->LocalUnconditionalUsageDiscountMap().clear();
        context->ConditionallyPreemptableJobSetMap().clear();

        // Preempt jobs if needed.
        std::sort(
            preemptableJobs.begin(),
            preemptableJobs.end(),
            [] (const TJobPtr& lhs, const TJobPtr& rhs) {
                return lhs->GetStartTime() > rhs->GetStartTime();
            });

        auto findPoolWithViolatedLimitsForJob = [&] (const TJobPtr& job) -> const TSchedulerCompositeElement* {
            auto* operationElement = treeSnapshotImpl->FindEnabledOperationElement(job->GetOperationId());
            if (!operationElement) {
                return nullptr;
            }

            auto* parent = operationElement->GetParent();
            while (parent) {
                if (parent->AreResourceLimitsViolated()) {
                    return parent;
                }
                parent = parent->GetParent();
            }
            return nullptr;
        };

        auto findOperationElementForJob = [&] (const TJobPtr& job) -> TSchedulerOperationElement* {
            auto operationElement = treeSnapshotImpl->FindEnabledOperationElement(job->GetOperationId());
            if (!operationElement || !operationElement->IsJobKnown(job->GetId())) {
                YT_LOG_DEBUG("Dangling preemptable job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());

                return nullptr;
            }

            return operationElement;
        };

        int currentJobIndex = 0;
        for (; currentJobIndex < std::ssize(preemptableJobs); ++currentJobIndex) {
            if (Dominates(context->SchedulingContext()->ResourceLimits(), context->SchedulingContext()->ResourceUsage())) {
                break;
            }

            const auto& job = preemptableJobs[currentJobIndex];
            auto operationElement = findOperationElementForJob(job);
            if (!operationElement) {
                continue;
            }

            if (jobStartedUsingPreemption) {
                // TODO(eshcherbin): Rethink preemption reason format to allow more variable attributes easily.
                job->SetPreemptionReason(Format(
                    "Preempted to start job %v of operation %v during %v preemptive stage, "
                    "job was %v and %v preemptable",
                    jobStartedUsingPreemption->GetId(),
                    jobStartedUsingPreemption->GetOperationId(),
                    isAggressive ? "aggressively" : "normal",
                    forcefullyPreemptableJobs.contains(job.Get()) ? "forcefully" : "nonforcefully",
                    conditionallyPreemptableJobs.contains(job.Get()) ? "conditionally" : "unconditionally"));

                job->SetPreemptedFor(TPreemptedFor{
                    .JobId = jobStartedUsingPreemption->GetId(),
                    .OperationId = jobStartedUsingPreemption->GetOperationId(),
                });
            } else {
                job->SetPreemptionReason(Format("Node resource limits violated"));
            }
            PreemptJob(job, operationElement, treeSnapshotImpl, context->SchedulingContext());
        }

        for (; currentJobIndex < std::ssize(preemptableJobs); ++currentJobIndex) {
            const auto& job = preemptableJobs[currentJobIndex];
            if (conditionallyPreemptableJobs.contains(job.Get())) {
                // Only unconditionally preemptable jobs can be preempted to recover violated resource limits.
                continue;
            }

            auto operationElement = findOperationElementForJob(job);
            if (!operationElement) {
                continue;
            }

            if (!Dominates(operationElement->GetResourceLimits(), operationElement->GetInstantResourceUsage())) {
                job->SetPreemptionReason(Format("Preempted due to violation of resource limits of operation %v",
                    operationElement->GetId()));
                PreemptJob(job, operationElement, treeSnapshotImpl, context->SchedulingContext());
                continue;
            }

            if (auto violatedPool = findPoolWithViolatedLimitsForJob(job)) {
                job->SetPreemptionReason(Format("Preempted due to violation of limits on pool %v",
                    violatedPool->GetId()));
                PreemptJob(job, operationElement, treeSnapshotImpl, context->SchedulingContext());
            }
        }

        if (!Dominates(context->SchedulingContext()->ResourceLimits(), context->SchedulingContext()->ResourceUsage())) {
            YT_LOG_INFO("Resource usage exceeds node resource limits even after preemption (ResourceLimits: %v, ResourceUsage: %v, NodeId: %v, Address: %v)",
                FormatResources(context->SchedulingContext()->ResourceLimits()),
                FormatResources(context->SchedulingContext()->ResourceUsage()),
                context->SchedulingContext()->GetNodeDescriptor().Id,
                context->SchedulingContext()->GetNodeDescriptor().Address);
        }
    }

    void DoPreemptJobsGracefully(
        const ISchedulingContextPtr& schedulingContext,
        const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl)
    {
        const auto& treeConfig = treeSnapshotImpl->TreeConfig();

        YT_LOG_TRACE("Looking for gracefully preemptable jobs");
        for (const auto& job : schedulingContext->RunningJobs()) {
            if (job->GetPreemptionMode() != EPreemptionMode::Graceful || job->GetPreempted()) {
                continue;
            }

            auto* operationElement = treeSnapshotImpl->FindEnabledOperationElement(job->GetOperationId());

            if (!operationElement || !operationElement->IsJobKnown(job->GetId())) {
                YT_LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            if (operationElement->IsJobPreemptable(job->GetId(), /* aggressivePreemptionEnabled */ false)) {
                schedulingContext->PreemptJob(job, treeConfig->JobGracefulInterruptTimeout);
            }
        }
    }

    void PreemptJob(
        const TJobPtr& job,
        const TSchedulerOperationElementPtr& operationElement,
        const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl,
        const ISchedulingContextPtr& schedulingContext) const
    {
        const auto& treeConfig = treeSnapshotImpl->TreeConfig();

        schedulingContext->ResourceUsage() -= job->ResourceUsage();
        operationElement->SetJobResourceUsage(job->GetId(), TJobResources());
        job->ResourceUsage() = {};

        schedulingContext->PreemptJob(job, treeConfig->JobInterruptTimeout);
    }

    void DoRegisterPool(const TSchedulerPoolElementPtr& pool)
    {
        int index = RegisterSchedulingTagFilter(pool->GetSchedulingTagFilter());
        pool->SetSchedulingTagFilterIndex(index);
        YT_VERIFY(Pools_.emplace(pool->GetId(), pool).second);
        YT_VERIFY(PoolToMinUnusedSlotIndex_.emplace(pool->GetId(), 0).second);

        TreeProfiler_->RegisterPool(pool);
    }

    void RegisterPool(const TSchedulerPoolElementPtr& pool, const TSchedulerCompositeElementPtr& parent)
    {
        DoRegisterPool(pool);

        pool->AttachParent(parent.Get());

        YT_LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void ReconfigurePool(const TSchedulerPoolElementPtr& pool, const TPoolConfigPtr& config)
    {
        auto oldSchedulingTagFilter = pool->GetSchedulingTagFilter();
        pool->SetConfig(config);
        auto newSchedulingTagFilter = pool->GetSchedulingTagFilter();
        if (oldSchedulingTagFilter != newSchedulingTagFilter) {
            UnregisterSchedulingTagFilter(oldSchedulingTagFilter);
            int index = RegisterSchedulingTagFilter(newSchedulingTagFilter);
            pool->SetSchedulingTagFilterIndex(index);
        }
    }

    void UnregisterPool(const TSchedulerPoolElementPtr& pool)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto userName = pool->GetUserName();
        if (userName && pool->IsEphemeralInDefaultParentPool()) {
            EraseOrCrash(UserToEphemeralPoolsInDefaultPool_[*userName], pool->GetId());
        }

        UnregisterSchedulingTagFilter(pool->GetSchedulingTagFilterIndex());

        EraseOrCrash(PoolToMinUnusedSlotIndex_, pool->GetId());

        // Pool may be not presented in this map.
        PoolToSpareSlotIndices_.erase(pool->GetId());

        TreeProfiler_->UnregisterPool(pool);

        // We cannot use pool after erase because Pools may contain last alive reference to it.
        auto extractedPool = std::move(Pools_[pool->GetId()]);
        EraseOrCrash(Pools_, pool->GetId());

        extractedPool->SetNonAlive();
        auto parent = extractedPool->GetParent();
        extractedPool->DetachParent();

        YT_LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
            extractedPool->GetId(),
            parent->GetId());
    }

    TSchedulerPoolElementPtr GetOrCreatePool(const TPoolName& poolName, TString userName)
    {
        auto pool = FindPool(poolName.GetPool());
        if (pool) {
            return pool;
        }

        // Create ephemeral pool.
        auto poolConfig = New<TPoolConfig>();
        if (poolName.GetParentPool()) {
            auto parentPoolConfig = GetPool(*poolName.GetParentPool())->GetConfig();
            poolConfig->Mode = parentPoolConfig->EphemeralSubpoolConfig->Mode;
            poolConfig->MaxOperationCount = parentPoolConfig->EphemeralSubpoolConfig->MaxOperationCount;
            poolConfig->MaxRunningOperationCount = parentPoolConfig->EphemeralSubpoolConfig->MaxRunningOperationCount;
            poolConfig->ResourceLimits = parentPoolConfig->EphemeralSubpoolConfig->ResourceLimits;
        }
        pool = New<TSchedulerPoolElement>(
            StrategyHost_,
            this,
            poolName.GetPool(),
            poolConfig,
            /* defaultConfigured */ true,
            Config_,
            TreeId_,
            Logger);

        pool->SetUserName(userName);

        TSchedulerCompositeElement* parent;
        if (poolName.GetParentPool()) {
            parent = GetPool(*poolName.GetParentPool()).Get();
        } else {
            parent = GetDefaultParentPoolForUser(userName).Get();
            pool->SetEphemeralInDefaultParentPool();
            UserToEphemeralPoolsInDefaultPool_[userName].insert(poolName.GetPool());
        }

        RegisterPool(pool, parent);
        return pool;
    }

    bool TryAllocatePoolSlotIndex(const TString& poolName, int slotIndex)
    {
        auto& minUnusedIndex = GetOrCrash(PoolToMinUnusedSlotIndex_, poolName);
        auto& spareSlotIndices = PoolToSpareSlotIndices_[poolName];

        if (slotIndex >= minUnusedIndex) {
            // Mark all indices as spare except #slotIndex.
            for (int index = minUnusedIndex; index < slotIndex; ++index) {
                YT_VERIFY(spareSlotIndices.insert(index).second);
            }

            minUnusedIndex = slotIndex + 1;

            return true;
        } else {
            return spareSlotIndices.erase(slotIndex) == 1;
        }
    }

    int AllocateOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName)
    {
        if (auto currentSlotIndex = state->GetHost()->FindSlotIndex(TreeId_)) {
            // Revive case
            if (TryAllocatePoolSlotIndex(poolName, *currentSlotIndex)) {
                YT_LOG_DEBUG("Operation slot index reused (OperationId: %v, Pool: %v, SlotIndex: %v)",
                    state->GetHost()->GetId(),
                    poolName,
                    *currentSlotIndex);
                return *currentSlotIndex;
            }
            YT_LOG_ERROR("Failed to reuse slot index during revive (OperationId: %v, Pool: %v, SlotIndex: %v)",
                state->GetHost()->GetId(),
                poolName,
                *currentSlotIndex);
        }

        int newSlotIndex = UndefinedSlotIndex;
        auto it = PoolToSpareSlotIndices_.find(poolName);
        if (it == PoolToSpareSlotIndices_.end() || it->second.empty()) {
            auto& minUnusedIndex = GetOrCrash(PoolToMinUnusedSlotIndex_, poolName);
            newSlotIndex = minUnusedIndex;
            ++minUnusedIndex;
        } else {
            auto spareIndexIt = it->second.begin();
            newSlotIndex = *spareIndexIt;
            it->second.erase(spareIndexIt);
        }

        YT_LOG_DEBUG("Operation slot index allocated (OperationId: %v, Pool: %v, SlotIndex: %v)",
            state->GetHost()->GetId(),
            poolName,
            newSlotIndex);
        return newSlotIndex;
    }

    void ReleaseOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName)
    {
        auto slotIndex = state->GetHost()->FindSlotIndex(TreeId_);
        YT_VERIFY(slotIndex);
        state->GetHost()->ReleaseSlotIndex(TreeId_);

        auto it = PoolToSpareSlotIndices_.find(poolName);
        if (it == PoolToSpareSlotIndices_.end()) {
            YT_VERIFY(PoolToSpareSlotIndices_.emplace(poolName, THashSet<int>{*slotIndex}).second);
        } else {
            it->second.insert(*slotIndex);
        }

        YT_LOG_DEBUG("Operation slot index released (OperationId: %v, Pool: %v, SlotIndex: %v)",
            state->GetHost()->GetId(),
            poolName,
            *slotIndex);
    }

    int RegisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
    {
        if (filter.IsEmpty()) {
            return EmptySchedulingTagFilterIndex;
        }
        auto it = SchedulingTagFilterToIndexAndCount_.find(filter);
        if (it == SchedulingTagFilterToIndexAndCount_.end()) {
            int index;
            if (FreeSchedulingTagFilterIndexes_.empty()) {
                auto guard = WriterGuard(RegisteredSchedulingTagFiltersLock_);

                index = RegisteredSchedulingTagFilters_.size();
                RegisteredSchedulingTagFilters_.push_back(filter);
            } else {
                index = FreeSchedulingTagFilterIndexes_.back();
                FreeSchedulingTagFilterIndexes_.pop_back();

                {
                    auto guard = WriterGuard(RegisteredSchedulingTagFiltersLock_);
                    RegisteredSchedulingTagFilters_[index] = filter;
                }
            }
            SchedulingTagFilterToIndexAndCount_.emplace(filter, TSchedulingTagFilterEntry({index, 1}));
            return index;
        } else {
            ++it->second.Count;
            return it->second.Index;
        }
    }

    void UnregisterSchedulingTagFilter(int index)
    {
        if (index == EmptySchedulingTagFilterIndex) {
            return;
        }

        TSchedulingTagFilter filter;
        {
            auto guard = ReaderGuard(RegisteredSchedulingTagFiltersLock_);
            filter = RegisteredSchedulingTagFilters_[index];
        }

        UnregisterSchedulingTagFilter(filter);
    }

    void UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
    {
        if (filter.IsEmpty()) {
            return;
        }
        auto it = SchedulingTagFilterToIndexAndCount_.find(filter);
        YT_VERIFY(it != SchedulingTagFilterToIndexAndCount_.end());
        --it->second.Count;
        if (it->second.Count == 0) {
            {
                auto guard = WriterGuard(RegisteredSchedulingTagFiltersLock_);
                RegisteredSchedulingTagFilters_[it->second.Index] = EmptySchedulingTagFilter;
            }

            FreeSchedulingTagFilterIndexes_.push_back(it->second.Index);
            SchedulingTagFilterToIndexAndCount_.erase(it);
        }
    }

    void OnOperationRemovedFromPool(
        const TFairShareStrategyOperationStatePtr& state,
        const TSchedulerOperationElementPtr& element,
        const TSchedulerCompositeElementPtr& parent)
    {
        auto operationId = state->GetHost()->GetId();
        ReleaseOperationSlotIndex(state, parent->GetId());

        if (element->IsOperationRunningInPool()) {
            CheckOperationsPendingByPool(parent.Get());
        } else if (auto blockedPoolName = element->PendingByPool()) {
            if (auto blockedPool = FindPool(*blockedPoolName)) {
                blockedPool->PendingOperationIds().remove(operationId);
            }
        }

        // We must do this recursively cause when ephemeral pool parent is deleted, it also become ephemeral.
        RemoveEmptyEphemeralPoolsRecursive(parent.Get());
    }

    // Returns true if all pool constraints are satisfied.
    bool OnOperationAddedToPool(
        const TFairShareStrategyOperationStatePtr& state,
        const TSchedulerOperationElementPtr& operationElement)
    {
        auto violatedPool = FindPoolViolatingMaxRunningOperationCount(operationElement->GetMutableParent());
        if (!violatedPool) {
            operationElement->MarkOperationRunningInPool();
            return true;
        }
        operationElement->MarkPendingBy(violatedPool);

        StrategyHost_->SetOperationAlert(
            state->GetHost()->GetId(),
            EOperationAlertType::OperationPending,
            TError("Max running operation count violated")
                << TErrorAttribute("pool", violatedPool->GetId())
                << TErrorAttribute("limit", violatedPool->GetMaxRunningOperationCount())
                << TErrorAttribute("pool_tree", TreeId_)
        );

        return false;
    }

    void RemoveEmptyEphemeralPoolsRecursive(TSchedulerCompositeElement* compositeElement)
    {
        if (!compositeElement->IsRoot() && compositeElement->IsEmpty()) {
            TSchedulerPoolElementPtr parentPool = static_cast<TSchedulerPoolElement*>(compositeElement);
            if (parentPool->IsDefaultConfigured()) {
                UnregisterPool(parentPool);
                RemoveEmptyEphemeralPoolsRecursive(parentPool->GetMutableParent());
            }
        }
    }

    void CheckOperationsPendingByPool(TSchedulerCompositeElement* pool)
    {
        auto* current = pool;
        while (current) {
            int availableOperationCount = current->GetAvailableRunningOperationCount();
            auto& pendingOperationIds = current->PendingOperationIds();
            auto it = pendingOperationIds.begin();
            while (it != pendingOperationIds.end() && availableOperationCount > 0) {
                auto pendingOperationId = *it;
                if (auto element = FindOperationElement(pendingOperationId)) {
                    YT_VERIFY(!element->IsOperationRunningInPool());
                    if (auto violatingPool = FindPoolViolatingMaxRunningOperationCount(element->GetMutableParent())) {
                        YT_VERIFY(current != violatingPool);
                        element->MarkPendingBy(violatingPool);
                    } else {
                        element->MarkOperationRunningInPool();
                        ActivatableOperationIds_.push_back(pendingOperationId);
                        --availableOperationCount;
                    }
                }
                auto toRemove = it++;
                pendingOperationIds.erase(toRemove);
            }

            current = current->GetMutableParent();
        }
    }

    TSchedulerCompositeElement* FindPoolViolatingMaxRunningOperationCount(TSchedulerCompositeElement* pool) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        while (pool) {
            if (pool->RunningOperationCount() >= pool->GetMaxRunningOperationCount()) {
                return pool;
            }
            pool = pool->GetMutableParent();
        }
        return nullptr;
    }

    const TSchedulerCompositeElement* FindPoolWithViolatedOperationCountLimit(const TSchedulerCompositeElementPtr& element) const
    {
        const TSchedulerCompositeElement* current = element.Get();
        while (current) {
            if (current->OperationCount() >= current->GetMaxOperationCount()) {
                return current;
            }
            current = current->GetParent();
        }
        return nullptr;
    }

    // Finds the lowest ancestor of |element| whose resource limits are too small to satisfy |neededResources|.
    const TSchedulerElement* FindAncestorWithInsufficientResourceLimits(const TSchedulerElement* element, const TJobResources& neededResources) const
    {
        const TSchedulerElement* current = element;
        while (current) {
            if (!Dominates(current->GetResourceLimits(), neededResources)) {
                return current;
            }
            current = current->GetParent();
        }

        return nullptr;
    }

    TYPath GetPoolPath(const TSchedulerCompositeElementPtr& element) const
    {
        std::vector<TString> tokens;
        const auto* current = element.Get();
        while (!current->IsRoot()) {
            if (current->IsExplicit()) {
                tokens.push_back(current->GetId());
            }
            current = current->GetParent();
        }

        std::reverse(tokens.begin(), tokens.end());

        TYPath path = "/" + NYPath::ToYPathLiteral(TreeId_);
        for (const auto& token : tokens) {
            path.append('/');
            path.append(NYPath::ToYPathLiteral(token));
        }
        return path;
    }

    TSchedulerCompositeElementPtr GetDefaultParentPoolForUser(const TString& userName) const
    {
        if (Config_->UseUserDefaultParentPoolMap) {
            auto it = UserToDefaultPoolMap_.find(userName);
            if (it != UserToDefaultPoolMap_.end()) {
                const auto& userDefaultParentPoolName = it->second;
                if (auto pool = FindPool(userDefaultParentPoolName)) {
                    return pool;
                } else {
                    YT_LOG_INFO("User default parent pool is not registered in tree (PoolName: %v, UserName: %v)",
                        userDefaultParentPoolName,
                        userName);
                }
            }
        }

        auto defaultParentPoolName = Config_->DefaultParentPool;
        if (auto pool = FindPool(defaultParentPoolName)) {
            return pool;
        } else {
            YT_LOG_INFO("Default parent pool is not registered in tree (PoolName: %v)",
                defaultParentPoolName);
        }

        YT_LOG_INFO("Using %v as default parent pool", RootPoolName);

        return RootElement_;
    }

    TSchedulerCompositeElementPtr GetPoolOrParent(const TPoolName& poolName, const TString& userName) const
    {
        TSchedulerCompositeElementPtr pool = FindPool(poolName.GetPool());
        if (pool) {
            return pool;
        }
        if (!poolName.GetParentPool()) {
            return GetDefaultParentPoolForUser(userName);
        }
        pool = FindPool(*poolName.GetParentPool());
        if (!pool) {
            THROW_ERROR_EXCEPTION("Parent pool %Qv does not exist", poolName.GetParentPool());
        }
        return pool;
    }

    void ValidateAllOperationsCountsOnPoolChange(TOperationId operationId, const TPoolName& newPoolName) const
    {
        for (const auto* currentPool : GetPoolsToValidateOperationCountsOnPoolChange(operationId, newPoolName)) {
            if (currentPool->OperationCount() >= currentPool->GetMaxOperationCount()) {
                THROW_ERROR_EXCEPTION("Max operation count of pool %Qv violated", currentPool->GetId());
            }
            if (currentPool->RunningOperationCount() >= currentPool->GetMaxRunningOperationCount()) {
                THROW_ERROR_EXCEPTION("Max running operation count of pool %Qv violated", currentPool->GetId());
            }
        }
    }

    std::vector<const TSchedulerCompositeElement*> GetPoolsToValidateOperationCountsOnPoolChange(TOperationId operationId, const TPoolName& newPoolName) const
    {
        auto operationElement = GetOperationElement(operationId);

        std::vector<const TSchedulerCompositeElement*> poolsToValidate;
        const auto* pool = GetPoolOrParent(newPoolName, operationElement->GetUserName()).Get();
        while (pool) {
            poolsToValidate.push_back(pool);
            pool = pool->GetParent();
        }

        if (!operationElement->IsOperationRunningInPool()) {
            // Operation is pending, we must validate all pools.
            return poolsToValidate;
        }

        // Operation is running, we can validate only tail of new pools.
        std::vector<const TSchedulerCompositeElement*> oldPools;
        pool = operationElement->GetParent();
        while (pool) {
            oldPools.push_back(pool);
            pool = pool->GetParent();
        }

        while (!poolsToValidate.empty() && !oldPools.empty() && poolsToValidate.back() == oldPools.back()) {
            poolsToValidate.pop_back();
            oldPools.pop_back();
        }

        return poolsToValidate;
    }

    void ValidateOperationCountLimit(const TPoolName& poolName, const TString& userName) const
    {
        auto poolWithViolatedLimit = FindPoolWithViolatedOperationCountLimit(GetPoolOrParent(poolName, userName));
        if (poolWithViolatedLimit) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::TooManyOperations,
                "Limit for the number of concurrent operations %v for pool %Qv in tree %Qv has been reached",
                poolWithViolatedLimit->GetMaxOperationCount(),
                poolWithViolatedLimit->GetId(),
                TreeId_);
        }
    }

    void ValidateEphemeralPoolLimit(const IOperationStrategyHost* operation, const TPoolName& poolName) const
    {
        auto pool = FindPool(poolName.GetPool());
        if (pool) {
            return;
        }

        const auto& userName = operation->GetAuthenticatedUser();

        if (!poolName.GetParentPool()) {
            auto it = UserToEphemeralPoolsInDefaultPool_.find(userName);
            if (it == UserToEphemeralPoolsInDefaultPool_.end()) {
                return;
            }

            if (std::ssize(it->second) + 1 > Config_->MaxEphemeralPoolsPerUser) {
                THROW_ERROR_EXCEPTION("Limit for number of ephemeral pools %v for user %Qv in tree %Qv has been reached",
                    Config_->MaxEphemeralPoolsPerUser,
                    userName,
                    TreeId_);
            }
        }
    }

    void DoValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName) const
    {
        TSchedulerCompositeElementPtr pool = FindPool(poolName.GetPool());
        // NB: Check is not performed if operation is started in default or unknown pool.
        if (pool && pool->AreImmediateOperationsForbidden()) {
            THROW_ERROR_EXCEPTION("Starting operations immediately in pool %Qv is forbidden", poolName.GetPool());
        }

        if (!pool) {
            pool = GetPoolOrParent(poolName, operation->GetAuthenticatedUser());
        }

        StrategyHost_->ValidatePoolPermission(GetPoolPath(pool), operation->GetAuthenticatedUser(), EPermission::Use);
    }

    int GetPoolCount() const
    {
        return Pools_.size();
    }

    TSchedulerPoolElementPtr FindPool(const TString& id) const
    {
        auto it = Pools_.find(id);
        return it == Pools_.end() ? nullptr : it->second;
    }

    TSchedulerPoolElementPtr GetPool(const TString& id) const
    {
        auto pool = FindPool(id);
        YT_VERIFY(pool);
        return pool;
    }

    TSchedulerOperationElementPtr FindOperationElement(TOperationId operationId) const
    {
        auto it = OperationIdToElement_.find(operationId);
        return it == OperationIdToElement_.end() ? nullptr : it->second;
    }

    TSchedulerOperationElementPtr GetOperationElement(TOperationId operationId) const
    {
        auto element = FindOperationElement(operationId);
        YT_VERIFY(element);
        return element;
    }

    TSchedulerOperationElement* FindOperationElementInSnapshot(TOperationId operationId) const
    {
        if (TreeSnapshotImpl_) {
            if (auto element = TreeSnapshotImpl_->FindEnabledOperationElement(operationId)) {
                return element;
            }
        }
        return nullptr;
    }

    void ReactivateBadPackingOperations(TScheduleJobsContext* context)
    {
        for (const auto& operation : context->BadPackingOperations()) {
            // TODO(antonkikh): multiple activations can be implemented more efficiently.
            operation->ActivateOperation(context);
        }
        context->BadPackingOperations().clear();
    }

    void DoProfileFairShare(const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl) const
    {
        TreeProfiler_->ProfileElements(treeSnapshotImpl);
    }

    void DoLogFairShare(const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl, NEventLog::TFluentLogEvent fluent) const
    {
        {
            TEventTimerGuard timer(FairShareFluentLogTimer_);
            fluent
                .Item(EventLogPoolTreeKey).Value(TreeId_)
                .Do(BIND(&TFairShareTree::DoBuildFairShareInfo, Unretained(this), treeSnapshotImpl));
        }

        {
            TEventTimerGuard timer(FairShareTextLogTimer_);
            LogPoolsInfo(treeSnapshotImpl);
            LogOperationsInfo(treeSnapshotImpl);
        }
    }

    void DoEssentialLogFairShare(const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl, NEventLog::TFluentLogEvent fluent) const
    {
        {
            TEventTimerGuard timer(FairShareFluentLogTimer_);
            fluent
                .Item(EventLogPoolTreeKey).Value(TreeId_)
                .Do(BIND(&TFairShareTree::DoBuildEssentialFairShareInfo, Unretained(this), treeSnapshotImpl));
        }

        {
            TEventTimerGuard timer(FairShareTextLogTimer_);
            LogPoolsInfo(treeSnapshotImpl);
            LogOperationsInfo(treeSnapshotImpl);
        }
    }

    void LogOperationsInfo(const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl) const
    {
        auto doLogOperationsInfo = [&] (const auto& operationIdToElement) {
            // Using structured bindings directly in the for-statement causes an ICE in GCC build.
            for (const auto& [operationId, element] : operationIdToElement) {
                YT_LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                    element->GetLoggingString(),
                    operationId);
            }
        };

        doLogOperationsInfo(treeSnapshotImpl->EnabledOperationMap());
        doLogOperationsInfo(treeSnapshotImpl->DisabledOperationMap());
    }

    void LogPoolsInfo(const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl) const
    {
        for (const auto& [poolName, element] : treeSnapshotImpl->PoolMap()) {
            YT_LOG_DEBUG("FairShareInfo: %v (Pool: %v)",
                element->GetLoggingString(),
                poolName);
        }
    }

    void DoBuildFairShareInfo(const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl, TFluentMap fluent) const
    {
        if (!treeSnapshotImpl) {
            YT_LOG_DEBUG("Skipping construction of fair share info, since shapshot is not constructed yet");
            return;
        }

        YT_LOG_DEBUG("Constructing fair share info");

        auto buildOperationsInfo = [&] (TFluentMap fluent, const TNonOwningOperationElementMap::value_type& pair) {
            const auto& [operationId, element] = pair;
            fluent
                .Item(ToString(operationId)).BeginMap()
                    .Do(BIND(&TFairShareTree::DoBuildOperationProgress, Unretained(this), Unretained(element)))
                .EndMap();
        };

        fluent
            .Do(BIND(&TFairShareTree::DoBuildPoolsInformation, Unretained(this), treeSnapshotImpl))
            .Item("resource_distribution_info").BeginMap()
                .Do(BIND(&TSchedulerRootElement::BuildResourceDistributionInfo, treeSnapshotImpl->RootElement()))
            .EndMap()
            .Item("operations").BeginMap()
                .DoFor(treeSnapshotImpl->EnabledOperationMap(), buildOperationsInfo)
                .DoFor(treeSnapshotImpl->DisabledOperationMap(), buildOperationsInfo)
            .EndMap();
    }

    void DoBuildPoolsInformation(const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl, TFluentMap fluent) const
    {
        auto buildCompositeElementInfo = [&] (const TSchedulerCompositeElement* element, TFluentMap fluent) {
            const auto& attributes = element->Attributes();
            fluent
                .Item("running_operation_count").Value(element->RunningOperationCount())
                .Item("operation_count").Value(element->OperationCount())
                .Item("max_running_operation_count").Value(element->GetMaxRunningOperationCount())
                .Item("max_operation_count").Value(element->GetMaxOperationCount())
                .Item("forbid_immediate_operations").Value(element->AreImmediateOperationsForbidden())
                .Item("aggressive_starvation_enabled").Value(element->IsAggressiveStarvationEnabled())
                .Item("effective_aggressive_starvation_enabled").Value(element->GetEffectiveAggressiveStarvationEnabled())
                .Item("aggressive_preemption_allowed").Value(element->IsAggressivePreemptionAllowed())
                .Item("effective_aggressive_preemption_allowed").Value(element->GetEffectiveAggressivePreemptionAllowed())
                .Item("total_resource_flow_ratio").Value(attributes.TotalResourceFlowRatio)
                .Item("total_burst_ratio").Value(attributes.TotalBurstRatio)
                .DoIf(element->GetParent(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("parent").Value(element->GetParent()->GetId());
                })
                .Do(std::bind(&TFairShareTree::DoBuildElementYson, this, element, std::placeholders::_1));
        };

        auto buildPoolInfo = [&] (const TSchedulerPoolElement* pool, TFluentMap fluent) {
            const auto& id = pool->GetId();
            fluent
                .Item(id).BeginMap()
                    .Item("mode").Value(pool->GetMode())
                    .Item("is_ephemeral").Value(pool->IsDefaultConfigured())
                    .Item("integral_guarantee_type").Value(pool->GetIntegralGuaranteeType())
                    .DoIf(pool->GetIntegralGuaranteeType() != EIntegralGuaranteeType::None, [&] (TFluentMap fluent) {
                        auto burstRatio = pool->GetSpecifiedBurstRatio();
                        auto resourceFlowRatio = pool->GetSpecifiedResourceFlowRatio();
                        fluent
                            .Item("integral_pool_capacity").Value(pool->GetIntegralPoolCapacity())
                            .Item("specified_burst_ratio").Value(burstRatio)
                            .Item("specified_burst_guarantee_resources").Value(pool->GetTotalResourceLimits() * burstRatio)
                            .Item("specified_resource_flow_ratio").Value(resourceFlowRatio)
                            .Item("specified_resource_flow").Value(pool->GetTotalResourceLimits() * resourceFlowRatio)
                            .Item("accumulated_resource_ratio_volume").Value(pool->GetAccumulatedResourceRatioVolume())
                            .Item("accumulated_resource_volume").Value(pool->GetAccumulatedResourceVolume());
                        if (burstRatio > resourceFlowRatio + RatioComparisonPrecision) {
                            fluent.Item("estimated_burst_usage_duration_seconds").Value(
                                pool->GetAccumulatedResourceRatioVolume() / (burstRatio - resourceFlowRatio));
                        }
                    })
                    .DoIf(pool->GetMode() == ESchedulingMode::Fifo, [&] (TFluentMap fluent) {
                        fluent
                            .Item("fifo_sort_parameters").Value(pool->GetFifoSortParameters());
                    })
                    .Item("abc").Value(pool->GetConfig()->Abc)
                    .Do(std::bind(buildCompositeElementInfo, pool, std::placeholders::_1))
                .EndMap();
        };

        fluent
            .Item("pool_count").Value(GetPoolCount())
            .Item("pools").BeginMap()
                .DoFor(treeSnapshotImpl->PoolMap(), [&] (TFluentMap fluent, const TNonOwningPoolElementMap::value_type& pair) {
                    buildPoolInfo(pair.second, fluent);
                })
                .Item(RootPoolName).BeginMap()
                    .Do(std::bind(buildCompositeElementInfo, treeSnapshotImpl->RootElement().Get(), std::placeholders::_1))
                .EndMap()
            .EndMap();
    }

    void DoBuildOperationProgress(const TSchedulerOperationElement* element, TFluentMap fluent) const
    {
        auto* parent = element->GetParent();
        fluent
            .Item("pool").Value(parent->GetId())
            .Item("slot_index").Value(element->GetSlotIndex())
            .Item("scheduling_segment").Value(element->SchedulingSegment())
            .Item("scheduling_segment_data_center").Value(element->PersistentAttributes().SchedulingSegmentDataCenter)
            .Item("start_time").Value(element->GetStartTime())
            .Item("preemptable_job_count").Value(element->GetPreemptableJobCount())
            .Item("aggressively_preemptable_job_count").Value(element->GetAggressivelyPreemptableJobCount())
            .Item("fifo_index").Value(element->Attributes().FifoIndex)
            .Item("deactivation_reasons").Value(element->GetDeactivationReasons())
            .Item("min_needed_resources_unsatisfied_count").Value(element->GetMinNeededResourcesUnsatisfiedCount())
            .Item("detailed_min_needed_job_resources").BeginList()
                .DoFor(element->GetDetailedMinNeededJobResources(), [&] (TFluentList fluent, const TJobResourcesWithQuota& jobResourcesWithQuota) {
                    fluent.Item().Do([&] (TFluentAny fluent) {
                        StrategyHost_->SerializeResources(jobResourcesWithQuota, fluent.GetConsumer());
                    });
                })
            .EndList()
            .Item("tentative").Value(element->GetRuntimeParameters()->Tentative)
            .Item("starving_since").Value(element->GetStarvationStatus() != EStarvationStatus::NonStarving
                ? std::make_optional(element->GetLastNonStarvingTime())
                : std::nullopt)
            .Do(BIND(&TFairShareTree::DoBuildElementYson, Unretained(this), Unretained(element)));
    }

    void DoBuildElementYson(const TSchedulerElement* element, TFluentMap fluent) const
    {
        const auto& attributes = element->Attributes();
        const auto& persistentAttributes = element->PersistentAttributes();

        auto promisedFairShareResources = element->GetTotalResourceLimits() * attributes.PromisedFairShare;

        // TODO(eshcherbin): Rethink which fields should be here and which should in in |TSchedulerElement::BuildYson|.
        // Also rethink which scalar fields should be exported to Orchid.
        fluent
            .Item("scheduling_status").Value(element->GetStatus())
            .Item("starvation_status").Value(element->GetStarvationStatus())
            .Item("fair_share_starvation_tolerance").Value(element->GetSpecifiedFairShareStarvationTolerance())
            .Item("fair_share_starvation_timeout").Value(element->GetSpecifiedFairShareStarvationTimeout())
            .Item("effective_fair_share_starvation_tolerance").Value(element->GetEffectiveFairShareStarvationTolerance())
            .Item("effective_fair_share_starvation_timeout").Value(element->GetEffectiveFairShareStarvationTimeout())
            .DoIf(element->IsEligibleForPreemptiveScheduling(/*isAggressive*/ true), [&] (TFluentMap fluent) {
                YT_VERIFY(element->GetLowestAggressivelyStarvingAncestor());
                fluent.Item("lowest_aggressively_starving_ancestor").Value(element->GetLowestAggressivelyStarvingAncestor()->GetId());
            })
            .DoIf(element->IsEligibleForPreemptiveScheduling(/*isAggressive*/ false), [&] (TFluentMap fluent) {
                YT_VERIFY(element->GetLowestStarvingAncestor());
                fluent.Item("lowest_starving_ancestor").Value(element->GetLowestStarvingAncestor()->GetId());
            })
            .Item("weight").Value(element->GetWeight())
            .Item("max_share_ratio").Value(element->GetMaxShareRatio())
            .Item("dominant_resource").Value(attributes.DominantResource)

            .Item("resource_usage").Value(element->GetResourceUsageAtUpdate())
            .Item("usage_share").Value(attributes.UsageShare)
            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("usage_ratio").Value(element->GetResourceDominantUsageShareAtUpdate())
            .Item("dominant_usage_share").Value(element->GetResourceDominantUsageShareAtUpdate())

            .Item("resource_demand").Value(element->GetResourceDemand())
            .Item("demand_share").Value(attributes.DemandShare)
            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("demand_ratio").Value(MaxComponent(attributes.DemandShare))
            .Item("dominant_demand_share").Value(MaxComponent(attributes.DemandShare))

            .Item("resource_limits").Value(element->GetResourceLimits())
            .Item("limits_share").Value(attributes.LimitsShare)
            .Item("scheduling_tag_filter_resource_limits").Value(element->GetSchedulingTagFilterResourceLimits())

            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("min_share").Value(attributes.StrongGuaranteeShare)
            .Item("strong_guarantee_share").Value(attributes.StrongGuaranteeShare)
            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("min_share_resources").Value(element->GetSpecifiedStrongGuaranteeResources())
            .Item("strong_guarantee_resources").Value(element->GetSpecifiedStrongGuaranteeResources())
            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("effective_min_share_resources").Value(attributes.EffectiveStrongGuaranteeResources)
            .Item("effective_strong_guarantee_resources").Value(attributes.EffectiveStrongGuaranteeResources)
            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("min_share_ratio").Value(MaxComponent(attributes.StrongGuaranteeShare))

            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("fair_share_ratio").Value(MaxComponent(attributes.FairShare.Total))
            .Item("detailed_fair_share").Value(attributes.FairShare)
            .Item("detailed_dominant_fair_share").Do(std::bind(&NFairShare::SerializeDominant, attributes.FairShare, std::placeholders::_1))

            .Item("promised_fair_share").Value(attributes.PromisedFairShare)
            .Item("promised_dominant_fair_share").Value(MaxComponent(attributes.PromisedFairShare))
            .Item("promised_fair_share_resources").Value(promisedFairShareResources)

            .Item("proposed_integral_share").Value(attributes.ProposedIntegralShare)
            .Item("best_allocation_share").Value(persistentAttributes.BestAllocationShare)

            .Item("satisfaction_ratio").Value(attributes.SatisfactionRatio)
            .Item("local_satisfaction_ratio").Value(attributes.LocalSatisfactionRatio);
    }

    void DoBuildEssentialFairShareInfo(const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl, TFluentMap fluent) const
    {
        auto buildOperationsInfo = [&] (TFluentMap fluent, const TNonOwningOperationElementMap::value_type& pair) {
            const auto& [operationId, element] = pair;
            fluent
                .Item(ToString(operationId)).BeginMap()
                    .Do(BIND(&TFairShareTree::DoBuildEssentialOperationProgress, Unretained(this), Unretained(element)))
                .EndMap();
        };

        fluent
            .Do(BIND(&TFairShareTree::DoBuildEssentialPoolsInformation, Unretained(this), treeSnapshotImpl))
            .Item("operations").BeginMap()
                .DoFor(treeSnapshotImpl->EnabledOperationMap(), buildOperationsInfo)
                .DoFor(treeSnapshotImpl->DisabledOperationMap(), buildOperationsInfo)
            .EndMap();
    }

    void DoBuildEssentialPoolsInformation(const TFairShareTreeSnapshotImplPtr& treeSnapshotImpl, TFluentMap fluent) const
    {
        const auto& poolMap = treeSnapshotImpl->PoolMap();
        fluent
            .Item("pool_count").Value(poolMap.size())
            .Item("pools").DoMapFor(poolMap, [&] (TFluentMap fluent, const TNonOwningPoolElementMap::value_type& pair) {
                const auto& [poolName, pool] = pair;
                fluent
                    .Item(poolName).BeginMap()
                        .Do(BIND(&TFairShareTree::DoBuildEssentialElementYson, Unretained(this), Unretained(pool)))
                    .EndMap();
            });
    }

    void DoBuildEssentialOperationProgress(const TSchedulerOperationElement* element, TFluentMap fluent) const
    {
        fluent
            .Do(BIND(&TFairShareTree::DoBuildEssentialElementYson, Unretained(this), Unretained(element)));
    }

    void DoBuildEssentialElementYson(const TSchedulerElement* element, TFluentMap fluent) const
    {
        const auto& attributes = element->Attributes();

        fluent
            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("usage_ratio").Value(element->GetResourceDominantUsageShareAtUpdate())
            .Item("dominant_usage_share").Value(element->GetResourceDominantUsageShareAtUpdate())
            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("demand_ratio").Value(MaxComponent(attributes.DemandShare))
            .Item("dominant_demand_share").Value(MaxComponent(attributes.DemandShare))
            // COMPAT(ignat): remove it after UI and other tools migration.
            .Item("fair_share_ratio").Value(MaxComponent(attributes.FairShare.Total))
            .Item("dominant_fair_share").Value(MaxComponent(attributes.FairShare.Total))
            .Item("satisfaction_ratio").Value(attributes.SatisfactionRatio)
            .Item("dominant_resource").Value(attributes.DominantResource)
            .DoIf(element->IsOperation(), [&] (TFluentMap fluent) {
                fluent
                    .Item("resource_usage").Value(element->GetResourceUsageAtUpdate());
            });
    }

    DEFINE_SIGNAL_OVERRIDE(void(TOperationId), OperationRunning);
};

////////////////////////////////////////////////////////////////////////////////

ISchedulerTreePtr CreateFairShareTree(
    TFairShareStrategyTreeConfigPtr config,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    ISchedulerStrategyHost* strategyHost,
    std::vector<IInvokerPtr> feasibleInvokers,
    TString treeId)
{
    return New<TFairShareTree>(
        std::move(config),
        std::move(controllerConfig),
        strategyHost,
        std::move(feasibleInvokers),
        std::move(treeId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
