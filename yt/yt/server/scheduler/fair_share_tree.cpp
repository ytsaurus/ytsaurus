#include "fair_share_tree.h"

#include "fair_share_tree_element.h"
#include "fair_share_tree_allocation_scheduler.h"
#include "fair_share_tree_snapshot.h"
#include "persistent_scheduler_state.h"
#include "public.h"
#include "pools_config_parser.h"
#include "resource_tree.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "serialize.h"
#include "fair_share_strategy_operation_controller.h"
#include "fair_share_tree_profiling.h"
#include "fields_filter.h"
#include "helpers.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>
#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>
#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/library/vector_hdrf/fair_share_update.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;

using NVectorHdrf::TFairShareUpdateExecutor;
using NVectorHdrf::TFairShareUpdateContext;
using NVectorHdrf::SerializeDominant;
using NVectorHdrf::RatioComparisonPrecision;

////////////////////////////////////////////////////////////////////////////////

class TAccumulatedResourceUsageInfo
{
public:
    TAccumulatedResourceUsageInfo(
        bool accumulateUsageForPools,
        bool accumulateUsageForOperations)
        : AccumulateUsageForPools_(accumulateUsageForPools)
        , AccumulateUsageForOperations_(accumulateUsageForOperations)
        , LastLocalUpdateTime_(TInstant::Now())
    { }

    void Update(const TFairShareTreeSnapshotPtr& treeSnapshot, const TResourceUsageSnapshotPtr& resourceUsageSnapshot)
    {
        auto now = TInstant::Now();
        auto updatePeriod = treeSnapshot->TreeConfig()->AccumulatedResourceUsageUpdatePeriod;
        auto period = now - LastLocalUpdateTime_;

        if (AccumulateUsageForPools_) {
            for (const auto& [poolName, resourceUsage] : resourceUsageSnapshot->PoolToResourceUsage) {
                LocalPoolToAccumulatedResourceUsage_[poolName] += TResourceVolume(resourceUsage, period);
            }
        }
        if (AccumulateUsageForOperations_) {
            for (const auto& [operationId, resourceUsage] : resourceUsageSnapshot->OperationIdToResourceUsage) {
                LocalOperationIdToAccumulatedResourceUsage_[operationId] += TResourceVolume(resourceUsage, period);
            }
        }

        if (LastUpdateTime_ + updatePeriod < now) {
            auto guard = Guard(Lock_);
            if (AccumulateUsageForPools_) {
                for (const auto& [poolName, resourceVolume] : LocalPoolToAccumulatedResourceUsage_) {
                    PoolToAccumulatedResourceUsage_[poolName] += resourceVolume;
                }
            }
            if (AccumulateUsageForOperations_) {
                for (const auto& [operationId, resourceVolume] : LocalOperationIdToAccumulatedResourceUsage_) {
                    OperationIdToAccumulatedResourceUsage_[operationId] += resourceVolume;
                }
            }
            LocalPoolToAccumulatedResourceUsage_.clear();
            LocalOperationIdToAccumulatedResourceUsage_.clear();
            LastUpdateTime_ = now;
        }

        LastLocalUpdateTime_ = now;
    }

    THashMap<TString, TResourceVolume> ExtractPoolResourceUsages()
    {
        YT_VERIFY(AccumulateUsageForPools_);

        auto guard = Guard(Lock_);
        auto result = std::move(PoolToAccumulatedResourceUsage_);
        PoolToAccumulatedResourceUsage_.clear();
        return result;
    }

    THashMap<TOperationId, TResourceVolume> ExtractOperationResourceUsages()
    {
        YT_VERIFY(AccumulateUsageForOperations_);

        auto guard = Guard(Lock_);
        auto result = std::move(OperationIdToAccumulatedResourceUsage_);
        OperationIdToAccumulatedResourceUsage_.clear();
        return result;
    }

    TResourceVolume ExtractOperationResourceUsage(TOperationId operationId)
    {
        YT_VERIFY(AccumulateUsageForOperations_);

        auto guard = Guard(Lock_);

        TResourceVolume usage;
        auto it = OperationIdToAccumulatedResourceUsage_.find(operationId);
        if (it != OperationIdToAccumulatedResourceUsage_.end()) {
            usage = it->second;
        }
        OperationIdToAccumulatedResourceUsage_.erase(it);
        return usage;
    }


private:
    const bool AccumulateUsageForPools_;
    const bool AccumulateUsageForOperations_;

    // This maps is updated regularly from some thread pool, no parallel updates are possible.
    THashMap<TString, TResourceVolume> LocalPoolToAccumulatedResourceUsage_;
    THashMap<TOperationId, TResourceVolume> LocalOperationIdToAccumulatedResourceUsage_;
    TInstant LastLocalUpdateTime_;

    // This maps is updated rarely and accessed from Control thread.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TString, TResourceVolume> PoolToAccumulatedResourceUsage_;
    THashMap<TOperationId, TResourceVolume> OperationIdToAccumulatedResourceUsage_;
    TInstant LastUpdateTime_;
};

////////////////////////////////////////////////////////////////////////////////


TFairShareStrategyOperationState::TFairShareStrategyOperationState(
    IOperationStrategyHost* host,
    const TFairShareStrategyOperationControllerConfigPtr& config,
    int NodeShardCount)
    : Host_(host)
    , Controller_(New<TFairShareStrategyOperationController>(host, config, NodeShardCount))
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

static const auto EmptyListYsonString = BuildYsonStringFluently()
    .List(std::vector<TString>())
    .ToString();

////////////////////////////////////////////////////////////////////////////////

//! This class represents fair share tree.
//!
//! We maintain following entities:
//!
//!   * Actual tree, it contains the latest and consistent structure of pools and operations.
//!     This tree represented by fields #RootElement_, #OperationIdToElement_, #Pools_.
//!     Update of this tree performed in sequentual manner from #Control thread.
//!
//!   * Snapshot of the tree with scheduling attributes (fair share ratios, best leaf descendants et. c).
//!     It is built repeatedly from actual tree by taking snapshot and calculating scheduling attributes.
//!     Clones of this tree are used in heartbeats for scheduling. Also, element attributes from this tree
//!     are used in orchid, for logging and for profiling.
//!     This tree represented by #TreeSnapshot_.
//!     NB: elements of this tree may be invalidated by #Alive flag in resource tree. In this case element cannot be safely used
//!     (corresponding operation or pool can be already deleted from all other scheduler structures).
//!
//!   * Resource tree, it is thread safe tree that maintain shared attributes of tree elements.
//!     More details can be find at #TResourceTree.
class TFairShareTree
    : public IFairShareTree
    , public IFairShareTreeElementHost
    , public IFairShareTreeAllocationSchedulerHost
{
public:
    using TFairShareTreePtr = TIntrusivePtr<TFairShareTree>;

    TFairShareTree(
        TFairShareStrategyTreeConfigPtr config,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        IFairShareTreeHost* host,
        ISchedulerStrategyHost* strategyHost,
        const std::vector<IInvokerPtr>& feasibleInvokers,
        TString treeId)
        : Config_(std::move(config))
        , ConfigNode_(ConvertToNode(Config_))
        , ControllerConfig_(std::move(controllerConfig))
        , TreeId_(std::move(treeId))
        , Logger(StrategyLogger.WithTag("TreeId: %v", TreeId_))
        , Host_(host)
        , StrategyHost_(strategyHost)
        , ResourceTree_(New<TResourceTree>(Config_, feasibleInvokers))
        , TreeProfiler_(
            SchedulerProfiler
                .WithGlobal()
                .WithProducerRemoveSupport()
                .WithRequiredTag("tree", TreeId_))
        , TreeScheduler_(New<TFairShareTreeAllocationScheduler>(
            TreeId_,
            Logger,
            MakeWeak(this),
            Host_,
            StrategyHost_,
            Config_,
            TreeProfiler_))
        , TreeProfileManager_(New<TFairShareTreeProfileManager>(
            TreeProfiler_,
            Config_->SparsifyFairShareProfiling,
            strategyHost->GetFairShareProfilingInvoker(),
            TreeScheduler_))
        , FeasibleInvokers_(feasibleInvokers)
        , FairSharePreUpdateTimer_(TreeProfileManager_->GetProfiler().Timer("/fair_share_preupdate_time"))
        , FairShareUpdateTimer_(TreeProfileManager_->GetProfiler().Timer("/fair_share_update_time"))
        , FairShareFluentLogTimer_(TreeProfileManager_->GetProfiler().Timer("/fair_share_fluent_log_time"))
        , FairShareTextLogTimer_(TreeProfileManager_->GetProfiler().Timer("/fair_share_text_log_time"))
        , AccumulatedPoolResourceUsageForMetering_(
            /*accumulateUsageForPools*/ true,
            /*accumulateUsageForOperations*/ false)
        , AccumulatedOperationsResourceUsageForProfiling_(
            /*accumulateUsageForPools*/ false,
            /*accumulateUsageForOperations*/ true)
        , AccumulatedOperationsResourceUsageForLogging_(
            /*accumulateUsageForPools*/ false,
            /*accumulateUsageForOperations*/ true)
    {
        RootElement_ = New<TSchedulerRootElement>(StrategyHost_, this, Config_, TreeId_, Logger);

        TreeProfileManager_->RegisterPool(RootElement_);

        YT_LOG_INFO("Fair share tree created");
    }


    TFairShareStrategyTreeConfigPtr GetConfig() const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return Config_;
    }

    TFairShareStrategyTreeConfigPtr GetSnapshottedConfig() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        return treeSnapshot->TreeConfig();
    }

    bool UpdateConfig(const TFairShareStrategyTreeConfigPtr& config) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto configNode = ConvertToNode(config);
        if (AreNodesEqual(configNode, ConfigNode_)) {
            // Offload destroying config node.
            StrategyHost_->GetBackgroundInvoker()->Invoke(BIND([configNode = std::move(configNode)] { }));

            return false;
        }

        Config_ = config;
        ConfigNode_ = std::move(configNode);
        RootElement_->UpdateTreeConfig(Config_);
        ResourceTree_->UpdateConfig(Config_);

        TreeScheduler_->UpdateConfig(Config_);

        if (!FindPool(Config_->DefaultParentPool) && Config_->DefaultParentPool != RootPoolName) {
            auto error = TError("Default parent pool %Qv in tree %Qv is not registered", Config_->DefaultParentPool, TreeId_);
            StrategyHost_->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
        }

        YT_LOG_INFO("Tree has updated with new config");

        return true;
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
    TFuture<std::pair<IFairShareTreePtr, TError>> OnFairShareUpdateAt(TInstant now) override
    {
        return BIND(&TFairShareTree::DoFairShareUpdateAt, MakeStrong(this), now)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    void FinishFairShareUpdate() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_VERIFY(TreeSnapshotPrecommit_);

        auto oldTreeSnapshot = std::move(TreeSnapshot_);
        TreeSnapshot_ = std::move(TreeSnapshotPrecommit_);
        TreeSnapshotPrecommit_.Reset();

        AtomicTreeSnapshot_ = TreeSnapshot_;

        YT_LOG_DEBUG("Stored updated fair share tree snapshot");

        // Offload destroying previous tree snapshot.
        StrategyHost_->GetBackgroundInvoker()->Invoke(BIND([oldTreeSnapshot = std::move(oldTreeSnapshot)] { }));
    }

    bool HasOperation(TOperationId operationId) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    TRegistrationResult RegisterOperation(
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
            state,
            StrategyHost_,
            this,
            state->GetHost(),
            TreeId_,
            Logger);

        TreeScheduler_->RegisterOperation(operationElement.Get());

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

        YT_LOG_INFO("Operation element registered in tree (OperationId: %v, Pool: %v, MarkedAsRunning: %v)",
            operationId,
            poolName.ToString(),
            isRunningInPool);

        return TRegistrationResult{
            .AllowIdleCpuPolicy = operationElement->IsIdleCpuPolicyAllowed(),
        };
    }

    void UnregisterOperation(const TFairShareStrategyOperationStatePtr& state) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationId = state->GetHost()->GetId();
        auto operationElement = GetOperationElement(operationId);

        auto* pool = operationElement->GetMutableParent();

        // Profile finished operation.
        TreeProfileManager_->ProfileOperationUnregistration(pool, state->GetHost()->GetState());

        TreeScheduler_->DisableOperation(operationElement.Get(), /*markAsNonAlive*/ true);
        operationElement->DetachParent();

        ReleaseOperationSlotIndex(state, pool->GetId());
        OnOperationRemovedFromPool(state, operationElement, pool);

        TreeScheduler_->UnregisterOperation(operationElement.Get());

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

        TreeScheduler_->EnableOperation(operationElement.Get());
    }

    void DisableOperation(const TFairShareStrategyOperationStatePtr& state) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationElement = GetOperationElement(state->GetHost()->GetId());
        TreeScheduler_->DisableOperation(operationElement.Get(), /*markAsNonAlive*/ false);
        operationElement->GetMutableParent()->DisableChild(operationElement);
    }

    void ChangeOperationPool(
        TOperationId operationId,
        const TPoolName& newPool,
        bool ensureRunning) override
    {
        auto element = FindOperationElement(operationId);
        if (!element) {
            THROW_ERROR_EXCEPTION("Operation element for operation %Qv not found", operationId);
        }

        ChangeOperationPool(element, newPool, ensureRunning);
    }

    void ChangeOperationPool(
        const TSchedulerOperationElementPtr& element,
        const TPoolName& newPool,
        bool ensureRunning)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        bool operationWasRunning = element->IsOperationRunningInPool();

        auto state = element->GetFairShareStrategyOperationState();

        auto oldParent = element->GetMutableParent();
        auto newParent = GetOrCreatePool(newPool, state->GetHost()->GetAuthenticatedUser());

        ReleaseOperationSlotIndex(state, oldParent->GetId());

        int newSlotIndex = AllocateOperationSlotIndex(state, newParent->GetId());
        element->ChangeParent(newParent.Get(), newSlotIndex);
        state->GetHost()->SetSlotIndex(TreeId_, newSlotIndex);

        OnOperationRemovedFromPool(state, element, oldParent);
        bool isRunningInNewPool = OnOperationAddedToPool(state, element);
        if (ensureRunning) {
            YT_VERIFY(isRunningInNewPool);
        }

        if (!operationWasRunning && isRunningInNewPool) {
            OperationRunning_.Fire(element->GetOperationId());
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

    void RegisterAllocationsFromRevivedOperation(TOperationId operationId, std::vector<TAllocationPtr> allocations) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& element = FindOperationElement(operationId);
        TreeScheduler_->RegisterAllocationsFromRevivedOperation(element.Get(), std::move(allocations));
    }

    void RegisterNode(TNodeId nodeId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        ++NodeCount_;

        TreeScheduler_->RegisterNode(nodeId);
    }

    void UnregisterNode(TNodeId nodeId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        --NodeCount_;

        TreeScheduler_->UnregisterNode(nodeId);
    }

    const TString& GetId() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return TreeId_;
    }

    // TODO(eshcherbin): Move this method to tree scheduler?
    TError CheckOperationIsHung(
        TOperationId operationId,
        TDuration safeTimeout,
        int minScheduleAllocationCallAttempts,
        const THashSet<EDeactivationReason>& deactivationReasons,
        TDuration limitingAncestorSafeTimeout) override
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
            if (!element->IsAlive()) {
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

        bool hasMinNeededResources = !element->DetailedMinNeededAllocationResources().empty();
        auto aggregatedMinNeededResources = element->AggregatedMinNeededAllocationResources();
        bool shouldCheckLimitingAncestor = hasMinNeededResources &&
            Config_->EnableLimitingAncestorCheck &&
            element->IsLimitingAncestorCheckEnabled();
        if (shouldCheckLimitingAncestor) {
            auto it = OperationIdToFirstFoundLimitingAncestorTime_.find(operationId);
            if (auto* limitingAncestor = FindAncestorWithInsufficientSpecifiedResourceLimits(element, aggregatedMinNeededResources)) {
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
                    return TError(
                        "Operation has an ancestor whose specified resource limits are too small to satisfy "
                        "operation's minimum allocation resource demand")
                        << TErrorAttribute("safe_timeout", limitingAncestorSafeTimeout)
                        << TErrorAttribute("limiting_ancestor", limitingAncestor->GetId())
                        << TErrorAttribute("resource_limits", limitingAncestor->MaybeSpecifiedResourceLimits())
                        << TErrorAttribute("min_needed_resources", aggregatedMinNeededResources);
                }
            } else if (it != OperationIdToFirstFoundLimitingAncestorTime_.end()) {
                it->second = TInstant::Max();
            }
        }

        auto allocationSchedulerError = TFairShareTreeAllocationScheduler::CheckOperationIsHung(
            GetTreeSnapshot(),
            element,
            now,
            activationTime,
            safeTimeout,
            minScheduleAllocationCallAttempts,
            deactivationReasons);
        if (!allocationSchedulerError.IsOK()) {
            return allocationSchedulerError;
        }

        return TError();
    }

    void ProcessActivatableOperations() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    const TOffloadingSettings& GetOffloadingSettingsFor(const TString& poolName) const override
    {
        const TSchedulerCompositeElement* pool = FindPool(poolName).Get();
        if (!pool) {
            return EmptyOffloadingSettings;
        }

        while (!pool->IsRoot()) {
            const auto& offloadingSettings = pool->GetOffloadingSettings();
            if (!offloadingSettings.empty()) {
                return offloadingSettings;
            }
            pool = pool->GetParent();
        }

        return EmptyOffloadingSettings;
    }

    TPoolsUpdateResult UpdatePools(const INodePtr& poolsNode, bool forceUpdate) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (!forceUpdate && LastPoolsNodeUpdate_ && AreNodesEqual(LastPoolsNodeUpdate_, poolsNode)) {
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
                        updatePoolAction.ObjectId,
                        updatePoolAction.PoolConfig,
                        /*defaultConfigured*/ false,
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

                        ApplyEphemeralSubpoolConfig(defaultParent, pool->GetConfig());
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
                    ReconfigurePool(pool, updatePoolAction.PoolConfig, updatePoolAction.ObjectId);
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

        std::vector<TSchedulerPoolElementPtr> staleEphemeralPools;
        for (const auto& [poolName, pool] : Pools_) {
            if (pool->IsDefaultConfigured() && pool->GetId().Contains(TPoolName::Delimiter) && !pool->GetParent()->IsEphemeralHub()) {
                staleEphemeralPools.push_back(pool);
            }
        }
        for (const auto& pool : staleEphemeralPools) {
            YT_LOG_INFO("Stale user ephemeral pool found, moving all its operations to parent pool (EphemeralPool: %v, ParentPool: %v)",
                pool->GetId(),
                pool->GetParent()->GetId());
            for (const auto& operation : pool->GetChildOperations()) {
                ChangeOperationPool(
                    operation->GetOperationId(),
                    TPoolName(pool->GetParent()->GetId(), /*parent*/ std::nullopt),
                    /*ensureRunning*/ false);
            }
        }

        LastPoolsNodeUpdateError_ = TError();

        return {LastPoolsNodeUpdateError_, true};
    }

    TError ValidateUserToDefaultPoolMap(const THashMap<TString, TString>& userToDefaultPoolMap) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (!Config_->UseUserDefaultParentPoolMap) {
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
        ValidateAllOperationCountsOnPoolChange(operation->GetId(), newPoolName);
    }

    TFuture<void> ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return BIND(&TFairShareTree::DoValidateOperationPoolsCanBeUsed, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operation, poolName);
    }

    TError CheckOperationNecessaryResourceDemand(TOperationId operationId) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);

        std::vector<EJobResourceType> resourcesWithDemandViolations;
        #define XX(name, Name) \
            if (element->GetAggregatedInitialMinNeededResources().Get##Name() == 0 && \
                Config_->NecessaryResourcesForOperation.contains(EJobResourceType::Name)) \
            { \
                resourcesWithDemandViolations.push_back(EJobResourceType::Name); \
            }
        ITERATE_JOB_RESOURCES(XX)
        #undef XX

        if (resourcesWithDemandViolations.empty()) {
            return TError();
        }

        return TError("Operation has zero demand for resources which are necessary in tree %Qlv", GetId())
            << TErrorAttribute("necessary_resources_with_zero_demand", resourcesWithDemandViolations);
    }

    TPersistentTreeStatePtr BuildPersistentState() const override
    {
        auto result = New<TPersistentTreeState>();
        for (const auto& [poolId, pool] : Pools_) {
            if (pool->GetIntegralGuaranteeType() != EIntegralGuaranteeType::None) {
                auto state = New<TPersistentPoolState>();
                state->AccumulatedResourceVolume = pool->IntegralResourcesState().AccumulatedVolume;
                result->PoolStates.emplace(poolId, std::move(state));
            }
        }

        result->AllocationSchedulerState = TreeScheduler_->BuildPersistentState();

        return result;
    }

    void InitPersistentState(const TPersistentTreeStatePtr& persistentState) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        for (const auto& [poolName, poolState] : persistentState->PoolStates) {
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

        TreeScheduler_->InitPersistentState(persistentState->AllocationSchedulerState);
    }

    void OnOperationMaterialized(TOperationId operationId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);
        TreeScheduler_->OnOperationMaterialized(element.Get());
    }

    TError CheckOperationSchedulingInSeveralTreesAllowed(TOperationId operationId) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);
        return TreeScheduler_->CheckOperationSchedulingInSeveralTreesAllowed(element.Get());
    }

    std::vector<TString> GetAncestorPoolNames(const TSchedulerOperationElement* element) const
    {
        std::vector<TString> result;
        const auto* current = element->GetParent();
        while (!current->IsRoot()) {
            result.push_back(current->GetId());
            current = current->GetParent();
        }
        std::reverse(result.begin(), result.end());
        return result;
    }

    void BuildOperationAttributes(TOperationId operationId, TFluentMap fluent) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);

        fluent
            .Item("pool").Value(element->GetParent()->GetId())
            .Item("ancestor_pools").Value(GetAncestorPoolNames(element.Get()));
    }

    void BuildOperationProgress(TOperationId operationId, TFluentMap fluent) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (auto treeSnapshot = GetTreeSnapshot()) {
            if (auto element = treeSnapshot->FindEnabledOperationElement(operationId)) {
                DoBuildOperationProgress(treeSnapshot, element, StrategyHost_, fluent);
            }
        }
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

        Y_UNUSED(WaitFor(BIND(&TFairShareTree::DoBuildFullFairShareInfo, MakeWeak(this), GetTreeSnapshot(), fluent)
            .AsyncVia(StrategyHost_->GetOrchidWorkerInvoker())
            .Run()));
    }

    static IYPathServicePtr FromProducer(
        TExtendedYsonProducer<const TFieldsFilter&> producer)
    {
        return IYPathService::FromProducer(BIND(
            [producer{std::move(producer)}] (IYsonConsumer* consumer, const IAttributeDictionaryPtr& options) {
                TFieldsFilter filter{options};
                producer.Run(consumer, filter);
            }));
    }

    IYPathServicePtr GetOrchidService() const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto dynamicOrchidService = New<TCompositeMapService>();

        dynamicOrchidService->AddChild("operations_by_pool", New<TOperationsByPoolOrchidService>(MakeStrong(this))
            ->Via(StrategyHost_->GetOrchidWorkerInvoker()));

        dynamicOrchidService->AddChild("pools", New<TPoolsOrchidService>(MakeStrong(this))
            ->Via(StrategyHost_->GetOrchidWorkerInvoker()));

        dynamicOrchidService->AddChild("child_pools_by_pool", New<TChildPoolsByPoolOrchidService>(MakeStrong(this))
            ->Via(StrategyHost_->GetOrchidWorkerInvoker()));

        dynamicOrchidService->AddChild("operations", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            const auto buildOperationInfo = [&] (TFluentMap fluent, const TSchedulerOperationElement* const operation) {
                fluent
                    .Item(operation->GetId()).BeginMap()
                        .Do(BIND(
                            &TFairShareTree::DoBuildOperationProgress,
                            ConstRef(treeSnapshot),
                            Unretained(operation),
                            StrategyHost_))
                    .EndMap();
            };

            BuildYsonFluently(consumer).BeginMap()
                    .Do([&] (TFluentMap fluent) {
                        for (const auto& [operationId, operation] : treeSnapshot->EnabledOperationMap()) {
                            buildOperationInfo(fluent, operation);
                        }

                        for (const auto& [operationId, operation] : treeSnapshot->DisabledOperationMap()) {
                            buildOperationInfo(fluent, operation);
                        }
                    })
                .EndMap();
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("config", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).Value(treeSnapshot->TreeConfig());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("resource_usage", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).Value(treeSnapshot->ResourceUsage());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("resource_limits", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).Value(treeSnapshot->ResourceLimits());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("node_count", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).Value(treeSnapshot->NodeCount());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("pool_count", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

            BuildYsonFluently(consumer).Value(GetPoolCount());
        })));

        dynamicOrchidService->AddChild("resource_distribution_info", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).BeginMap()
                .Do(BIND(&TSchedulerRootElement::BuildResourceDistributionInfo, treeSnapshot->RootElement()))
            .EndMap();
        }))->Via(StrategyHost_->GetOrchidWorkerInvoker()));

        return dynamicOrchidService;
    }

    TResourceTree* GetResourceTree() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ResourceTree_.Get();
    }

    TFairShareTreeProfileManager* GetProfiler()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return TreeProfileManager_.Get();
    }

    void SetResourceUsageSnapshot(TResourceUsageSnapshotPtr snapshot)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (snapshot != nullptr) {
            ResourceUsageSnapshot_.Store(std::move(snapshot));
        } else {
            if (ResourceUsageSnapshot_.Acquire()) {
                ResourceUsageSnapshot_.Store(nullptr);
            }
        }
    }


private:
    TFairShareStrategyTreeConfigPtr Config_;
    INodePtr ConfigNode_;

    TFairShareStrategyOperationControllerConfigPtr ControllerConfig_;

    const TString TreeId_;
    const NLogging::TLogger Logger;

    IFairShareTreeHost* const Host_;
    ISchedulerStrategyHost* const StrategyHost_;

    TResourceTreePtr ResourceTree_;

    const NProfiling::TProfiler TreeProfiler_;
    TFairShareTreeAllocationSchedulerPtr TreeScheduler_;
    TFairShareTreeProfileManagerPtr TreeProfileManager_;

    const std::vector<IInvokerPtr> FeasibleInvokers_;


    INodePtr LastPoolsNodeUpdate_;
    TError LastPoolsNodeUpdateError_;

    TPoolElementMap Pools_;

    std::optional<TInstant> LastFairShareUpdateTime_;

    THashMap<TString, THashSet<TString>> UserToEphemeralPoolsInDefaultPool_;

    THashMap<TString, THashSet<int>> PoolToSpareSlotIndices_;
    THashMap<TString, int> PoolToMinUnusedSlotIndex_;

    TOperationElementMap OperationIdToElement_;

    THashMap<TOperationId, TInstant> OperationIdToActivationTime_;
    THashMap<TOperationId, TInstant> OperationIdToFirstFoundLimitingAncestorTime_;

    TAtomicIntrusivePtr<TResourceUsageSnapshot> ResourceUsageSnapshot_;

    std::vector<TOperationId> ActivatableOperationIds_;

    TSchedulerRootElementPtr RootElement_;

    // NB(eshcherbin): We have the set of nodes both in strategy and in tree allocation scheduler.
    // Here we only keep current node count to have it ready for snapshot.
    int NodeCount_ = 0;

    class TPoolsOrchidServiceBase
        : public TYPathServiceBase
        , public TSupportsList
        , public TSupportsExists
    {
    protected:
        explicit TPoolsOrchidServiceBase(TIntrusivePtr<const TFairShareTree> tree)
            : FairShareTree_(std::move(tree))
        { }


    private:
        TIntrusivePtr<const TFairShareTree> FairShareTree_;

        virtual IYPathServicePtr GetSelfServiceProducer(TFairShareTreeSnapshotPtr&& fairShareTreeSnapshot) = 0;

        TResolveResult ResolveSelf(
            const TYPath& path,
            const IYPathServiceContextPtr& context) final
        {
            if (context->GetMethod() == "List") {
                auto typedContext = New<TCtxList>(context, NRpc::THandlerInvocationOptions{});
                if (!typedContext->DeserializeRequest()) {
                    THROW_ERROR_EXCEPTION("Error deserializing request");
                }

                const auto& request = typedContext->Request();
                if (!request.has_attributes() || NYT::FromProto<TAttributeFilter>(request.attributes()).IsEmpty()) {
                    return TResolveResultHere{path};
                }
            }

            return TResolveResultThere{
                GetSelfServiceProducer(FairShareTree_->GetTreeSnapshotForOrchid()),
                path
            };
        }

        TResolveResult ResolveAttributes(
            const TYPath& path,
            const IYPathServiceContextPtr& context) final
        {
            return ResolveSelf("/@" + path, context);
        }

        virtual IYPathServicePtr GetRecursiveServiceProducer(TFairShareTreeSnapshotPtr&& fairShareTreeSnapshot, const TString& poolName) = 0;

        TResolveResult ResolveRecursive(
            const TYPath& path,
            const IYPathServiceContextPtr& context) final
        {
            auto fairShareTreeSnapshot = FairShareTree_->GetTreeSnapshotForOrchid();

            NYPath::TTokenizer tokenizer(path);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);

            const auto& poolName = tokenizer.GetLiteralValue();
            if (poolName != RootPoolName && !fairShareTreeSnapshot->PoolMap().contains(poolName)) {
                // TODO(omgronny): rewrite it properly
                if (context->GetMethod() == "Exists") {
                    return TResolveResultHere{path};
                }
                THROW_ERROR_EXCEPTION("Pool tree %Qv has no pool %Qv",
                    FairShareTree_->TreeId_,
                    poolName);
            }

            // TODO(pogorelov): May be support limit here
            return TResolveResultThere{
                GetRecursiveServiceProducer(std::move(fairShareTreeSnapshot), poolName),
                NYPath::TYPath(tokenizer.GetSuffix())
            };
        }

        bool DoInvoke(const IYPathServiceContextPtr& context) final
        {
            DISPATCH_YPATH_SERVICE_METHOD(List);
            DISPATCH_YPATH_SERVICE_METHOD(Exists);
            return TYPathServiceBase::DoInvoke(context);
        }

        void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) final
        {
            i64 limit = request->has_limit()
                ? request->limit()
                : DefaultVirtualChildLimit;

            if (limit <= 0) {
                THROW_ERROR_EXCEPTION("Invalid value for limit: %v", limit);
            }

            auto fairShareTreeSnapshot = FairShareTree_->GetTreeSnapshotForOrchid();

            bool incomplete = false;
            const auto& poolMap = fairShareTreeSnapshot->PoolMap();

            std::vector<TString> result;
            result.reserve(std::ssize(poolMap) + 1);
            result.push_back(RootPoolName);
            for (const auto& [name, _] : poolMap) {
                result.push_back(name);
            }

            // NB: We do not have many pools, so we can just sort all of it, without finding top min elements.
            std::sort(std::begin(result), std::end(result));
            if (std::ssize(result) > limit) {
                result.resize(limit);
                incomplete = true;
            }

            auto ysonString = BuildYsonStringFluently().BeginAttributes()
                    .DoIf(incomplete, [] (TFluentMap fluent) {
                        fluent.Item("incomplete").Value(true);
                    })
                .EndAttributes()
                .List(result);

            response->set_value(ysonString.ToString());
            context->Reply();
        }

        void ListRecursive(const TYPath& /*path*/, TReqList* /*request*/, TRspList* /*response*/, const TCtxListPtr& /*context*/) final
        {
            YT_ABORT();
        }

        void ListAttribute(const TYPath& /*path*/, TReqList* /*request*/, TRspList* response, const TCtxListPtr& context) final
        {
            response->set_value(EmptyListYsonString);
            context->Reply();
        }
    };

    class TPoolsOrchidService
        : public TPoolsOrchidServiceBase
    {
    public:
        explicit TPoolsOrchidService(TIntrusivePtr<const TFairShareTree> tree)
            : TPoolsOrchidServiceBase(std::move(tree))
        { }


    private:
        IYPathServicePtr GetSelfServiceProducer(TFairShareTreeSnapshotPtr&& fairShareTreeSnapshot) override
        {
            return TFairShareTree::FromProducer(BIND(
                [fairShareTreeSnapshot = std::move(fairShareTreeSnapshot)]
                (IYsonConsumer* consumer, const TFieldsFilter& filter) mutable {
                    BuildYsonFluently(consumer).BeginMap()
                        .Do(
                            std::bind(
                                &TFairShareTree::BuildPoolsInfo,
                                std::move(fairShareTreeSnapshot),
                                std::cref(filter),
                                std::placeholders::_1))
                    .EndMap();
                }));
        }

        IYPathServicePtr GetRecursiveServiceProducer(TFairShareTreeSnapshotPtr&& fairShareTreeSnapshot, const TString& poolName) override
        {
            return TFairShareTree::FromProducer(BIND(
                [fairShareTreeSnapshot = std::move(fairShareTreeSnapshot), poolName]
                (IYsonConsumer* consumer, const TFieldsFilter& filter) {
                    BuildYsonFluently(consumer).BeginMap()
                        .Do([&] (TFluentMap fluent) {
                            if (poolName == RootPoolName) {
                                TFairShareTree::BuildCompositeElementInfo(
                                    fairShareTreeSnapshot,
                                    fairShareTreeSnapshot->RootElement().Get(),
                                    filter,
                                    std::move(fluent));
                            } else {
                                auto* pool = GetOrCrash(fairShareTreeSnapshot->PoolMap(), poolName);
                                TFairShareTree::BuildPoolInfo(
                                    fairShareTreeSnapshot,
                                    pool,
                                    filter,
                                    std::move(fluent));
                            }
                        })
                    .EndMap();
                }));
        }
    };

    class TChildPoolsByPoolOrchidService
        : public TPoolsOrchidServiceBase
    {
    public:
        explicit TChildPoolsByPoolOrchidService(TIntrusivePtr<const TFairShareTree> tree)
            : TPoolsOrchidServiceBase{std::move(tree)}
        { }


    private:
        IYPathServicePtr GetSelfServiceProducer(TFairShareTreeSnapshotPtr&& fairShareTreeSnapshot) override
        {
            return TFairShareTree::FromProducer(BIND(
                [fairShareTreeSnapshot = std::move(fairShareTreeSnapshot)]
                (IYsonConsumer* consumer, const TFieldsFilter& filter) mutable {
                    BuildYsonFluently(consumer).BeginMap()
                        .Do(
                            std::bind(
                                &TFairShareTree::BuildChildPoolsByPoolInfos,
                                std::move(fairShareTreeSnapshot),
                                std::cref(filter),
                                std::placeholders::_1))
                    .EndMap();
                }));
        }

        IYPathServicePtr GetRecursiveServiceProducer(TFairShareTreeSnapshotPtr&& fairShareTreeSnapshot, const TString& poolName) override
        {
            return TFairShareTree::FromProducer(BIND(
                [fairShareTreeSnapshot = std::move(fairShareTreeSnapshot), poolName]
                (IYsonConsumer* consumer, const TFieldsFilter& filter) {
                    BuildYsonFluently(consumer).BeginMap()
                        .Do(
                            std::bind(
                                &TFairShareTree::BuildChildPoolsByPoolInfo,
                                std::move(fairShareTreeSnapshot),
                                std::cref(filter),
                                poolName,
                                std::placeholders::_1))
                    .EndMap();
                }));
        }
    };

    class TOperationsByPoolOrchidService
        : public TVirtualMapBase
    {
    public:
        explicit TOperationsByPoolOrchidService(TIntrusivePtr<const TFairShareTree> tree)
            : FairShareTree_{std::move(tree)}
        { }

        i64 GetSize() const final
        {
            VERIFY_INVOKER_AFFINITY(FairShareTree_->StrategyHost_->GetOrchidWorkerInvoker());

            auto fairShareTreeSnapshot = FairShareTree_->GetTreeSnapshotForOrchid();

            return std::ssize(fairShareTreeSnapshot->PoolMap());
        }

        std::vector<TString> GetKeys(const i64 limit) const final
        {
            VERIFY_INVOKER_AFFINITY(FairShareTree_->StrategyHost_->GetOrchidWorkerInvoker());

            if (!limit) {
                return {};
            }

            const auto fairShareTreeSnapshot = FairShareTree_->GetTreeSnapshotForOrchid();

            std::vector<TString> result;
            result.reserve(std::min(limit, std::ssize(fairShareTreeSnapshot->PoolMap())));

            for (const auto& [name, _] : fairShareTreeSnapshot->PoolMap()) {
                result.push_back(name);
                if (std::ssize(result) == limit) {
                    break;
                }
            }

            return result;
        }

        IYPathServicePtr FindItemService(const TStringBuf poolName) const final
        {
            VERIFY_INVOKER_AFFINITY(FairShareTree_->StrategyHost_->GetOrchidWorkerInvoker());

            const auto fairShareTreeSnapshot = FairShareTree_->GetTreeSnapshotForOrchid();

            const auto poolIterator = fairShareTreeSnapshot->PoolMap().find(poolName);
            if (poolIterator == std::cend(fairShareTreeSnapshot->PoolMap())) {
                return nullptr;
            }

            const auto& [_, element] = *poolIterator;
            const auto operations = element->GetChildOperations();

            auto operationsYson = BuildYsonStringFluently().BeginMap()
                    .Do([&] (TFluentMap fluent) {
                        for (const auto operation : operations) {
                            fluent
                                .Item(operation->GetId()).BeginMap()
                                    .Do(std::bind(
                                        &TFairShareTree::DoBuildOperationProgress,
                                        std::cref(fairShareTreeSnapshot),
                                        operation,
                                        FairShareTree_->StrategyHost_,
                                        std::placeholders::_1))
                                .EndMap();
                        }
                    })
                .EndMap();

            auto producer = TYsonProducer(BIND([yson = std::move(operationsYson)] (IYsonConsumer* consumer) {
                consumer->OnRaw(yson);
            }));

            return IYPathService::FromProducer(std::move(producer));
        }

    private:
        TIntrusivePtr<const TFairShareTree> FairShareTree_;
    };

    friend class TOperationsByPoolOrchidService;

    // Thread affinity: Control.
    TFairShareTreeSnapshotPtr TreeSnapshot_;
    // Thread affinity: any.
    TAtomicIntrusivePtr<TFairShareTreeSnapshot> AtomicTreeSnapshot_;

    TFairShareTreeSnapshotPtr TreeSnapshotPrecommit_;

    TEventTimer FairSharePreUpdateTimer_;
    TEventTimer FairShareUpdateTimer_;
    TEventTimer FairShareFluentLogTimer_;
    TEventTimer FairShareTextLogTimer_;

    // Used only in fair share logging invoker.
    mutable TTreeSnapshotId LastLoggedTreeSnapshotId_;

    mutable TAccumulatedResourceUsageInfo AccumulatedPoolResourceUsageForMetering_;
    mutable TAccumulatedResourceUsageInfo AccumulatedOperationsResourceUsageForProfiling_;
    mutable TAccumulatedResourceUsageInfo AccumulatedOperationsResourceUsageForLogging_;

    void ThrowOrchidIsNotReady() const
    {
        THROW_ERROR_EXCEPTION("Fair share tree orchid is not ready yet")
            << TErrorAttribute("tree_id", TreeId_);
    }

    TFairShareTreeSnapshotPtr GetTreeSnapshot() const noexcept override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return TreeSnapshot_;
    }

    TFairShareTreeSnapshotPtr GetAtomicTreeSnapshot() const noexcept
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return AtomicTreeSnapshot_.Acquire();
    }

    TFairShareTreeSnapshotPtr GetTreeSnapshotForOrchid() const
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();
        if (!treeSnapshot) {
            ThrowOrchidIsNotReady();
        }

        return treeSnapshot;
    }

    std::pair<IFairShareTreePtr, TError> DoFairShareUpdateAt(TInstant now)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        ResourceTree_->PerformPostponedActions();

        const auto resourceUsage = StrategyHost_->GetResourceUsage(GetNodesFilter());
        const auto resourceLimits = StrategyHost_->GetResourceLimits(GetNodesFilter());
        const int nodeCount = NodeCount_;
        TFairShareUpdateContext updateContext(
            resourceLimits,
            Config_->MainResource,
            Config_->IntegralGuarantees->PoolCapacitySaturationPeriod,
            Config_->IntegralGuarantees->SmoothPeriod,
            now,
            LastFairShareUpdateTime_);

        auto rootElement = RootElement_->Clone();
        {
            TEventTimerGuard timer(FairSharePreUpdateTimer_);
            rootElement->PreUpdate(&updateContext);
        }

        TFairSharePostUpdateContext fairSharePostUpdateContext{
            .TreeConfig = Config_,
            .Now = updateContext.Now,
        };
        auto allocationSchedulerPostUpdateContext = TreeScheduler_->CreatePostUpdateContext(rootElement.Get());

        auto asyncUpdate = BIND([&]
            {
                TForbidContextSwitchGuard contextSwitchGuard;
                {
                    TEventTimerGuard timer(FairShareUpdateTimer_);

                    TFairShareUpdateExecutor updateExecutor(rootElement, &updateContext);
                    updateExecutor.Run();

                    rootElement->PostUpdate(&fairSharePostUpdateContext);
                    rootElement->UpdateStarvationStatuses(now, fairSharePostUpdateContext.TreeConfig->EnablePoolStarvation);

                    TreeScheduler_->PostUpdate(&fairSharePostUpdateContext, &allocationSchedulerPostUpdateContext);
                }

                MaybeDelay(fairSharePostUpdateContext.TreeConfig->TestingOptions->DelayInsideFairShareUpdate);
            })
            .AsyncVia(StrategyHost_->GetFairShareUpdateInvoker())
            .Run();
        WaitFor(asyncUpdate)
            .ThrowOnError();

        YT_LOG_DEBUG(
            "Fair share tree update finished "
            "(TreeSize: %v, SchedulableElementCount: %v, UnschedulableReasons: %v)",
            rootElement->GetTreeSize(),
            rootElement->SchedulableElementCount(),
            fairSharePostUpdateContext.UnschedulableReasons);

        TError error;
        if (!updateContext.Errors.empty()) {
            error = TError("Found pool configuration issues during fair share update in tree %Qv", TreeId_)
                << TErrorAttribute("pool_tree", TreeId_)
                << std::move(updateContext.Errors);
        }

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

        rootElement->MarkImmutable();

        auto treeSnapshotId = TTreeSnapshotId::Create();
        auto treeSchedulingSnapshot = TreeScheduler_->CreateSchedulingSnapshot(&allocationSchedulerPostUpdateContext);
        auto treeSnapshot = New<TFairShareTreeSnapshot>(
            treeSnapshotId,
            std::move(rootElement),
            std::move(fairSharePostUpdateContext.EnabledOperationIdToElement),
            std::move(fairSharePostUpdateContext.DisabledOperationIdToElement),
            std::move(fairSharePostUpdateContext.PoolNameToElement),
            Config_,
            ControllerConfig_,
            resourceUsage,
            resourceLimits,
            nodeCount,
            std::move(treeSchedulingSnapshot));

        if (Config_->EnableResourceUsageSnapshot) {
            TreeScheduler_->OnResourceUsageSnapshotUpdate(treeSnapshot, ResourceUsageSnapshot_.Acquire());
        }

        YT_LOG_DEBUG("Fair share tree snapshot created (TreeSnapshotId: %v)", treeSnapshotId);

        TreeSnapshotPrecommit_ = std::move(treeSnapshot);
        LastFairShareUpdateTime_ = now;

        return std::pair(MakeStrong(this), error);
    }

    void DoRegisterPool(const TSchedulerPoolElementPtr& pool)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_VERIFY(Pools_.emplace(pool->GetId(), pool).second);
        YT_VERIFY(PoolToMinUnusedSlotIndex_.emplace(pool->GetId(), 0).second);

        TreeProfileManager_->RegisterPool(pool);
    }

    void RegisterPool(const TSchedulerPoolElementPtr& pool, const TSchedulerCompositeElementPtr& parent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        DoRegisterPool(pool);

        pool->AttachParent(parent.Get());

        YT_LOG_INFO("Pool registered (Pool: %v, Parent: %v, IsEphemeral: %v)",
            pool->GetId(),
            parent->GetId(),
            pool->IsDefaultConfigured());
    }

    void ReconfigurePool(
        const TSchedulerPoolElementPtr& pool,
        const TPoolConfigPtr& config,
        TGuid objectId)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        bool lightweightOperationsEnabledBefore = pool->GetEffectiveLightweightOperationsEnabled();

        pool->SetConfig(config);
        pool->SetObjectId(objectId);

        if (pool->GetConfig()->EphemeralSubpoolConfig.has_value()) {
            for (const auto& child : pool->EnabledChildren()) {
                auto* childPool = dynamic_cast<TSchedulerPoolElement*>(child.Get());

                if (childPool && childPool->IsDefaultConfigured()) {
                    ApplyEphemeralSubpoolConfig(pool, childPool->GetConfig());
                }
            }
        }

        if (lightweightOperationsEnabledBefore != pool->GetEffectiveLightweightOperationsEnabled()) {
            ReaccountLightweightRunningOperationsInPool(pool);
        }
    }

    void ReaccountLightweightRunningOperationsInPool(const TSchedulerPoolElementPtr& pool)
    {
        if (!pool->GetEffectiveLightweightOperationsEnabled()) {
            // We just increase the regular running operation count allowing overcommit.
            pool->IncreaseRunningOperationCount(pool->LightweightRunningOperationCount());
            pool->IncreaseLightweightRunningOperationCount(-pool->LightweightRunningOperationCount());

            return;
        }

        YT_VERIFY(pool->GetChildPoolCount() == 0);

        int lightweightRunningOperationCount = 0;
        std::vector<TSchedulerOperationElement*> pendingLightweightOperations;
        for (auto* operation : pool->GetChildOperations()) {
            if (!operation->IsLightweightEligible()) {
                continue;
            }

            if (operation->IsOperationRunningInPool()) {
                ++lightweightRunningOperationCount;
            } else {
                pendingLightweightOperations.push_back(operation);
            }
        }

        // NB(eshcherbin): Pending operations that can become running after this change will be processed in |TryRunAllPendingOperations|.
        pool->IncreaseRunningOperationCount(-lightweightRunningOperationCount);
        pool->IncreaseLightweightRunningOperationCount(lightweightRunningOperationCount);

        for (auto* operation : pendingLightweightOperations) {
            auto operationId = operation->GetOperationId();
            if (auto blockedPoolName = operation->PendingByPool()) {
                if (auto blockedPool = FindPool(*blockedPoolName)) {
                    blockedPool->PendingOperationIds().remove(operationId);
                }
            }

            operation->MarkOperationRunningInPool();
            OperationRunning_.Fire(operationId);
        }
    }

    void UnregisterPool(const TSchedulerPoolElementPtr& pool)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto userName = pool->GetUserName();
        if (userName && pool->IsEphemeralInDefaultParentPool()) {
            EraseOrCrash(UserToEphemeralPoolsInDefaultPool_[*userName], pool->GetId());
        }

        EraseOrCrash(PoolToMinUnusedSlotIndex_, pool->GetId());

        // Pool may be not presented in this map.
        PoolToSpareSlotIndices_.erase(pool->GetId());

        TreeProfileManager_->UnregisterPool(pool);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto pool = FindPool(poolName.GetPool());
        if (pool) {
            return pool;
        }

        // Create ephemeral pool.
        auto poolConfig = New<TPoolConfig>();

        TSchedulerCompositeElement* parent = poolName.GetParentPool()
            ? GetPool(*poolName.GetParentPool()).Get()
            : GetDefaultParentPoolForUser(userName).Get();

        ApplyEphemeralSubpoolConfig(parent, poolConfig);

        pool = New<TSchedulerPoolElement>(
            StrategyHost_,
            this,
            poolName.GetPool(),
            TGuid(),
            poolConfig,
            /*defaultConfigured*/ true,
            Config_,
            TreeId_,
            Logger);

        if (!poolName.GetParentPool()) {
            pool->SetEphemeralInDefaultParentPool();
            UserToEphemeralPoolsInDefaultPool_[userName].insert(poolName.GetPool());
        }

        pool->SetUserName(userName);

        RegisterPool(pool, parent);
        return pool;
    }

    void ApplyEphemeralSubpoolConfig(const TSchedulerCompositeElementPtr& parent, const TPoolConfigPtr& targetConfig)
    {
        if (parent->IsRoot()) {
            return;
        }

        auto* parentPool = dynamic_cast<TSchedulerPoolElement*>(parent.Get());
        YT_VERIFY(parentPool);
        auto maybeConfig = parentPool->GetConfig()->EphemeralSubpoolConfig;
        if (!maybeConfig.has_value()) {
            return;
        }

        const auto& source = *maybeConfig;
        targetConfig->Mode = source->Mode;
        targetConfig->MaxOperationCount = source->MaxOperationCount;
        targetConfig->MaxRunningOperationCount = source->MaxRunningOperationCount;
        targetConfig->ResourceLimits = source->ResourceLimits;
    }

    bool TryAllocatePoolSlotIndex(const TString& poolName, int slotIndex)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    void BuildElementLoggingStringAttributes(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override
    {
        TreeScheduler_->BuildElementLoggingStringAttributes(treeSnapshot, element, delimitedBuilder);
    }

    void OnOperationRemovedFromPool(
        const TFairShareStrategyOperationStatePtr& state,
        const TSchedulerOperationElementPtr& element,
        const TSchedulerCompositeElementPtr& parent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationId = state->GetHost()->GetId();
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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto violatedPool = FindPoolViolatingMaxRunningOperationCount(operationElement->GetMutableParent());
        if (operationElement->IsLightweight() || !violatedPool) {
            operationElement->MarkOperationRunningInPool();
            return true;
        }
        operationElement->MarkPendingBy(violatedPool);

        YT_UNUSED_FUTURE(StrategyHost_->SetOperationAlert(
            state->GetHost()->GetId(),
            EOperationAlertType::OperationPending,
            TError("Max running operation count violated")
                << TErrorAttribute("pool", violatedPool->GetId())
                << TErrorAttribute("limit", violatedPool->GetMaxRunningOperationCount())
                << TErrorAttribute("pool_tree", TreeId_)
        ));

        return false;
    }

    void RemoveEmptyEphemeralPoolsRecursive(TSchedulerCompositeElement* compositeElement)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
    const TSchedulerElement* FindAncestorWithInsufficientSpecifiedResourceLimits(
        const TSchedulerElement* element,
        const TJobResources& neededResources) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const TSchedulerElement* current = element;
        while (current) {
            // NB(eshcherbin): We expect that |GetSpecifiedResourcesLimits| return infinite limits when no limits were specified.
            if (const auto& specifiedLimits = current->MaybeSpecifiedResourceLimits();
                specifiedLimits && !Dominates(*specifiedLimits, neededResources))
            {
                return current;
            }
            current = current->GetParent();
        }

        return nullptr;
    }

    TSchedulerCompositeElementPtr GetDefaultParentPoolForUser(const TString& userName) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (Config_->UseUserDefaultParentPoolMap) {
            const auto& userToDefaultPoolMap = StrategyHost_->GetUserDefaultParentPoolMap();
            auto it = userToDefaultPoolMap.find(userName);
            if (it != userToDefaultPoolMap.end()) {
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

    void ActualizeEphemeralPoolParents(const THashMap<TString, TString>& userToDefaultPoolMap) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (!Config_->UseUserDefaultParentPoolMap) {
            return;
        }

        for (const auto& [_, ephemeralPools] : UserToEphemeralPoolsInDefaultPool_) {
            for (const auto& poolName : ephemeralPools) {
                auto ephemeralPool = GetOrCrash(Pools_, poolName);
                const auto& actualParentName = ephemeralPool->GetParent()->GetId();
                auto it = userToDefaultPoolMap.find(poolName);
                if (it != userToDefaultPoolMap.end() && it->second != actualParentName) {
                    const auto& configuredParentName = it->second;
                    auto newParent = FindPool(configuredParentName);
                    if (!newParent) {
                        YT_LOG_DEBUG(
                            "Configured parent of ephemeral pool not found; skipping (Pool: %v, ActualParent: %v, ConfiguredParent: %v)",
                            poolName,
                            actualParentName,
                            configuredParentName);
                    } else {
                        YT_LOG_DEBUG(
                            "Actual parent of ephemeral pool differs from configured by default parent pool map; will change parent (Pool: %v, ActualParent: %v, ConfiguredParent: %v)",
                            poolName,
                            actualParentName,
                            configuredParentName);
                        ephemeralPool->ChangeParent(newParent.Get());
                    }
                }
            }
        }
    }

    TSchedulerCompositeElementPtr GetPoolOrParent(const TPoolName& poolName, const TString& userName) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    void ValidateAllOperationCountsOnPoolChange(TOperationId operationId, const TPoolName& newPoolName) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationElement = GetOperationElement(operationId);
        auto newPoolElement = GetPoolOrParent(newPoolName, operationElement->GetUserName());
        bool lightweightInNewPool = operationElement->IsLightweightEligible() && newPoolElement->GetEffectiveLightweightOperationsEnabled();
        for (const auto* currentPool : GetPoolsToValidateOperationCountsOnPoolChange(operationElement, newPoolElement)) {
            if (currentPool->OperationCount() >= currentPool->GetMaxOperationCount()) {
                THROW_ERROR_EXCEPTION("Max operation count of pool %Qv violated", currentPool->GetId());
            }

            if (!lightweightInNewPool && currentPool->RunningOperationCount() >= currentPool->GetMaxRunningOperationCount()) {
                THROW_ERROR_EXCEPTION("Max running operation count of pool %Qv violated", currentPool->GetId());
            }
        }
    }

    std::vector<const TSchedulerCompositeElement*> GetPoolsToValidateOperationCountsOnPoolChange(
        const TSchedulerOperationElementPtr& operationElement,
        const TSchedulerCompositeElementPtr& newPoolElement) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        std::vector<const TSchedulerCompositeElement*> poolsToValidate;
        const auto* pool = newPoolElement.Get();
        while (pool) {
            poolsToValidate.push_back(pool);
            pool = pool->GetParent();
        }

        if (!operationElement->IsOperationRunningInPool() || operationElement->IsLightweight()) {
            // If operation is pending or lightweight, it isn't counted as running in any pool, so we must check all new ancestors.
            return poolsToValidate;
        }

        // Otherwise, the operation is already counted as running in the common ancestors, so we can skip those pools validation.
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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
                THROW_ERROR_EXCEPTION(
                    "Cannot create new ephemeral pool %Qv as limit for number of ephemeral pools %v for user %Qv in tree %Qv has been reached; "
                    "previously created pools are [%v]",
                    poolName.GetPool(),
                    Config_->MaxEphemeralPoolsPerUser,
                    userName,
                    TreeId_,
                    JoinToString(it->second));
            }
        }
    }

    void ValidateSpecifiedResourceLimits(
        const IOperationStrategyHost* operation,
        const TSchedulerCompositeElement* pool,
        const TJobResourcesConfigPtr& requiredLimitsConfig) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto requiredLimits = ToJobResources(requiredLimitsConfig, TJobResources::Infinite());

        YT_LOG_DEBUG("Validating operation resource limits (RequiredResourceLimits: %v, Pool: %v, OperationId: %v)",
            requiredLimits,
            pool->GetId(),
            operation->GetId());

        auto actualLimits = TJobResources::Infinite();
        const auto* current = pool;
        while (!current->IsRoot()) {
            if (const auto& specifiedLimits = current->ComputeMaybeSpecifiedResourceLimits()) {
                actualLimits = Min(actualLimits, *specifiedLimits);
            }

            if (Dominates(requiredLimits, actualLimits)) {
                return;
            }

            current = current->GetParent();
        }

        THROW_ERROR_EXCEPTION(
            "Operations of type %Qlv must have small enough specified resource limits in some of ancestor pools",
            operation->GetType())
            << TErrorAttribute("operation_id", operation->GetId())
            << TErrorAttribute("pool", pool->GetId())
            << TErrorAttribute("required_resource_limits", requiredLimitsConfig)
            << TErrorAttribute("tree_id", TreeId_);
    }

    void DoValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const TSchedulerCompositeElement* pool = FindPool(poolName.GetPool()).Get();
        // NB: Check is not performed if operation is started in default or unknown pool.
        if (pool && pool->AreImmediateOperationsForbidden()) {
            THROW_ERROR_EXCEPTION("Starting operations immediately in pool %Qv is forbidden", poolName.GetPool());
        }

        if (!pool) {
            // Validate pool name only if pool does not exist.
            ValidatePoolName(poolName.GetSpecifiedPoolName(), Host_->GetEphemeralPoolNameRegex());

            pool = GetPoolOrParent(poolName, operation->GetAuthenticatedUser()).Get();
        }

        if (pool->IsDefaultConfigured()) {
            pool = pool->GetParent();
        }

        if (operation->GetType() == EOperationType::RemoteCopy && Config_->FailRemoteCopyOnMissingResourceLimits) {
            ValidateSpecifiedResourceLimits(operation, pool, Config_->RequiredResourceLimitsForRemoteCopy);
        }
        StrategyHost_->ValidatePoolPermission(
            pool->GetObjectId(),
            pool->GetId(),
            operation->GetAuthenticatedUser(),
            EPermission::Use);
    }

    int GetPoolCount() const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return Pools_.size();
    }

    TSchedulerPoolElementPtr FindPool(const TString& id) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto it = Pools_.find(id);
        return it == Pools_.end() ? nullptr : it->second;
    }

    TSchedulerPoolElementPtr GetPool(const TString& id) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto pool = FindPool(id);
        YT_VERIFY(pool);
        return pool;
    }

    TSchedulerOperationElementPtr FindOperationElement(TOperationId operationId) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto it = OperationIdToElement_.find(operationId);
        return it == OperationIdToElement_.end() ? nullptr : it->second;
    }

    TSchedulerOperationElementPtr GetOperationElement(TOperationId operationId) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = FindOperationElement(operationId);
        YT_VERIFY(element);
        return element;
    }

    TSchedulerOperationElement* FindOperationElementInSnapshot(TOperationId operationId) const
    {
        if (auto treeSnapshot = GetTreeSnapshot()) {
            return treeSnapshot->FindEnabledOperationElement(operationId);
        }
        return nullptr;
    }

    TFuture<void> ProcessSchedulingHeartbeat(const ISchedulingContextPtr& schedulingContext, bool skipScheduleAllocations) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        auto processSchedulingHeartbeatFuture = BIND(
            &TFairShareTreeAllocationScheduler::ProcessSchedulingHeartbeat,
            TreeScheduler_,
            schedulingContext,
            treeSnapshot,
            skipScheduleAllocations)
            .AsyncVia(GetCurrentInvoker())
            .Run();

        return processSchedulingHeartbeatFuture
            .Apply(BIND(
                &TFairShareTree::ApplyScheduledAndPreemptedResourcesDelta,
                MakeStrong(this),
                schedulingContext,
                treeSnapshot));
    }

    int GetSchedulingHeartbeatComplexity() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        return treeSnapshot->RootElement()->SchedulableElementCount();
    }

    void ProcessAllocationUpdates(
        const std::vector<TAllocationUpdate>& allocationUpdates,
        THashSet<TAllocationId>* allocationsToPostpone,
        THashMap<TAllocationId, EAbortReason>* allocationsToAbort) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        for (const auto& allocationUpdate : allocationUpdates) {
            switch (allocationUpdate.Status) {
                case EAllocationUpdateStatus::Running: {
                    std::optional<EAbortReason> maybeAbortReason;
                    ProcessUpdatedAllocation(
                        treeSnapshot,
                        allocationUpdate.OperationId,
                        allocationUpdate.AllocationId,
                        allocationUpdate.AllocationResources,
                        allocationUpdate.AllocationDataCenter,
                        allocationUpdate.AllocationInfinibandCluster,
                        &maybeAbortReason);

                    if (maybeAbortReason) {
                        EmplaceOrCrash(*allocationsToAbort, allocationUpdate.AllocationId, *maybeAbortReason);
                        // NB(eshcherbin): We want the node shard to send us an allocation finished update,
                        // this is why we have to postpone the allocation here. This is very ad-hoc, but I hope it'll
                        // soon be rewritten as a part of the new GPU scheduler. See: YT-15062.
                        allocationsToPostpone->insert(allocationUpdate.AllocationId);
                    }

                    break;
                }
                case EAllocationUpdateStatus::Finished: {
                    if (!ProcessFinishedAllocation(treeSnapshot, allocationUpdate.OperationId, allocationUpdate.AllocationId)) {
                        YT_LOG_DEBUG(
                            "Postpone allocation update since operation is disabled or missing in snapshot (OperationId: %v, AllocationId: %v)",
                            allocationUpdate.OperationId,
                            allocationUpdate.AllocationId);

                        allocationsToPostpone->insert(allocationUpdate.AllocationId);
                    }
                    break;
                }
                default:
                    YT_ABORT();
            }
        }
    }

    void ProcessUpdatedAllocation(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TOperationId operationId,
        TAllocationId allocationId,
        const TJobResources& allocationResources,
        const std::optional<TString>& allocationDataCenter,
        const std::optional<TString>& allocationInfinibandCluster,
        std::optional<EAbortReason>* maybeAbortReason)
    {
        // NB: Should be filtered out on large clusters.
        YT_LOG_DEBUG(
            "Processing updated allocation (OperationId: %v, AllocationId: %v, Resources: %v)",
            operationId,
            allocationId,
            allocationResources);

        if (auto* operationElement = treeSnapshot->FindEnabledOperationElement(operationId)) {
            TreeScheduler_->ProcessUpdatedAllocation(
                treeSnapshot,
                operationElement,
                allocationId,
                allocationResources,
                allocationDataCenter,
                allocationInfinibandCluster,
                maybeAbortReason);
        }
    }

    bool ProcessFinishedAllocation(const TFairShareTreeSnapshotPtr& treeSnapshot, TOperationId operationId, TAllocationId allocationId)
    {
        // NB: Should be filtered out on large clusters.
        YT_LOG_DEBUG("Processing finished allocation (OperationId: %v, AllocationId: %v)", operationId, allocationId);

        if (auto* operationElement = treeSnapshot->FindEnabledOperationElement(operationId)) {
            TreeScheduler_->ProcessFinishedAllocation(treeSnapshot, operationElement, allocationId);
            return true;
        }

        return false;
    }

    bool IsSnapshottedOperationRunningInTree(TOperationId operationId) const override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        if (auto* element = treeSnapshot->FindEnabledOperationElement(operationId)) {
            return element->IsOperationRunningInPool();
        }

        if (auto* element = treeSnapshot->FindDisabledOperationElement(operationId)) {
            return element->IsOperationRunningInPool();
        }

        return false;
    }

    void ApplyJobMetricsDelta(THashMap<TOperationId, TJobMetrics> jobMetricsPerOperation) override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        for (const auto& [operationId, _] : jobMetricsPerOperation) {
            YT_VERIFY(
                treeSnapshot->EnabledOperationMap().contains(operationId) ||
                treeSnapshot->DisabledOperationMap().contains(operationId));
        }

        StrategyHost_->GetFairShareProfilingInvoker()->Invoke(BIND(
            &TFairShareTreeProfileManager::ApplyJobMetricsDelta,
            TreeProfileManager_,
            treeSnapshot,
            Passed(std::move(jobMetricsPerOperation))));
    }

    void ApplyScheduledAndPreemptedResourcesDelta(
        const ISchedulingContextPtr& schedulingContext,
        const TFairShareTreeSnapshotPtr& treeSnapshot)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!treeSnapshot->TreeConfig()->EnableScheduledAndPreemptedResourcesProfiling) {
            return;
        }

        THashMap<std::optional<EAllocationSchedulingStage>, TOperationIdToJobResources> scheduledAllocationResources;
        TEnumIndexedArray<EAllocationPreemptionReason, TOperationIdToJobResources> preemptedAllocationResources;
        TEnumIndexedArray<EAllocationPreemptionReason, TOperationIdToJobResources> preemptedAllocationResourceTimes;
        TEnumIndexedArray<EAllocationPreemptionReason, TOperationIdToJobResources> improperlyPreemptedAllocationResources;

        for (const auto& allocation : schedulingContext->StartedAllocations()) {
            TOperationId operationId = allocation->GetOperationId();
            const TJobResources& scheduledResourcesDelta = allocation->ResourceLimits();
            scheduledAllocationResources[allocation->GetSchedulingStage()][operationId] += scheduledResourcesDelta;
        }
        for (const auto& preemptedAllocation : schedulingContext->PreemptedAllocations()) {
            const TAllocationPtr& allocation = preemptedAllocation.Allocation;
            TOperationId operationId = allocation->GetOperationId();
            const TJobResources& preemptedResourcesDelta = allocation->ResourceLimits();
            EAllocationPreemptionReason preemptionReason = preemptedAllocation.PreemptionReason;
            preemptedAllocationResources[preemptionReason][operationId] += preemptedResourcesDelta;
            preemptedAllocationResourceTimes[preemptionReason][operationId] += preemptedResourcesDelta * static_cast<i64>(
                allocation->PreemptibleProgressTime().Seconds());

            if (allocation->GetPreemptedFor() && !allocation->GetPreemptedForProperlyStarvingOperation()) {
                improperlyPreemptedAllocationResources[preemptionReason][operationId] += preemptedResourcesDelta;
            }
        }

        StrategyHost_->GetFairShareProfilingInvoker()->Invoke(BIND(
            &TFairShareTreeProfileManager::ApplyScheduledAndPreemptedResourcesDelta,
            TreeProfileManager_,
            treeSnapshot,
            Passed(std::move(scheduledAllocationResources)),
            Passed(std::move(preemptedAllocationResources)),
            Passed(std::move(preemptedAllocationResourceTimes)),
            Passed(std::move(improperlyPreemptedAllocationResources))));
    }

    TJobResources GetSnapshottedTotalResourceLimits() const override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        return treeSnapshot->ResourceLimits();
    }

    std::optional<TSchedulerElementStateSnapshot> GetMaybeStateSnapshotForPool(const TString& poolId) const override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        if (auto* element = treeSnapshot->FindPool(poolId)) {
            return TSchedulerElementStateSnapshot{
                element->Attributes().DemandShare,
                element->Attributes().EstimatedGuaranteeShare};
        }

        return std::nullopt;
    }

    void BuildResourceMetering(
        TMeteringMap* meteringMap,
        THashMap<TString, TString>* customMeteringTags) const override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        auto rootElement = treeSnapshot->RootElement();
        auto accumulatedResourceUsageMap = AccumulatedPoolResourceUsageForMetering_.ExtractPoolResourceUsages();
        rootElement->BuildResourceMetering(/*parentKey*/ std::nullopt, accumulatedResourceUsageMap, meteringMap);

        *customMeteringTags = treeSnapshot->TreeConfig()->MeteringTags;
    }

    void BuildSchedulingAttributesStringForNode(TNodeId nodeId, TDelimitedStringBuilderWrapper& delimitedBuilder) const override
    {
        TreeScheduler_->BuildSchedulingAttributesStringForNode(nodeId, delimitedBuilder);
    }

    void BuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const override
    {
        TreeScheduler_->BuildSchedulingAttributesForNode(nodeId, fluent);
    }

    void BuildSchedulingAttributesStringForOngoingAllocations(
        const std::vector<TAllocationPtr>& allocations,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override
    {
        TreeScheduler_->BuildSchedulingAttributesStringForOngoingAllocations(GetAtomicTreeSnapshot(), allocations, now, delimitedBuilder);
    }

    void ProfileFairShare() const override
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        TreeProfileManager_->ProfileTree(
            treeSnapshot,
            AccumulatedOperationsResourceUsageForProfiling_.ExtractOperationResourceUsages());
    }

    void LogFairShareAt(TInstant now) const override
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        auto treeSnapshotId = treeSnapshot->GetId();
        if (treeSnapshotId == LastLoggedTreeSnapshotId_) {
            YT_LOG_DEBUG("Skipping fair share tree logging since the tree snapshot is the same as before (TreeSnapshotId: %v)",
                treeSnapshotId);

            return;
        }
        LastLoggedTreeSnapshotId_ = treeSnapshotId;

        {
            TEventTimerGuard timer(FairShareFluentLogTimer_);

            auto fairShareInfo = BuildSerializedFairShareInfo(
                treeSnapshot,
                treeSnapshot->TreeConfig()->MaxEventLogPoolBatchSize,
                treeSnapshot->TreeConfig()->MaxEventLogOperationBatchSize);
            auto logFairShareEventFluently = [&] {
                return StrategyHost_->LogFairShareEventFluently(now)
                    .Item(EventLogPoolTreeKey).Value(TreeId_)
                    .Item("tree_snapshot_id").Value(treeSnapshotId);
            };

            // NB(eshcherbin, YTADMIN-11230): First we log a single event with general pools info and resource distribution info.
            // Then we split all pools' and operations' info into several batches and log every batch in a separate event.
            logFairShareEventFluently()
                .Items(fairShareInfo.PoolCount)
                .Items(fairShareInfo.ResourceDistributionInfo);

            for (int batchIndex = 0; batchIndex < std::ssize(fairShareInfo.SplitPoolsInfo); ++batchIndex) {
                const auto& batch = fairShareInfo.SplitPoolsInfo[batchIndex];
                logFairShareEventFluently()
                    .Item("pools_batch_index").Value(batchIndex)
                    .Item("pools").BeginMap()
                        .Items(batch)
                    .EndMap();
            }

            for (int batchIndex = 0; batchIndex < std::ssize(fairShareInfo.SplitOperationsInfo); ++batchIndex) {
                const auto& batch = fairShareInfo.SplitOperationsInfo[batchIndex];
                logFairShareEventFluently()
                    .Item("operations_batch_index").Value(batchIndex)
                    .Item("operations").BeginMap()
                        .Items(batch)
                    .EndMap();
            }
        }

        {
            TEventTimerGuard timer(FairShareTextLogTimer_);
            LogPoolsInfo(treeSnapshot);
            LogOperationsInfo(treeSnapshot);
        }
    }

    void LogAccumulatedUsage() const override
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        StrategyHost_->LogAccumulatedUsageEventFluently(TInstant::Now())
            .Item(EventLogPoolTreeKey).Value(TreeId_)
            .Item("pools").BeginMap()
                .Do(BIND(&TFairShareTree::DoBuildPoolsStructureInfo, Unretained(this), ConstRef(treeSnapshot)))
            .EndMap()
            .Item("operations").BeginMap()
                .Do(BIND(&TFairShareTree::DoBuildOperationsAccumulatedUsageInfo, Unretained(this), ConstRef(treeSnapshot)))
            .EndMap();
    }

    void EssentialLogFairShareAt(TInstant now) const override
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        {
            TEventTimerGuard timer(FairShareFluentLogTimer_);
            StrategyHost_->LogFairShareEventFluently(now)
                .Item(EventLogPoolTreeKey).Value(TreeId_)
                .Item("tree_snapshot_id").Value(treeSnapshot->GetId())
                .Do(BIND(&TFairShareTree::DoBuildEssentialFairShareInfo, Unretained(this), ConstRef(treeSnapshot)));
        }

        {
            TEventTimerGuard timer(FairShareTextLogTimer_);
            LogPoolsInfo(treeSnapshot);
            LogOperationsInfo(treeSnapshot);
        }
    }

    void UpdateResourceUsages() override
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();
        auto resourceUsageSnapshot = BuildResourceUsageSnapshot(treeSnapshot);

        AccumulatedPoolResourceUsageForMetering_.Update(treeSnapshot, resourceUsageSnapshot);
        AccumulatedOperationsResourceUsageForProfiling_.Update(treeSnapshot, resourceUsageSnapshot);
        AccumulatedOperationsResourceUsageForLogging_.Update(treeSnapshot, resourceUsageSnapshot);

        if (!treeSnapshot->TreeConfig()->EnableResourceUsageSnapshot) {
            resourceUsageSnapshot = nullptr;
            YT_LOG_DEBUG("Resource usage snapshot is disabled");
        } else {
            YT_LOG_DEBUG("Updating resources usage snapshot");
        }

        TreeScheduler_->OnResourceUsageSnapshotUpdate(treeSnapshot, resourceUsageSnapshot);
        SetResourceUsageSnapshot(std::move(resourceUsageSnapshot));
    }

    TResourceVolume ExtractAccumulatedUsageForLogging(TOperationId operationId) override
    {
        // NB: We can loose some of usage, up to the AccumulatedResourceUsageUpdatePeriod duration.
        return AccumulatedOperationsResourceUsageForLogging_.ExtractOperationResourceUsage(operationId);
    }

    void LogOperationsInfo(const TFairShareTreeSnapshotPtr& treeSnapshot) const
    {
        auto Logger = this->Logger.WithTag("TreeSnapshotId: %v", treeSnapshot->GetId());

        auto doLogOperationsInfo = [&] (const auto& operationIdToElement) {
            for (const auto& [operationId, element] : operationIdToElement) {
                // TODO(eshcherbin): Rethink format of fair share info log message.
                YT_LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                    element->GetLoggingString(treeSnapshot),
                    operationId);
            }
        };

        doLogOperationsInfo(treeSnapshot->EnabledOperationMap());
        doLogOperationsInfo(treeSnapshot->DisabledOperationMap());
    }

    void LogPoolsInfo(const TFairShareTreeSnapshotPtr& treeSnapshot) const
    {
        auto Logger = this->Logger.WithTag("TreeSnapshotId: %v", treeSnapshot->GetId());

        for (const auto& [poolName, element] : treeSnapshot->PoolMap()) {
            YT_LOG_DEBUG("FairShareInfo: %v (Pool: %v)",
                element->GetLoggingString(treeSnapshot),
                poolName);
        }
    }

    void DoBuildFullFairShareInfo(const TFairShareTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        VERIFY_INVOKER_AFFINITY(StrategyHost_->GetOrchidWorkerInvoker());

        if (!treeSnapshot) {
            YT_LOG_DEBUG("Skipping construction of full fair share info, since shapshot is not constructed yet");
            return;
        }

        YT_LOG_DEBUG("Constructing full fair share info");

        auto fairShareInfo = BuildSerializedFairShareInfo(treeSnapshot);
        fluent
            .Items(fairShareInfo.PoolCount)
            .Item("pools").BeginMap()
                .DoFor(fairShareInfo.SplitPoolsInfo, [&] (TFluentMap fluent, const TYsonString& batch) {
                    fluent.Items(batch);
                })
            .EndMap()
            .Item("operations").BeginMap()
                .DoFor(fairShareInfo.SplitOperationsInfo, [&] (TFluentMap fluent, const TYsonString& batch) {
                    fluent.Items(batch);
                })
            .EndMap()
            .Items(fairShareInfo.ResourceDistributionInfo);
    }

    struct TSerializedFairShareInfo
    {
        TYsonString PoolCount;
        std::vector<TYsonString> SplitPoolsInfo;
        std::vector<TYsonString> SplitOperationsInfo;
        TYsonString ResourceDistributionInfo;
    };

    TSerializedFairShareInfo BuildSerializedFairShareInfo(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        int maxPoolBatchSize = std::numeric_limits<int>::max(),
        int maxOperationBatchSize = std::numeric_limits<int>::max()) const
    {
        YT_LOG_DEBUG("Started building serialized fair share info (MaxPoolBatchSize: %v, MaxOperationBatchSize: %v)",
            maxPoolBatchSize,
            maxOperationBatchSize);

        TSerializedFairShareInfo fairShareInfo;
        fairShareInfo.PoolCount = BuildYsonStringFluently<EYsonType::MapFragment>()
            .Item("pool_count").Value(std::ssize(treeSnapshot->PoolMap()))
            .Finish();
        fairShareInfo.ResourceDistributionInfo = BuildYsonStringFluently<EYsonType::MapFragment>()
            .Item("resource_distribution_info").BeginMap()
                .Do(std::bind(&TSchedulerRootElement::BuildResourceDistributionInfo, treeSnapshot->RootElement(), std::placeholders::_1))
            .EndMap()
            .Finish();

        TYsonMapFragmentBatcher poolsConsumer(&fairShareInfo.SplitPoolsInfo, maxPoolBatchSize);
        BuildYsonMapFragmentFluently(&poolsConsumer)
            .Do(std::bind(&TFairShareTree::BuildPoolsInfo, std::cref(treeSnapshot), TFieldsFilter{}, std::placeholders::_1));
        poolsConsumer.Flush();

        auto buildOperationInfo = [&] (TFluentMap fluent, const TNonOwningOperationElementMap::value_type& pair) {
            const auto& [_, element] = pair;
            fluent
                .Item(element->GetId()).BeginMap()
                    .Do(std::bind(&TFairShareTree::DoBuildOperationProgress, std::cref(treeSnapshot), element, StrategyHost_, std::placeholders::_1))
                .EndMap();
        };

        TYsonMapFragmentBatcher operationsConsumer(&fairShareInfo.SplitOperationsInfo, maxOperationBatchSize);
        BuildYsonMapFragmentFluently(&operationsConsumer)
            .DoFor(treeSnapshot->EnabledOperationMap(), buildOperationInfo)
            .DoFor(treeSnapshot->DisabledOperationMap(), buildOperationInfo);
        operationsConsumer.Flush();

        YT_LOG_DEBUG(
            "Finished building serialized fair share info "
            "(MaxPoolBatchSize: %v, PoolCount: %v, PoolBatchCount: %v, "
            "MaxOperationBatchSize: %v, OperationCount: %v, OperationBatchCount: %v)",
            maxPoolBatchSize,
            treeSnapshot->PoolMap().size() + 1,
            fairShareInfo.SplitPoolsInfo.size(),
            maxOperationBatchSize,
            treeSnapshot->EnabledOperationMap().size() + treeSnapshot->DisabledOperationMap().size(),
            fairShareInfo.SplitOperationsInfo.size());

        return fairShareInfo;
    }

    static void BuildCompositeElementInfo(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerCompositeElement* element,
        const TFieldsFilter& filter,
        TFluentMap fluent)
    {
        const auto& attributes = element->Attributes();
        fluent
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "running_operation_count", element->RunningOperationCount())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "lightweight_running_operation_count", element->LightweightRunningOperationCount())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "pool_operation_count", element->GetChildOperationCount())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "operation_count", element->OperationCount())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "max_running_operation_count", element->GetMaxRunningOperationCount())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "max_operation_count", element->GetMaxOperationCount())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "forbid_immediate_operations", element->AreImmediateOperationsForbidden())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "total_resource_flow_ratio", attributes.TotalResourceFlowRatio)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "total_burst_ratio", attributes.TotalBurstRatio)
            .DoIf(element->GetParent(), ([&] (TFluentMap fluent) {
                fluent
                    .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "parent", element->GetParent()->GetId());
            }))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_fifo_pool_scheduling_order",
                element->GetEffectiveFifoPoolSchedulingOrder())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_use_pool_satisfaction_for_scheduling",
                element->GetEffectiveUsePoolSatisfactionForScheduling())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "lightweight_operations_enabled", element->AreLightweightOperationsEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "effective_lightweight_operations_enabled", element->GetEffectiveLightweightOperationsEnabled())
            .Do(std::bind(&TFairShareTree::DoBuildElementYson, std::cref(treeSnapshot), element, std::cref(filter), std::placeholders::_1));
    }

    static void BuildPoolInfo(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerPoolElement* pool,
        const TFieldsFilter& filter,
        TFluentMap fluent)
    {
        fluent
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "mode", pool->GetMode())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "is_ephemeral", pool->IsDefaultConfigured())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "integral_guarantee_type", pool->GetIntegralGuaranteeType())
            .DoIf(pool->GetIntegralGuaranteeType() != EIntegralGuaranteeType::None, [&] (TFluentMap fluent) {
                auto burstRatio = pool->GetSpecifiedBurstRatio();
                auto resourceFlowRatio = pool->GetSpecifiedResourceFlowRatio();
                fluent
                    .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "integral_pool_capacity", pool->GetIntegralPoolCapacity())
                    .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "specified_burst_ratio", burstRatio)
                    .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "specified_burst_guarantee_resources", pool->GetTotalResourceLimits() * burstRatio)
                    .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "specified_resource_flow_ratio", resourceFlowRatio)
                    .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "specified_resource_flow", pool->GetTotalResourceLimits() * resourceFlowRatio)
                    .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "accumulated_resource_ratio_volume", pool->GetAccumulatedResourceRatioVolume())
                    .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "accumulated_resource_volume", pool->GetAccumulatedResourceVolume());
                if (burstRatio > resourceFlowRatio + RatioComparisonPrecision) {
                    fluent
                        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "estimated_burst_usage_duration_seconds",
                            pool->GetAccumulatedResourceRatioVolume() / (burstRatio - resourceFlowRatio));
                }
            })
            .DoIf(pool->GetMode() == ESchedulingMode::Fifo, [&] (TFluentMap fluent) {
                fluent
                    .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "fifo_sort_parameters", pool->GetFifoSortParameters());
            })
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "abc", pool->GetConfig()->Abc)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "full_path", pool->GetFullPath(/*explicitOnly*/ false, /*withTreeId*/ false))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "child_pool_count", pool->GetChildPoolCount())
            .Do(std::bind(&TFairShareTree::BuildCompositeElementInfo, std::cref(treeSnapshot), pool, std::cref(filter), std::placeholders::_1));
    }

    static void BuildPoolsInfo(const TFairShareTreeSnapshotPtr& treeSnapshot, const TFieldsFilter& filter, TFluentMap fluent)
    {
        const auto& poolMap = treeSnapshot->PoolMap();
        fluent
            .DoFor(poolMap, [&] (TFluentMap fluent, const TNonOwningPoolElementMap::value_type& pair) {
                const auto& [poolName, pool] = pair;
                fluent.Item(poolName)
                    .BeginMap()
                        .Do(std::bind(&TFairShareTree::BuildPoolInfo, std::cref(treeSnapshot), pool, std::cref(filter), std::placeholders::_1))
                    .EndMap();
            })
            .Do(std::bind(&TFairShareTree::DoBuildRootElementInfo, std::cref(treeSnapshot), std::cref(filter), std::placeholders::_1));
    }

    static void DoBuildRootElementInfo(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TFieldsFilter& filter,
        TFluentMap fluent)
    {
        fluent
            .Item(RootPoolName).BeginMap()
                .Do(std::bind(
                    &TFairShareTree::BuildCompositeElementInfo,
                    std::cref(treeSnapshot),
                    treeSnapshot->RootElement().Get(),
                    std::cref(filter),
                    std::placeholders::_1))
            .EndMap();
    }

    static void BuildChildPoolsByPoolInfo(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TFieldsFilter& filter,
        const TString& parentPoolName,
        TFluentMap fluent)
    {
        auto* parentPool = [&] () -> TSchedulerCompositeElement* {
            if (parentPoolName == RootPoolName) {
                return treeSnapshot->RootElement().Get();
            } else {
                return GetOrCrash(treeSnapshot->PoolMap(), parentPoolName);
            }
        }();

        fluent
            .Do(std::bind(&TFairShareTree::DoBuildChildPoolsByPoolInfo,
                std::cref(treeSnapshot),
                std::cref(filter),
                parentPool,
                std::placeholders::_1));
    }

    static void BuildChildPoolsByPoolInfos(const TFairShareTreeSnapshotPtr& treeSnapshot, const TFieldsFilter& /*filter*/, TFluentMap fluent)
    {
        fluent
            .DoFor(treeSnapshot->PoolMap(), [&] (TFluentMap fluent, const TNonOwningPoolElementMap::value_type& pair) {
                const auto& [poolName, pool] = pair;
                fluent.Item(poolName).Entity();
            })
            .Item(RootPoolName).Entity();
    }

    static void DoBuildChildPoolsByPoolInfo(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TFieldsFilter& filter,
        const TSchedulerCompositeElement* parentPool,
        TFluentMap fluent)
    {
        fluent
            .DoFor(parentPool->EnabledChildren(), [&] (TFluentMap fluent, const TSchedulerElementPtr& child) {
                if (!child->IsOperation()) {
                    fluent.Item(child->GetId())
                        .BeginMap().Do(
                            std::bind(&TFairShareTree::BuildPoolInfo,
                            std::cref(treeSnapshot),
                            static_cast<TSchedulerPoolElement*>(child.Get()),
                            std::cref(filter),
                            std::placeholders::_1))
                        .EndMap();
                }
            });
    }

    void DoBuildPoolsStructureInfo(const TFairShareTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        auto buildPoolInfo = [&] (const TSchedulerPoolElement* pool, TFluentMap fluent) {
            const auto& id = pool->GetId();
            fluent
                .Item(id).BeginMap()
                    .Item("abc").Value(pool->GetConfig()->Abc)
                    .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                        auto burstRatio = pool->GetSpecifiedBurstRatio();
                        auto resourceFlowRatio = pool->GetSpecifiedResourceFlowRatio();
                        fluent
                            .Item("parent").Value(pool->GetParent()->GetId())
                            .Item("strong_guarantee_resources").Value(pool->GetSpecifiedStrongGuaranteeResources())
                            .Item("burst_guarantee_resources").Value(pool->GetTotalResourceLimits() * burstRatio)
                            .Item("resource_flow").Value(pool->GetTotalResourceLimits() * resourceFlowRatio);
                    })
                .EndMap();
        };

        fluent
            .DoFor(treeSnapshot->PoolMap(), [&] (TFluentMap fluent, const TNonOwningPoolElementMap::value_type& pair) {
                buildPoolInfo(pair.second, fluent);
            })
            .Item(RootPoolName).BeginMap()
            .EndMap();
    }

    void DoBuildOperationsAccumulatedUsageInfo(const TFairShareTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        auto operationIdToAccumulatedResourceUsage = AccumulatedOperationsResourceUsageForLogging_.ExtractOperationResourceUsages();

        auto buildOperationInfo = [&] (const TSchedulerOperationElement* operation, TFluentMap fluent) {
            auto operationId = operation->GetOperationId();
            auto* parent = operation->GetParent();

            TResourceVolume accumulatedUsage;
            {
                auto it = operationIdToAccumulatedResourceUsage.find(operationId);
                if (it != operationIdToAccumulatedResourceUsage.end()) {
                    accumulatedUsage = it->second;
                }
            }

            fluent
                .Item(operation->GetId()).BeginMap()
                    .Item("pool").Value(parent->GetId())
                    .Item("accumulated_resource_usage").Value(accumulatedUsage)
                    .Item("user").Value(operation->GetUserName())
                    .Item("operation_type").Value(operation->GetOperationType())
                    .OptionalItem("trimmed_annotations", operation->GetTrimmedAnnotations())
                .EndMap();
        };

        fluent
            .DoFor(treeSnapshot->EnabledOperationMap(), [&] (TFluentMap fluent, const TNonOwningOperationElementMap::value_type& pair) {
                buildOperationInfo(pair.second, fluent);
            })
            .DoFor(treeSnapshot->DisabledOperationMap(), [&] (TFluentMap fluent, const TNonOwningOperationElementMap::value_type& pair) {
                buildOperationInfo(pair.second, fluent);
            });
    }

    static void DoBuildOperationProgress(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerOperationElement* element,
        ISchedulerStrategyHost* const strategyHost,
        TFluentMap fluent)
    {
        auto* parent = element->GetParent();
        fluent
            .Item("pool").Value(parent->GetId())
            .Item("slot_index").Value(element->GetSlotIndex())
            .Item("start_time").Value(element->GetStartTime())
            .OptionalItem("fifo_index", element->Attributes().FifoIndex)
            .Item("detailed_min_needed_job_resources").BeginList()
                .DoFor(element->DetailedMinNeededAllocationResources(), [&] (TFluentList fluent, const TJobResourcesWithQuota& jobResourcesWithQuota) {
                    fluent.Item().Do([&] (TFluentAny fluent) {
                        strategyHost->SerializeResources(jobResourcesWithQuota, fluent.GetConsumer());
                    });
                })
            .EndList()
            .Item("aggregated_min_needed_job_resources").Value(element->AggregatedMinNeededAllocationResources())
            .Item("tentative").Value(element->GetRuntimeParameters()->Tentative)
            .Item("probing").Value(element->GetRuntimeParameters()->Probing)
            .Item("offloading").Value(element->GetRuntimeParameters()->Offloading)
            .Item("starving_since").Value(element->GetStarvationStatus() != EStarvationStatus::NonStarving
                ? std::optional(element->GetLastNonStarvingTime())
                : std::nullopt)
            .Item("lightweight").Value(element->IsLightweight())
            .Item("disk_request_media").DoListFor(element->DiskRequestMedia(), [&] (TFluentList fluent, int mediumIndex) {
                fluent.Item().Value(strategyHost->GetMediumNameByIndex(mediumIndex));
            })
            .Item("unschedulable_reason").Value(element->GetUnschedulableReason())
            .Do(BIND(&TFairShareTreeAllocationScheduler::BuildOperationProgress, ConstRef(treeSnapshot), Unretained(element), strategyHost))
            .Do(BIND(&TFairShareTree::DoBuildElementYson, ConstRef(treeSnapshot), Unretained(element), TFieldsFilter{}));
    }

    static void DoBuildElementYson(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerElement* element,
        const TFieldsFilter& filter,
        TFluentMap fluent)
    {
        const auto& attributes = element->Attributes();
        const auto& persistentAttributes = element->PersistentAttributes();

        auto promisedFairShareResources = element->GetTotalResourceLimits() * attributes.PromisedFairShare;

        // TODO(eshcherbin): Rethink which fields should be here and which should be in |TSchedulerElement::BuildYson|.
        // Also rethink which scalar fields should be exported to Orchid.
        fluent
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "scheduling_status", element->GetStatus())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "starvation_status", element->GetStarvationStatus())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "fair_share_starvation_tolerance",
                element->GetSpecifiedFairShareStarvationTolerance())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "fair_share_starvation_timeout",
                element->GetSpecifiedFairShareStarvationTimeout())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_fair_share_starvation_tolerance",
                element->GetEffectiveFairShareStarvationTolerance())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_fair_share_starvation_timeout",
                element->GetEffectiveFairShareStarvationTimeout())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_non_preemptible_resource_usage_threshold",
                element->EffectiveNonPreemptibleResourceUsageThresholdConfig())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "aggressive_starvation_enabled", element->IsAggressiveStarvationEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_aggressive_starvation_enabled",
                element->GetEffectiveAggressiveStarvationEnabled())
            .DoIf(element->GetLowestAggressivelyStarvingAncestor(), [&] (TFluentMap fluent) {
                fluent.ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                    filter,
                    "lowest_aggressively_starving_ancestor",
                    element->GetLowestAggressivelyStarvingAncestor()->GetId());
            })
            .DoIf(element->GetLowestStarvingAncestor(), [&] (TFluentMap fluent) {
                fluent.ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "lowest_starving_ancestor", element->GetLowestStarvingAncestor()->GetId());
            })
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "weight", element->GetWeight())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "max_share_ratio", element->GetMaxShareRatio())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_resource", attributes.DominantResource)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "resource_usage", element->GetResourceUsageAtUpdate())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "usage_share", attributes.UsageShare)
            // COMPAT(ignat): remove it after UI and other tools migration.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "usage_ratio", element->GetResourceDominantUsageShareAtUpdate())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_usage_share", element->GetResourceDominantUsageShareAtUpdate())

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "resource_demand", element->GetResourceDemand())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "demand_share", attributes.DemandShare)
            // COMPAT(ignat): remove it after UI and other tools migration.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "demand_ratio", MaxComponent(attributes.DemandShare))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_demand_share", MaxComponent(attributes.DemandShare))

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "resource_limits", element->GetResourceLimits())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "limits_share", attributes.LimitsShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "scheduling_tag_filter_resource_limits", element->GetSchedulingTagFilterResourceLimits())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "specified_resource_limits", element->GetSpecifiedResourceLimitsConfig())

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "limited_resource_demand", element->GetTotalResourceLimits() * element->LimitedDemandShare())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "limited_demand_share", element->LimitedDemandShare())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_limited_demand_share", MaxComponent(element->LimitedDemandShare()))

            // COMPAT(ignat): remove it after UI and other tools migration.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "min_share", attributes.StrongGuaranteeShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "strong_guarantee_share", attributes.StrongGuaranteeShare)
            // COMPAT(ignat): remove it after UI and other tools migration.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "min_share_resources", element->GetSpecifiedStrongGuaranteeResources())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "strong_guarantee_resources", element->GetSpecifiedStrongGuaranteeResources())
            // COMPAT(ignat): remove it after UI and other tools migration.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "effective_min_share_resources", attributes.EffectiveStrongGuaranteeResources)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "effective_strong_guarantee_resources", attributes.EffectiveStrongGuaranteeResources)
            // COMPAT(ignat): remove it after UI and other tools migration.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "min_share_ratio", MaxComponent(attributes.StrongGuaranteeShare))

            // COMPAT(ignat): remove it after UI and other tools migration.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "fair_share_ratio", MaxComponent(attributes.FairShare.Total))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "detailed_fair_share", attributes.FairShare)
            .ITEM_DO_IF_SUITABLE_FOR_FILTER(
                filter,
                "detailed_dominant_fair_share",
                std::bind(&SerializeDominant, std::cref(attributes.FairShare), std::placeholders::_1))

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "detailed_promised_guarantee_fair_share", attributes.PromisedGuaranteeFairShare)
            .ITEM_DO_IF_SUITABLE_FOR_FILTER(
                filter,
                "detailed_dominant_promised_guarantee_fair_share",
                std::bind(&SerializeDominant, std::cref(attributes.PromisedGuaranteeFairShare), std::placeholders::_1))

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "promised_fair_share", attributes.PromisedFairShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "promised_dominant_fair_share", MaxComponent(attributes.PromisedFairShare))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "promised_fair_share_resources", promisedFairShareResources)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "estimated_guarantee_share", attributes.EstimatedGuaranteeShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_estimated_guarantee_share", MaxComponent(attributes.EstimatedGuaranteeShare))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "estimated_guarantee_resources", element->GetTotalResourceLimits() * attributes.EstimatedGuaranteeShare)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "proposed_integral_share", attributes.ProposedIntegralShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "best_allocation_share", persistentAttributes.BestAllocationShare)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "satisfaction_ratio", element->PostUpdateAttributes().SatisfactionRatio)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "local_satisfaction_ratio", element->PostUpdateAttributes().LocalSatisfactionRatio)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "schedulable", element->IsSchedulable())
            .Do(BIND(&TFairShareTreeAllocationScheduler::BuildElementYson, ConstRef(treeSnapshot), Unretained(element), filter));
    }

    void DoBuildEssentialFairShareInfo(const TFairShareTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        auto buildOperationsInfo = [&] (TFluentMap fluent, const TNonOwningOperationElementMap::value_type& pair) {
            const auto& [operationId, element] = pair;
            fluent
                .Item(ToString(operationId)).BeginMap()
                    .Do(BIND(&TFairShareTree::DoBuildEssentialOperationProgress, Unretained(this), Unretained(element)))
                .EndMap();
        };

        fluent
            .Do(BIND(&TFairShareTree::DoBuildEssentialPoolsInformation, Unretained(this), treeSnapshot))
            .Item("operations").BeginMap()
                .DoFor(treeSnapshot->EnabledOperationMap(), buildOperationsInfo)
                .DoFor(treeSnapshot->DisabledOperationMap(), buildOperationsInfo)
            .EndMap();
    }

    void DoBuildEssentialPoolsInformation(const TFairShareTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        const auto& poolMap = treeSnapshot->PoolMap();
        fluent
            .Item("pool_count").Value(std::ssize(poolMap))
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
            .Item("satisfaction_ratio").Value(element->PostUpdateAttributes().SatisfactionRatio)
            .Item("dominant_resource").Value(attributes.DominantResource)
            .DoIf(element->IsOperation(), [&] (TFluentMap fluent) {
                fluent
                    .Item("resource_usage").Value(element->GetResourceUsageAtUpdate());
            });
    }

    DEFINE_SIGNAL_OVERRIDE(void(TOperationId), OperationRunning);
};

////////////////////////////////////////////////////////////////////////////////

IFairShareTreePtr CreateFairShareTree(
    TFairShareStrategyTreeConfigPtr config,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    IFairShareTreeHost* host,
    ISchedulerStrategyHost* strategyHost,
    std::vector<IInvokerPtr> feasibleInvokers,
    TString treeId)
{
    return New<TFairShareTree>(
        std::move(config),
        std::move(controllerConfig),
        host,
        strategyHost,
        std::move(feasibleInvokers),
        std::move(treeId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
