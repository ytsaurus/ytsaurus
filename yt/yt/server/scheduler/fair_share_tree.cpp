#include "fair_share_tree.h"
#include "fair_share_tree_element.h"
#include "fair_share_tree_job_scheduler.h"
#include "fair_share_tree_snapshot.h"
#include "persistent_scheduler_state.h"
#include "public.h"
#include "pools_config_parser.h"
#include "resource_tree.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "scheduling_segment_manager.h"
#include "serialize.h"
#include "fair_share_strategy_operation_controller.h"
#include "fair_share_tree_profiling.h"

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

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/library/vector_hdrf/fair_share_update.h>

#include <library/cpp/yt/threading/spin_lock.h>

#define ITEM_DO_IF_SUITABLE_FOR_FILTER(filter, field, ...) DoIf(filter.IsFieldSuitable(field), [&] (TFluentMap fluent) { \
        fluent.Item(field).Do(__VA_ARGS__); \
    })

#define ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, field, ...) DoIf(filter.IsFieldSuitable(field), [&] (TFluentMap fluent) { \
        fluent.Item(field).Value(__VA_ARGS__); \
    })

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NJobTrackerClient;
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

    // This maps is updated regularly from some thread pool, no paralell updates are possible.
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
//!     This tree represented by #TreeSnapshot_.
//!     NB: elements of this tree may be invalidated by #Alive flag in resource tree. In this case element cannot be safely used
//!     (corresponding operation or pool can be already deleted from all other scheduler structures).
//!
//!   * Resource tree, it is thread safe tree that maintain shared attributes of tree elements.
//!     More details can be find at #TResourceTree.
class TFairShareTree
    : public IFairShareTree
    , public IFairShareTreeElementHost
{
public:
    using TFairShareTreePtr = TIntrusivePtr<TFairShareTree>;

    TFairShareTree(
        TFairShareStrategyTreeConfigPtr config,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        ISchedulerStrategyHost* strategyHost,
        const std::vector<IInvokerPtr>& feasibleInvokers,
        TString treeId)
        : Config_(std::move(config))
        , ControllerConfig_(std::move(controllerConfig))
        , ResourceTree_(New<TResourceTree>(Config_, feasibleInvokers))
        , TreeProfiler_(New<TFairShareTreeProfileManager>(
            treeId,
            Config_->SparsifyFairShareProfiling,
            strategyHost->GetFairShareProfilingInvoker()))
        , StrategyHost_(strategyHost)
        , FeasibleInvokers_(feasibleInvokers)
        , TreeId_(std::move(treeId))
        , Logger(StrategyLogger.WithTag("TreeId: %v", TreeId_))
        , TreeScheduler_(New<TFairShareTreeJobScheduler>(treeId, StrategyHost_, Config_, TreeProfiler_))
        , FairSharePreUpdateTimer_(TreeProfiler_->GetProfiler().Timer("/fair_share_preupdate_time"))
        , FairShareUpdateTimer_(TreeProfiler_->GetProfiler().Timer("/fair_share_update_time"))
        , FairShareFluentLogTimer_(TreeProfiler_->GetProfiler().Timer("/fair_share_fluent_log_time"))
        , FairShareTextLogTimer_(TreeProfiler_->GetProfiler().Timer("/fair_share_text_log_time"))
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

        TreeProfiler_->RegisterPool(RootElement_);

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

        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        return treeSnapshot->TreeConfig();
    }

    bool UpdateConfig(const TFairShareStrategyTreeConfigPtr& config) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (AreNodesEqual(ConvertToNode(config), ConvertToNode(Config_))) {
            return false;
        }

        Config_ = config;
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

        {
            auto guard = Guard(TreeSnapshotLock_);
            TreeSnapshot_ = std::move(TreeSnapshotPrecommit_);
        }
        TreeSnapshotPrecommit_.Reset();
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

        int index = TreeScheduler_->RegisterSchedulingTagFilter(TSchedulingTagFilter(spec->SchedulingTagFilter));
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

        if (const auto& schedulingSegmentModule = runtimeParameters->SchedulingSegmentModule) {
            YT_LOG_DEBUG(
                "Recovering operation's scheduling segment module assignment from runtime parameters "
                "(OperationId: %v, SchedulingSegmentModule: %v)",
                operationId,
                schedulingSegmentModule);

            operationElement->PersistentAttributes().SchedulingSegmentModule = schedulingSegmentModule;
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

        ReleaseOperationSlotIndex(state, pool->GetId());
        OnOperationRemovedFromPool(state, operationElement, pool);

        TreeScheduler_->UnregisterSchedulingTagFilter(operationElement->GetSchedulingTagFilterIndex());

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

        ReleaseOperationSlotIndex(state, oldParent->GetId());

        int newSlotIndex = AllocateOperationSlotIndex(state, newParent->GetId());
        element->ChangeParent(newParent.Get(), newSlotIndex);
        state->GetHost()->SetSlotIndex(TreeId_, newSlotIndex);

        OnOperationRemovedFromPool(state, element, oldParent);
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
            TJobResourcesWithQuota resourceUsageWithQuota = job->ResourceUsage();
            resourceUsageWithQuota.SetDiskQuota(job->DiskQuota());
            element->OnJobStarted(
                job->GetId(),
                resourceUsageWithQuota,
                /*precommittedResources*/ {},
                // NB: |scheduleJobEpoch| is ignored in case |force| is true.
                /*scheduleJobEpoch*/ 0,
                /*force*/ true);
        }
    }

    TError CheckOperationIsHung(
        TOperationId operationId,
        TDuration safeTimeout,
        int minScheduleJobCallAttempts,
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

        bool hasMinNeededResources = !element->DetailedMinNeededJobResources().empty();
        auto aggregatedMinNeededResources = element->AggregatedMinNeededJobResources();
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
                    return TError("Operation has an ancestor whose specified resource limits are too small to satisfy operation's minimum job resource demand")
                        << TErrorAttribute("safe_timeout", limitingAncestorSafeTimeout)
                        << TErrorAttribute("limiting_ancestor", limitingAncestor->GetId())
                        << TErrorAttribute("resource_limits", limitingAncestor->GetSpecifiedResourceLimits())
                        << TErrorAttribute("min_needed_resources", aggregatedMinNeededResources);
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
            const auto& schedulingSegmentModule = element->PersistentAttributes().SchedulingSegmentModule;
            if (segment && IsModuleAwareSchedulingSegment(*segment) && schedulingSegmentModule && !element->GetSchedulingTagFilter().IsEmpty()) {
                auto tagFilter = element->GetSchedulingTagFilter().GetBooleanFormula().GetFormula();
                bool isModuleFilter = false;
                for (const auto& possibleModule : Config_->SchedulingSegments->GetModules()) {
                    auto moduleTag = TNodeSchedulingSegmentManager::GetNodeTagFromModuleName(
                        possibleModule,
                        Config_->SchedulingSegments->ModuleType);
                    // NB(eshcherbin): This doesn't cover all the cases, only the most usual.
                    // Don't really want to check boolean formula satisfiability here.
                    if (tagFilter == moduleTag) {
                        isModuleFilter = true;
                        break;
                    }
                }

                auto operationModuleTag = TNodeSchedulingSegmentManager::GetNodeTagFromModuleName(
                    *schedulingSegmentModule,
                    Config_->SchedulingSegments->ModuleType);
                if (isModuleFilter && tagFilter != operationModuleTag) {
                    return TError(
                        "Operation has a module specified in the scheduling tag filter, which causes scheduling problems; "
                        "use \"scheduling_segment_modules\" spec option instead")
                        << TErrorAttribute("scheduling_tag_filter", tagFilter)
                        << TErrorAttribute("available_modules", Config_->SchedulingSegments->GetModules());
                }
            }
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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
        element->InitOrUpdateSchedulingSegment(Config_->SchedulingSegments);

        YT_VERIFY(element->SchedulingSegment());
        return *element->SchedulingSegment();
    }

    TTreeSchedulingSegmentsState GetSchedulingSegmentsState() const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return TreeSnapshot_
            ? TreeSnapshot_->SchedulingSnapshot()->SchedulingSegmentsState()
            : TTreeSchedulingSegmentsState{};
    }

    TOperationIdWithSchedulingSegmentModuleList GetOperationSchedulingSegmentModuleUpdates() const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        TOperationIdWithSchedulingSegmentModuleList result;
        for (const auto& [operationId, element] : OperationIdToElement_) {
            auto params = element->GetRuntimeParameters();
            const auto& schedulingSegmentModule = element->PersistentAttributes().SchedulingSegmentModule;
            if (params->SchedulingSegmentModule != schedulingSegmentModule) {
                result.push_back({operationId, schedulingSegmentModule});
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
        if (TreeSnapshot_) {
            if (auto elementFromSnapshot = TreeSnapshot_->FindEnabledOperationElement(operationId)) {
                element = elementFromSnapshot;
            }
        }

        if (!element) {
            return;
        }

        DoBuildOperationProgress(element, StrategyHost_, fluent);
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

        Y_UNUSED(WaitFor(BIND(&TFairShareTree::DoBuildFullFairShareInfo, MakeWeak(this), TreeSnapshot_, fluent)
            .AsyncVia(StrategyHost_->GetOrchidWorkerInvoker())
            .Run()));
    }

    class TFieldsFilter final
    {
    public:
        TFieldsFilter()
            : Filter_{ParseFiterFromOptions({})}
        { }

        TFieldsFilter(const IAttributeDictionaryPtr& options)
            : Filter_{ParseFiterFromOptions(options)}
        { }

        bool IsFieldSuitable(TStringBuf field) const
        {
            if (!Filter_) {
                return true;
            }

            return Filter_->contains(field);
        }

    private:
        std::optional<THashSet<TString>> Filter_;

        static std::optional<THashSet<TString>> ParseFiterFromOptions(
            const IAttributeDictionaryPtr& options)
        {
            if (!options) {
                return std::nullopt;
            }

            auto fields = options->Find<THashSet<TString>>("fields");
            if (!fields) {
                return std::nullopt;
            }

            return std::move(*fields);
        }
    };

    static IYPathServicePtr FromProducer(
        NYson::TExtendedYsonProducer<const TFieldsFilter&> producer)
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

        dynamicOrchidService->AddChild("operations", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshot();
            if (!treeSnapshot) {
                ThrowOrchidIsNotReady();
            }

            const auto buildOperationInfo = [&] (TFluentMap fluent, const TSchedulerOperationElement* const operation) {
                fluent
                    .Item(operation->GetId()).BeginMap()
                        .Do(BIND(
                            &TFairShareTree::DoBuildOperationProgress,
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
            auto treeSnapshot = GetTreeSnapshot();
            if (!treeSnapshot) {
                ThrowOrchidIsNotReady();
            }

            BuildYsonFluently(consumer).Value(treeSnapshot->TreeConfig());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("resource_usage", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshot();
            if (!treeSnapshot) {
                ThrowOrchidIsNotReady();
            }

            BuildYsonFluently(consumer).Value(treeSnapshot->ResourceUsage());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("resource_limits", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshot();
            if (!treeSnapshot) {
                ThrowOrchidIsNotReady();
            }

            BuildYsonFluently(consumer).Value(treeSnapshot->ResourceLimits());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("pool_count", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

            BuildYsonFluently(consumer)
                .Value(GetPoolCount());
        })));

        dynamicOrchidService->AddChild("pools", FromProducer(BIND(
            [this_ = MakeStrong(this), this] (IYsonConsumer* consumer, const TFieldsFilter& filter) {
                auto treeSnapshot = GetTreeSnapshot();
                if (!treeSnapshot) {
                    ThrowOrchidIsNotReady();
                }

                BuildYsonFluently(consumer).BeginMap()
                    .Do(BIND(&TFairShareTree::DoBuildPoolsInformation, Unretained(this), std::move(treeSnapshot), filter))
                .EndMap();
            }))->Via(StrategyHost_->GetOrchidWorkerInvoker()));

        dynamicOrchidService->AddChild("resource_distribution_info", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshot();
            if (!treeSnapshot) {
                ThrowOrchidIsNotReady();
            }

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

        return TreeProfiler_.Get();
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
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig_;

    TResourceTreePtr ResourceTree_;
    TFairShareTreeProfileManagerPtr TreeProfiler_;

    ISchedulerStrategyHost* const StrategyHost_;

    const std::vector<IInvokerPtr> FeasibleInvokers_;

    INodePtr LastPoolsNodeUpdate_;
    TError LastPoolsNodeUpdateError_;

    const TString TreeId_;

    const NLogging::TLogger Logger;

    TFairShareTreeJobSchedulerPtr TreeScheduler_;

    TPoolElementMap Pools_;

    std::optional<TInstant> LastFairShareUpdateTime_;

    THashMap<TString, THashSet<TString>> UserToEphemeralPoolsInDefaultPool_;

    THashMap<TString, THashSet<int>> PoolToSpareSlotIndices_;
    THashMap<TString, int> PoolToMinUnusedSlotIndex_;

    TOperationElementMap OperationIdToElement_;

    THashMap<TOperationId, TInstant> OperationIdToActivationTime_;
    THashMap<TOperationId, TInstant> OperationIdToFirstFoundLimitingAncestorTime_;

    TAtomicPtr<TResourceUsageSnapshot> ResourceUsageSnapshot_;

    std::vector<TOperationId> ActivatableOperationIds_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, NodeIdToLastPreemptiveSchedulingTimeLock_);
    THashMap<TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime_;

    TSchedulerRootElementPtr RootElement_;

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

            return std::ssize(FairShareTree_->GetTreeSnapshot()->PoolMap());
        }

        std::vector<TString> GetKeys(const i64 limit) const final
        {
            VERIFY_INVOKER_AFFINITY(FairShareTree_->StrategyHost_->GetOrchidWorkerInvoker());

            if (!limit) {
                return {};
            }

            const auto fairShareTreeSnapshot = FairShareTree_->GetTreeSnapshot();
            if (!fairShareTreeSnapshot) {
                FairShareTree_->ThrowOrchidIsNotReady();
            }

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

            const auto fairShareTreeSnapshot = FairShareTree_->GetTreeSnapshot();
            if (!fairShareTreeSnapshot) {
                FairShareTree_->ThrowOrchidIsNotReady();
            }

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
                                    .Do(BIND(
                                        &TFairShareTree::DoBuildOperationProgress,
                                        Unretained(operation),
                                        FairShareTree_->StrategyHost_))
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

    TFairShareTreeSnapshotPtr TreeSnapshot_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, TreeSnapshotLock_);

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

    TFairShareTreeSnapshotPtr GetTreeSnapshot() const noexcept
    {
        VERIFY_THREAD_AFFINITY_ANY();
        auto guard = Guard(TreeSnapshotLock_);
        return TreeSnapshot_;
    }

    std::pair<IFairShareTreePtr, TError> DoFairShareUpdateAt(TInstant now)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        ResourceTree_->PerformPostponedActions();

        auto totalResourceLimits = StrategyHost_->GetResourceLimits(Config_->NodesFilter);
        TFairShareUpdateContext updateContext(
            totalResourceLimits,
            Config_->MainResource,
            Config_->IntegralGuarantees->PoolCapacitySaturationPeriod,
            Config_->IntegralGuarantees->SmoothPeriod,
            now,
            LastFairShareUpdateTime_);

        TFairSharePostUpdateContext fairSharePostUpdateContext{
            .TreeConfig = Config_,
            .Now = updateContext.Now,
        };
        auto jobSchedulerPostUpdateContext = TreeScheduler_->CreatePostUpdateContext(totalResourceLimits);

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

                    TFairShareUpdateExecutor updateExecutor(rootElement, &updateContext);
                    updateExecutor.Run();

                    rootElement->PostUpdate(&fairSharePostUpdateContext);
                    TreeScheduler_->PostUpdate(&fairSharePostUpdateContext, &jobSchedulerPostUpdateContext);
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
            rootElement->SchedulableElementCount(),
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

        rootElement->MarkImmutable();

        auto treeSnapshotId = TTreeSnapshotId::Create();

        const auto resourceUsage = StrategyHost_->GetResourceUsage(GetNodesFilter());
        const auto resourceLimits = StrategyHost_->GetResourceLimits(GetNodesFilter());

        auto treeSchedulingSnapshot = TreeScheduler_->CreateSchedulingSnapshot(rootElement, &jobSchedulerPostUpdateContext);
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
            std::move(treeSchedulingSnapshot));

        if (Config_->EnableResourceUsageSnapshot) {
            TreeScheduler_->OnResourceUsageSnapshotUpdate(treeSnapshot, ResourceUsageSnapshot_.Acquire());
        }

        YT_LOG_DEBUG("Fair share tree snapshot created (TreeSnapshotId: %v)", treeSnapshotId);

        TreeSnapshotPrecommit_ = std::move(treeSnapshot);
        LastFairShareUpdateTime_ = now;

        return std::make_pair(MakeStrong(this), error);
    }

    void DoRegisterPool(const TSchedulerPoolElementPtr& pool)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        int index = TreeScheduler_->RegisterSchedulingTagFilter(pool->GetSchedulingTagFilter());
        pool->SetSchedulingTagFilterIndex(index);
        YT_VERIFY(Pools_.emplace(pool->GetId(), pool).second);
        YT_VERIFY(PoolToMinUnusedSlotIndex_.emplace(pool->GetId(), 0).second);

        TreeProfiler_->RegisterPool(pool);
    }

    void RegisterPool(const TSchedulerPoolElementPtr& pool, const TSchedulerCompositeElementPtr& parent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        DoRegisterPool(pool);

        pool->AttachParent(parent.Get());

        YT_LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void ReconfigurePool(const TSchedulerPoolElementPtr& pool, const TPoolConfigPtr& config)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto oldSchedulingTagFilter = pool->GetSchedulingTagFilter();
        pool->SetConfig(config);
        auto newSchedulingTagFilter = pool->GetSchedulingTagFilter();
        if (oldSchedulingTagFilter != newSchedulingTagFilter) {
            TreeScheduler_->UnregisterSchedulingTagFilter(oldSchedulingTagFilter);
            int index = TreeScheduler_->RegisterSchedulingTagFilter(newSchedulingTagFilter);
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

        TreeScheduler_->UnregisterSchedulingTagFilter(pool->GetSchedulingTagFilterIndex());

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
    const TSchedulerElement* FindAncestorWithInsufficientSpecifiedResourceLimits(const TSchedulerElement* element, const TJobResources& neededResources) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const TSchedulerElement* current = element;
        while (current) {
            // NB(eshcherbin): We expect that |GetSpecifiedResourcesLimits| return infinite limits when no limits were specified.
            if (!Dominates(current->GetSpecifiedResourceLimits(), neededResources)) {
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

    void ValidateAllOperationsCountsOnPoolChange(TOperationId operationId, const TPoolName& newPoolName) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

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
                THROW_ERROR_EXCEPTION("Limit for number of ephemeral pools %v for user %Qv in tree %Qv has been reached",
                    Config_->MaxEphemeralPoolsPerUser,
                    userName,
                    TreeId_);
            }
        }
    }

    void ValidateSpecifiedResourceLimits(
        const IOperationStrategyHost* operation,
        const TSchedulerCompositeElementPtr& pool,
        const TJobResourcesConfigPtr& requiredResourceLimits) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        // TODO(egor-gutrov): remove after fixing test_user_slots_validation
        YT_LOG_DEBUG("Validating resource limits (RequiredResourceLimits.UserSlots: %v)", requiredResourceLimits->UserSlots);
        auto requiredLimits = ToJobResources(requiredResourceLimits, TJobResources::Infinite());
        auto actualLimits = ToJobResources(operation->GetStrategySpec()->ResourceLimits, TJobResources::Infinite());
        if (Dominates(requiredLimits, actualLimits)) {
            return;
        }
        const auto* current = pool.Get();
        while (!current->IsRoot()) {
            actualLimits = Min(actualLimits, current->GetSpecifiedResourceLimits());
            if (Dominates(requiredLimits, actualLimits)) {
                return;
            }
            current = current->GetParent();
        }
        THROW_ERROR_EXCEPTION(
            "Operations of type %Qv must have small enough specified resource limits in spec or in some of ancestor pools",
            operation->GetType())
            << TErrorAttribute("operation_id", operation->GetId())
            << TErrorAttribute("required_resource_limits", requiredResourceLimits)
            << TErrorAttribute("tree_id", TreeId_);
    }

    void DoValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName) const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        TSchedulerCompositeElementPtr pool = FindPool(poolName.GetPool());
        // NB: Check is not performed if operation is started in default or unknown pool.
        if (pool && pool->AreImmediateOperationsForbidden()) {
            THROW_ERROR_EXCEPTION("Starting operations immediately in pool %Qv is forbidden", poolName.GetPool());
        }

        if (!pool) {
            pool = GetPoolOrParent(poolName, operation->GetAuthenticatedUser());
        }

        if (operation->GetType() == EOperationType::RemoteCopy && Config_->FailRemoteCopyOnMissingResourceLimits) {
            ValidateSpecifiedResourceLimits(operation, pool, Config_->RequiredResourceLimitsForRemoteCopy);
        }
        StrategyHost_->ValidatePoolPermission(
            pool->GetFullPath(/*explicitOnly*/ true),
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
        auto treeSnapshot = GetTreeSnapshot();
        if (treeSnapshot) {
            if (auto element = treeSnapshot->FindEnabledOperationElement(operationId)) {
                return element;
            }
        }
        return nullptr;
    }

    TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        auto scheduleJobsFuture = BIND(
            &TFairShareTreeJobScheduler::ScheduleJobs,
            TreeScheduler_,
            schedulingContext,
            treeSnapshot)
            .AsyncVia(GetCurrentInvoker())
            .Run();

        return scheduleJobsFuture
            .Apply(BIND(
                &TFairShareTree::ApplyScheduledAndPreemptedResourcesDelta,
                MakeStrong(this),
                schedulingContext,
                treeSnapshot));
    }

    void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext) override
    {
        auto treeSnapshot = GetTreeSnapshot();
        YT_VERIFY(treeSnapshot);
        TreeScheduler_->PreemptJobsGracefully(schedulingContext, treeSnapshot);
    }

    void ProcessUpdatedJob(
        TOperationId operationId,
        TJobId jobId,
        const TJobResources& jobResources,
        const std::optional<TString>& jobDataCenter,
        const std::optional<TString>& jobInfinibandCluster,
        bool* shouldAbortJob) override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        // NB: Should be filtered out on large clusters.
        YT_LOG_DEBUG("Processing updated job (OperationId: %v, JobId: %v, Resources: %v)", operationId, jobId, jobResources);

        *shouldAbortJob = false;

        auto* operationElement = treeSnapshot->FindEnabledOperationElement(operationId);
        if (operationElement) {
            operationElement->SetJobResourceUsage(jobId, jobResources);

            const auto& operationSchedulingSegment = operationElement->SchedulingSegment();
            if (operationSchedulingSegment && IsModuleAwareSchedulingSegment(*operationSchedulingSegment)) {
                const auto& operationModule = operationElement->PersistentAttributes().SchedulingSegmentModule;
                const auto& jobModule = TNodeSchedulingSegmentManager::GetNodeModule(
                    jobDataCenter,
                    jobInfinibandCluster,
                    treeSnapshot->TreeConfig()->SchedulingSegments->ModuleType);
                bool jobIsRunningInTheRightModule = operationModule && (operationModule == jobModule);
                if (!jobIsRunningInTheRightModule) {
                    *shouldAbortJob = true;

                    YT_LOG_DEBUG(
                        "Requested to abort job because it is running in a wrong module "
                        "(OperationId: %v, JobId: %v, OperationModule: %v, JobModule: %v)",
                        operationId,
                        jobId,
                        operationModule,
                        jobModule);
                }
            }
        }
    }

    bool ProcessFinishedJob(TOperationId operationId, TJobId jobId) override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        // NB: Should be filtered out on large clusters.
        YT_LOG_DEBUG("Processing finished job (OperationId: %v, JobId: %v)", operationId, jobId);
        auto* operationElement = treeSnapshot->FindEnabledOperationElement(operationId);
        if (operationElement) {
            operationElement->OnJobFinished(jobId);
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
            TreeProfiler_,
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

        THashMap<std::optional<EJobSchedulingStage>, TOperationIdToJobResources> scheduledJobResources;
        TEnumIndexedVector<EJobPreemptionReason, TOperationIdToJobResources> preemptedJobResources;
        TEnumIndexedVector<EJobPreemptionReason, TOperationIdToJobResources> preemptedJobResourceTimes;

        for (const auto& job : schedulingContext->StartedJobs()) {
            TOperationId operationId = job->GetOperationId();
            const TJobResources& scheduledResourcesDelta = job->ResourceLimits();
            scheduledJobResources[job->GetSchedulingStage()][operationId] += scheduledResourcesDelta;
        }
        for (const auto& preemptedJob : schedulingContext->PreemptedJobs()) {
            const TJobPtr& job = preemptedJob.Job;
            TOperationId operationId = job->GetOperationId();
            const TJobResources& preemptedResourcesDelta = job->ResourceLimits();
            EJobPreemptionReason preemptionReason = preemptedJob.PreemptionReason;
            preemptedJobResources[preemptionReason][operationId] += preemptedResourcesDelta;
            // TODO(eshcherbin): Maybe use some other time statistic.
            // Exec duration does not capture the job preparation time (e.g. downloading artifacts).
            preemptedJobResourceTimes[preemptionReason][operationId] += preemptedResourcesDelta * static_cast<i64>(job->GetExecDuration().Seconds());
        }

        StrategyHost_->GetFairShareProfilingInvoker()->Invoke(BIND(
            &TFairShareTreeProfileManager::ApplyScheduledAndPreemptedResourcesDelta,
            TreeProfiler_,
            treeSnapshot,
            Passed(std::move(scheduledJobResources)),
            Passed(std::move(preemptedJobResources)),
            Passed(std::move(preemptedJobResourceTimes))));
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
                element->Attributes().PromisedFairShare};
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

    TCachedJobPreemptionStatuses GetCachedJobPreemptionStatuses() const override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        return treeSnapshot->SchedulingSnapshot()->CachedJobPreemptionStatuses();
    }

    void ProfileFairShare() const override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        TreeProfiler_->ProfileElements(
            treeSnapshot,
            AccumulatedOperationsResourceUsageForProfiling_.ExtractOperationResourceUsages());
    }

    void LogFairShareAt(TInstant now) const override
    {
        auto treeSnapshot = GetTreeSnapshot();

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
                treeSnapshot->TreeConfig()->MaxEventLogOperationBatchSize);
            auto logFairShareEventFluently = [&] {
                return StrategyHost_->LogFairShareEventFluently(now)
                    .Item(EventLogPoolTreeKey).Value(TreeId_)
                    .Item("tree_snapshot_id").Value(treeSnapshotId);
            };

            // NB(eshcherbin, YTADMIN-11230): First we log a single event with pools info and resource distribution info.
            // Then we split all operations' info into several batches and log every batch in a separate event.
            logFairShareEventFluently()
                .Items(fairShareInfo.PoolsInfo)
                .Items(fairShareInfo.ResourceDistributionInfo);

            for (int batchIndex = 0; batchIndex < std::ssize(fairShareInfo.SplitOperationsInfo); ++batchIndex) {
                const auto& operationsInfoBatch = fairShareInfo.SplitOperationsInfo[batchIndex];
                logFairShareEventFluently()
                    .Item("operations_batch_index").Value(batchIndex)
                    .Item("operations").BeginMap()
                        .Items(operationsInfoBatch)
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
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        StrategyHost_->LogAccumulatedUsageEventFluently(TInstant::Now())
            .Item(EventLogPoolTreeKey).Value(TreeId_)
            .Item("pools").BeginMap()
                .Do(BIND(&TFairShareTree::DoBuildPoolsStructureInfo, Unretained(this), treeSnapshot))
            .EndMap()
            .Item("operations").BeginMap()
                .Do(BIND(&TFairShareTree::DoBuildOperationsAccumulatedUsageInfo, Unretained(this), treeSnapshot))
            .EndMap();
    }

    void EssentialLogFairShareAt(TInstant now) const override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        {
            TEventTimerGuard timer(FairShareFluentLogTimer_);
            StrategyHost_->LogFairShareEventFluently(now)
                .Item(EventLogPoolTreeKey).Value(TreeId_)
                .Item("tree_snapshot_id").Value(treeSnapshot->GetId())
                .Do(BIND(&TFairShareTree::DoBuildEssentialFairShareInfo, Unretained(this), treeSnapshot));
        }

        {
            TEventTimerGuard timer(FairShareTextLogTimer_);
            LogPoolsInfo(treeSnapshot);
            LogOperationsInfo(treeSnapshot);
        }
    }

    void UpdateResourceUsages() override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        auto operationResourceUsageMap = THashMap<TOperationId, TJobResources>(treeSnapshot->EnabledOperationMap().size());
        auto poolResourceUsageMap = THashMap<TString, TJobResources>(treeSnapshot->PoolMap().size());
        auto aliveOperationIds = THashSet<TOperationId>(treeSnapshot->EnabledOperationMap().size());

        for (const auto& [operationId, element] : treeSnapshot->EnabledOperationMap()) {
            bool isAlive = element->IsAlive();
            if (!isAlive) {
                continue;
            }
            aliveOperationIds.insert(operationId);
            auto resourceUsage = element->GetInstantResourceUsage();
            operationResourceUsageMap[operationId] = resourceUsage;
            const TSchedulerCompositeElement* parentPool = element->GetParent();
            while (parentPool) {
                poolResourceUsageMap[parentPool->GetId()] += resourceUsage;
                parentPool = parentPool->GetParent();
            }
        }

        auto resourceUsageSnapshot = New<TResourceUsageSnapshot>();
        resourceUsageSnapshot->OperationIdToResourceUsage = std::move(operationResourceUsageMap);
        resourceUsageSnapshot->PoolToResourceUsage = std::move(poolResourceUsageMap);
        resourceUsageSnapshot->AliveOperationIds = std::move(aliveOperationIds);

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
                YT_LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                    element->GetLoggingString(),
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
                element->GetLoggingString(),
                poolName);
        }
    }

    void DoBuildFullFairShareInfo(const TFairShareTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        if (!treeSnapshot) {
            YT_LOG_DEBUG("Skipping construction of full fair share info, since shapshot is not constructed yet");
            return;
        }

        YT_LOG_DEBUG("Constructing full fair share info");

        auto fairShareInfo = BuildSerializedFairShareInfo(treeSnapshot);
        fluent
            .Items(fairShareInfo.PoolsInfo)
            .Items(fairShareInfo.ResourceDistributionInfo)
            .Item("operations").BeginMap()
                .DoFor(fairShareInfo.SplitOperationsInfo, [&] (TFluentMap fluent, const TYsonString& operationsInfoBatch) {
                    fluent.Items(operationsInfoBatch);
                })
            .EndMap();
    }

    struct TSerializedFairShareInfo
    {
        NYson::TYsonString PoolsInfo;
        NYson::TYsonString ResourceDistributionInfo;
        std::vector<NYson::TYsonString> SplitOperationsInfo;
    };

    TSerializedFairShareInfo BuildSerializedFairShareInfo(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        int maxOperationBatchSize = std::numeric_limits<int>::max()) const
    {
        YT_LOG_DEBUG("Started building serialized fair share info (MaxOperationBatchSize: %v)",
            maxOperationBatchSize);

        TSerializedFairShareInfo fairShareInfo;
        fairShareInfo.PoolsInfo = BuildYsonStringFluently<EYsonType::MapFragment>()
            .Item("pool_count").Value(treeSnapshot->PoolMap().size())
            .Item("pools").BeginMap()
                .Do(BIND(&TFairShareTree::DoBuildPoolsInformation, Unretained(this), treeSnapshot, TFieldsFilter{}))
            .EndMap()
            .Finish();
        fairShareInfo.ResourceDistributionInfo = BuildYsonStringFluently<EYsonType::MapFragment>()
            .Item("resource_distribution_info").BeginMap()
                .Do(BIND(&TSchedulerRootElement::BuildResourceDistributionInfo, treeSnapshot->RootElement()))
            .EndMap()
            .Finish();

        std::vector<TSchedulerOperationElement*> operations;
        operations.reserve(treeSnapshot->EnabledOperationMap().size() + treeSnapshot->DisabledOperationMap().size());
        for (const auto& [_, element] : treeSnapshot->EnabledOperationMap()) {
            operations.push_back(element);
        }
        for (const auto& [_, element] : treeSnapshot->DisabledOperationMap()) {
            operations.push_back(element);
        }

        int operationBatchCount = 0;
        auto batchStart = operations.begin();
        while (batchStart < operations.end()) {
            auto batchEnd = (std::distance(batchStart, operations.end()) > maxOperationBatchSize)
                ? std::next(batchStart, maxOperationBatchSize)
                : operations.end();
            auto operationsInfoBatch = BuildYsonStringFluently<EYsonType::MapFragment>()
                .DoFor(batchStart, batchEnd, [&] (TFluentMap fluent, std::vector<TSchedulerOperationElement*>::iterator it) {
                    auto* element = *it;
                    fluent
                        .Item(element->GetId()).BeginMap()
                            .Do(BIND(&TFairShareTree::DoBuildOperationProgress, Unretained(element), StrategyHost_))
                        .EndMap();
                })
                .Finish();
            fairShareInfo.SplitOperationsInfo.push_back(std::move(operationsInfoBatch));

            batchStart = batchEnd;
            ++operationBatchCount;
        }

        YT_LOG_DEBUG("Finished building serialized fair share info (MaxOperationBatchSize: %v, OperationCount: %v, OperationBatchCount: %v)",
            maxOperationBatchSize,
            operations.size(),
            operationBatchCount);

        return fairShareInfo;
    }

    void DoBuildPoolsInformation(const TFairShareTreeSnapshotPtr& treeSnapshot, const TFieldsFilter& filter, TFluentMap fluent) const
    {
        auto buildCompositeElementInfo = [&] (const TSchedulerCompositeElement* element, TFluentMap fluent) {
            const auto& attributes = element->Attributes();
            fluent
                .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "running_operation_count", element->RunningOperationCount())
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
                .Do(std::bind(&TFairShareTree::DoBuildElementYson, element, filter, std::placeholders::_1));
        };

        auto buildPoolInfo = [&] (const TSchedulerPoolElement* pool, TFluentMap fluent) {
            const auto& id = pool->GetId();
            fluent
                .Item(id).BeginMap()
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
                    .Do(std::bind(buildCompositeElementInfo, pool, std::placeholders::_1))
                .EndMap();
        };

        fluent
            .DoFor(treeSnapshot->PoolMap(), [&] (TFluentMap fluent, const TNonOwningPoolElementMap::value_type& pair) {
                buildPoolInfo(pair.second, fluent);
            })
            .Item(RootPoolName).BeginMap()
                .Do(std::bind(buildCompositeElementInfo, treeSnapshot->RootElement().Get(), std::placeholders::_1))
            .EndMap();
    }

    void DoBuildPoolsStructureInfo(const TFairShareTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        auto buildPoolInfo = [&] (const TSchedulerPoolElement* pool, TFluentMap fluent) {
            const auto& id = pool->GetId();
            fluent
                .Item(id).BeginMap()
                    .Item("abc").Value(pool->GetConfig()->Abc)
                    .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                        fluent
                            .Item("parent").Value(pool->GetParent()->GetId());
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
                    .Item("operation_type").Value(operation->GetType())
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
        const TSchedulerOperationElement* element,
        ISchedulerStrategyHost* const strategyHost,
        TFluentMap fluent)
    {
        auto* parent = element->GetParent();
        fluent
            .Item("pool").Value(parent->GetId())
            .Item("slot_index").Value(element->GetSlotIndex())
            .Item("scheduling_segment").Value(element->SchedulingSegment())
            .Item("scheduling_segment_module").Value(element->PersistentAttributes().SchedulingSegmentModule)
            .Item("start_time").Value(element->GetStartTime())
            .Item("preemptable_job_count").Value(element->GetPreemptableJobCount())
            .Item("aggressively_preemptable_job_count").Value(element->GetAggressivelyPreemptableJobCount())
            .OptionalItem("fifo_index", element->Attributes().FifoIndex)
            .Item("scheduling_index").Value(element->GetSchedulingIndex())
            .Item("deactivation_reasons").Value(element->GetDeactivationReasons())
            .Item("min_needed_resources_unsatisfied_count").Value(element->GetMinNeededResourcesUnsatisfiedCount())
            .Item("detailed_min_needed_job_resources").BeginList()
                .DoFor(element->DetailedMinNeededJobResources(), [&] (TFluentList fluent, const TJobResourcesWithQuota& jobResourcesWithQuota) {
                    fluent.Item().Do([&] (TFluentAny fluent) {
                        strategyHost->SerializeResources(jobResourcesWithQuota, fluent.GetConsumer());
                    });
                })
            .EndList()
            .Item("aggregated_min_needed_job_resources").Value(element->AggregatedMinNeededJobResources())
            .Item("tentative").Value(element->GetRuntimeParameters()->Tentative)
            .Item("starving_since").Value(element->GetStarvationStatus() != EStarvationStatus::NonStarving
                ? std::make_optional(element->GetLastNonStarvingTime())
                : std::nullopt)
            .Item("disk_request_media").DoListFor(element->DiskRequestMedia(), [&] (TFluentList fluent, int mediumIndex) {
                fluent.Item().Value(strategyHost->GetMediumNameByIndex(mediumIndex));
            })
            .Item("disk_quota_usage").BeginMap()
                .Do([&] (TFluentMap fluent) {
                    strategyHost->SerializeDiskQuota(element->GetTotalDiskQuota(), fluent.GetConsumer());
                })
            .EndMap()
            .Do(BIND(&TFairShareTree::DoBuildElementYson, Unretained(element), TFieldsFilter{}));
    }

    static void DoBuildElementYson(const TSchedulerElement* element, const TFieldsFilter& filter, TFluentMap fluent)
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
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "aggressive_starvation_enabled", element->IsAggressiveStarvationEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_aggressive_starvation_enabled",
                element->GetEffectiveAggressiveStarvationEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "aggressive_preemption_allowed", element->IsAggressivePreemptionAllowed())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_aggressive_preemption_allowed",
                element->GetEffectiveAggressivePreemptionAllowed())
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
                std::bind(&SerializeDominant, attributes.FairShare, std::placeholders::_1))

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "promised_fair_share", attributes.PromisedFairShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "promised_dominant_fair_share", MaxComponent(attributes.PromisedFairShare))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "promised_fair_share_resources", promisedFairShareResources)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "proposed_integral_share", attributes.ProposedIntegralShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "best_allocation_share", persistentAttributes.BestAllocationShare)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "satisfaction_ratio", attributes.SatisfactionRatio)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "local_satisfaction_ratio", attributes.LocalSatisfactionRatio);
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

IFairShareTreePtr CreateFairShareTree(
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
