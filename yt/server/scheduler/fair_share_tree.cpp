#include "fair_share_tree.h"
#include "fair_share_tree_element.h"
#include "fair_share_tree_element_classic.h"
#include "fair_share_implementations.h"
#include "public.h"
#include "pools_config_parser.h"
#include "resource_tree.h"
#include "scheduler_strategy.h"
#include "scheduler_tree.h"
#include "scheduling_context.h"
#include "fair_share_strategy_operation_controller.h"

#include "operation_log.h"

#include <yt/server/lib/scheduler/config.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/async_rw_lock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/algorithm_helpers.h>
#include <yt/core/misc/finally.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>
#include <yt/core/profiling/metrics_accumulator.h>

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

static const auto& Profiler = SchedulerProfiler;

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyOperationState::TFairShareStrategyOperationState(IOperationStrategyHost* host)
    : Host_(host)
    , Controller_(New<TFairShareStrategyOperationController>(host))
{ }

TPoolName TFairShareStrategyOperationState::GetPoolNameByTreeId(const TString& treeId) const
{
    return GetOrCrash(TreeIdToPoolNameMap_, treeId);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

TTagId GetSlotIndexProfilingTag(int slotIndex)
{
    static THashMap<int, TTagId> slotIndexToTagIdMap;

    auto it = slotIndexToTagIdMap.find(slotIndex);
    if (it == slotIndexToTagIdMap.end()) {
        it = slotIndexToTagIdMap.emplace(
            slotIndex,
            TProfileManager::Get()->RegisterTag("slot_index", ToString(slotIndex))
        ).first;
    }
    return it->second;
};

TTagId GetUserNameProfilingTag(const TString& userName)
{
    static THashMap<TString, TTagId> userNameToTagIdMap;

    auto it = userNameToTagIdMap.find(userName);
    if (it == userNameToTagIdMap.end()) {
        it = userNameToTagIdMap.emplace(
            userName,
            TProfileManager::Get()->RegisterTag("user_name", userName)
        ).first;
    }
    return it->second;
};

} // namespace

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

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TRootElementSnapshot::FindOperationElement(
    TOperationId operationId) const -> TOperationElement*
{
    auto it = OperationIdToElement.find(operationId);
    return it != OperationIdToElement.end() ? it->second : nullptr;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TRootElementSnapshot::FindDisabledOperationElement(
    TOperationId operationId) const -> TOperationElement*
{
    auto it = DisabledOperationIdToElement.find(operationId);
    return it != DisabledOperationIdToElement.end() ? it->second : nullptr;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TRootElementSnapshot::FindPool(const TString& poolName) const -> TPool*
{
    auto it = PoolNameToElement.find(poolName);
    return it != PoolNameToElement.end() ? it->second : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

template <class TFairShareImpl>
TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::TFairShareTreeSnapshot(
    TFairShareTreePtr tree,
    TFairShareTree::TRootElementSnapshotPtr rootElementSnapshot,
    TSchedulingTagFilter nodesFilter,
    TJobResources totalResourceLimits,
    const NLogging::TLogger& logger)
    : Tree_(std::move(tree))
    , RootElementSnapshot_(std::move(rootElementSnapshot))
    , NodesFilter_(std::move(nodesFilter))
    , TotalResourceLimits_(std::move(totalResourceLimits))
    , Logger(logger)
{ }

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::ScheduleJobs(
    const ISchedulingContextPtr& schedulingContext) -> TFuture<void>
{
    return BIND(&TFairShareTree::DoScheduleJobs,
        Tree_,
        schedulingContext,
        RootElementSnapshot_)
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::PreemptJobsGracefully(
    const ISchedulingContextPtr& schedulingContext) -> void
{
    Tree_->DoPreemptJobsGracefully(schedulingContext, RootElementSnapshot_);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::ProcessUpdatedJob(
    TOperationId operationId,
    TJobId jobId,
    const TJobResources& delta) -> void
{
    // NB: Should be filtered out on large clusters.
    YT_LOG_DEBUG("Processing updated job (OperationId: %v, JobId: %v)", operationId, jobId);
    auto* operationElement = RootElementSnapshot_->FindOperationElement(operationId);
    if (operationElement) {
        operationElement->IncreaseJobResourceUsage(jobId, delta);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::ProcessFinishedJob(
    TOperationId operationId,
    TJobId jobId) -> void
{
    // NB: Should be filtered out on large clusters.
    YT_LOG_DEBUG("Processing finished job (OperationId: %v, JobId: %v)", operationId, jobId);
    auto* operationElement = RootElementSnapshot_->FindOperationElement(operationId);
    if (operationElement) {
        operationElement->OnJobFinished(jobId);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::ApplyJobMetricsDelta(
    TOperationId operationId,
    const TJobMetrics& jobMetricsDelta) -> void
{
    auto* operationElement = RootElementSnapshot_->FindOperationElement(operationId);
    if (operationElement) {
        operationElement->ApplyJobMetricsDelta(jobMetricsDelta);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::ProfileFairShare() const -> void
{
    Tree_->DoProfileFairShare(RootElementSnapshot_);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::LogFairShare(NEventLog::TFluentLogEvent fluent) const -> void
{
    Tree_->DoLogFairShare(RootElementSnapshot_, std::move(fluent));
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::EssentialLogFairShare(NEventLog::TFluentLogEvent fluent) const -> void
{
    Tree_->DoEssentialLogFairShare(RootElementSnapshot_, std::move(fluent));
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::HasOperation(TOperationId operationId) const -> bool
{
    auto* operationElement = RootElementSnapshot_->FindOperationElement(operationId);
    return operationElement != nullptr;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::IsOperationRunningInTree(TOperationId operationId) const -> bool
{
    if (auto* element = RootElementSnapshot_->FindOperationElement(operationId)) {
        auto res = element->IsOperationRunningInPool();
        return res;
    }

    if (auto* element = RootElementSnapshot_->FindDisabledOperationElement(operationId)) {
        auto res = element->IsOperationRunningInPool();
        return res;
    }

    return false;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::IsOperationDisabled(TOperationId operationId) const -> bool
{
    return RootElementSnapshot_->DisabledOperationIdToElement.contains(operationId);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::GetNodesFilter() const -> const TSchedulingTagFilter&
{
    return NodesFilter_;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::GetTotalResourceLimits() const -> TJobResources
{
    return TotalResourceLimits_;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TFairShareTreeSnapshot::GetMaybeStateSnapshotForPool(
    const TString& poolId) const -> std::optional<TSchedulerElementStateSnapshot>
{
    if (auto* element = RootElementSnapshot_->FindPool(poolId)) {
        return TSchedulerElementStateSnapshot{
            element->ResourceDemand(),
            element->Attributes().GetGuaranteedResourcesRatio()};
    }

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

template <class TFairShareImpl>
TFairShareTree<TFairShareImpl>::TFairShareTree(
    TFairShareStrategyTreeConfigPtr config,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    ISchedulerStrategyHost* strategyHost,
    ITreeHost* treeHost,
    std::vector<IInvokerPtr> feasibleInvokers,
    const TString& treeId)
    : Config_(std::move(config))
    , ControllerConfig_(std::move(controllerConfig))
    , ResourceTree_(New<TResourceTree>())
    , StrategyHost_(strategyHost)
    , TreeHost_(treeHost)
    , FeasibleInvokers_(std::move(feasibleInvokers))
    , TreeId_(treeId)
    , TreeIdProfilingTag_(TProfileManager::Get()->RegisterTag("tree", TreeId_))
    , Logger(NLogging::TLogger(SchedulerLogger)
        .AddTag("TreeId: %v", treeId))
    , NonPreemptiveSchedulingStage_(
        /* nameInLogs */ "Non preemptive",
        TScheduleJobsProfilingCounters("/non_preemptive", {TreeIdProfilingTag_}))
    , PreemptiveSchedulingStage_(
        /* nameInLogs */ "Preemptive",
        TScheduleJobsProfilingCounters("/preemptive", {TreeIdProfilingTag_}))
    , PackingFallbackSchedulingStage_(
        /* nameInLogs */ "Packing fallback",
        TScheduleJobsProfilingCounters("/packing_fallback", {TreeIdProfilingTag_}))
    , FairSharePreUpdateTimeCounter_("/fair_share_preupdate_time", {TreeIdProfilingTag_})
    , FairShareUpdateTimeCounter_("/fair_share_update_time", {TreeIdProfilingTag_})
    , FairShareFluentLogTimeCounter_("/fair_share_fluent_log_time", {TreeIdProfilingTag_})
    , FairShareTextLogTimeCounter_("/fair_share_text_log_time", {TreeIdProfilingTag_})
    , AnalyzePreemptableJobsTimeCounter_("/analyze_preemptable_jobs_time", {TreeIdProfilingTag_})
{
    RootElement_ = New<TRootElement>(StrategyHost_, this, Config_, GetPoolProfilingTag(RootPoolName), TreeId_, Logger);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetConfig() const -> TFairShareStrategyTreeConfigPtr
{
    return Config_;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ValidateOperationPoolsCanBeUsed(
    const IOperationStrategyHost* operation,
    const TPoolName& poolName) -> TFuture<void>
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    return BIND(&TFairShareTree::DoValidateOperationPoolsCanBeUsed, MakeStrong(this))
        .AsyncVia(GetCurrentInvoker())
        .Run(operation, poolName);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ValidatePoolLimits(
    const IOperationStrategyHost* operation,
    const TPoolName& poolName) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    ValidateOperationCountLimit(operation, poolName);
    ValidateEphemeralPoolLimit(operation, poolName);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ValidatePoolLimitsOnPoolChange(const IOperationStrategyHost* operation, const TPoolName& newPoolName) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    ValidateEphemeralPoolLimit(operation, newPoolName);
    ValidateAllOperationsCountsOnPoolChange(operation->GetId(), newPoolName);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetPoolsToValidateOperationCountsOnPoolChange(
    TOperationId operationId,
    const TPoolName& newPoolName) -> std::vector<const TCompositeSchedulerElement*>
{
    auto operationElement = GetOperationElement(operationId);

    std::vector<const TCompositeSchedulerElement*> poolsToValidate;
    const auto* pool = GetPoolOrParent(newPoolName).Get();
    while (pool) {
        poolsToValidate.push_back(pool);
        pool = pool->GetParent();
    }

    if (!operationElement->IsOperationRunningInPool()) {
        // Operation is pending, we must validate all pools.
        return poolsToValidate;
    }

    // Operation is running, we can validate only tail of new pools.
    std::vector<const TCompositeSchedulerElement*> oldPools;
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

template <class TFairShareImpl>
void TFairShareTree<TFairShareImpl>::ValidateAllOperationsCountsOnPoolChange(TOperationId operationId, const TPoolName& newPoolName)
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

template <class TFairShareImpl>
void TFairShareTree<TFairShareImpl>::RegisterOperation(
    const TFairShareStrategyOperationStatePtr& state,
    const TStrategyOperationSpecPtr& spec,
    const TOperationFairShareTreeRuntimeParametersPtr& runtimeParameters)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto operationId = state->GetHost()->GetId();

    auto clonedSpec = CloneYsonSerializable(spec);
    auto optionsIt = spec->SchedulingOptionsPerPoolTree.find(TreeId_);
    if (optionsIt != spec->SchedulingOptionsPerPoolTree.end()) {
        ReconfigureYsonSerializable(clonedSpec, ConvertToNode(optionsIt->second));
    }

    auto operationElement = New<TOperationElement>(
        Config_,
        clonedSpec,
        runtimeParameters,
        state->GetController(),
        ControllerConfig_,
        StrategyHost_,
        this,
        state->GetHost(),
        TreeId_,
        Logger);

    int index = RegisterSchedulingTagFilter(TSchedulingTagFilter(clonedSpec->SchedulingTagFilter));
    operationElement->SetSchedulingTagFilterIndex(index);

    YT_VERIFY(OperationIdToElement_.insert(std::make_pair(operationId, operationElement)).second);

    auto poolName = state->GetPoolNameByTreeId(TreeId_);
    auto pool = GetOrCreatePool(poolName, state->GetHost()->GetAuthenticatedUser());

    operationElement->AttachParent(pool.Get(), /* enabled */ false);

    bool isRunningInPool = OnOperationAddedToPool(state, operationElement);
    if (isRunningInPool) {
        TreeHost_->OnOperationReadyInTree(operationId, this);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::UnregisterOperation(
    const TFairShareStrategyOperationStatePtr& state) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto operationId = state->GetHost()->GetId();
    auto operationElement = GetOperationElement(operationId);

    auto* pool = operationElement->GetMutableParent();

    operationElement->Disable();
    operationElement->DetachParent();
    operationElement->SetAlive(false);

    OnOperationRemovedFromPool(state, operationElement, pool);

    UnregisterSchedulingTagFilter(operationElement->GetSchedulingTagFilterIndex());

    YT_VERIFY(OperationIdToElement_.erase(operationId) == 1);

    // Operation can be missing in this map.
    OperationIdToActivationTime_.erase(operationId);
}

template <class TFairShareImpl>
void TFairShareTree<TFairShareImpl>::TryRunAllWaitingOperations()
{
    std::vector<TOperationId> readyOperationIds;
    std::vector<std::pair<TOperationElementPtr, TCompositeSchedulerElement*>> stillWaiting;
    for (const auto& [_, pool] : Pools_) {
        for (auto waitingOperationId : pool->WaitingOperationIds()) {
            if (auto element = FindOperationElement(waitingOperationId)) {
                YT_VERIFY(!element->IsOperationRunningInPool());
                if (auto violatingPool = FindPoolViolatingMaxRunningOperationCount(element->GetMutableParent())) {
                    stillWaiting.emplace_back(std::move(element), violatingPool);
                } else {
                    element->MarkOperationRunningInPool();
                    readyOperationIds.push_back(waitingOperationId);
                }
            }
        }
        pool->WaitingOperationIds().clear();
    }

    for (const auto& [operation, pool] : stillWaiting) {
        operation->MarkWaitingFor(pool);
    }

    for (auto operationId : readyOperationIds) {
        TreeHost_->OnOperationReadyInTree(operationId, this);
    }
}

template <class TFairShareImpl>
void TFairShareTree<TFairShareImpl>::CheckOperationsWaitingForPool(TCompositeSchedulerElement* pool)
{
    auto* current = pool;
    while (current) {
        int availableOperationCount = current->GetAvailableRunningOperationCount();
        auto& waitingOperations = current->WaitingOperationIds();
        auto it = waitingOperations.begin();
        while (it != waitingOperations.end() && availableOperationCount > 0) {
            auto waitingOperationId = *it;
            if (auto element = FindOperationElement(waitingOperationId)) {
                YT_VERIFY(!element->IsOperationRunningInPool());
                if (auto violatingPool = FindPoolViolatingMaxRunningOperationCount(element->GetMutableParent())) {
                    YT_VERIFY(current != violatingPool);
                    element->MarkWaitingFor(violatingPool);
                } else {
                    element->MarkOperationRunningInPool();
                    ActivatableOperationIds_.push_back(waitingOperationId);
                    --availableOperationCount;
                }
            }
            auto toRemove = it++;
            waitingOperations.erase(toRemove);
        }

        current = current->GetMutableParent();
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::OnOperationRemovedFromPool(
    const TFairShareStrategyOperationStatePtr& state,
    const TOperationElementPtr& element,
    const TCompositeSchedulerElementPtr& parent) -> void
{
    auto operationId = state->GetHost()->GetId();
    ReleaseOperationSlotIndex(state, parent->GetId());

    if (element->IsOperationRunningInPool()) {
        CheckOperationsWaitingForPool(parent.Get());
    } else if (auto blockedPoolName = element->WaitingForPool()) {
        if (auto blockedPool = FindPool(*blockedPoolName)) {
            blockedPool->WaitingOperationIds().remove(operationId);
        }
    }

    // We must do this recursively cause when ephemeral pool parent is deleted, it also become ephemeral.
    RemoveEmptyEphemeralPoolsRecursive(parent.Get());
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::RemoveEmptyEphemeralPoolsRecursive(TCompositeSchedulerElement* compositeElement) -> void
{
    if (!compositeElement->IsRoot() && compositeElement->IsEmpty()) {
        TPoolPtr parentPool = static_cast<TPool*>(compositeElement);
        if (parentPool->IsDefaultConfigured()) {
            UnregisterPool(parentPool);
            RemoveEmptyEphemeralPoolsRecursive(parentPool->GetMutableParent());
        }
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::OnOperationAddedToPool(
    const TFairShareStrategyOperationStatePtr& state,
    const TOperationElementPtr& operationElement) -> bool
{
    AllocateOperationSlotIndex(state, operationElement->GetParent()->GetId());

    auto violatedPool = FindPoolViolatingMaxRunningOperationCount(operationElement->GetMutableParent());
    if (!violatedPool) {
        operationElement->MarkOperationRunningInPool();
        return true;
    }
    operationElement->MarkWaitingFor(violatedPool);

    StrategyHost_->SetOperationAlert(
        state->GetHost()->GetId(),
        EOperationAlertType::OperationPending,
        TError("Max running operation count violated")
            << TErrorAttribute("pool", violatedPool->GetId())
            << TErrorAttribute("limit", violatedPool->GetMaxRunningOperationCount())
            << TErrorAttribute("tree", TreeId_)
    );

    return false;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DisableOperation(const TFairShareStrategyOperationStatePtr& state) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto operationElement = GetOperationElement(state->GetHost()->GetId());
    operationElement->Disable();
    operationElement->GetMutableParent()->DisableChild(operationElement);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::EnableOperation(const TFairShareStrategyOperationStatePtr& state) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto operationId = state->GetHost()->GetId();
    auto operationElement = GetOperationElement(operationId);

    operationElement->GetMutableParent()->EnableChild(operationElement);

    operationElement->Enable();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::UpdatePools(const INodePtr& poolsNode) -> TPoolsUpdateResult
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

    TPoolsConfigParser poolsConfigParser(std::move(poolToParentMap), std::move(ephemeralPools));

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
                auto pool = New<TPool>(
                    StrategyHost_,
                    this,
                    updatePoolAction.Name,
                    updatePoolAction.PoolConfig,
                    /* defaultConfigured */ false,
                    Config_,
                    GetPoolProfilingTag(updatePoolAction.Name),
                    TreeId_,
                    Logger);
                const auto& parent = updatePoolAction.ParentName == RootPoolName
                    ? static_cast<TCompositeSchedulerElementPtr>(RootElement_)
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

                    auto defaultParent = GetDefaultParentPool();
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
                        YT_VERIFY(UserToEphemeralPoolsInDefaultPool_[userName].erase(pool->GetId()) == 1);
                    }
                    pool->SetUserName(std::nullopt);
                }
                ReconfigurePool(pool, updatePoolAction.PoolConfig);
                if (updatePoolAction.Type == EUpdatePoolActionType::Move) {
                    const auto& parent = updatePoolAction.ParentName == RootPoolName
                        ? static_cast<TCompositeSchedulerElementPtr>(RootElement_)
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

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ChangeOperationPool(
    TOperationId operationId,
    const TFairShareStrategyOperationStatePtr& state,
    const TPoolName& newPool) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto element = FindOperationElement(operationId);
    if (!element) {
        THROW_ERROR_EXCEPTION("Operation element for operation %Qv not found", operationId);
    }
    bool operationWasRunning = element->IsOperationRunningInPool();

    auto oldParent = element->GetMutableParent();
    auto newParent = GetOrCreatePool(newPool, state->GetHost()->GetAuthenticatedUser());
    element->ChangeParent(newParent.Get());

    OnOperationRemovedFromPool(state, element, oldParent);

    YT_VERIFY(OnOperationAddedToPool(state, element));

    if (!operationWasRunning) {
        TreeHost_->OnOperationReadyInTree(operationId, this);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::CheckOperationUnschedulable(
    TOperationId operationId,
    TDuration safeTimeout,
    int minScheduleJobCallAttempts,
    THashSet<EDeactivationReason> deactivationReasons) -> TError
{
    // TODO(ignat): Could we guarantee that operation must be in tree?
    auto element = FindRecentOperationElementSnapshot(operationId);
    if (!element) {
        return TError();
    }

    auto now = TInstant::Now();
    TInstant activationTime;

    auto it = OperationIdToActivationTime_.find(operationId);
    if (!GetDynamicAttributes(RootElementSnapshot_, element).Active) {
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
        return TError("Operation has no successfull scheduled jobs for a long period")
            << TErrorAttribute("period", safeTimeout)
            << TErrorAttribute("deactivation_count", deactivationCount)
            << TErrorAttribute("last_schedule_job_success_time", element->GetLastScheduleJobSuccessTime())
            << TErrorAttribute("last_non_starving_time", element->GetLastNonStarvingTime());
    }

    return TError();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::UpdateOperationRuntimeParameters(
    TOperationId operationId,
    const TOperationFairShareTreeRuntimeParametersPtr& newParams) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    if (const auto& element = FindOperationElement(operationId)) {
        element->SetRuntimeParameters(newParams);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::UpdateConfig(const TFairShareStrategyTreeConfigPtr& config) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    Config_ = config;
    RootElement_->UpdateTreeConfig(Config_);

    if (!FindPool(Config_->DefaultParentPool) && Config_->DefaultParentPool != RootPoolName) {
        auto error = TError("Default parent pool %Qv in tree %Qv is not registered", Config_->DefaultParentPool, TreeId_);
        StrategyHost_->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    ControllerConfig_ = config;

    for (const auto& [operationId, element] : OperationIdToElement_) {
        element->UpdateControllerConfig(config);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::BuildOperationAttributes(TOperationId operationId, TFluentMap fluent) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    const auto& element = GetOperationElement(operationId);
    auto serializedParams = ConvertToAttributes(element->GetRuntimeParameters());
    fluent
        .Items(*serializedParams)
        .Item("pool").Value(element->GetParent()->GetId());
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::BuildOperationProgress(TOperationId operationId, TFluentMap fluent) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto* element = FindRecentOperationElementSnapshot(operationId);
    if (!element) {
        return;
    }

    DoBuildOperationProgress(element, RootElementSnapshot_, fluent);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::BuildBriefOperationProgress(TOperationId operationId, TFluentMap fluent) -> void
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
        .Item("fair_share_ratio").Value(attributes.GetFairShareRatio());
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::BuildUserToEphemeralPoolsInDefaultPool(TFluentAny fluent) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .DoMapFor(UserToEphemeralPoolsInDefaultPool_, [] (TFluentMap fluent, const auto& pair) {
            const auto& [userName, ephemeralPools] = pair;
            fluent
                .Item(userName).Value(ephemeralPools);
        });
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::OnFairShareUpdateAt(TInstant now) -> TFuture<std::pair<IFairShareTreeSnapshotPtr, TError>>
{
    return BIND(&TFairShareTree::DoFairShareUpdateAt, MakeStrong(this), now)
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::RegisterJobsFromRevivedOperation(TOperationId operationId, const std::vector<TJobPtr>& jobs) -> void
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

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::BuildStaticPoolsInformation(TFluentAny fluent) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .DoMapFor(Pools_, [&] (TFluentMap fluent, const auto& pair) {
            const auto& [poolName, pool] = pair;
            fluent
                .Item(poolName).Value(pool->GetConfig());
        });
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::BuildOrchid(TFluentMap fluent) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .Item("resource_usage").Value(GetRecentRootSnapshot()->ResourceUsageAtUpdate());
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::BuildFairShareInfo(TFluentMap fluent) -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    DoBuildFairShareInfo(RootElementSnapshot_, fluent);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ResetState() -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    LastPoolsNodeUpdate_.Reset();
    LastPoolsNodeUpdateError_ = TError();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetNodesFilter() const -> const TSchedulingTagFilter&
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    return Config_->NodesFilter;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::CreatePoolName(const std::optional<TString>& poolFromSpec, const TString& user) -> TPoolName
{
    if (!poolFromSpec) {
        return TPoolName(user, std::nullopt);
    }
    auto pool = FindPool(*poolFromSpec);
    if (pool && pool->GetConfig()->CreateEphemeralSubpools) {
        return TPoolName(user, *poolFromSpec);
    }
    return TPoolName(*poolFromSpec, std::nullopt);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::HasOperation(TOperationId operationId) -> bool
{
    return static_cast<bool>(FindOperationElement(operationId));
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::HasRunningOperation(TOperationId operationId) -> bool
{
    if (auto element = FindOperationElement(operationId)) {
        return element->IsOperationRunningInPool();
    }
    return false;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetResourceTree() -> TResourceTree*
{
    return ResourceTree_.Get();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetProfilingCounter(const TString& name) -> TAggregateGauge&
{
    TGuard<TSpinLock> guard(CustomProfilingCountersLock_);

    auto it = CustomProfilingCounters_.find(name);
    if (it == CustomProfilingCounters_.end()) {
        auto tag = TProfileManager::Get()->RegisterTag("tree", TreeId_);
        auto ptr = std::make_unique<TAggregateGauge>(name, TTagIdList{tag});
        it = CustomProfilingCounters_.emplace(name, std::move(ptr)).first;
    }
    return *it->second;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ReactivateBadPackingOperations(TFairShareContext* context) -> void
{
    for (const auto& operation : context->BadPackingOperations) {
        context->DynamicAttributesList[operation->GetTreeIndex()].Active = true;
        // TODO(antonkikh): This can be implemented more efficiently.
        operation->UpdateAncestorsDynamicAttributes(context, /* activateAncestors */ true);
    }
    context->BadPackingOperations.clear();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoFairShareUpdateAt(TInstant now) -> std::pair<IFairShareTreeSnapshotPtr, TError>
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    TUpdateFairShareContext updateContext;
    TDynamicAttributesList dynamicAttributes;

    updateContext.Now = now;

    auto rootElement = RootElement_->Clone();
    PROFILE_AGGREGATED_TIMING(FairSharePreUpdateTimeCounter_) {
        rootElement->PreUpdate(&dynamicAttributes, &updateContext);
    }

    TRootElementSnapshotPtr rootElementSnapshot = nullptr;
    auto asyncUpdate = BIND([&]
        {
            PROFILE_AGGREGATED_TIMING(FairShareUpdateTimeCounter_) {
                rootElement->Update(&dynamicAttributes, &updateContext);
            }

            rootElementSnapshot = New<TRootElementSnapshot>();
            rootElement->BuildElementMapping(
                &rootElementSnapshot->OperationIdToElement,
                &rootElementSnapshot->DisabledOperationIdToElement,
                &rootElementSnapshot->PoolNameToElement);
            std::swap(rootElementSnapshot->DynamicAttributes, dynamicAttributes);
            std::swap(rootElementSnapshot->ElementIndexes, updateContext.ElementIndexes);
        })
        .AsyncVia(StrategyHost_->GetFairShareUpdateInvoker())
        .Run();
    WaitFor(asyncUpdate)
        .ThrowOnError();

    YT_VERIFY(rootElementSnapshot);

    YT_LOG_DEBUG("Fair share tree update finished (UnschedulableReasons: %v)",
        updateContext.UnschedulableReasons);

    TError error;
    if (!updateContext.Errors.empty()) {
        error = TError("Found pool configuration issues during fair share update in tree %Qv", TreeId_)
            << TErrorAttribute("pool_tree", TreeId_)
            << std::move(updateContext.Errors);
    }

    // Update starvation flags for operations and pools.
    for (const auto& [operationId, element] : rootElementSnapshot->OperationIdToElement) {
        element->CheckForStarvation(now);
    }
    if (Config_->EnablePoolStarvation) {
        for (const auto& [poolName, element] : rootElementSnapshot->PoolNameToElement) {
            element->CheckForStarvation(now);
        }
    }

    // Copy persistent attributes back to the original tree.
    for (const auto& [operationId, element] : rootElementSnapshot->OperationIdToElement) {
        if (auto originalElement = FindOperationElement(operationId)) {
            originalElement->PersistentAttributes() = element->PersistentAttributes();
        }
    }
    for (const auto& [poolName, element] : rootElementSnapshot->PoolNameToElement) {
        if (auto originalElement = FindPool(poolName)) {
            originalElement->PersistentAttributes() = element->PersistentAttributes();
        }
    }
    RootElement_->PersistentAttributes() = rootElement->PersistentAttributes();

    rootElement->MarkUnmutable();

    rootElementSnapshot->RootElement = rootElement;
    rootElementSnapshot->Config = Config_;

    RootElementSnapshotPrecommit_ = rootElementSnapshot;

    auto treeSnapshot = New<TFairShareTreeSnapshot>(
        this,
        std::move(rootElementSnapshot),
        GetNodesFilter(),
        StrategyHost_->GetResourceLimits(GetNodesFilter()),
        Logger);
    return std::make_pair(treeSnapshot, error);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::FinishFairShareUpdate() -> void
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    YT_VERIFY(RootElementSnapshotPrecommit_);
    RootElementSnapshot_ = std::move(RootElementSnapshotPrecommit_);
    RootElementSnapshotPrecommit_.Reset();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoScheduleJobsWithoutPreemptionImpl(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFairShareContext* context,
    TCpuInstant startTime,
    bool ignorePacking,
    bool oneJobOnly) -> void
{
    auto& rootElement = rootElementSnapshot->RootElement;

    {
        bool prescheduleExecuted = false;
        TCpuInstant schedulingDeadline = startTime + DurationToCpuDuration(ControllerConfig_->ScheduleJobsTimeout);

        TWallTimer scheduleTimer;
        while (context->SchedulingContext->CanStartMoreJobs() && context->SchedulingContext->GetNow() < schedulingDeadline)
        {
            if (!prescheduleExecuted) {
                TWallTimer prescheduleTimer;
                if (!context->Initialized) {
                    context->Initialize(rootElement->GetTreeSize(), RegisteredSchedulingTagFilters_);
                }
                rootElement->PrescheduleJob(context, /* starvingOnly */ false, /* aggressiveStarvationEnabled */ false);
                context->StageState->PrescheduleDuration = prescheduleTimer.GetElapsedTime();
                prescheduleExecuted = true;
                context->PrescheduleCalled = true;
            }
            ++context->StageState->ScheduleJobAttemptCount;
            auto scheduleJobResult = rootElement->ScheduleJob(context, ignorePacking);
            if (scheduleJobResult.Scheduled) {
                ReactivateBadPackingOperations(context);
            }
            if (scheduleJobResult.Finished || (oneJobOnly && scheduleJobResult.Scheduled)) {
                break;
            }
        }

        context->StageState->TotalDuration = scheduleTimer.GetElapsedTime();
        context->ProfileStageTimingsAndLogStatistics();
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoScheduleJobsWithoutPreemption(
    const TFairShareTree::TRootElementSnapshotPtr& rootElementSnapshot,
    TFairShareContext* context,
    NProfiling::TCpuInstant startTime) -> void
{
    YT_LOG_TRACE("Scheduling new jobs");

    DoScheduleJobsWithoutPreemptionImpl(
        rootElementSnapshot,
        context,
        startTime,
        /* ignorePacking */ false,
        /* oneJobOnly */ false);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoScheduleJobsPackingFallback(
    const TFairShareTree::TRootElementSnapshotPtr& rootElementSnapshot,
    TFairShareContext* context,
    NProfiling::TCpuInstant startTime) -> void
{
    YT_LOG_TRACE("Scheduling jobs with packing ignored");

    // Schedule at most one job with packing ignored in case all operations have rejected the heartbeat.
    DoScheduleJobsWithoutPreemptionImpl(
        rootElementSnapshot,
        context,
        startTime,
        /* ignorePacking */ true,
        /* oneJobOnly */ true);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoScheduleJobsWithPreemption(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFairShareContext* context,
    TCpuInstant startTime) -> void
{
    auto& rootElement = rootElementSnapshot->RootElement;
    auto& config = rootElementSnapshot->Config;

    if (!context->Initialized) {
        context->Initialize(rootElement->GetTreeSize(), RegisteredSchedulingTagFilters_);
    }

    if (!context->PrescheduleCalled) {
        context->SchedulingStatistics.HasAggressivelyStarvingElements = rootElement->HasAggressivelyStarvingElements(context, false);
    }

    // Compute discount to node usage.
    YT_LOG_TRACE("Looking for preemptable jobs");
    THashSet<const TCompositeSchedulerElement *> discountedPools;
    std::vector<TJobPtr> preemptableJobs;
    PROFILE_AGGREGATED_TIMING(AnalyzePreemptableJobsTimeCounter_) {
        for (const auto& job : context->SchedulingContext->RunningJobs()) {
            auto* operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());
            if (!operationElement || !operationElement->IsJobKnown(job->GetId())) {
                YT_LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            if (!operationElement->IsPreemptionAllowed(*context, config)) {
                continue;
            }

            bool aggressivePreemptionEnabled = context->SchedulingStatistics.HasAggressivelyStarvingElements &&
                operationElement->IsAggressiveStarvationPreemptionAllowed();
            if (operationElement->IsJobPreemptable(job->GetId(), aggressivePreemptionEnabled)) {
                const auto* parent = operationElement->GetParent();
                while (parent) {
                    discountedPools.insert(parent);
                    context->DynamicAttributesFor(parent).ResourceUsageDiscount += job->ResourceUsage();
                    parent = parent->GetParent();
                }
                context->SchedulingContext->ResourceUsageDiscount() += job->ResourceUsage();
                preemptableJobs.push_back(job);
            }
        }
    }

    context->SchedulingStatistics.ResourceUsageDiscount = context->SchedulingContext->ResourceUsageDiscount();

    int startedBeforePreemption = context->SchedulingContext->StartedJobs().size();

    // NB: Schedule at most one job with preemption.
    TJobPtr jobStartedUsingPreemption;
    {
        YT_LOG_TRACE(
            "Scheduling new jobs with preemption (PreemptableJobs: %v ResourceUsageDiscount: %v)",
            preemptableJobs.size(),
            FormatResources(context->SchedulingContext->ResourceUsageDiscount()));

        bool prescheduleExecuted = false;
        TCpuInstant schedulingDeadline = startTime + DurationToCpuDuration(ControllerConfig_->ScheduleJobsTimeout);

        TWallTimer timer;
        while (context->SchedulingContext->CanStartMoreJobs() && context->SchedulingContext->GetNow() < schedulingDeadline)
        {
            if (!prescheduleExecuted) {
                TWallTimer prescheduleTimer;
                rootElement->PrescheduleJob(context, /* starvingOnly */ true, /* aggressiveStarvationEnabled */ false);
                context->StageState->PrescheduleDuration = prescheduleTimer.GetElapsedTime();
                prescheduleExecuted = true;
            }

            ++context->StageState->ScheduleJobAttemptCount;
            auto scheduleJobResult = rootElement->ScheduleJob(context, /* ignorePacking */ true);
            if (scheduleJobResult.Scheduled) {
                jobStartedUsingPreemption = context->SchedulingContext->StartedJobs().back();
                break;
            }
            if (scheduleJobResult.Finished) {
                break;
            }
        }

        context->StageState->TotalDuration = timer.GetElapsedTime();
        context->ProfileStageTimingsAndLogStatistics();
    }

    int startedAfterPreemption = context->SchedulingContext->StartedJobs().size();

    context->SchedulingStatistics.ScheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

    // Reset discounts.
    context->SchedulingContext->ResourceUsageDiscount() = {};
    for (const auto& pool : discountedPools) {
        context->DynamicAttributesFor(pool).ResourceUsageDiscount = {};
    }

    // Preempt jobs if needed.
    std::sort(
        preemptableJobs.begin(),
        preemptableJobs.end(),
        [] (const TJobPtr& lhs, const TJobPtr& rhs) {
            return lhs->GetStartTime() > rhs->GetStartTime();
        });

    auto findPoolWithViolatedLimitsForJob = [&] (const TJobPtr& job) -> const TCompositeSchedulerElement* {
        auto* operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());
        if (!operationElement) {
            return nullptr;
        }

        auto* parent = operationElement->GetParent();
        while (parent) {
            if (!Dominates(parent->ResourceLimits(), parent->GetInstantResourceUsage())) {
                return parent;
            }
            parent = parent->GetParent();
        }
        return nullptr;
    };

    auto findOperationElementForJob = [&] (const TJobPtr& job) -> TOperationElement* {
        auto operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());
        if (!operationElement || !operationElement->IsJobKnown(job->GetId())) {
            YT_LOG_DEBUG("Dangling preemptable job found (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());

            return nullptr;
        }

        return operationElement;
    };

    context->SchedulingStatistics.PreemptableJobCount = preemptableJobs.size();

    int currentJobIndex = 0;
    for (; currentJobIndex < preemptableJobs.size(); ++currentJobIndex) {
        if (Dominates(context->SchedulingContext->ResourceLimits(), context->SchedulingContext->ResourceUsage())) {
            break;
        }

        const auto& job = preemptableJobs[currentJobIndex];
        auto operationElement = findOperationElementForJob(job);
        if (!operationElement) {
            continue;
        }

        if (jobStartedUsingPreemption) {
            job->SetPreemptionReason(Format("Preempted to start job %v of operation %v",
                jobStartedUsingPreemption->GetId(),
                jobStartedUsingPreemption->GetOperationId()));

            job->SetPreemptedFor(TPreemptedFor{
                .JobId = jobStartedUsingPreemption->GetId(),
                .OperationId = jobStartedUsingPreemption->GetOperationId(),
            });
        } else {
            job->SetPreemptionReason(Format("Node resource limits violated"));
        }
        PreemptJob(job, operationElement, context->SchedulingContext);
    }

    for (; currentJobIndex < preemptableJobs.size(); ++currentJobIndex) {
        const auto& job = preemptableJobs[currentJobIndex];

        auto operationElement = findOperationElementForJob(job);
        if (!operationElement) {
            continue;
        }

        if (!Dominates(operationElement->ResourceLimits(), operationElement->GetInstantResourceUsage())) {
            job->SetPreemptionReason(Format("Preempted due to violation of resource limits of operation %v",
                operationElement->GetId()));
            PreemptJob(job, operationElement, context->SchedulingContext);
            continue;
        }

        auto violatedPool = findPoolWithViolatedLimitsForJob(job);
        if (violatedPool) {
            job->SetPreemptionReason(Format("Preempted due to violation of limits on pool %v",
                violatedPool->GetId()));
            PreemptJob(job, operationElement, context->SchedulingContext);
        }
    }

    if (!Dominates(context->SchedulingContext->ResourceLimits(), context->SchedulingContext->ResourceUsage())) {
        YT_LOG_INFO("Resource usage exceeds node resource limits even after preemption");
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoScheduleJobs(
    const ISchedulingContextPtr& schedulingContext,
    const TRootElementSnapshotPtr& rootElementSnapshot) -> void
{
    bool enableSchedulingInfoLogging = false;
    auto now = schedulingContext->GetNow();
    const auto& config = rootElementSnapshot->Config;
    if (LastSchedulingInformationLoggedTime_ + DurationToCpuDuration(config->HeartbeatTreeSchedulingInfoLogBackoff) < now) {
        enableSchedulingInfoLogging = true;
        LastSchedulingInformationLoggedTime_ = now;
    }

    TFairShareContext context(schedulingContext, enableSchedulingInfoLogging, Logger);

    bool needPackingFallback;
    {
        context.StartStage(&NonPreemptiveSchedulingStage_);
        DoScheduleJobsWithoutPreemption(rootElementSnapshot, &context, now);
        context.SchedulingStatistics.NonPreemptiveScheduleJobAttempts = context.StageState->ScheduleJobAttemptCount;
        needPackingFallback = schedulingContext->StartedJobs().empty() && !context.BadPackingOperations.empty();
        ReactivateBadPackingOperations(&context);
        context.FinishStage();
    }

    auto nodeId = schedulingContext->GetNodeDescriptor().Id;

    bool scheduleJobsWithPreemption = false;
    {
        bool nodeIsMissing = false;
        {
            TReaderGuard guard(NodeIdToLastPreemptiveSchedulingTimeLock_);
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
            TWriterGuard guard(NodeIdToLastPreemptiveSchedulingTimeLock_);
            NodeIdToLastPreemptiveSchedulingTime_[nodeId] = now;
        }
    }

    if (scheduleJobsWithPreemption) {
        context.StartStage(&PreemptiveSchedulingStage_);
        DoScheduleJobsWithPreemption(rootElementSnapshot, &context, now);
        context.SchedulingStatistics.PreemptiveScheduleJobAttempts = context.StageState->ScheduleJobAttemptCount;
        context.FinishStage();
    } else {
        YT_LOG_DEBUG("Skip preemptive scheduling");
    }

    if (needPackingFallback) {
        context.StartStage(&PackingFallbackSchedulingStage_);
        DoScheduleJobsPackingFallback(rootElementSnapshot, &context, now);
        context.SchedulingStatistics.PackingFallbackScheduleJobAttempts = context.StageState->ScheduleJobAttemptCount;
        context.FinishStage();
    }

    // Interrupt some jobs if usage is greater that limit.
    if (schedulingContext->ShouldAbortJobsSinceResourcesOvercommit()) {
        YT_LOG_DEBUG("Interrupting jobs on node since resources are overcommitted (NodeId: %v, Address: %v)",
            schedulingContext->GetNodeDescriptor().Id,
            schedulingContext->GetNodeDescriptor().Address);

        std::vector<TJobWithPreemptionInfo> jobInfos;
        for (const auto& job : schedulingContext->RunningJobs()) {
            auto* operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());
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
                PreemptJob(jobInfo.Job, jobInfo.OperationElement, schedulingContext);
            } else {
                currentResources += jobInfo.Job->ResourceUsage();
            }
        }
    }

    schedulingContext->SetSchedulingStatistics(context.SchedulingStatistics);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoPreemptJobsGracefully(
    const ISchedulingContextPtr& schedulingContext,
    const TRootElementSnapshotPtr& rootElementSnapshot) -> void
{
    YT_LOG_TRACE("Looking for gracefully preemptable jobs");
    for (const auto& job : schedulingContext->RunningJobs()) {
        if (job->GetPreemptionMode() != EPreemptionMode::Graceful || job->GetGracefullyPreempted()) {
            continue;
        }

        auto* operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());

        if (!operationElement || !operationElement->IsJobKnown(job->GetId())) {
            YT_LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
            continue;
        }

        if (operationElement->IsJobPreemptable(job->GetId(), /* aggressivePreemptionEnabled */ false)) {
            schedulingContext->PreemptJobGracefully(job);
        }
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoProfileFairShare(const TRootElementSnapshotPtr& rootElementSnapshotPtr) const -> void
{
    TMetricsAccumulator accumulator;

    for (const auto& [poolName, pool] : rootElementSnapshotPtr->PoolNameToElement) {
        ProfileCompositeSchedulerElement(accumulator, pool);
    }
    ProfileCompositeSchedulerElement(accumulator, rootElementSnapshotPtr->RootElement.Get());
    if (Config_->EnableOperationsProfiling) {
        for (const auto& [operationId, element] : rootElementSnapshotPtr->OperationIdToElement) {
            ProfileOperationElement(accumulator, element);
        }
    }

    accumulator.Add(
        "/pool_count",
        rootElementSnapshotPtr->PoolNameToElement.size(),
        EMetricType::Gauge,
        {TreeIdProfilingTag_});

    accumulator.Publish(&Profiler);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoLogFairShare(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    NEventLog::TFluentLogEvent fluent) const -> void
{
    PROFILE_AGGREGATED_TIMING(FairShareFluentLogTimeCounter_) {
        fluent
            .Item("tree_id").Value(TreeId_)
            .Do(BIND(&TFairShareTree::DoBuildFairShareInfo, Unretained(this), rootElementSnapshot));
    }

    PROFILE_AGGREGATED_TIMING(FairShareTextLogTimeCounter_) {
        LogPoolsInfo(rootElementSnapshot);
        LogOperationsInfo(rootElementSnapshot);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoEssentialLogFairShare(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    NEventLog::TFluentLogEvent fluent) const -> void
{
    PROFILE_AGGREGATED_TIMING(FairShareFluentLogTimeCounter_) {
        fluent
            .Item("tree_id").Value(TreeId_)
            .Do(BIND(&TFairShareTree::DoBuildEssentialFairShareInfo, Unretained(this), rootElementSnapshot));
    }

    PROFILE_AGGREGATED_TIMING(FairShareTextLogTimeCounter_) {
        LogPoolsInfo(rootElementSnapshot);
        LogOperationsInfo(rootElementSnapshot);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoBuildFairShareInfo(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    NYTree::TFluentMap fluent) const -> void
{
    if (!rootElementSnapshot) {
        return;
    }

    auto buildOperationsInfo = [&] (TFluentMap fluent, const typename TRawOperationElementMap::value_type& pair) {
        const auto& [operationId, element] = pair;
        fluent
            .Item(ToString(operationId)).BeginMap()
                .Do(BIND(&TFairShareTree::DoBuildOperationProgress, Unretained(this), Unretained(element), rootElementSnapshot))
            .EndMap();
    };

    fluent
        .Do(BIND(&TFairShareTree::DoBuildPoolsInformation, Unretained(this), rootElementSnapshot))
        .Item("operations").BeginMap()
            .DoFor(rootElementSnapshot->OperationIdToElement, buildOperationsInfo)
            .DoFor(rootElementSnapshot->DisabledOperationIdToElement, buildOperationsInfo)
        .EndMap();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoBuildPoolsInformation(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFluentMap fluent) const -> void
{
    auto buildPoolInfo = [&] (const TCompositeSchedulerElement* pool, TFluentMap fluent) {
        const auto& id = pool->GetId();
        fluent
            .Item(id).BeginMap()
                .Item("mode").Value(pool->GetMode())
                .Item("running_operation_count").Value(pool->RunningOperationCount())
                .Item("operation_count").Value(pool->OperationCount())
                .Item("max_running_operation_count").Value(pool->GetMaxRunningOperationCount())
                .Item("max_operation_count").Value(pool->GetMaxOperationCount())
                .Item("aggressive_starvation_enabled").Value(pool->IsAggressiveStarvationEnabled())
                .Item("forbid_immediate_operations").Value(pool->AreImmediateOperationsForbidden())
                .Item("is_ephemeral").Value(pool->IsDefaultConfigured())
                .DoIf(pool->GetMode() == ESchedulingMode::Fifo, [&] (TFluentMap fluent) {
                    fluent
                        .Item("fifo_sort_parameters").Value(pool->GetFifoSortParameters());
                })
                .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("parent").Value(pool->GetParent()->GetId());
                })
                .Do(BIND(&TFairShareTree::DoBuildElementYson, Unretained(this), Unretained(pool), rootElementSnapshot))
            .EndMap();
    };

    fluent
        .Item("pool_count").Value(GetPoolCount())
        .Item("pools").BeginMap()
            .DoFor(rootElementSnapshot->PoolNameToElement, [&] (TFluentMap fluent, const typename TRawPoolMap::value_type& pair) {
                buildPoolInfo(pair.second, fluent);
            })
            .Do(BIND(buildPoolInfo, Unretained(rootElementSnapshot->RootElement.Get())))
        .EndMap();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoBuildOperationProgress(
    const TOperationElement* element,
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFluentMap fluent) const -> void
{
    auto* parent = element->GetParent();
    fluent
        .Item("pool").Value(parent->GetId())
        .Item("slot_index").Value(element->GetMaybeSlotIndex())
        .Item("start_time").Value(element->GetStartTime())
        .Item("preemptable_job_count").Value(element->GetPreemptableJobCount())
        .Item("aggressively_preemptable_job_count").Value(element->GetAggressivelyPreemptableJobCount())
        .Item("fifo_index").Value(element->Attributes().FifoIndex)
        .Item("deactivation_reasons").Value(element->GetDeactivationReasons())
        .Item("tentative").Value(element->GetRuntimeParameters()->Tentative)
        .Do(BIND(&TFairShareTree::DoBuildElementYson, Unretained(this), Unretained(element), rootElementSnapshot));
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoBuildElementYson(
    const TSchedulerElement* element,
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFluentMap fluent) const -> void
{
    const auto& attributes = element->Attributes();
    const auto& dynamicAttributes = GetDynamicAttributes(rootElementSnapshot, element);

    auto guaranteedResources = element->GetTotalResourceLimits() * attributes.GetGuaranteedResourcesShare();

    fluent
        .Item("scheduling_status").Value(element->GetStatus())
        .Item("starving").Value(element->GetStarving())
        .Item("fair_share_starvation_tolerance").Value(element->GetFairShareStarvationTolerance())
        .Item("min_share_preemption_timeout").Value(element->GetMinSharePreemptionTimeout())
        .Item("fair_share_preemption_timeout").Value(element->GetFairSharePreemptionTimeout())
        .Item("adjusted_fair_share_starvation_tolerance").Value(attributes.AdjustedFairShareStarvationTolerance)
        .Item("adjusted_min_share_preemption_timeout").Value(attributes.AdjustedMinSharePreemptionTimeout)
        .Item("adjusted_fair_share_preemption_timeout").Value(attributes.AdjustedFairSharePreemptionTimeout)
        .Item("resource_demand").Value(element->ResourceDemand())
        .Item("resource_usage").Value(element->ResourceUsageAtUpdate())
        .Item("resource_limits").Value(element->ResourceLimits())
        .Item("dominant_resource").Value(attributes.DominantResource)
        .Item("weight").Value(element->GetWeight())
        .Item("max_share_ratio").Value(element->GetMaxShareRatio())
        .Item("min_share_resources").Value(element->GetMinShareResources())
        .Item("adjusted_min_share_ratio").Value(attributes.GetAdjustedMinShareRatio())
        .Item("min_share_ratio").Value(attributes.GetMinShareRatio())
        .Item("guaranteed_resources_ratio").Value(attributes.GetGuaranteedResourcesRatio())
        .Item("guaranteed_resources").Value(guaranteedResources)
        .Item("max_possible_usage_ratio").Value(attributes.GetMaxPossibleUsageRatio())
        .Item("usage_ratio").Value(element->GetResourceUsageRatio())
        .Item("demand_ratio").Value(attributes.GetDemandRatio())
        .Item("fair_share_ratio").Value(attributes.GetFairShareRatio())
        .Item("best_allocation_ratio").Value(element->GetBestAllocationRatio())
        .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoBuildEssentialFairShareInfo(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFluentMap fluent) const -> void
{
    auto buildOperationsInfo = [&] (TFluentMap fluent, const typename TRawOperationElementMap::value_type& pair) {
        const auto& [operationId, element] = pair;
        fluent
            .Item(ToString(operationId)).BeginMap()
                .Do(BIND(&TFairShareTree::DoBuildEssentialOperationProgress, Unretained(this), Unretained(element), rootElementSnapshot))
            .EndMap();
    };

    fluent
        .Do(BIND(&TFairShareTree::DoBuildEssentialPoolsInformation, Unretained(this), rootElementSnapshot))
        .Item("operations").BeginMap()
            .DoFor(rootElementSnapshot->OperationIdToElement, buildOperationsInfo)
            .DoFor(rootElementSnapshot->DisabledOperationIdToElement, buildOperationsInfo)
        .EndMap();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoBuildEssentialPoolsInformation(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFluentMap fluent) const -> void
{
    const auto& poolMap = rootElementSnapshot->PoolNameToElement;
    fluent
        .Item("pool_count").Value(poolMap.size())
        .Item("pools").DoMapFor(poolMap, [&] (TFluentMap fluent, const typename TRawPoolMap::value_type& pair) {
            const auto& [poolName, pool] = pair;
            fluent
                .Item(poolName).BeginMap()
                    .Do(BIND(&TFairShareTree::DoBuildEssentialElementYson, Unretained(this), Unretained(pool), rootElementSnapshot))
                .EndMap();
        });
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoBuildEssentialOperationProgress(
    const TOperationElement* element,
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFluentMap fluent) const -> void
{
    fluent
        .Do(BIND(&TFairShareTree::DoBuildEssentialElementYson, Unretained(this), Unretained(element), rootElementSnapshot));
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoBuildEssentialElementYson(
    const TSchedulerElement* element,
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFluentMap fluent) const -> void
{
    const auto& attributes = element->Attributes();
    const auto& dynamicAttributes = GetDynamicAttributes(rootElementSnapshot, element);

    fluent
        .Item("usage_ratio").Value(element->GetResourceUsageRatio())
        .Item("demand_ratio").Value(attributes.GetDemandRatio())
        .Item("fair_share_ratio").Value(attributes.GetFairShareRatio())
        .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio)
        .Item("dominant_resource").Value(attributes.DominantResource)
        .DoIf(element->IsOperation(), [&] (TFluentMap fluent) {
            fluent
                .Item("resource_usage").Value(element->ResourceUsageAtUpdate());
        });
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::LogOperationsInfo(const TRootElementSnapshotPtr& rootElementSnapshot) const -> void
{
    auto doLogOperationsInfo = [&] (const auto& operationIdToElement) {
        // Using structured bindings directly in the for-statement causes an ICE in GCC build.
        for (const auto& pair : operationIdToElement) {
            const auto& [operationId, element] = pair;
            YT_LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                element->GetLoggingString(GetDynamicAttributes(rootElementSnapshot, element)),
                operationId);
        }
    };

    doLogOperationsInfo(rootElementSnapshot->OperationIdToElement);
    doLogOperationsInfo(rootElementSnapshot->DisabledOperationIdToElement);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::LogPoolsInfo(const TRootElementSnapshotPtr& rootElementSnapshot) const -> void
{
    for (const auto& [poolName, element] : rootElementSnapshot->PoolNameToElement) {
        YT_LOG_DEBUG("FairShareInfo: %v (Pool: %v)",
            element->GetLoggingString(GetDynamicAttributes(rootElementSnapshot, element)),
            poolName);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetDynamicAttributes(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    const TSchedulerElement* element) const -> TDynamicAttributes
{
    if (!rootElementSnapshot) {
        return {};
    }

    auto it = rootElementSnapshot->ElementIndexes.find(element->GetId());
    if (it == rootElementSnapshot->ElementIndexes.end()) {
        return {};
    }

    auto index = it->second;
    YT_VERIFY(index < rootElementSnapshot->DynamicAttributes.size());
    return rootElementSnapshot->DynamicAttributes[index];
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::PreemptJob(
    const TJobPtr& job,
    const TOperationElementPtr& operationElement,
    const ISchedulingContextPtr& schedulingContext) const -> void
{
    schedulingContext->ResourceUsage() -= job->ResourceUsage();
    operationElement->IncreaseJobResourceUsage(job->GetId(), -job->ResourceUsage());
    job->ResourceUsage() = {};

    schedulingContext->PreemptJob(job);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::FindPoolViolatingMaxRunningOperationCount(TCompositeSchedulerElement* pool) -> TCompositeSchedulerElement*
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

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::FindPoolWithViolatedOperationCountLimit(const TCompositeSchedulerElementPtr& element) -> const TCompositeSchedulerElement*
{
    const TCompositeSchedulerElement* current = element.Get();
    while (current) {
        if (current->OperationCount() >= current->GetMaxOperationCount()) {
            return current;
        }
        current = current->GetParent();
    }
    return nullptr;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoRegisterPool(const TPoolPtr& pool) -> void
{
    int index = RegisterSchedulingTagFilter(pool->GetSchedulingTagFilter());
    pool->SetSchedulingTagFilterIndex(index);
    YT_VERIFY(Pools_.insert(std::make_pair(pool->GetId(), pool)).second);
    YT_VERIFY(PoolToMinUnusedSlotIndex_.insert(std::make_pair(pool->GetId(), 0)).second);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent) -> void
{
    DoRegisterPool(pool);

    pool->AttachParent(parent.Get());

    YT_LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
        pool->GetId(),
        parent->GetId());
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ReconfigurePool(const TPoolPtr& pool, const TPoolConfigPtr& config) -> void
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

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::UnregisterPool(const TPoolPtr& pool) -> void
{
    auto userName = pool->GetUserName();
    if (userName && pool->IsEphemeralInDefaultParentPool()) {
        YT_VERIFY(UserToEphemeralPoolsInDefaultPool_[*userName].erase(pool->GetId()) == 1);
    }

    UnregisterSchedulingTagFilter(pool->GetSchedulingTagFilterIndex());

    YT_VERIFY(PoolToMinUnusedSlotIndex_.erase(pool->GetId()) == 1);
    YT_VERIFY(PoolToSpareSlotIndices_.erase(pool->GetId()) <= 1);

    // We cannot use pool after erase because Pools may contain last alive reference to it.
    auto extractedPool = std::move(Pools_[pool->GetId()]);
    YT_VERIFY(Pools_.erase(pool->GetId()) == 1);

    extractedPool->SetAlive(false);
    auto parent = extractedPool->GetParent();
    extractedPool->DetachParent();

    YT_LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
        extractedPool->GetId(),
        parent->GetId());
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::TryAllocatePoolSlotIndex(const TString& poolName, int slotIndex) -> bool
{
    auto& minUnusedIndex = GetOrCrash(PoolToMinUnusedSlotIndex_, poolName);
    auto& spareSlotIndices = PoolToSpareSlotIndices_[poolName];

    if (slotIndex >= minUnusedIndex) {
        for (int index = minUnusedIndex; index < slotIndex; ++index) {
            spareSlotIndices.insert(index);
        }

        minUnusedIndex = slotIndex + 1;

        return true;
    } else {
        return spareSlotIndices.erase(slotIndex) == 1;
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::AllocateOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName) -> void
{
    auto slotIndex = state->GetHost()->FindSlotIndex(TreeId_);

    if (slotIndex) {
        // Revive case
        if (TryAllocatePoolSlotIndex(poolName, *slotIndex)) {
            return;
        }
        YT_LOG_ERROR("Failed to reuse slot index during revive (OperationId: %v, SlotIndex: %v)",
            state->GetHost()->GetId(),
            *slotIndex);
    }

    auto it = PoolToSpareSlotIndices_.find(poolName);
    if (it == PoolToSpareSlotIndices_.end() || it->second.empty()) {
        auto minUnusedIndexIt = PoolToMinUnusedSlotIndex_.find(poolName);
        YT_VERIFY(minUnusedIndexIt != PoolToMinUnusedSlotIndex_.end());
        slotIndex = minUnusedIndexIt->second;
        ++minUnusedIndexIt->second;
    } else {
        auto spareIndexIt = it->second.begin();
        slotIndex = *spareIndexIt;
        it->second.erase(spareIndexIt);
    }

    state->GetHost()->SetSlotIndex(TreeId_, *slotIndex);

    YT_LOG_DEBUG("Operation slot index allocated (OperationId: %v, SlotIndex: %v)",
        state->GetHost()->GetId(),
        *slotIndex);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ReleaseOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName) -> void
{
    auto slotIndex = state->GetHost()->FindSlotIndex(TreeId_);
    YT_VERIFY(slotIndex);

    auto it = PoolToSpareSlotIndices_.find(poolName);
    if (it == PoolToSpareSlotIndices_.end()) {
        YT_VERIFY(PoolToSpareSlotIndices_.insert(std::make_pair(poolName, THashSet<int>{*slotIndex})).second);
    } else {
        it->second.insert(*slotIndex);
    }

    YT_LOG_DEBUG("Operation slot index released (OperationId: %v, SlotIndex: %v)",
        state->GetHost()->GetId(),
        *slotIndex);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::RegisterSchedulingTagFilter(const TSchedulingTagFilter& filter) -> int
{
    if (filter.IsEmpty()) {
        return EmptySchedulingTagFilterIndex;
    }
    auto it = SchedulingTagFilterToIndexAndCount_.find(filter);
    if (it == SchedulingTagFilterToIndexAndCount_.end()) {
        int index;
        if (FreeSchedulingTagFilterIndexes_.empty()) {
            index = RegisteredSchedulingTagFilters_.size();
            RegisteredSchedulingTagFilters_.push_back(filter);
        } else {
            index = FreeSchedulingTagFilterIndexes_.back();
            RegisteredSchedulingTagFilters_[index] = filter;
            FreeSchedulingTagFilterIndexes_.pop_back();
        }
        SchedulingTagFilterToIndexAndCount_.emplace(filter, TSchedulingTagFilterEntry({index, 1}));
        return index;
    } else {
        ++it->second.Count;
        return it->second.Index;
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::UnregisterSchedulingTagFilter(int index) -> void
{
    if (index == EmptySchedulingTagFilterIndex) {
        return;
    }
    UnregisterSchedulingTagFilter(RegisteredSchedulingTagFilters_[index]);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter) -> void
{
    if (filter.IsEmpty()) {
        return;
    }
    auto it = SchedulingTagFilterToIndexAndCount_.find(filter);
    YT_VERIFY(it != SchedulingTagFilterToIndexAndCount_.end());
    --it->second.Count;
    if (it->second.Count == 0) {
        RegisteredSchedulingTagFilters_[it->second.Index] = EmptySchedulingTagFilter;
        FreeSchedulingTagFilterIndexes_.push_back(it->second.Index);
        SchedulingTagFilterToIndexAndCount_.erase(it);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::FindPool(const TString& id) const -> TPoolPtr
{
    auto it = Pools_.find(id);
    return it == Pools_.end() ? nullptr : it->second;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetPool(const TString& id) const -> TPoolPtr
{
    auto pool = FindPool(id);
    YT_VERIFY(pool);
    return pool;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::FindRecentPoolSnapshot(const TString& poolName) const -> TPool*
{
    if (RootElementSnapshot_) {
        if (auto elementFromSnapshot = RootElementSnapshot_->FindPool(poolName)) {
            return elementFromSnapshot;
        }
    }
    return FindPool(poolName).Get();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetPoolCount() const -> int
{
    return Pools_.size();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetOrCreatePool(const TPoolName& poolName, TString userName) -> TPoolPtr
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
    }
    pool = New<TPool>(
        StrategyHost_,
        this,
        poolName.GetPool(),
        poolConfig,
        /* defaultConfigured */ true,
        Config_,
        GetPoolProfilingTag(poolName.GetPool()),
        TreeId_,
        Logger);

    pool->SetUserName(userName);

    TCompositeSchedulerElement* parent;
    if (poolName.GetParentPool()) {
        parent = GetPool(*poolName.GetParentPool()).Get();
    } else {
        parent = GetDefaultParentPool().Get();
        pool->SetEphemeralInDefaultParentPool();
        UserToEphemeralPoolsInDefaultPool_[userName].insert(poolName.GetPool());
    }

    RegisterPool(pool, parent);
    return pool;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetPoolProfilingTag(const TString& id) -> NProfiling::TTagId
{
    auto it = PoolIdToProfilingTagId_.find(id);
    if (it == PoolIdToProfilingTagId_.end()) {
        it = PoolIdToProfilingTagId_.emplace(
            id,
            NProfiling::TProfileManager::Get()->RegisterTag("pool", id)
        ).first;
    }
    return it->second;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::FindOperationElement(TOperationId operationId) const -> TOperationElementPtr
{
    auto it = OperationIdToElement_.find(operationId);
    return it == OperationIdToElement_.end() ? nullptr : it->second;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetOperationElement(TOperationId operationId) const -> TOperationElementPtr
{
    auto element = FindOperationElement(operationId);
    YT_VERIFY(element);
    return element;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::FindRecentOperationElementSnapshot(TOperationId operationId) const -> TOperationElement*
{
    if (RootElementSnapshot_) {
        if (auto elementFromSnapshot = RootElementSnapshot_->FindOperationElement(operationId)) {
            return elementFromSnapshot;
        }
    }
    return FindOperationElement(operationId).Get();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetRecentRootSnapshot() const -> TCompositeSchedulerElement*
{
    if (RootElementSnapshot_) {
        return RootElementSnapshot_->RootElement.Get();
    }
    return RootElement_.Get();
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetPoolPath(const TCompositeSchedulerElementPtr& element) -> TYPath
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

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetDefaultParentPool() -> TCompositeSchedulerElementPtr
{
    auto defaultPool = FindPool(Config_->DefaultParentPool);
    if (!defaultPool) {
        if (Config_->DefaultParentPool != RootPoolName) {
            auto error = TError("Default parent pool %Qv in tree %Qv is not registered", Config_->DefaultParentPool, TreeId_);
            StrategyHost_->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
        }
        return RootElement_;
    }

    return defaultPool;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetPoolOrParent(const TPoolName& poolName) -> TCompositeSchedulerElementPtr
{
    TCompositeSchedulerElementPtr pool = FindPool(poolName.GetPool());
    if (pool) {
        return pool;
    }
    if (!poolName.GetParentPool()) {
        return GetDefaultParentPool();
    }
    pool = FindPool(*poolName.GetParentPool());
    if (!pool) {
        THROW_ERROR_EXCEPTION("Parent pool %Qv does not exist", poolName.GetParentPool());
    }
    return pool;
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ValidateOperationCountLimit(const IOperationStrategyHost* operation, const TPoolName& poolName) -> void
{
    auto poolWithViolatedLimit = FindPoolWithViolatedOperationCountLimit(GetPoolOrParent(poolName));
    if (poolWithViolatedLimit) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::TooManyOperations,
            "Limit for the number of concurrent operations %v for pool %Qv in tree %Qv has been reached",
            poolWithViolatedLimit->GetMaxOperationCount(),
            poolWithViolatedLimit->GetId(),
            TreeId_);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ValidateEphemeralPoolLimit(const IOperationStrategyHost* operation, const TPoolName& poolName) -> void
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

        if (it->second.size() + 1 > Config_->MaxEphemeralPoolsPerUser) {
            THROW_ERROR_EXCEPTION("Limit for number of ephemeral pools %v for user %v in tree %Qv has been reached",
                Config_->MaxEphemeralPoolsPerUser,
                userName,
                TreeId_);
        }
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::DoValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName) -> void
{
    TCompositeSchedulerElementPtr pool = FindPool(poolName.GetPool());
    // NB: Check is not performed if operation is started in default or unknown pool.
    if (pool && pool->AreImmediateOperationsForbidden()) {
        THROW_ERROR_EXCEPTION("Starting operations immediately in pool %Qv is forbidden", poolName.GetPool());
    }

    if (!pool) {
        pool = GetPoolOrParent(poolName);
    }

    StrategyHost_->ValidatePoolPermission(GetPoolPath(pool), operation->GetAuthenticatedUser(), EPermission::Use);
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ProfileOperationElement(TMetricsAccumulator& accumulator, TOperationElementPtr element) const -> void
{
    if (auto slotIndex = element->GetMaybeSlotIndex()) {
        auto poolTag = element->GetParent()->GetProfilingTag();
        auto slotIndexTag = GetSlotIndexProfilingTag(*slotIndex);

        ProfileSchedulerElement(accumulator, element, "/operations_by_slot", {poolTag, slotIndexTag, TreeIdProfilingTag_});
    }

    auto parent = element->GetParent();
    while (parent != nullptr) {
        bool enableProfiling = false;
        if (!parent->IsRoot()) {
            const auto* pool = static_cast<const TPool*>(parent);
            if (pool->GetConfig()->EnableByUserProfiling) {
                enableProfiling = *pool->GetConfig()->EnableByUserProfiling;
            } else {
                enableProfiling = Config_->EnableByUserProfiling;
            }
        } else {
            enableProfiling = Config_->EnableByUserProfiling;
        }

        auto poolTag = parent->GetProfilingTag();
        auto userNameTag = GetUserNameProfilingTag(element->GetUserName());
        TTagIdList byUserTags = {poolTag, userNameTag, TreeIdProfilingTag_};
        auto customTag = element->GetCustomProfilingTag();
        if (customTag) {
            byUserTags.push_back(*customTag);
        }
        ProfileSchedulerElement(accumulator, element, "/operations_by_user", byUserTags);

        parent = parent->GetParent();
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ProfileCompositeSchedulerElement(TMetricsAccumulator& accumulator, TCompositeSchedulerElementPtr element) const -> void
{
    auto tag = element->GetProfilingTag();
    ProfileSchedulerElement(accumulator, element, "/pools", {tag, TreeIdProfilingTag_});

    accumulator.Add(
        "/pools/max_operation_count",
        element->GetMaxOperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});
    accumulator.Add(
        "/pools/max_running_operation_count",
        element->GetMaxRunningOperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});
    accumulator.Add(
        "/pools/running_operation_count",
        element->RunningOperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});
    accumulator.Add(
        "/pools/total_operation_count",
        element->OperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});

    ProfileResources(accumulator, element->GetMinShareResources(), "/pools/min_share_resources", {tag, TreeIdProfilingTag_});

    // TODO(eshcherbin): Add historic usage profiling.
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::ProfileSchedulerElement(
    TMetricsAccumulator& accumulator,
    const TSchedulerElementPtr& element,
    const TString& profilingPrefix,
    const TTagIdList& tags) const -> void
{
    accumulator.Add(
        profilingPrefix + "/fair_share_ratio_x100000",
        static_cast<i64>(element->Attributes().GetFairShareRatio() * 1e5),
        EMetricType::Gauge,
        tags);
    accumulator.Add(
        profilingPrefix + "/usage_ratio_x100000",
        static_cast<i64>(element->GetResourceUsageRatio() * 1e5),
        EMetricType::Gauge,
        tags);
    accumulator.Add(
        profilingPrefix + "/demand_ratio_x100000",
        static_cast<i64>(element->Attributes().GetDemandRatio() * 1e5),
        EMetricType::Gauge,
        tags);
    accumulator.Add(
        profilingPrefix + "/guaranteed_resource_ratio_x100000",
        static_cast<i64>(element->Attributes().GetGuaranteedResourcesRatio() * 1e5),
        EMetricType::Gauge,
        tags);

    ProfileResources(accumulator, element->ResourceUsageAtUpdate(), profilingPrefix + "/resource_usage", tags);
    ProfileResources(accumulator, element->ResourceLimits(), profilingPrefix + "/resource_limits", tags);
    ProfileResources(accumulator, element->ResourceDemand(), profilingPrefix + "/resource_demand", tags);

    element->GetJobMetrics().Profile(
        accumulator,
        profilingPrefix + "/metrics",
        tags);
}

template <class TFairShareImpl>
void TFairShareTree<TFairShareImpl>::ProcessActivatableOperations()
{
    while (!ActivatableOperationIds_.empty()) {
        auto operationId = ActivatableOperationIds_.back();
        ActivatableOperationIds_.pop_back();
        TreeHost_->OnOperationReadyInTree(operationId, this);
    }
}

template <class TFairShareImpl>
auto TFairShareTree<TFairShareImpl>::GetOperationCount() const -> int
{
    return OperationIdToElement_.size();
}

////////////////////////////////////////////////////////////////////////////////

template class TFairShareTree<TVectorFairShareImpl>;
template class TFairShareTree<TClassicFairShareImpl>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
