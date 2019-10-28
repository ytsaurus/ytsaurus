#include "fair_share_tree.h"
#include "fair_share_tree_element.h"
#include "public.h"
#include "pools_config_parser.h"
#include "resource_tree.h"
#include "scheduler_strategy.h"
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

struct TJobWithPreemptionInfo
{
    TJobPtr Job;
    bool IsPreemptable;
    TOperationElementPtr OperationElement;
};

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyOperationState::TFairShareStrategyOperationState(IOperationStrategyHost* host)
    : Host_(host)
    , Controller_(New<TFairShareStrategyOperationController>(host))
{ }

TPoolName TFairShareStrategyOperationState::GetPoolNameByTreeId(const TString& treeId) const
{
    return GetOrCrash(TreeIdToPoolNameMap_, treeId);
}

void TFairShareStrategyOperationState::EraseTree(const TString& treeId)
{
    Host_->EraseTree(treeId);
    YT_VERIFY(TreeIdToPoolNameMap_.erase(treeId) == 1);
}

////////////////////////////////////////////////////////////////////////////////

TTagIdList GetFailReasonProfilingTags(EScheduleJobFailReason reason)
{
    static const NProfiling::TEnumMemberTagCache<EScheduleJobFailReason> ReasonTagCache("reason");
    return {ReasonTagCache.GetTag(reason)};
}

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

TFairShareStrategyOperationStatePtr
CreateFairShareStrategyOperationState(IOperationStrategyHost* host)
{
    auto state = New<TFairShareStrategyOperationState>(host);
    auto treeIdToPoolNameMap = GetOperationPools(host->GetRuntimeParameters());

    for (const auto& treeId : host->ErasedTrees()) {
        treeIdToPoolNameMap.erase(treeId);
    }

    state->TreeIdToPoolNameMap() = std::move(treeIdToPoolNameMap);
    return std::move(state);
}

////////////////////////////////////////////////////////////////////////////////

TOperationElement* TFairShareTree::TRootElementSnapshot::FindOperationElement(TOperationId operationId) const
{
    auto it = OperationIdToElement.find(operationId);
    return it != OperationIdToElement.end() ? it->second : nullptr;
}

TPool* TFairShareTree::TRootElementSnapshot::FindPool(const TString& poolName) const
{
    auto it = PoolNameToElement.find(poolName);
    return it != PoolNameToElement.end() ? it->second : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTree::TFairShareTreeSnapshot::TFairShareTreeSnapshot(
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

TFuture<void> TFairShareTree::TFairShareTreeSnapshot::ScheduleJobs(const ISchedulingContextPtr& schedulingContext)
{
    return BIND(&TFairShareTree::DoScheduleJobs,
        Tree_,
        schedulingContext,
        RootElementSnapshot_)
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TFairShareTree::TFairShareTreeSnapshot::PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext)
{
    Tree_->DoPreemptJobsGracefully(schedulingContext, RootElementSnapshot_);
}

void TFairShareTree::TFairShareTreeSnapshot::ProcessUpdatedJob(
    TOperationId operationId,
    TJobId jobId,
    const TJobResources& delta)
{
    // NB: Should be filtered out on large clusters.
    YT_LOG_DEBUG("Processing updated job (OperationId: %v, JobId: %v)", operationId, jobId);
    auto* operationElement = RootElementSnapshot_->FindOperationElement(operationId);
    if (operationElement) {
        operationElement->IncreaseJobResourceUsage(jobId, delta);
    }
}

void TFairShareTree::TFairShareTreeSnapshot::ProcessFinishedJob(TOperationId operationId, TJobId jobId)
{
    // NB: Should be filtered out on large clusters.
    YT_LOG_DEBUG("Processing finished job (OperationId: %v, JobId: %v)", operationId, jobId);
    auto* operationElement = RootElementSnapshot_->FindOperationElement(operationId);
    if (operationElement) {
        operationElement->OnJobFinished(jobId);
    }
}

void TFairShareTree::TFairShareTreeSnapshot::ApplyJobMetricsDelta(
    TOperationId operationId,
    const TJobMetrics& jobMetricsDelta)
{
    auto* operationElement = RootElementSnapshot_->FindOperationElement(operationId);
    if (operationElement) {
        operationElement->ApplyJobMetricsDelta(jobMetricsDelta);
    }
}

void TFairShareTree::TFairShareTreeSnapshot::ProfileFairShare() const
{
    Tree_->DoProfileFairShare(RootElementSnapshot_);
}

bool TFairShareTree::TFairShareTreeSnapshot::HasOperation(TOperationId operationId) const
{
    auto* operationElement = RootElementSnapshot_->FindOperationElement(operationId);
    return operationElement != nullptr;
}

bool TFairShareTree::TFairShareTreeSnapshot::IsOperationDisabled(TOperationId operationId) const
{
    return RootElementSnapshot_->DisabledOperations.contains(operationId);
}

const TSchedulingTagFilter& TFairShareTree::TFairShareTreeSnapshot::GetNodesFilter() const
{
    return NodesFilter_;
}

TJobResources TFairShareTree::TFairShareTreeSnapshot::GetTotalResourceLimits() const
{
    return TotalResourceLimits_;
}

std::optional<TSchedulerElementStateSnapshot> TFairShareTree::TFairShareTreeSnapshot::GetMaybeStateSnapshotForPool(const TString& poolId) const
{
    if (auto* element = RootElementSnapshot_->FindPool(poolId)) {
        return TSchedulerElementStateSnapshot{
            element->ResourceDemand(),
            element->GetMinShareResources()};
    }

    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTree::TFairShareTree(
    TFairShareStrategyTreeConfigPtr config,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    ISchedulerStrategyHost* host,
    const std::vector<IInvokerPtr>& feasibleInvokers,
    const TString& treeId)
    : Config_(config)
    , ControllerConfig_(std::move(controllerConfig))
    , ResourceTree_(New<TResourceTree>())
    , Host_(host)
    , FeasibleInvokers_(feasibleInvokers)
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
    , FairShareUpdateTimeCounter_("/fair_share_update_time", {TreeIdProfilingTag_})
    , FairShareLogTimeCounter_("/fair_share_log_time", {TreeIdProfilingTag_})
    , AnalyzePreemptableJobsTimeCounter_("/analyze_preemptable_jobs_time", {TreeIdProfilingTag_})
{
    RootElement_ = New<TRootElement>(Host_, this, config, GetPoolProfilingTag(RootPoolName), TreeId_, Logger);
}

TFuture<void> TFairShareTree::ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    return BIND(&TFairShareTree::DoValidateOperationPoolsCanBeUsed, MakeStrong(this))
        .AsyncVia(GetCurrentInvoker())
        .Run(operation, poolName);
}

void TFairShareTree::ValidatePoolLimits(const IOperationStrategyHost* operation, const TPoolName& poolName)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    ValidateOperationCountLimit(operation, poolName);
    ValidateEphemeralPoolLimit(operation, poolName);
}

void TFairShareTree::ValidatePoolLimitsOnPoolChange(const IOperationStrategyHost* operation, const TPoolName& newPoolName)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    ValidateEphemeralPoolLimit(operation, newPoolName);
    ValidateAllOperationsCountsOnPoolChange(operation->GetId(), newPoolName);
}

void TFairShareTree::ValidateAllOperationsCountsOnPoolChange(TOperationId operationId, const TPoolName& newPoolName)
{
    auto operationElement = GetOperationElement(operationId);
    std::vector<TString> oldPools;
    const auto* pool = operationElement->GetParent();
    while (pool) {
        oldPools.push_back(pool->GetId());
        pool = pool->GetParent();
    }

    std::vector<TString> newPools;
    pool = GetPoolOrParent(newPoolName).Get();
    while (pool) {
        newPools.push_back(pool->GetId());
        pool = pool->GetParent();
    }

    while (!newPools.empty() && !oldPools.empty() && newPools.back() == oldPools.back()) {
        newPools.pop_back();
        oldPools.pop_back();
    }

    for (const auto& newPool : newPools) {
        auto currentPool = GetPool(newPool);
        if (currentPool->OperationCount() >= currentPool->GetMaxOperationCount()) {
            THROW_ERROR_EXCEPTION("Max operation count of pool %Qv violated", newPool);
        }
        if (currentPool->RunningOperationCount() >= currentPool->GetMaxRunningOperationCount()) {
            THROW_ERROR_EXCEPTION("Max running operation count of pool %Qv violated", newPool);
        }
    }
}

bool TFairShareTree::RegisterOperation(
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
        Host_,
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

    return OnOperationAddedToPool(state, operationElement);
}

void TFairShareTree::UnregisterOperation(
    const TFairShareStrategyOperationStatePtr& state)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto operationId = state->GetHost()->GetId();
    auto operationElement = FindOperationElement(operationId);

    auto* pool = operationElement->GetMutableParent();

    operationElement->Disable();
    operationElement->DetachParent();
    operationElement->SetAlive(false);

    OnOperationRemovedFromPool(state, pool);

    UnregisterSchedulingTagFilter(operationElement->GetSchedulingTagFilterIndex());

    YT_VERIFY(OperationIdToElement_.erase(operationId) == 1);

    // Operation can be missing in this map.
    OperationIdToActivationTime_.erase(operationId);
}

void TFairShareTree::OnOperationRemovedFromPool(
    const TFairShareStrategyOperationStatePtr& state,
    const TCompositeSchedulerElementPtr& parent)
{
    auto operationId = state->GetHost()->GetId();
    ReleaseOperationSlotIndex(state, parent->GetId());

    WaitingOperationQueue_.remove(operationId);

    if (!parent->IsRoot() && parent->IsEmpty()) {
        TPool* pool = static_cast<TPool*>(parent.Get());
        if (pool->IsDefaultConfigured()) {
            UnregisterPool(pool);
        }
    }
}

bool TFairShareTree::OnOperationAddedToPool(
    const TFairShareStrategyOperationStatePtr& state,
    const TOperationElementPtr& operationElement)
{
    AllocateOperationSlotIndex(state, operationElement->GetParent()->GetId());

    auto violatedPool = FindPoolViolatingMaxRunningOperationCount(operationElement->GetParent());
    if (!violatedPool) {
        operationElement->MarkOperationRunningInPool();
        return true;
    }

    auto operationId = state->GetHost()->GetId();
    WaitingOperationQueue_.push_back(operationId);

    YT_LOG_DEBUG("Operation is pending since max running operation count violated (OperationId: %v, Pool: %v, Limit: %v)",
        operationId,
        violatedPool->GetId(),
        violatedPool->GetMaxRunningOperationCount());
    Host_->SetOperationAlert(
        operationId,
        EOperationAlertType::OperationPending,
        TError("Max running operation count violated")
            << TErrorAttribute("pool", violatedPool->GetId())
            << TErrorAttribute("limit", violatedPool->GetMaxRunningOperationCount())
            << TErrorAttribute("tree", TreeId_)
    );

    return false;
}

void TFairShareTree::DisableOperation(const TFairShareStrategyOperationStatePtr& state)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto operationElement = GetOperationElement(state->GetHost()->GetId());
    operationElement->Disable();
    operationElement->GetMutableParent()->DisableChild(operationElement);
}

void TFairShareTree::EnableOperation(const TFairShareStrategyOperationStatePtr& state)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto operationId = state->GetHost()->GetId();
    auto operationElement = GetOperationElement(operationId);

    operationElement->GetMutableParent()->EnableChild(operationElement);

    operationElement->Enable();
}

TPoolsUpdateResult TFairShareTree::UpdatePools(const INodePtr& poolsNode)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    if (LastPoolsNodeUpdate_ && AreNodesEqual(LastPoolsNodeUpdate_, poolsNode)) {
        YT_LOG_INFO("Pools are not changed, skipping update");
        return {LastPoolsNodeUpdateError_, false};
    }

    LastPoolsNodeUpdate_ = poolsNode;

    THashMap<TString, TString> poolToParentMap;
    for (const auto& [poolId, pool] : Pools_) {
        poolToParentMap[poolId] = pool->GetParent()->GetId();
    }

    TPoolsConfigParser poolsConfigParser(poolToParentMap);

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
                    Host_,
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

void TFairShareTree::ChangeOperationPool(
    TOperationId operationId,
    const TFairShareStrategyOperationStatePtr& state,
    const TPoolName& newPool)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto element = FindOperationElement(operationId);
    if (!element) {
        THROW_ERROR_EXCEPTION("Operation element for operation %Qv not found", operationId);
    }

    auto oldParent = element->GetMutableParent();

    auto newParent = GetOrCreatePool(newPool, state->GetHost()->GetAuthenticatedUser());
    element->ChangeParent(newParent.Get());

    OnOperationRemovedFromPool(state, oldParent);

    YT_VERIFY(OnOperationAddedToPool(state, element));
}

TError TFairShareTree::CheckOperationUnschedulable(
    TOperationId operationId,
    TDuration safeTimeout,
    int minScheduleJobCallAttempts,
    THashSet<EDeactivationReason> deactivationReasons)
{
    // TODO(ignat): Could we guarantee that operation must be in tree?
    auto element = FindRecentOperationElementSnapshot(operationId);
    if (!element) {
        return TError();
    }

    auto now = TInstant::Now();
    TInstant activationTime;

    auto it = OperationIdToActivationTime_.find(operationId);
    if (!GetGlobalDynamicAttributes(element).Active) {
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

void TFairShareTree::UpdateOperationRuntimeParameters(
    TOperationId operationId,
    const TOperationFairShareTreeRuntimeParametersPtr& newParams)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    if (const auto& element = FindOperationElement(operationId)) {
        element->SetRuntimeParameters(newParams);
    }
}

void TFairShareTree::UpdateConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    Config_ = config;
    RootElement_->UpdateTreeConfig(Config_);

    if (!FindPool(Config_->DefaultParentPool) && Config_->DefaultParentPool != RootPoolName) {
        auto error = TError("Default parent pool %Qv is not registered", Config_->DefaultParentPool);
        Host_->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
    }
}

void TFairShareTree::UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    ControllerConfig_ = config;

    for (const auto& [operationId, element] : OperationIdToElement_) {
        element->UpdateControllerConfig(config);
    }
}

void TFairShareTree::BuildOperationAttributes(TOperationId operationId, TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    const auto& element = GetOperationElement(operationId);
    auto serializedParams = ConvertToAttributes(element->GetRuntimeParameters());
    fluent
        .Items(*serializedParams)
        .Item("pool").Value(element->GetParent()->GetId());
}

void TFairShareTree::BuildOperationProgress(TOperationId operationId, TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    const auto& element = FindRecentOperationElementSnapshot(operationId);
    if (!element) {
        return;
    }

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
        .Do(std::bind(&TFairShareTree::BuildElementYson, this, element, std::placeholders::_1));
}

void TFairShareTree::BuildBriefOperationProgress(TOperationId operationId, TFluentMap fluent)
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
        .Item("fair_share_ratio").Value(attributes.FairShareRatio);
}

void TFairShareTree::BuildUserToEphemeralPoolsInDefaultPool(TFluentAny fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .DoMapFor(UserToEphemeralPoolsInDefaultPool_, [] (TFluentMap fluent, const auto& pair) {
            const auto& [userName, ephemeralPools] = pair;
            fluent
                .Item(userName).Value(ephemeralPools);
        });
}

TFuture<std::pair<IFairShareTreeSnapshotPtr, TError>> TFairShareTree::OnFairShareUpdateAt(TInstant now)
{
    return BIND(&TFairShareTree::DoFairShareUpdateAt, MakeStrong(this), now)
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

void TFairShareTree::LogOperationsInfo()
{
    for (const auto& [operationId, element] : OperationIdToElement_) {
        auto* recentOperationElement = FindRecentOperationElementSnapshot(operationId);
        YT_LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
            recentOperationElement->GetLoggingString(GetGlobalDynamicAttributes(recentOperationElement)),
            operationId);
    }
}

void TFairShareTree::LogPoolsInfo()
{
    for (const auto& [poolName, element] : Pools_) {
        auto recentPoolElement = FindRecentPoolSnapshot(poolName);
        YT_LOG_DEBUG("FairShareInfo: %v (Pool: %v)",
            recentPoolElement->GetLoggingString(GetGlobalDynamicAttributes(recentPoolElement)),
            poolName);
    }
}

// NB: This function is public for testing purposes.
void TFairShareTree::OnFairShareLoggingAt(TInstant now)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    PROFILE_AGGREGATED_TIMING(FairShareLogTimeCounter_) {
        // Log pools information.
        Host_->LogEventFluently(ELogEventType::FairShareInfo, now)
            .Item("tree_id").Value(TreeId_)
            .Do(BIND(&TFairShareTree::BuildFairShareInfo, Unretained(this)));

        LogOperationsInfo();
    }
}

// NB: This function is public for testing purposes.
void TFairShareTree::OnFairShareEssentialLoggingAt(TInstant now)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    PROFILE_AGGREGATED_TIMING(FairShareLogTimeCounter_) {
        // Log pools information.
        Host_->LogEventFluently(ELogEventType::FairShareInfo, now)
            .Item("tree_id").Value(TreeId_)
            .Do(BIND(&TFairShareTree::BuildEssentialFairShareInfo, Unretained(this)));

        LogOperationsInfo();
    }
}

void TFairShareTree::RegisterJobsFromRevivedOperation(TOperationId operationId, const std::vector<TJobPtr>& jobs)
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

void TFairShareTree::BuildPoolsInformation(TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto buildPoolInfo = [=] (const TCompositeSchedulerElement* pool, TFluentMap fluent) {
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
                .DoIf(pool->GetMode() == ESchedulingMode::Fifo, [&] (TFluentMap fluent) {
                    fluent
                        .Item("fifo_sort_parameters").Value(pool->GetFifoSortParameters());
                })
                .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("parent").Value(pool->GetParent()->GetId());
                })
                .Do(std::bind(&TFairShareTree::BuildElementYson, this, pool, std::placeholders::_1))
            .EndMap();
    };

    fluent
        .Item("pools").BeginMap()
            .DoFor(Pools_, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                buildPoolInfo(FindRecentPoolSnapshot(pair.first), fluent);
            })
            .Do(std::bind(buildPoolInfo, GetRecentRootSnapshot(), std::placeholders::_1))
        .EndMap();
}

void TFairShareTree::BuildStaticPoolsInformation(TFluentAny fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .DoMapFor(Pools_, [&] (TFluentMap fluent, const auto& pair) {
            const auto& [poolName, pool] = pair;
            fluent
                .Item(poolName).Value(pool->GetConfig());
        });
}

void TFairShareTree::BuildOrchid(TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .Item("resource_usage").Value(GetRecentRootSnapshot()->GetLocalResourceUsage());
}

void TFairShareTree::BuildFairShareInfo(TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .Do(BIND(&TFairShareTree::BuildPoolsInformation, Unretained(this)))
        .Item("operations").DoMapFor(
            OperationIdToElement_,
            [=] (TFluentMap fluent, const TOperationElementMap::value_type& pair) {
                auto operationId = pair.first;
                fluent
                    .Item(ToString(operationId)).BeginMap()
                        .Do(BIND(&TFairShareTree::BuildOperationProgress, Unretained(this), operationId))
                    .EndMap();
            });
}

void TFairShareTree::BuildEssentialFairShareInfo(TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .Do(BIND(&TFairShareTree::BuildEssentialPoolsInformation, Unretained(this)))
        .Item("operations").DoMapFor(
            OperationIdToElement_,
            [=] (TFluentMap fluent, const TOperationElementMap::value_type& pair) {
                auto operationId = pair.first;
                fluent
                    .Item(ToString(operationId)).BeginMap()
                        .Do(BIND(&TFairShareTree::BuildEssentialOperationProgress, Unretained(this), operationId))
                    .EndMap();
            });
}

void TFairShareTree::ResetState()
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    LastPoolsNodeUpdate_.Reset();
    LastPoolsNodeUpdateError_ = TError();
}

const TSchedulingTagFilter& TFairShareTree::GetNodesFilter() const
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    return Config_->NodesFilter;
}

TPoolName TFairShareTree::CreatePoolName(const std::optional<TString>& poolFromSpec, const TString& user)
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

bool TFairShareTree::HasOperation(TOperationId operationId)
{
    return static_cast<bool>(FindOperationElement(operationId));
}

TResourceTree* TFairShareTree::GetResourceTree()
{
    return ResourceTree_.Get();
}

TAggregateGauge& TFairShareTree::GetProfilingCounter(const TString& name)
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

void ReactivateBadPackingOperations(TFairShareContext* context)
{
    for (const auto& operation : context->BadPackingOperations) {
        context->DynamicAttributesList[operation->GetTreeIndex()].Active = true;
        // TODO(antonkikh): This can be implemented more efficiently.
        operation->UpdateAncestorsAttributes(context);
    }
    context->BadPackingOperations.clear();
}

TDynamicAttributes TFairShareTree::GetGlobalDynamicAttributes(const TSchedulerElement* element) const
{
    TReaderGuard guard(GlobalDynamicAttributesLock_);
    auto it = ElementIndexes_.find(element->GetId());
    if (it == ElementIndexes_.end()) {
        return TDynamicAttributes();
    } else {
        int index = it->second;
        YT_VERIFY(index < GlobalDynamicAttributes_.size());
        return GlobalDynamicAttributes_[index];
    }
}

std::pair<IFairShareTreeSnapshotPtr, TError> TFairShareTree::DoFairShareUpdateAt(TInstant now)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    TUpdateFairShareContext updateContext;
    TDynamicAttributesList dynamicAttributes;

    updateContext.Now = now;

    auto rootElement = RootElement_->Clone();
    auto asyncUpdate = BIND([&]
        {
            PROFILE_AGGREGATED_TIMING(FairShareUpdateTimeCounter_) {
                rootElement->Update(&dynamicAttributes, &updateContext);
            }
        })
        .AsyncVia(Host_->GetFairShareUpdateInvoker())
        .Run();
    WaitFor(asyncUpdate)
        .ThrowOnError();

    {
        TWriterGuard guard(GlobalDynamicAttributesLock_);
        std::swap(GlobalDynamicAttributes_, dynamicAttributes);
        std::swap(ElementIndexes_, updateContext.ElementIndexes);
    }

    TError error;
    if (!updateContext.Errors.empty()) {
        error = TError("Found pool configuration issues during fair share update in tree %Qv", TreeId_)
            << TErrorAttribute("pool_tree", TreeId_)
            << std::move(updateContext.Errors);
    }

    auto rootElementSnapshot = New<TRootElementSnapshot>();
    rootElement->BuildElementMapping(
        &rootElementSnapshot->OperationIdToElement,
        &rootElementSnapshot->PoolNameToElement,
        &rootElementSnapshot->DisabledOperations);

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

    RootElementSnapshot_ = rootElementSnapshot;

    auto treeSnapshot = New<TFairShareTreeSnapshot>(
        this,
        std::move(rootElementSnapshot),
        GetNodesFilter(),
        Host_->GetResourceLimits(GetNodesFilter()),
        Logger);
    return std::make_pair(treeSnapshot, error);
}

void TFairShareTree::DoScheduleJobsWithoutPreemptionImpl(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFairShareContext* context,
    TCpuInstant startTime,
    bool ignorePacking,
    bool oneJobOnly)
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
            ++context->StageState->ScheduleJobAttempts;
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

void TFairShareTree::DoScheduleJobsWithoutPreemption(
    const NYT::NScheduler::TFairShareTree::TRootElementSnapshotPtr& rootElementSnapshot,
    NYT::NScheduler::TFairShareContext* context,
    NYT::NProfiling::TCpuInstant startTime)
{
    YT_LOG_TRACE("Scheduling new jobs");

    DoScheduleJobsWithoutPreemptionImpl(
        rootElementSnapshot,
        context,
        startTime,
        /* ignorePacking */ false,
        /* oneJobOnly */ false);
}

void TFairShareTree::DoScheduleJobsPackingFallback(
    const NYT::NScheduler::TFairShareTree::TRootElementSnapshotPtr& rootElementSnapshot,
    NYT::NScheduler::TFairShareContext* context,
    NYT::NProfiling::TCpuInstant startTime)
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

void TFairShareTree::DoScheduleJobsWithPreemption(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFairShareContext* context,
    TCpuInstant startTime)
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
        YT_LOG_TRACE("Scheduling new jobs with preemption");

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

            ++context->StageState->ScheduleJobAttempts;
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
            if (!Dominates(parent->ResourceLimits(), parent->GetLocalResourceUsage())) {
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

        if (!Dominates(operationElement->ResourceLimits(), operationElement->GetLocalResourceUsage())) {
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

void TFairShareTree::DoScheduleJobs(
    const ISchedulingContextPtr& schedulingContext,
    const TRootElementSnapshotPtr& rootElementSnapshot)
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
        context.SchedulingStatistics.NonPreemptiveScheduleJobAttempts = context.StageState->ScheduleJobAttempts;
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
        context.SchedulingStatistics.PreemptiveScheduleJobAttempts = context.StageState->ScheduleJobAttempts;
        context.FinishStage();
    } else {
        YT_LOG_DEBUG("Skip preemptive scheduling");
    }

    if (needPackingFallback) {
        context.StartStage(&PackingFallbackSchedulingStage_);
        DoScheduleJobsPackingFallback(rootElementSnapshot, &context, now);
        context.SchedulingStatistics.PackingFallbackScheduleJobAttempts = context.StageState->ScheduleJobAttempts;
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

        std::sort(
            jobInfos.begin(),
            jobInfos.end(),
            [&] (const TJobWithPreemptionInfo& lhs, const TJobWithPreemptionInfo& rhs) {
                if (lhs.IsPreemptable != rhs.IsPreemptable) {
                    return lhs.IsPreemptable < rhs.IsPreemptable;
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

void TFairShareTree::DoPreemptJobsGracefully(
    const ISchedulingContextPtr& schedulingContext,
    const TRootElementSnapshotPtr& rootElementSnapshot)
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

void TFairShareTree::DoProfileFairShare(const TRootElementSnapshotPtr& rootElementSnapshotPtr) const
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

    accumulator.Publish(&Profiler);
}


void TFairShareTree::PreemptJob(
    const TJobPtr& job,
    const TOperationElementPtr& operationElement,
    const ISchedulingContextPtr& schedulingContext) const
{
    schedulingContext->ResourceUsage() -= job->ResourceUsage();
    operationElement->IncreaseJobResourceUsage(job->GetId(), -job->ResourceUsage());
    job->ResourceUsage() = {};

    schedulingContext->PreemptJob(job);
}

const TCompositeSchedulerElement* TFairShareTree::FindPoolViolatingMaxRunningOperationCount(const TCompositeSchedulerElement* pool)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    while (pool) {
        if (pool->RunningOperationCount() >= pool->GetMaxRunningOperationCount()) {
            return pool;
        }
        pool = pool->GetParent();
    }
    return nullptr;
}

const TCompositeSchedulerElement* TFairShareTree::FindPoolWithViolatedOperationCountLimit(const TCompositeSchedulerElementPtr& element)
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

void TFairShareTree::DoRegisterPool(const TPoolPtr& pool)
{
    int index = RegisterSchedulingTagFilter(pool->GetSchedulingTagFilter());
    pool->SetSchedulingTagFilterIndex(index);
    YT_VERIFY(Pools_.insert(std::make_pair(pool->GetId(), pool)).second);
    YT_VERIFY(PoolToMinUnusedSlotIndex_.insert(std::make_pair(pool->GetId(), 0)).second);
}

void TFairShareTree::RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
{
    DoRegisterPool(pool);

    pool->AttachParent(parent.Get());

    YT_LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
        pool->GetId(),
        parent->GetId());
}

void TFairShareTree::ReconfigurePool(const TPoolPtr& pool, const TPoolConfigPtr& config)
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

void TFairShareTree::UnregisterPool(const TPoolPtr& pool)
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

bool TFairShareTree::TryAllocatePoolSlotIndex(const TString& poolName, int slotIndex)
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

void TFairShareTree::AllocateOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName)
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

void TFairShareTree::ReleaseOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName)
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

std::vector<TOperationId> TFairShareTree::RunWaitingOperations()
{
    std::vector<TOperationId> result;
    auto it = WaitingOperationQueue_.begin();
    while (it != WaitingOperationQueue_.end() && RootElement_->RunningOperationCount() < Config_->MaxRunningOperationCount) {
        auto operationId = *it;
        auto element = GetOperationElement(operationId);
        auto* operationPool = element->GetParent();
        if (FindPoolViolatingMaxRunningOperationCount(operationPool) == nullptr) {
            result.push_back(operationId);
            element->MarkOperationRunningInPool();
            auto toRemove = it++;
            WaitingOperationQueue_.erase(toRemove);
        } else {
            ++it;
        }
    }
    return result;
}

void TFairShareTree::BuildEssentialOperationProgress(TOperationId operationId, TFluentMap fluent)
{
    const auto* element = FindRecentOperationElementSnapshot(operationId);
    if (!element) {
        return;
    }

    fluent
        .Do(std::bind(&TFairShareTree::BuildEssentialOperationElementYson, this, element, std::placeholders::_1));
}

int TFairShareTree::RegisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
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

void TFairShareTree::UnregisterSchedulingTagFilter(int index)
{
    if (index == EmptySchedulingTagFilterIndex) {
        return;
    }
    UnregisterSchedulingTagFilter(RegisteredSchedulingTagFilters_[index]);
}

void TFairShareTree::UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
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

TPoolPtr TFairShareTree::FindPool(const TString& id) const
{
    auto it = Pools_.find(id);
    return it == Pools_.end() ? nullptr : it->second;
}

TPoolPtr TFairShareTree::GetPool(const TString& id) const
{
    auto pool = FindPool(id);
    YT_VERIFY(pool);
    return pool;
}

TPool* TFairShareTree::FindRecentPoolSnapshot(const TString& poolName) const
{
    if (RootElementSnapshot_) {
        if (auto elementFromSnapshot = RootElementSnapshot_->FindPool(poolName)) {
            return elementFromSnapshot;
        }
    }
    return FindPool(poolName).Get();
}

TPoolPtr TFairShareTree::GetOrCreatePool(const TPoolName& poolName, TString userName)
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
        Host_,
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

NProfiling::TTagId TFairShareTree::GetPoolProfilingTag(const TString& id)
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

TOperationElementPtr TFairShareTree::FindOperationElement(TOperationId operationId) const
{
    auto it = OperationIdToElement_.find(operationId);
    return it == OperationIdToElement_.end() ? nullptr : it->second;
}

TOperationElementPtr TFairShareTree::GetOperationElement(TOperationId operationId) const
{
    auto element = FindOperationElement(operationId);
    YT_VERIFY(element);
    return element;
}

TOperationElement* TFairShareTree::FindRecentOperationElementSnapshot(TOperationId operationId) const
{
    if (RootElementSnapshot_) {
        if (auto elementFromSnapshot = RootElementSnapshot_->FindOperationElement(operationId)) {
            return elementFromSnapshot;
        }
    }
    return FindOperationElement(operationId).Get();
}

TCompositeSchedulerElement* TFairShareTree::GetRecentRootSnapshot() const
{
    if (RootElementSnapshot_) {
        return RootElementSnapshot_->RootElement.Get();
    }
    return RootElement_.Get();
}

void TFairShareTree::BuildEssentialPoolsInformation(TFluentMap fluent)
{
    fluent
        .Item("pools").DoMapFor(Pools_, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
            const auto& [poolName, pool] = pair;
            fluent
                .Item(poolName).BeginMap()
                    .Do(std::bind(&TFairShareTree::BuildEssentialPoolElementYson, this, FindRecentPoolSnapshot(poolName), std::placeholders::_1))
                .EndMap();
        });
}

void TFairShareTree::BuildElementYson(const TSchedulerElement* element, TFluentMap fluent)
{
    const auto& attributes = element->Attributes();
    auto dynamicAttributes = GetGlobalDynamicAttributes(element);

    auto guaranteedResources = Host_->GetResourceLimits(Config_->NodesFilter) * attributes.GuaranteedResourcesRatio;

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
        .Item("resource_usage").Value(element->GetLocalResourceUsage())
        .Item("resource_limits").Value(element->ResourceLimits())
        .Item("dominant_resource").Value(attributes.DominantResource)
        .Item("weight").Value(element->GetWeight())
        .Item("min_share_ratio").Value(element->GetMinShareRatio())
        .Item("max_share_ratio").Value(element->GetMaxShareRatio())
        .Item("min_share_resources").Value(element->GetMinShareResources())
        .Item("adjusted_min_share_ratio").Value(attributes.AdjustedMinShareRatio)
        .Item("recursive_min_share_ratio").Value(attributes.RecursiveMinShareRatio)
        .Item("guaranteed_resources_ratio").Value(attributes.GuaranteedResourcesRatio)
        .Item("guaranteed_resources").Value(guaranteedResources)
        .Item("max_possible_usage_ratio").Value(attributes.MaxPossibleUsageRatio)
        .Item("usage_ratio").Value(element->GetResourceUsageRatio())
        .Item("demand_ratio").Value(attributes.DemandRatio)
        .Item("fair_share_ratio").Value(attributes.FairShareRatio)
        .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio)
        .Item("best_allocation_ratio").Value(attributes.BestAllocationRatio);
}

void TFairShareTree::BuildEssentialElementYson(const TSchedulerElement* element, TFluentMap fluent, bool shouldPrintResourceUsage)
{
    const auto& attributes = element->Attributes();
    auto dynamicAttributes = GetGlobalDynamicAttributes(element);

    fluent
        .Item("usage_ratio").Value(element->GetResourceUsageRatio())
        .Item("demand_ratio").Value(attributes.DemandRatio)
        .Item("fair_share_ratio").Value(attributes.FairShareRatio)
        .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio)
        .Item("dominant_resource").Value(attributes.DominantResource)
        .DoIf(shouldPrintResourceUsage, [&] (TFluentMap fluent) {
            fluent
                .Item("resource_usage").Value(element->GetLocalResourceUsage());
        });
}

void TFairShareTree::BuildEssentialPoolElementYson(const TSchedulerElement* element, TFluentMap fluent)
{
    BuildEssentialElementYson(element, fluent, false);
}

void TFairShareTree::BuildEssentialOperationElementYson(const TSchedulerElement* element, TFluentMap fluent)
{
    BuildEssentialElementYson(element, fluent, true);
}

TYPath TFairShareTree::GetPoolPath(const TCompositeSchedulerElementPtr& element)
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

TCompositeSchedulerElementPtr TFairShareTree::GetDefaultParentPool()
{
    auto defaultPool = FindPool(Config_->DefaultParentPool);
    if (!defaultPool) {
        if (Config_->DefaultParentPool != RootPoolName) {
            auto error = TError("Default parent pool %Qv is not registered", Config_->DefaultParentPool);
            Host_->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
        }
        return RootElement_;
    }

    return defaultPool;
}

TCompositeSchedulerElementPtr TFairShareTree::GetPoolOrParent(const TPoolName& poolName)
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

void TFairShareTree::ValidateOperationCountLimit(const IOperationStrategyHost* operation, const TPoolName& poolName)
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

void TFairShareTree::ValidateEphemeralPoolLimit(const IOperationStrategyHost* operation, const TPoolName& poolName)
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

void TFairShareTree::DoValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName)
{
    TCompositeSchedulerElementPtr pool = FindPool(poolName.GetPool());
    // NB: Check is not performed if operation is started in default or unknown pool.
    if (pool && pool->AreImmediateOperationsForbidden()) {
        THROW_ERROR_EXCEPTION("Starting operations immediately in pool %Qv is forbidden", poolName.GetPool());
    }

    if (!pool) {
        pool = GetPoolOrParent(poolName);
    }

    Host_->ValidatePoolPermission(GetPoolPath(pool), operation->GetAuthenticatedUser(), EPermission::Use);
}

void TFairShareTree::ProfileOperationElement(TMetricsAccumulator& accumulator, TOperationElementPtr element) const
{
    if (auto slotIndex = element->GetMaybeSlotIndex()) {
        auto poolTag = element->GetParent()->GetProfilingTag();
        auto slotIndexTag = GetSlotIndexProfilingTag(*slotIndex);

        ProfileSchedulerElement(accumulator, element, "/operations_by_slot", {poolTag, slotIndexTag, TreeIdProfilingTag_});
    }

    auto parent = element->GetParent();
    while (parent != nullptr) {
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

void TFairShareTree::ProfileCompositeSchedulerElement(TMetricsAccumulator& accumulator, TCompositeSchedulerElementPtr element) const
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

    const auto& minShareResources = element->GetMinShareResources();
    accumulator.Add(
        "/pools/min_share_resources/cpu",
        static_cast<i64>(minShareResources.GetCpu()),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});
    accumulator.Add(
        "/pools/min_share_resources/memory",
        minShareResources.GetMemory(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});
    accumulator.Add(
        "/pools/min_share_resources/user_slots",
        minShareResources.GetUserSlots(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});

    // TODO(eshcherbin): Add historic usage profiling.
}

void TFairShareTree::ProfileSchedulerElement(
    TMetricsAccumulator& accumulator,
    const TSchedulerElementPtr& element,
    const TString& profilingPrefix,
    const TTagIdList& tags) const
{
    accumulator.Add(
        profilingPrefix + "/fair_share_ratio_x100000",
        static_cast<i64>(element->Attributes().FairShareRatio * 1e5),
        EMetricType::Gauge,
        tags);
    accumulator.Add(
        profilingPrefix + "/usage_ratio_x100000",
        static_cast<i64>(element->GetResourceUsageRatio() * 1e5),
        EMetricType::Gauge,
        tags);
    accumulator.Add(
        profilingPrefix + "/demand_ratio_x100000",
        static_cast<i64>(element->Attributes().DemandRatio * 1e5),
        EMetricType::Gauge,
        tags);
    accumulator.Add(
        profilingPrefix + "/guaranteed_resource_ratio_x100000",
        static_cast<i64>(element->Attributes().GuaranteedResourcesRatio * 1e5),
        EMetricType::Gauge,
        tags);

    auto profileResources = [&] (const TString& path, const TJobResources& resources) {
        #define XX(name, Name) accumulator.Add(path + "/" #name, static_cast<i64>(resources.Get##Name()), EMetricType::Gauge, tags);
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
    };

    profileResources(profilingPrefix + "/resource_usage", element->GetLocalResourceUsage());
    profileResources(profilingPrefix + "/resource_limits", element->ResourceLimits());
    profileResources(profilingPrefix + "/resource_demand", element->ResourceDemand());

    element->GetJobMetrics().Profile(
        accumulator,
        profilingPrefix + "/metrics",
        tags);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
