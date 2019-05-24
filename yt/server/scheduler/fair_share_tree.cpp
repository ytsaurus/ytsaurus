#include "fair_share_tree.h"
#include "fair_share_tree_element.h"
#include "public.h"
#include "pools_config_parser.h"
#include "resource_tree.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "fair_share_strategy_operation_controller.h"

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
    , FairShareUpdateTimeCounter_("/fair_share_update_time", {TreeIdProfilingTag_})
    , FairShareLogTimeCounter_("/fair_share_log_time", {TreeIdProfilingTag_})
    , AnalyzePreemptableJobsTimeCounter_("/analyze_preemptable_jobs_time", {TreeIdProfilingTag_})
{
    RootElement_ = New<TRootElement>(Host_, this, config, GetPoolProfilingTag(RootPoolName), TreeId_);
}

IFairShareTreeSnapshotPtr TFairShareTree::CreateSnapshot()
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    return New<TFairShareTreeSnapshot>(this, RootElementSnapshot_, Logger);
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
    const TOperationFairShareTreeRuntimeParametersPtr& runtimeParams)
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
        runtimeParams,
        state->GetController(),
        ControllerConfig_,
        Host_,
        this,
        state->GetHost(),
        TreeId_);

    int index = RegisterSchedulingTagFilter(TSchedulingTagFilter(clonedSpec->SchedulingTagFilter));
    operationElement->SetSchedulingTagFilterIndex(index);

    YCHECK(OperationIdToElement_.insert(std::make_pair(operationId, operationElement)).second);

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

    YCHECK(OperationIdToElement_.erase(operationId) == 1);

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

    TPoolsConfigParser poolsConfigParser(poolToParentMap, RootElement_->GetId());

    TError parseResult = poolsConfigParser.TryParse(poolsNode);
    if (!parseResult.IsOK()) {
        auto wrappedError = TError("Found pool configuration issues in tree %Qv; update skipped", TreeId_)
            << parseResult;
        LastPoolsNodeUpdateError_ = wrappedError;
        return {wrappedError, false};
    }

    const auto& parsedPoolMap = poolsConfigParser.GetPoolConfigMap();

    for (const auto& poolId: poolsConfigParser.GetNewPoolsByInOrderTraversal()) {
        auto it = parsedPoolMap.find(poolId);
        YCHECK(it != parsedPoolMap.end());
        auto parsedPool = it->second;
        auto pool = New<TPool>(
            Host_,
            this,
            poolId,
            parsedPool.PoolConfig,
            /* defaultConfigured */ false,
            Config_,
            GetPoolProfilingTag(poolId),
            TreeId_);
        const auto& parent = parsedPool.ParentId == RootPoolName
            ? static_cast<TCompositeSchedulerElementPtr>(RootElement_)
            : GetPool(parsedPool.ParentId);

        RegisterPool(pool, parent);
    }

    for (const auto& [poolId, parsedPool] : parsedPoolMap) {
        if (parsedPool.IsNew) {
            continue;
        }
        auto pool = GetPool(poolId);
        if (pool->GetUserName()) {
            YCHECK(UserToEphemeralPools_[pool->GetUserName().value()].erase(pool->GetId()) == 1);
            pool->SetUserName(std::nullopt);
        }
        ReconfigurePool(pool, parsedPool.PoolConfig);
        if (parsedPool.ParentIsChanged) {
            const auto& parent = parsedPool.ParentId == RootPoolName
                ? static_cast<TCompositeSchedulerElementPtr>(RootElement_)
                : GetPool(parsedPool.ParentId);
            pool->ChangeParent(parent.Get());
        }
    }

    std::vector<TPoolPtr> erasedPools;
    for (const auto& [poolId, pool] : Pools_) {
        if (!parsedPoolMap.contains(poolId)) {
            erasedPools.push_back(pool);
        }
    }

    for (const auto& pool : erasedPools) {
        if (pool->IsEmpty()) {
            UnregisterPool(pool);
        } else {
            pool->SetDefaultConfig();

            auto defaultParent = GetDefaultParentPool();
            if (pool->GetId() == defaultParent->GetId()) {  // Someone is deleting default pool
                defaultParent = RootElement_;
            }
            if (pool->GetParent()->GetId() != defaultParent->GetId()) {
                pool->ChangeParent(defaultParent.Get());
            }
        }
    }

    LastPoolsNodeUpdateError_ = TError();

    // TODO(ignat): do not ignore error.
    Y_UNUSED(OnFairShareUpdateAt(TInstant::Now()));

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

    YCHECK(OnOperationAddedToPool(state, element));
}

TError TFairShareTree::CheckOperationUnschedulable(
    TOperationId operationId,
    TDuration safeTimeout,
    int minScheduleJobCallAttempts,
    THashSet<EDeactivationReason> deactivationReasons)
{
    // TODO(ignat): Could we guarantee that operation must be in tree?
    auto element = FindOperationElement(operationId);
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
        deactivationCount > minScheduleJobCallAttempts)
    {
        return TError("Operation has no successfull scheduled jobs for a long period")
            << TErrorAttribute("period", safeTimeout)
            << TErrorAttribute("deactivation_count", deactivationCount);
    }

    return TError();
}

void TFairShareTree::UpdateOperationRuntimeParameters(
    TOperationId operationId,
    const TOperationFairShareTreeRuntimeParametersPtr& newParams)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    if (const auto& element = FindOperationElement(operationId)) {
        element->SetRuntimeParams(newParams);
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

    for (const auto& pair : OperationIdToElement_) {
        const auto& element = pair.second;
        element->UpdateControllerConfig(config);
    }
}

void TFairShareTree::BuildOperationAttributes(TOperationId operationId, TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    const auto& element = GetOperationElement(operationId);
    auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
    fluent
        .Items(*serializedParams)
        .Item("pool").Value(element->GetParent()->GetId());
}

void TFairShareTree::BuildOperationProgress(TOperationId operationId, TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    const auto& element = FindOperationElement(operationId);
    if (!element) {
        return;
    }

    auto* parent = element->GetParent();
    fluent
        .Item("pool").Value(parent->GetId())
        .Item("slot_index").Value(element->GetSlotIndex())
        .Item("start_time").Value(element->GetStartTime())
        .Item("preemptable_job_count").Value(element->GetPreemptableJobCount())
        .Item("aggressively_preemptable_job_count").Value(element->GetAggressivelyPreemptableJobCount())
        .Item("fifo_index").Value(element->Attributes().FifoIndex)
        .Item("deactivation_reasons").Value(element->GetDeactivationReasons())
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

void TFairShareTree::BuildUserToEphemeralPools(TFluentAny fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .DoMapFor(UserToEphemeralPools_, [] (TFluentMap fluent, const auto& value) {
            fluent
                .Item(value.first).Value(value.second);
        });
}

// NB: This function is public for testing purposes.
TError TFairShareTree::OnFairShareUpdateAt(TInstant now)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    TError error;

    // Run periodic update.
    PROFILE_AGGREGATED_TIMING(FairShareUpdateTimeCounter_) {
        // The root element gets the whole cluster.
        ResetTreeIndexes();

        TUpdateFairShareContext updateContext;
        RootElement_->Update(GlobalDynamicAttributes_, &updateContext);

        if (!updateContext.Errors.empty()) {
            error = TError("Found pool configuration issues during fair share update in tree %Qv", TreeId_)
                << TErrorAttribute("pool_tree", TreeId_)
                << std::move(updateContext.Errors);
        }

        // Update starvation flags for all operations.
        for (const auto& pair : OperationIdToElement_) {
            pair.second->CheckForStarvation(now);
        }

        // Update starvation flags for all pools.
        if (Config_->EnablePoolStarvation) {
            for (const auto& pair : Pools_) {
                pair.second->CheckForStarvation(now);
            }
        }

        RootElementSnapshot_ = CreateRootElementSnapshot();
    }

    return error;
}

void TFairShareTree::ProfileFairShare(TMetricsAccumulator& accumulator) const
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    for (const auto& pair : Pools_) {
        ProfileCompositeSchedulerElement(accumulator, pair.second);
    }
    ProfileCompositeSchedulerElement(accumulator, RootElement_);
    if (Config_->EnableOperationsProfiling) {
        for (const auto& pair : OperationIdToElement_) {
            ProfileOperationElement(accumulator, pair.second);
        }
    }
}

void TFairShareTree::ResetTreeIndexes()
{
    for (const auto& pair : OperationIdToElement_) {
        auto& element = pair.second;
        element->SetTreeIndex(UnassignedTreeIndex);
    }
}

void TFairShareTree::LogOperationsInfo()
{
    for (const auto& pair : OperationIdToElement_) {
        const auto& operationId = pair.first;
        const auto& element = pair.second;
        YT_LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
            element->GetLoggingString(GlobalDynamicAttributes_),
            operationId);
    }
}

void TFairShareTree::LogPoolsInfo()
{
    for (const auto& pair : Pools_) {
        const auto& poolName = pair.first;
        const auto& pool = pair.second;
        YT_LOG_DEBUG("FairShareInfo: %v (Pool: %v)",
            pool->GetLoggingString(GlobalDynamicAttributes_),
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

    auto buildPoolInfo = [=] (const TCompositeSchedulerElementPtr pool, TFluentMap fluent) {
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
                buildPoolInfo(pair.second, fluent);
            })
            .Do(std::bind(buildPoolInfo, RootElement_, std::placeholders::_1))
        .EndMap();
}

void TFairShareTree::BuildStaticPoolsInformation(TFluentAny fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .DoMapFor(Pools_, [&] (TFluentMap fluent, const auto& pair) {
            const auto& id = pair.first;
            const auto& pool = pair.second;
            fluent
                .Item(id).Value(pool->GetConfig());
        });
}

void TFairShareTree::BuildOrchid(TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .Item("resource_usage").Value(RootElement_->GetLocalResourceUsage());
}

void TFairShareTree::BuildFairShareInfo(TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

    fluent
        .Do(BIND(&TFairShareTree::BuildPoolsInformation, Unretained(this)))
        .Item("operations").DoMapFor(
            OperationIdToElement_,
            [=] (TFluentMap fluent, const TOperationElementPtrByIdMap::value_type& pair) {
                const auto& operationId = pair.first;
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
            [=] (TFluentMap fluent, const TOperationElementPtrByIdMap::value_type& pair) {
                const auto& operationId = pair.first;
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
};

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

void TFairShareTree::DoScheduleJobsWithoutPreemption(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFairShareContext* context,
    TCpuInstant startTime)
{
    auto& rootElement = rootElementSnapshot->RootElement;

    {
        YT_LOG_TRACE("Scheduling new jobs");

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
            if (!rootElement->ScheduleJob(context)) {
                break;
            }
        }

        context->StageState->TotalDuration = scheduleTimer.GetElapsedTime();
        context->ProfileStageTimingsAndLogStatistics();
    }
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
            if (!rootElement->ScheduleJob(context)) {
                break;
            }
            if (context->SchedulingContext->StartedJobs().size() > startedBeforePreemption) {
                jobStartedUsingPreemption = context->SchedulingContext->StartedJobs().back();
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
        PreemptJob(job, operationElement, context);
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
            PreemptJob(job, operationElement, context);
            continue;
        }

        auto violatedPool = findPoolWithViolatedLimitsForJob(job);
        if (violatedPool) {
            job->SetPreemptionReason(Format("Preempted due to violation of limits on pool %v",
                violatedPool->GetId()));
            PreemptJob(job, operationElement, context);
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

    TFairShareContext context(schedulingContext, enableSchedulingInfoLogging);

    {
        context.StartStage(&NonPreemptiveSchedulingStage_);
        DoScheduleJobsWithoutPreemption(rootElementSnapshot, &context, now);
        context.SchedulingStatistics.NonPreemptiveScheduleJobAttempts = context.StageState->ScheduleJobAttempts;
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

    schedulingContext->SetSchedulingStatistics(context.SchedulingStatistics);
}

void TFairShareTree::PreemptJob(
    const TJobPtr& job,
    const TOperationElementPtr& operationElement,
    TFairShareContext* context) const
{
    context->SchedulingContext->ResourceUsage() -= job->ResourceUsage();
    operationElement->IncreaseJobResourceUsage(job->GetId(), -job->ResourceUsage());
    job->ResourceUsage() = {};

    context->SchedulingContext->PreemptJob(job);
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
    YCHECK(Pools_.insert(std::make_pair(pool->GetId(), pool)).second);
    YCHECK(PoolToMinUnusedSlotIndex_.insert(std::make_pair(pool->GetId(), 0)).second);
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
    if (userName) {
        YCHECK(UserToEphemeralPools_[*userName].erase(pool->GetId()) == 1);
    }

    UnregisterSchedulingTagFilter(pool->GetSchedulingTagFilterIndex());

    YCHECK(PoolToMinUnusedSlotIndex_.erase(pool->GetId()) == 1);
    YCHECK(PoolToSpareSlotIndices_.erase(pool->GetId()) <= 1);

    // We cannot use pool after erase because Pools may contain last alive reference to it.
    auto extractedPool = std::move(Pools_[pool->GetId()]);
    YCHECK(Pools_.erase(pool->GetId()) == 1);

    extractedPool->SetAlive(false);
    auto parent = extractedPool->GetParent();
    extractedPool->DetachParent();

    YT_LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
        extractedPool->GetId(),
        parent->GetId());
}

bool TFairShareTree::TryAllocatePoolSlotIndex(const TString& poolName, int slotIndex)
{
    auto minUnusedIndexIt = PoolToMinUnusedSlotIndex_.find(poolName);
    YCHECK(minUnusedIndexIt != PoolToMinUnusedSlotIndex_.end());

    auto& spareSlotIndices = PoolToSpareSlotIndices_[poolName];

    if (slotIndex >= minUnusedIndexIt->second) {
        for (int index = minUnusedIndexIt->second; index < slotIndex; ++index) {
            spareSlotIndices.insert(index);
        }

        minUnusedIndexIt->second = slotIndex + 1;

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
        YCHECK(minUnusedIndexIt != PoolToMinUnusedSlotIndex_.end());
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
    YCHECK(slotIndex);

    auto it = PoolToSpareSlotIndices_.find(poolName);
    if (it == PoolToSpareSlotIndices_.end()) {
        YCHECK(PoolToSpareSlotIndices_.insert(std::make_pair(poolName, THashSet<int>{*slotIndex})).second);
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
        const auto& operationId = *it;
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
    const auto& element = FindOperationElement(operationId);
    if (!element) {
        return;
    }

    fluent
        .Do(BIND(&TFairShareTree::BuildEssentialOperationElementYson, Unretained(this), element));
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
    YCHECK(it != SchedulingTagFilterToIndexAndCount_.end());
    --it->second.Count;
    if (it->second.Count == 0) {
        RegisteredSchedulingTagFilters_[it->second.Index] = EmptySchedulingTagFilter;
        FreeSchedulingTagFilterIndexes_.push_back(it->second.Index);
        SchedulingTagFilterToIndexAndCount_.erase(it);
    }
}

TPoolPtr TFairShareTree::FindPool(const TString& id)
{
    auto it = Pools_.find(id);
    return it == Pools_.end() ? nullptr : it->second;
}

TPoolPtr TFairShareTree::GetPool(const TString& id)
{
    auto pool = FindPool(id);
    YCHECK(pool);
    return pool;
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
        poolConfig->Mode = GetPool(*poolName.GetParentPool())->GetConfig()->EphemeralSubpoolsMode;
    }
    pool = New<TPool>(
        Host_,
        this,
        poolName.GetPool(),
        poolConfig,
        /* defaultConfigured */ true,
        Config_,
        GetPoolProfilingTag(poolName.GetPool()),
        TreeId_);

    pool->SetUserName(userName);
    UserToEphemeralPools_[userName].insert(poolName.GetPool());

    TCompositeSchedulerElement* parent;
    if (poolName.GetParentPool()) {
        parent = GetPool(*poolName.GetParentPool()).Get();
    } else {
        parent = GetDefaultParentPool().Get();
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

TOperationElementPtr TFairShareTree::FindOperationElement(TOperationId operationId)
{
    auto it = OperationIdToElement_.find(operationId);
    return it == OperationIdToElement_.end() ? nullptr : it->second;
}

TOperationElementPtr TFairShareTree::GetOperationElement(TOperationId operationId)
{
    auto element = FindOperationElement(operationId);
    YCHECK(element);
    return element;
}

TFairShareTree::TRootElementSnapshotPtr TFairShareTree::CreateRootElementSnapshot()
{
    auto snapshot = New<TRootElementSnapshot>();
    snapshot->RootElement = RootElement_->Clone();
    snapshot->RootElement->BuildOperationToElementMapping(&snapshot->OperationIdToElement);
    snapshot->Config = Config_;
    return snapshot;
}

void TFairShareTree::BuildEssentialPoolsInformation(TFluentMap fluent)
{
    fluent
        .Item("pools").DoMapFor(Pools_, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
            const auto& id = pair.first;
            const auto& pool = pair.second;
            fluent
                .Item(id).BeginMap()
                    .Do(BIND(&TFairShareTree::BuildEssentialPoolElementYson, Unretained(this), pool))
                .EndMap();
        });
}

void TFairShareTree::BuildElementYson(const TSchedulerElementPtr& element, TFluentMap fluent)
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

void TFairShareTree::BuildEssentialElementYson(const TSchedulerElementPtr& element, TFluentMap fluent, bool shouldPrintResourceUsage)
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

void TFairShareTree::BuildEssentialPoolElementYson(const TSchedulerElementPtr& element, TFluentMap fluent)
{
    BuildEssentialElementYson(element, fluent, false);
}

void TFairShareTree::BuildEssentialOperationElementYson(const TSchedulerElementPtr& element, TFluentMap fluent)
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

    auto it = UserToEphemeralPools_.find(userName);
    if (it == UserToEphemeralPools_.end()) {
        return;
    }

    if (it->second.size() + 1 > Config_->MaxEphemeralPoolsPerUser) {
        THROW_ERROR_EXCEPTION("Limit for number of ephemeral pools %v for user %v in tree %Qv has been reached",
            Config_->MaxEphemeralPoolsPerUser,
            userName,
            TreeId_);
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
    {
        auto poolTag = element->GetParent()->GetProfilingTag();
        auto slotIndexTag = GetSlotIndexProfilingTag(element->GetSlotIndex());

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
        "/pools/running_operation_count",
        element->RunningOperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});
    accumulator.Add(
        "/pools/total_operation_count",
        element->OperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});

    // Deprecated.
    accumulator.Add(
        "/running_operation_count",
        element->RunningOperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});
    accumulator.Add(
        "/total_operation_count",
        element->OperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag_});
}

void TFairShareTree::ProfileSchedulerElement(TMetricsAccumulator& accumulator, const TSchedulerElementPtr& element, const TString& profilingPrefix, const TTagIdList& tags) const
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
