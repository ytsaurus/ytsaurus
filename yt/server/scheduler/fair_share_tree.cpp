#include "fair_share_tree.h"
#include "fair_share_tree_element.h"
#include "public.h"
#include "config.h"
#include "profiler.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "fair_share_strategy_operation_controller.h"
#include "fair_share_tree.h"

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/async_rw_lock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/algorithm_helpers.h>
#include <yt/core/misc/finally.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

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
    static THashMap<EScheduleJobFailReason, TTagId> tagId;

    auto it = tagId.find(reason);
    if (it == tagId.end()) {
        it = tagId.emplace(
            reason,
            TProfileManager::Get()->RegisterTag("reason", FormatEnum(reason))
        ).first;
    }
    return {it->second};
};

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
    : Config(config)
    , ControllerConfig(controllerConfig)
    , Host(host)
    , FeasibleInvokers(feasibleInvokers)
    , TreeId(treeId)
    , TreeIdProfilingTag(TProfileManager::Get()->RegisterTag("tree", TreeId))
    , Logger(NLogging::TLogger(SchedulerLogger)
        .AddTag("TreeId: %v", treeId))
    , NonPreemptiveProfilingCounters("/non_preemptive", {TreeIdProfilingTag})
    , PreemptiveProfilingCounters("/preemptive", {TreeIdProfilingTag})
    , FairShareUpdateTimeCounter("/fair_share_update_time", {TreeIdProfilingTag})
    , FairShareLogTimeCounter("/fair_share_log_time", {TreeIdProfilingTag})
    , AnalyzePreemptableJobsTimeCounter("/analyze_preemptable_jobs_time", {TreeIdProfilingTag})
{
    RootElement = New<TRootElement>(Host, this, config, GetPoolProfilingTag(RootPoolName), TreeId);
}

IFairShareTreeSnapshotPtr TFairShareTree::CreateSnapshot()
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    return New<TFairShareTreeSnapshot>(this, RootElementSnapshot, Logger);
}

TFuture<void> TFairShareTree::ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    return BIND(&TFairShareTree::DoValidateOperationPoolsCanBeUsed, MakeStrong(this))
        .AsyncVia(GetCurrentInvoker())
        .Run(operation, poolName);
}

void TFairShareTree::ValidatePoolLimits(const IOperationStrategyHost* operation, const TPoolName& poolName)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    ValidateOperationCountLimit(operation, poolName);
    ValidateEphemeralPoolLimit(operation, poolName);
}

void TFairShareTree::ValidatePoolLimitsOnPoolChange(const IOperationStrategyHost* operation, const TPoolName& newPoolName)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    ValidateEphemeralPoolLimit(operation, newPoolName);
    ValidateAllOperationsCountsOnPoolChange(operation->GetId(), newPoolName);
}

void TFairShareTree::ValidateAllOperationsCountsOnPoolChange(const TOperationId& operationId, const TPoolName& newPoolName)
{
    auto operationElement = GetOperationElement(operationId);
    std::vector<TString> oldPools;
    TCompositeSchedulerElement* pool = operationElement->GetParent();
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
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    auto operationId = state->GetHost()->GetId();

    auto clonedSpec = CloneYsonSerializable(spec);
    auto optionsIt = spec->SchedulingOptionsPerPoolTree.find(TreeId);
    if (optionsIt != spec->SchedulingOptionsPerPoolTree.end()) {
        ReconfigureYsonSerializable(clonedSpec, ConvertToNode(optionsIt->second));
    }

    auto operationElement = New<TOperationElement>(
        Config,
        clonedSpec,
        runtimeParams,
        state->GetController(),
        ControllerConfig,
        Host,
        this,
        state->GetHost(),
        TreeId);

    int index = RegisterSchedulingTagFilter(TSchedulingTagFilter(clonedSpec->SchedulingTagFilter));
    operationElement->SetSchedulingTagFilterIndex(index);

    YCHECK(OperationIdToElement.insert(std::make_pair(operationId, operationElement)).second);

    auto poolName = state->GetPoolNameByTreeId(TreeId);

    if (!AttachOperation(state, operationElement, poolName)) {
        WaitingOperationQueue.push_back(operationId);
        return false;
    }
    return true;
}

// Attaches operation to tree and returns if it can be activated (pools limits are satisfied)
bool TFairShareTree::AttachOperation(
    const TFairShareStrategyOperationStatePtr& state,
    TOperationElementPtr& operationElement,
    const TPoolName& poolName)
{
    auto operationId = state->GetHost()->GetId();

    auto pool = FindPool(poolName.GetPool());
    if (!pool) {
        auto poolConfig = New<TPoolConfig>();
        if (poolName.GetParentPool()) {
            poolConfig->Mode = GetPool(*poolName.GetParentPool())->GetConfig()->EphemeralSubpoolsMode;
        }
        pool = New<TPool>(
            Host,
            this,
            poolName.GetPool(),
            poolConfig,
            /* defaultConfigured */ true,
            Config,
            GetPoolProfilingTag(poolName.GetPool()),
            TreeId);

        const auto& userName = state->GetHost()->GetAuthenticatedUser();
        pool->SetUserName(userName);
        UserToEphemeralPools[userName].insert(poolName.GetPool());
        RegisterPool(pool);
    }
    if (!pool->GetParent()) {
        if (poolName.GetParentPool()) {
            SetPoolParent(pool, GetPool(*poolName.GetParentPool()));
        } else {
            SetPoolDefaultParent(pool);
        }
    }

    pool->IncreaseOperationCount(1);

    pool->AddChild(operationElement, false);
    pool->IncreaseHierarchicalResourceUsage(operationElement->GetLocalResourceUsage());
    operationElement->SetParent(pool.Get());

    AllocateOperationSlotIndex(state, poolName.GetPool());

    auto violatedPool = FindPoolViolatingMaxRunningOperationCount(pool.Get());
    if (!violatedPool) {
        AddOperationToPool(operationId);
        return true;
    }

    YT_LOG_DEBUG("Max running operation count violated (OperationId: %v, Pool: %v, Limit: %v)",
        operationId,
        violatedPool->GetId(),
        violatedPool->GetMaxRunningOperationCount());
    Host->SetOperationAlert(
        operationId,
        EOperationAlertType::OperationPending,
        TError("Max running operation count violated")
            << TErrorAttribute("pool", violatedPool->GetId())
            << TErrorAttribute("limit", violatedPool->GetMaxRunningOperationCount())
        );
    return false;
}

TOperationUnregistrationResult TFairShareTree::UnregisterOperation(
    const TFairShareStrategyOperationStatePtr& state)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    auto operationId = state->GetHost()->GetId();
    const auto& operationElement = FindOperationElement(operationId);
    auto wasActive = DetachOperation(state, operationElement);

    UnregisterSchedulingTagFilter(operationElement->GetSchedulingTagFilterIndex());

    operationElement->Disable();
    YCHECK(OperationIdToElement.erase(operationId) == 1);
    operationElement->SetAlive(false);

    // Operation can be missing in this map.
    OperationIdToActivationTime_.erase(operationId);

    TOperationUnregistrationResult result;
    if (wasActive) {
        TryActivateOperationsFromQueue(&result.OperationsToActivate);
    }
    return result;
}

// Detaches operation element from tree but leaves it eligible to be attached in another place in the same tree.
// Removes operation from waiting queue if operation wasn't active. Returns true if operation was active.
bool TFairShareTree::DetachOperation(const TFairShareStrategyOperationStatePtr& state, const TOperationElementPtr& operationElement)
{
    auto operationId = state->GetHost()->GetId();
    auto* pool = static_cast<TPool*>(operationElement->GetParent());

    ReleaseOperationSlotIndex(state, pool->GetId());

    pool->RemoveChild(operationElement);
    pool->IncreaseOperationCount(-1);
    pool->IncreaseHierarchicalResourceUsage(-operationElement->GetLocalResourceUsage());

    YT_LOG_INFO("Operation removed from pool (OperationId: %v, Pool: %v)",
        operationId,
        pool->GetId());

    bool wasActive = true;
    for (auto it = WaitingOperationQueue.begin(); it != WaitingOperationQueue.end(); ++it) {
        if (*it == operationId) {
            wasActive = false;
            WaitingOperationQueue.erase(it);
            break;
        }
    }

    if (wasActive) {
        pool->IncreaseRunningOperationCount(-1);
    }

    if (pool->IsEmpty() && pool->IsDefaultConfigured()) {
        UnregisterPool(pool);
    }

    return wasActive;
}

void TFairShareTree::DisableOperation(const TFairShareStrategyOperationStatePtr& state)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    auto operationElement = GetOperationElement(state->GetHost()->GetId());
    auto usage = operationElement->GetLocalResourceUsage();
    operationElement->Disable();

    auto* parent = operationElement->GetParent();
    parent->IncreaseHierarchicalResourceUsage(-usage);
    parent->DisableChild(operationElement);
}

void TFairShareTree::EnableOperation(const TFairShareStrategyOperationStatePtr& state)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    auto operationId = state->GetHost()->GetId();
    auto operationElement = GetOperationElement(operationId);

    auto* parent = operationElement->GetParent();
    parent->EnableChild(operationElement);

    operationElement->Enable();
}

TPoolsUpdateResult TFairShareTree::UpdatePools(const INodePtr& poolsNode)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    if (LastPoolsNodeUpdate && AreNodesEqual(LastPoolsNodeUpdate, poolsNode)) {
        YT_LOG_INFO("Pools are not changed, skipping update");
        return {LastPoolsNodeUpdateError, false};
    }

    LastPoolsNodeUpdate = poolsNode;

    std::vector<TError> errors;

    try {
        // Build the set of potential orphans.
        THashSet<TString> orphanPoolIds;
        for (const auto& pair : Pools) {
            YCHECK(orphanPoolIds.insert(pair.first).second);
        }

        // Track ids appearing in various branches of the tree.
        THashMap<TString, TYPath> poolIdToPath;

        // NB: std::function is needed by parseConfig to capture itself.
        std::function<void(INodePtr, TCompositeSchedulerElementPtr)> parseConfig =
            [&] (INodePtr configNode, TCompositeSchedulerElementPtr parent) {
                auto configMap = configNode->AsMap();
                for (const auto& pair : configMap->GetChildren()) {
                    const auto& childId = pair.first;
                    const auto& childNode = pair.second;
                    auto childPath = childNode->GetPath();
                    if (!poolIdToPath.insert(std::make_pair(childId, childPath)).second) {
                        errors.emplace_back(
                            "Pool %Qv is defined both at %v and %v; skipping second occurrence",
                            childId,
                            poolIdToPath[childId],
                            childPath);
                        continue;
                    }

                    // Parse config.
                    auto poolConfigNode = ConvertToNode(childNode->Attributes());
                    TPoolConfigPtr poolConfig;
                    try {
                        poolConfig = ConvertTo<TPoolConfigPtr>(poolConfigNode);
                    } catch (const std::exception& ex) {
                        errors.emplace_back(
                            TError(
                                "Error parsing configuration of pool %Qv; using defaults",
                                childPath)
                            << ex);
                        poolConfig = New<TPoolConfig>();
                    }

                    try {
                        poolConfig->Validate();
                    } catch (const std::exception& ex) {
                        errors.emplace_back(
                            TError(
                                "Misconfiguration of pool %Qv found",
                                childPath)
                            << ex);
                    }

                    auto pool = FindPool(childId);
                    if (pool) {
                        // Reconfigure existing pool.
                        ReconfigurePool(pool, poolConfig);
                        YCHECK(orphanPoolIds.erase(childId) == 1);
                    } else {
                        // Create new pool.
                        pool = New<TPool>(
                            Host,
                            this,
                            childId,
                            poolConfig,
                            /* defaultConfigured */ false,
                            Config,
                            GetPoolProfilingTag(childId),
                            TreeId);
                        RegisterPool(pool, parent);
                    }
                    SetPoolParent(pool, parent);

                    if (parent->GetMode() == ESchedulingMode::Fifo) {
                        parent->SetMode(ESchedulingMode::FairShare);
                        errors.emplace_back(
                            TError(
                                "Pool %Qv cannot have subpools since it is in %Qlv mode",
                                parent->GetId(),
                                ESchedulingMode::Fifo));
                    }

                    // Parse children.
                    parseConfig(childNode, pool.Get());
                }
            };

        // Run recursive descent parsing.
        parseConfig(poolsNode, RootElement);

        // Unregister orphan pools.
        for (const auto& id : orphanPoolIds) {
            auto pool = GetPool(id);
            if (pool->IsEmpty()) {
                UnregisterPool(pool);
            } else {
                pool->SetDefaultConfig();
                SetPoolDefaultParent(pool);
            }
        }

        ResetTreeIndexes();
        RootElement->Update(GlobalDynamicAttributes_);
        RootElementSnapshot = CreateRootElementSnapshot();
    } catch (const std::exception& ex) {
        auto error = TError("Error updating pools in tree %Qv", TreeId)
            << ex;
        LastPoolsNodeUpdateError = error;
        return {error, true};
    }

    if (!errors.empty()) {
        auto combinedError = TError("Found pool configuration issues in tree %Qv", TreeId)
            << std::move(errors);
        LastPoolsNodeUpdateError = combinedError;
        return {combinedError, true};
    }

    LastPoolsNodeUpdateError = TError();

    return {LastPoolsNodeUpdateError, true};
}

bool TFairShareTree::ChangeOperationPool(
    const TOperationId& operationId,
    const TFairShareStrategyOperationStatePtr& state,
    const TPoolName& newPool)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    auto element = FindOperationElement(operationId);
    if (!element) {
        THROW_ERROR_EXCEPTION("Operation element for operation %Qv not found", operationId);
    }

    YT_LOG_INFO("Operation is changing operation pool (OperationId: %v, OldPool: %v NewPool: %v)",
        operationId,
        element->GetParent()->GetId(),
        newPool.GetPool());

    auto wasActive = DetachOperation(state, element);
    YCHECK(AttachOperation(state, element, newPool));
    return wasActive;
}

TError TFairShareTree::CheckOperationUnschedulable(
    const TOperationId& operationId,
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
    const TOperationId& operationId,
    const TOperationFairShareTreeRuntimeParametersPtr& runtimeParams)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    const auto& element = FindOperationElement(operationId);
    if (element) {
        element->SetRuntimeParams(runtimeParams);
    }
}

void TFairShareTree::UpdateConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    Config = config;
    RootElement->UpdateTreeConfig(Config);
}

void TFairShareTree::UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    ControllerConfig = config;

    for (const auto& pair : OperationIdToElement) {
        const auto& element = pair.second;
        element->UpdateControllerConfig(config);
    }
}

void TFairShareTree::BuildOperationAttributes(const TOperationId& operationId, TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    const auto& element = GetOperationElement(operationId);
    auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
    fluent
        .Items(*serializedParams)
        .Item("pool").Value(element->GetParent()->GetId());
}

void TFairShareTree::BuildOperationProgress(const TOperationId& operationId, TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

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

void TFairShareTree::BuildBriefOperationProgress(const TOperationId& operationId, TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

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
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    fluent
        .DoMapFor(UserToEphemeralPools, [] (TFluentMap fluent, const auto& value) {
            fluent
                .Item(value.first).Value(value.second);
        });
}

// NB: This function is public for testing purposes.
TError TFairShareTree::OnFairShareUpdateAt(TInstant now)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    TError error;

    // Run periodic update.
    PROFILE_AGGREGATED_TIMING(FairShareUpdateTimeCounter) {
        // The root element gets the whole cluster.
        ResetTreeIndexes();
        RootElement->Update(GlobalDynamicAttributes_);

        // Collect alerts after update.
        std::vector<TError> alerts;

        for (const auto& pair : Pools) {
            const auto& poolAlerts = pair.second->UpdateFairShareAlerts();
            alerts.insert(alerts.end(), poolAlerts.begin(), poolAlerts.end());
        }

        const auto& rootElementAlerts = RootElement->UpdateFairShareAlerts();
        alerts.insert(alerts.end(), rootElementAlerts.begin(), rootElementAlerts.end());

        if (!alerts.empty()) {
            error = TError("Found pool configuration issues during fair share update in tree %Qv", TreeId)
                << std::move(alerts);
        }

        // Update starvation flags for all operations.
        for (const auto& pair : OperationIdToElement) {
            pair.second->CheckForStarvation(now);
        }

        // Update starvation flags for all pools.
        if (Config->EnablePoolStarvation) {
            for (const auto& pair : Pools) {
                pair.second->CheckForStarvation(now);
            }
        }

        RootElementSnapshot = CreateRootElementSnapshot();
    }

    return error;
}

void TFairShareTree::ProfileFairShare() const
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    TProfileCollector collector(&Profiler);

    for (const auto& pair : Pools) {
        ProfileCompositeSchedulerElement(collector, pair.second);
    }
    ProfileCompositeSchedulerElement(collector, RootElement);
    if (Config->EnableOperationsProfiling) {
        for (const auto& pair : OperationIdToElement) {
            ProfileOperationElement(collector, pair.second);
        }
    }

    collector.Publish();
}

void TFairShareTree::ResetTreeIndexes()
{
    for (const auto& pair : OperationIdToElement) {
        auto& element = pair.second;
        element->SetTreeIndex(UnassignedTreeIndex);
    }
}

void TFairShareTree::LogOperationsInfo()
{
    for (const auto& pair : OperationIdToElement) {
        const auto& operationId = pair.first;
        const auto& element = pair.second;
        YT_LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
            element->GetLoggingString(GlobalDynamicAttributes_),
            operationId);
    }
}

void TFairShareTree::LogPoolsInfo()
{
    for (const auto& pair : Pools) {
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
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    PROFILE_AGGREGATED_TIMING(FairShareLogTimeCounter) {
        // Log pools information.
        Host->LogEventFluently(ELogEventType::FairShareInfo, now)
            .Item("tree_id").Value(TreeId)
            .Do(BIND(&TFairShareTree::BuildFairShareInfo, Unretained(this)));

        LogOperationsInfo();
    }
}

// NB: This function is public for testing purposes.
void TFairShareTree::OnFairShareEssentialLoggingAt(TInstant now)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    PROFILE_AGGREGATED_TIMING(FairShareLogTimeCounter) {
        // Log pools information.
        Host->LogEventFluently(ELogEventType::FairShareInfo, now)
            .Item("tree_id").Value(TreeId)
            .Do(BIND(&TFairShareTree::BuildEssentialFairShareInfo, Unretained(this)));

        LogOperationsInfo();
    }
}

void TFairShareTree::RegisterJobs(const TOperationId& operationId, const std::vector<TJobPtr>& jobs)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    const auto& element = FindOperationElement(operationId);
    for (const auto& job : jobs) {
        element->OnJobStarted(
            job->GetId(),
            job->ResourceUsage(),
            /* precommittedResources */ ZeroJobResources(),
            /* force */ true);
    }
}

void TFairShareTree::BuildPoolsInformation(TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

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
            .DoFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                buildPoolInfo(pair.second, fluent);
            })
            .Do(std::bind(buildPoolInfo, RootElement, std::placeholders::_1))
        .EndMap();
}

void TFairShareTree::BuildStaticPoolsInformation(TFluentAny fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    fluent
        .DoMapFor(Pools, [&] (TFluentMap fluent, const auto& pair) {
            const auto& id = pair.first;
            const auto& pool = pair.second;
            fluent
                .Item(id).Value(pool->GetConfig());
        });
}

void TFairShareTree::BuildOrchid(TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    fluent
        .Item("resource_usage").Value(RootElement->GetLocalResourceUsage());
}

void TFairShareTree::BuildFairShareInfo(TFluentMap fluent)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    fluent
        .Do(BIND(&TFairShareTree::BuildPoolsInformation, Unretained(this)))
        .Item("operations").DoMapFor(
            OperationIdToElement,
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
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    fluent
        .Do(BIND(&TFairShareTree::BuildEssentialPoolsInformation, Unretained(this)))
        .Item("operations").DoMapFor(
            OperationIdToElement,
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
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    LastPoolsNodeUpdate.Reset();
    LastPoolsNodeUpdateError = TError();
}

const TSchedulingTagFilter& TFairShareTree::GetNodesFilter() const
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    return Config->NodesFilter;
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

bool TFairShareTree::HasOperation(const TOperationId& operationId)
{
    return static_cast<bool>(FindOperationElement(operationId));
}

TAggregateGauge& TFairShareTree::GetProfilingCounter(const TString& name)
{
    TGuard<TSpinLock> guard(CustomProfilingCountersLock_);

    auto it = CustomProfilingCounters_.find(name);
    if (it == CustomProfilingCounters_.end()) {
        auto tag = TProfileManager::Get()->RegisterTag("tree", TreeId);
        auto ptr = std::make_unique<TAggregateGauge>(name, TTagIdList{tag});
        it = CustomProfilingCounters_.emplace(name, std::move(ptr)).first;
    }
    return *it->second;
}

void TFairShareTree::DoScheduleJobsWithoutPreemption(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFairShareContext* context,
    TCpuInstant startTime,
    const std::function<void(TScheduleJobsProfilingCounters&, int, TDuration)> profileTimings,
    const std::function<void(TStringBuf)> logAndCleanSchedulingStatistics)
{
    auto& rootElement = rootElementSnapshot->RootElement;

    {
        YT_LOG_TRACE("Scheduling new jobs");

        bool prescheduleExecuted = false;
        TDuration prescheduleDuration;

        TWallTimer scheduleTimer;
        while (context->SchedulingContext->CanStartMoreJobs() &&
            GetCpuInstant() < startTime + DurationToCpuDuration(ControllerConfig->ScheduleJobsTimeout))
        {
            if (!prescheduleExecuted) {
                TWallTimer prescheduleTimer;
                context->Initialize(rootElement->GetTreeSize(), RegisteredSchedulingTagFilters);
                rootElement->PrescheduleJob(context, /*starvingOnly*/ false, /*aggressiveStarvationEnabled*/ false);
                prescheduleDuration = prescheduleTimer.GetElapsedTime();
                Profiler.Update(NonPreemptiveProfilingCounters.PrescheduleJobTime, DurationToCpuDuration(prescheduleDuration));
                prescheduleExecuted = true;
                context->PrescheduledCalled = true;
            }
            ++context->SchedulingStatistics.NonPreemptiveScheduleJobAttempts;
            if (!rootElement->ScheduleJob(context)) {
                break;
            }
        }
        profileTimings(
            NonPreemptiveProfilingCounters,
            context->SchedulingStatistics.NonPreemptiveScheduleJobAttempts,
            scheduleTimer.GetElapsedTime() - prescheduleDuration - context->TotalScheduleJobDuration);

        if (context->SchedulingStatistics.NonPreemptiveScheduleJobAttempts > 0) {
            logAndCleanSchedulingStatistics(AsStringBuf("Non preemptive"));
        }
    }
}

void TFairShareTree::DoScheduleJobsWithPreemption(
    const TRootElementSnapshotPtr& rootElementSnapshot,
    TFairShareContext* context,
    TCpuInstant startTime,
    const std::function<void(TScheduleJobsProfilingCounters&, int, TDuration)>& profileTimings,
    const std::function<void(TStringBuf)>& logAndCleanSchedulingStatistics)
{
    auto& rootElement = rootElementSnapshot->RootElement;
    auto& config = rootElementSnapshot->Config;

    if (!context->Initialized) {
        context->Initialize(rootElement->GetTreeSize(), RegisteredSchedulingTagFilters);
    }

    if (!context->PrescheduledCalled) {
        context->SchedulingStatistics.HasAggressivelyStarvingNodes = rootElement->HasAggressivelyStarvingNodes(context, false);
    }

    // Compute discount to node usage.
    YT_LOG_TRACE("Looking for preemptable jobs");
    THashSet<TCompositeSchedulerElementPtr> discountedPools;
    std::vector<TJobPtr> preemptableJobs;
    PROFILE_AGGREGATED_TIMING(AnalyzePreemptableJobsTimeCounter) {
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

            bool aggressivePreemptionEnabled = context->SchedulingStatistics.HasAggressivelyStarvingNodes &&
                operationElement->IsAggressiveStarvationPreemptionAllowed();
            if (operationElement->IsJobPreemptable(job->GetId(), aggressivePreemptionEnabled)) {
                auto* parent = operationElement->GetParent();
                while (parent) {
                    discountedPools.insert(parent);
                    context->DynamicAttributes(parent).ResourceUsageDiscount += job->ResourceUsage();
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

        // Clean data from previous profiling.
        context->TotalScheduleJobDuration = TDuration::Zero();
        context->ExecScheduleJobDuration = TDuration::Zero();
        context->ScheduleJobFailureCount = 0;
        std::fill(context->FailedScheduleJob.begin(), context->FailedScheduleJob.end(), 0);

        bool prescheduleExecuted = false;
        TDuration prescheduleDuration;

        TWallTimer timer;
        while (context->SchedulingContext->CanStartMoreJobs() &&
            GetCpuInstant() < startTime + DurationToCpuDuration(ControllerConfig->ScheduleJobsTimeout))
        {
            if (!prescheduleExecuted) {
                TWallTimer prescheduleTimer;
                rootElement->PrescheduleJob(context, /*starvingOnly*/ true, /*aggressiveStarvationEnabled*/ false);
                prescheduleDuration = prescheduleTimer.GetElapsedTime();
                Profiler.Update(PreemptiveProfilingCounters.PrescheduleJobTime, DurationToCpuDuration(prescheduleDuration));
                prescheduleExecuted = true;
            }

            ++context->SchedulingStatistics.PreemptiveScheduleJobAttempts;
            if (!rootElement->ScheduleJob(context)) {
                break;
            }
            if (context->SchedulingContext->StartedJobs().size() > startedBeforePreemption) {
                jobStartedUsingPreemption = context->SchedulingContext->StartedJobs().back();
                break;
            }
        }
        profileTimings(
            PreemptiveProfilingCounters,
            context->SchedulingStatistics.PreemptiveScheduleJobAttempts,
            timer.GetElapsedTime() - prescheduleDuration - context->TotalScheduleJobDuration);
        if (context->SchedulingStatistics.PreemptiveScheduleJobAttempts > 0) {
            logAndCleanSchedulingStatistics(AsStringBuf("Preemptive"));
        }
    }

    int startedAfterPreemption = context->SchedulingContext->StartedJobs().size();

    context->SchedulingStatistics.ScheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

    // Reset discounts.
    context->SchedulingContext->ResourceUsageDiscount() = ZeroJobResources();
    for (const auto& pool : discountedPools) {
        context->DynamicAttributes(pool.Get()).ResourceUsageDiscount = ZeroJobResources();
    }

    // Preempt jobs if needed.
    std::sort(
        preemptableJobs.begin(),
        preemptableJobs.end(),
        [] (const TJobPtr& lhs, const TJobPtr& rhs) {
            return lhs->GetStartTime() > rhs->GetStartTime();
        });

    auto findPoolWithViolatedLimitsForJob = [&] (const TJobPtr& job) -> TCompositeSchedulerElement* {
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
        YT_LOG_INFO("Resource usage exceeds node resorce limits even after preemption.");
    }
}

void TFairShareTree::DoScheduleJobs(
    const ISchedulingContextPtr& schedulingContext,
    const TRootElementSnapshotPtr& rootElementSnapshot)
{
    TFairShareContext context(schedulingContext);

    auto profileTimings = [&] (
        TScheduleJobsProfilingCounters& counters,
        int scheduleJobCount,
        TDuration scheduleJobDurationWithoutControllers)
    {
        Profiler.Update(
            counters.StrategyScheduleJobTime,
            scheduleJobDurationWithoutControllers.MicroSeconds());

        Profiler.Update(
            counters.TotalControllerScheduleJobTime,
            context.TotalScheduleJobDuration.MicroSeconds());

        Profiler.Update(
            counters.ExecControllerScheduleJobTime,
            context.ExecScheduleJobDuration.MicroSeconds());

        Profiler.Increment(counters.ScheduleJobCount, scheduleJobCount);
        Profiler.Increment(counters.ScheduleJobFailureCount, context.ScheduleJobFailureCount);

        for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
            Profiler.Increment(
                counters.ControllerScheduleJobFail[reason],
                context.FailedScheduleJob[reason]);
        }
    };

    bool enableSchedulingInfoLogging = false;
    auto now = GetCpuInstant();
    const auto& config = rootElementSnapshot->Config;
    if (LastSchedulingInformationLoggedTime_ + DurationToCpuDuration(config->HeartbeatTreeSchedulingInfoLogBackoff) < now) {
        enableSchedulingInfoLogging = true;
        LastSchedulingInformationLoggedTime_ = now;
    }

    auto logAndCleanSchedulingStatistics = [&] (TStringBuf stageName) {
        if (!enableSchedulingInfoLogging) {
            return;
        }
        YT_LOG_DEBUG("%v scheduling statistics (ActiveTreeSize: %v, ActiveOperationCount: %v, DeactivationReasons: %v, CanStartMoreJobs: %v, Address: %v)",
            stageName,
            context.ActiveTreeSize,
            context.ActiveOperationCount,
            context.DeactivationReasons,
            schedulingContext->CanStartMoreJobs(),
            schedulingContext->GetNodeDescriptor().Address);
        context.ActiveTreeSize = 0;
        context.ActiveOperationCount = 0;
        std::fill(context.DeactivationReasons.begin(), context.DeactivationReasons.end(), 0);
    };

    DoScheduleJobsWithoutPreemption(rootElementSnapshot, &context, now, profileTimings, logAndCleanSchedulingStatistics);

    auto nodeId = schedulingContext->GetNodeDescriptor().Id;

    bool scheduleJobsWithPreemption = false;
    {
        bool nodeIsMissing = false;
        {
            TReaderGuard guard(NodeIdToLastPreemptiveSchedulingTimeLock);
            auto it = NodeIdToLastPreemptiveSchedulingTime.find(nodeId);
            if (it == NodeIdToLastPreemptiveSchedulingTime.end()) {
                nodeIsMissing = true;
                scheduleJobsWithPreemption = true;
            } else if (it->second + DurationToCpuDuration(config->PreemptiveSchedulingBackoff) <= now) {
                scheduleJobsWithPreemption = true;
                it->second = now;
            }
        }
        if (nodeIsMissing) {
            TWriterGuard guard(NodeIdToLastPreemptiveSchedulingTimeLock);
            NodeIdToLastPreemptiveSchedulingTime[nodeId] = now;
        }
    }

    if (scheduleJobsWithPreemption) {
        DoScheduleJobsWithPreemption(rootElementSnapshot, &context, now, profileTimings, logAndCleanSchedulingStatistics);
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
    job->ResourceUsage() = ZeroJobResources();

    context->SchedulingContext->PreemptJob(job);
}

TCompositeSchedulerElement* TFairShareTree::FindPoolViolatingMaxRunningOperationCount(TCompositeSchedulerElement* pool)
{
    VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

    while (pool) {
        if (pool->RunningOperationCount() >= pool->GetMaxRunningOperationCount()) {
            return pool;
        }
        pool = pool->GetParent();
    }
    return nullptr;
}

TCompositeSchedulerElementPtr TFairShareTree::FindPoolWithViolatedOperationCountLimit(const TCompositeSchedulerElementPtr& element)
{
    auto current = element;
    while (current) {
        if (current->OperationCount() >= current->GetMaxOperationCount()) {
            return current;
        }
        current = current->GetParent();
    }
    return nullptr;
}

void TFairShareTree::AddOperationToPool(const TOperationId& operationId)
{
    TForbidContextSwitchGuard contextSwitchGuard;

    const auto& operationElement = GetOperationElement(operationId);
    auto* parent = operationElement->GetParent();
    parent->IncreaseRunningOperationCount(1);

    YT_LOG_INFO("Operation added to pool (OperationId: %v, Pool: %v)",
        operationId,
        parent->GetId());
}

void TFairShareTree::DoRegisterPool(const TPoolPtr& pool)
{
    int index = RegisterSchedulingTagFilter(pool->GetSchedulingTagFilter());
    pool->SetSchedulingTagFilterIndex(index);
    YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
    YCHECK(PoolToMinUnusedSlotIndex.insert(std::make_pair(pool->GetId(), 0)).second);
}

void TFairShareTree::RegisterPool(const TPoolPtr& pool)
{
    DoRegisterPool(pool);

    YT_LOG_INFO("Pool registered (Pool: %v)", pool->GetId());
}

void TFairShareTree::RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
{
    DoRegisterPool(pool);

    pool->SetParent(parent.Get());
    parent->AddChild(pool);

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
        YCHECK(UserToEphemeralPools[*userName].erase(pool->GetId()) == 1);
    }

    UnregisterSchedulingTagFilter(pool->GetSchedulingTagFilterIndex());

    YCHECK(PoolToMinUnusedSlotIndex.erase(pool->GetId()) == 1);
    YCHECK(PoolToSpareSlotIndices.erase(pool->GetId()) <= 1);
    YCHECK(Pools.erase(pool->GetId()) == 1);

    pool->SetAlive(false);
    auto parent = pool->GetParent();
    SetPoolParent(pool, nullptr);

    YT_LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
        pool->GetId(),
        parent->GetId());
}

bool TFairShareTree::TryAllocatePoolSlotIndex(const TString& poolName, int slotIndex)
{
    auto minUnusedIndexIt = PoolToMinUnusedSlotIndex.find(poolName);
    YCHECK(minUnusedIndexIt != PoolToMinUnusedSlotIndex.end());

    auto& spareSlotIndices = PoolToSpareSlotIndices[poolName];

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
    auto slotIndex = state->GetHost()->FindSlotIndex(TreeId);

    if (slotIndex) {
        // Revive case
        if (TryAllocatePoolSlotIndex(poolName, *slotIndex)) {
            return;
        }
        YT_LOG_ERROR("Failed to reuse slot index during revive (OperationId: %v, SlotIndex: %v)",
            state->GetHost()->GetId(),
            *slotIndex);
    }

    auto it = PoolToSpareSlotIndices.find(poolName);
    if (it == PoolToSpareSlotIndices.end() || it->second.empty()) {
        auto minUnusedIndexIt = PoolToMinUnusedSlotIndex.find(poolName);
        YCHECK(minUnusedIndexIt != PoolToMinUnusedSlotIndex.end());
        slotIndex = minUnusedIndexIt->second;
        ++minUnusedIndexIt->second;
    } else {
        auto spareIndexIt = it->second.begin();
        slotIndex = *spareIndexIt;
        it->second.erase(spareIndexIt);
    }

    state->GetHost()->SetSlotIndex(TreeId, *slotIndex);

    YT_LOG_DEBUG("Operation slot index allocated (OperationId: %v, SlotIndex: %v)",
        state->GetHost()->GetId(),
        *slotIndex);
}

void TFairShareTree::ReleaseOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName)
{
    auto slotIndex = state->GetHost()->FindSlotIndex(TreeId);
    YCHECK(slotIndex);

    auto it = PoolToSpareSlotIndices.find(poolName);
    if (it == PoolToSpareSlotIndices.end()) {
        YCHECK(PoolToSpareSlotIndices.insert(std::make_pair(poolName, THashSet<int>{*slotIndex})).second);
    } else {
        it->second.insert(*slotIndex);
    }

    YT_LOG_DEBUG("Operation slot index released (OperationId: %v, SlotIndex: %v)",
        state->GetHost()->GetId(),
        *slotIndex);
}

void TFairShareTree::TryActivateOperationsFromQueue(std::vector<TOperationId>* operationsToActivate)
{
    // Try to run operations from queue.
    auto it = WaitingOperationQueue.begin();
    while (it != WaitingOperationQueue.end() && RootElement->RunningOperationCount() < Config->MaxRunningOperationCount) {
        const auto& operationId = *it;
        auto* operationPool = GetOperationElement(operationId)->GetParent();
        if (FindPoolViolatingMaxRunningOperationCount(operationPool) == nullptr) {
            operationsToActivate->push_back(operationId);
            AddOperationToPool(operationId);
            auto toRemove = it++;
            WaitingOperationQueue.erase(toRemove);
        } else {
            ++it;
        }
    }
}

void TFairShareTree::BuildEssentialOperationProgress(const TOperationId& operationId, TFluentMap fluent)
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
    auto it = SchedulingTagFilterToIndexAndCount.find(filter);
    if (it == SchedulingTagFilterToIndexAndCount.end()) {
        int index;
        if (FreeSchedulingTagFilterIndexes.empty()) {
            index = RegisteredSchedulingTagFilters.size();
            RegisteredSchedulingTagFilters.push_back(filter);
        } else {
            index = FreeSchedulingTagFilterIndexes.back();
            RegisteredSchedulingTagFilters[index] = filter;
            FreeSchedulingTagFilterIndexes.pop_back();
        }
        SchedulingTagFilterToIndexAndCount.emplace(filter, TSchedulingTagFilterEntry({index, 1}));
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
    UnregisterSchedulingTagFilter(RegisteredSchedulingTagFilters[index]);
}

void TFairShareTree::UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
{
    if (filter.IsEmpty()) {
        return;
    }
    auto it = SchedulingTagFilterToIndexAndCount.find(filter);
    YCHECK(it != SchedulingTagFilterToIndexAndCount.end());
    --it->second.Count;
    if (it->second.Count == 0) {
        RegisteredSchedulingTagFilters[it->second.Index] = EmptySchedulingTagFilter;
        FreeSchedulingTagFilterIndexes.push_back(it->second.Index);
        SchedulingTagFilterToIndexAndCount.erase(it);
    }
}

void TFairShareTree::SetPoolParent(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
{
    if (pool->GetParent() == parent) {
        return;
    }

    auto* oldParent = pool->GetParent();
    if (oldParent) {
        oldParent->IncreaseHierarchicalResourceUsage(-pool->GetLocalResourceUsage());
        oldParent->IncreaseOperationCount(-pool->OperationCount());
        oldParent->IncreaseRunningOperationCount(-pool->RunningOperationCount());
        oldParent->RemoveChild(pool);
    }

    pool->SetParent(parent.Get());
    if (parent) {
        parent->AddChild(pool);
        parent->IncreaseHierarchicalResourceUsage(pool->GetLocalResourceUsage());
        parent->IncreaseOperationCount(pool->OperationCount());
        parent->IncreaseRunningOperationCount(pool->RunningOperationCount());

        YT_LOG_INFO("Parent pool set (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }
}

void TFairShareTree::SetPoolDefaultParent(const TPoolPtr& pool)
{
    auto defaultParentPool = FindPool(Config->DefaultParentPool);
    if (!defaultParentPool || defaultParentPool == pool) {
        // NB: root element is not a pool, so we should suppress warning in this special case.
        if (Config->DefaultParentPool != RootPoolName) {
            auto error = TError("Default parent pool %Qv is not registered", Config->DefaultParentPool);
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
        }
        SetPoolParent(pool, RootElement);
    } else {
        SetPoolParent(pool, defaultParentPool);
    }
}

TPoolPtr TFairShareTree::FindPool(const TString& id)
{
    auto it = Pools.find(id);
    return it == Pools.end() ? nullptr : it->second;
}

TPoolPtr TFairShareTree::GetPool(const TString& id)
{
    auto pool = FindPool(id);
    YCHECK(pool);
    return pool;
}

NProfiling::TTagId TFairShareTree::GetPoolProfilingTag(const TString& id)
{
    auto it = PoolIdToProfilingTagId.find(id);
    if (it == PoolIdToProfilingTagId.end()) {
        it = PoolIdToProfilingTagId.emplace(
            id,
            NProfiling::TProfileManager::Get()->RegisterTag("pool", id)
        ).first;
    }
    return it->second;
}

TOperationElementPtr TFairShareTree::FindOperationElement(const TOperationId& operationId)
{
    auto it = OperationIdToElement.find(operationId);
    return it == OperationIdToElement.end() ? nullptr : it->second;
}

TOperationElementPtr TFairShareTree::GetOperationElement(const TOperationId& operationId)
{
    auto element = FindOperationElement(operationId);
    YCHECK(element);
    return element;
}

TFairShareTree::TRootElementSnapshotPtr TFairShareTree::CreateRootElementSnapshot()
{
    auto snapshot = New<TRootElementSnapshot>();
    snapshot->RootElement = RootElement->Clone();
    snapshot->RootElement->BuildOperationToElementMapping(&snapshot->OperationIdToElement);
    snapshot->RegisteredSchedulingTagFilters = RegisteredSchedulingTagFilters;
    snapshot->Config = Config;
    return snapshot;
}

void TFairShareTree::BuildEssentialPoolsInformation(TFluentMap fluent)
{
    fluent
        .Item("pools").DoMapFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
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

    auto guaranteedResources = Host->GetResourceLimits(Config->NodesFilter) * attributes.GuaranteedResourcesRatio;

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
        .Item("usage_ratio").Value(element->GetLocalResourceUsageRatio())
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
        .Item("usage_ratio").Value(element->GetLocalResourceUsageRatio())
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
    auto current = element;
    while (!current->IsRoot()) {
        if (current->IsExplicit()) {
            tokens.push_back(current->GetId());
        }
        current = current->GetParent();
    }

    std::reverse(tokens.begin(), tokens.end());

    TYPath path = "/" + NYPath::ToYPathLiteral(TreeId);
    for (const auto& token : tokens) {
        path.append('/');
        path.append(NYPath::ToYPathLiteral(token));
    }
    return path;
}

TCompositeSchedulerElementPtr TFairShareTree::GetDefaultParent()
{
    auto defaultPool = FindPool(Config->DefaultParentPool);
    if (defaultPool) {
        return defaultPool;
    } else {
        return RootElement;
    }
}

TCompositeSchedulerElementPtr TFairShareTree::GetPoolOrParent(const TPoolName& poolName)
{
    TCompositeSchedulerElementPtr pool = FindPool(poolName.GetPool());
    if (pool) {
        return pool;
    }
    if (!poolName.GetParentPool()) {
        return GetDefaultParent();
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
            TreeId);
    }
}

void TFairShareTree::ValidateEphemeralPoolLimit(const IOperationStrategyHost* operation, const TPoolName& poolName)
{
    auto pool = FindPool(poolName.GetPool());
    if (pool) {
        return;
    }

    const auto& userName = operation->GetAuthenticatedUser();

    auto it = UserToEphemeralPools.find(userName);
    if (it == UserToEphemeralPools.end()) {
        return;
    }

    if (it->second.size() + 1 > Config->MaxEphemeralPoolsPerUser) {
        THROW_ERROR_EXCEPTION("Limit for number of ephemeral pools %v for user %v in tree %Qv has been reached",
            Config->MaxEphemeralPoolsPerUser,
            userName,
            TreeId);
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

    Host->ValidatePoolPermission(GetPoolPath(pool), operation->GetAuthenticatedUser(), EPermission::Use);
}

void TFairShareTree::ProfileOperationElement(TProfileCollector& collector, TOperationElementPtr element) const
{
    {
        auto poolTag = element->GetParent()->GetProfilingTag();
        auto slotIndexTag = GetSlotIndexProfilingTag(element->GetSlotIndex());

        ProfileSchedulerElement(collector, element, "/operations_by_slot", {poolTag, slotIndexTag, TreeIdProfilingTag});
    }

    auto parent = element->GetParent();
    while (parent != nullptr) {
        auto poolTag = parent->GetProfilingTag();
        auto userNameTag = GetUserNameProfilingTag(element->GetUserName());
        TTagIdList byUserTags = {poolTag, userNameTag, TreeIdProfilingTag};
        auto customTag = element->GetCustomProfilingTag();
        if (customTag) {
            byUserTags.push_back(*customTag);
        }
        ProfileSchedulerElement(collector, element, "/operations_by_user", byUserTags);

        parent = parent->GetParent();
    }
}

void TFairShareTree::ProfileCompositeSchedulerElement(TProfileCollector& collector, TCompositeSchedulerElementPtr element) const
{
    auto tag = element->GetProfilingTag();
    ProfileSchedulerElement(collector, element, "/pools", {tag, TreeIdProfilingTag});

    collector.Add(
        "/running_operation_count",
        element->RunningOperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag});
    collector.Add(
        "/total_operation_count",
        element->OperationCount(),
        EMetricType::Gauge,
        {tag, TreeIdProfilingTag});
}

void TFairShareTree::ProfileSchedulerElement(TProfileCollector& collector, const TSchedulerElementPtr& element, const TString& profilingPrefix, const TTagIdList& tags) const
{
    collector.Add(
        profilingPrefix + "/fair_share_ratio_x100000",
        static_cast<i64>(element->Attributes().FairShareRatio * 1e5),
        EMetricType::Gauge,
        tags);
    collector.Add(
        profilingPrefix + "/usage_ratio_x100000",
        static_cast<i64>(element->GetLocalResourceUsageRatio() * 1e5),
        EMetricType::Gauge,
        tags);
    collector.Add(
        profilingPrefix + "/demand_ratio_x100000",
        static_cast<i64>(element->Attributes().DemandRatio * 1e5),
        EMetricType::Gauge,
        tags);
    collector.Add(
        profilingPrefix + "/guaranteed_resource_ratio_x100000",
        static_cast<i64>(element->Attributes().GuaranteedResourcesRatio * 1e5),
        EMetricType::Gauge,
        tags);

    auto profileResources = [&] (const TString& path, const TJobResources& resources) {
        #define XX(name, Name) collector.Add(path + "/" #name, static_cast<i64>(resources.Get##Name()), EMetricType::Gauge, tags);
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
    };

    profileResources(profilingPrefix + "/resource_usage", element->GetLocalResourceUsage());
    profileResources(profilingPrefix + "/resource_limits", element->ResourceLimits());
    profileResources(profilingPrefix + "/resource_demand", element->ResourceDemand());

    element->GetJobMetrics().Profile(
        collector,
        profilingPrefix + "/metrics",
        tags);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
