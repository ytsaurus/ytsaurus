#include "fair_share_strategy.h"
#include "fair_share_tree_element.h"
#include "public.h"
#include "config.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "fair_share_strategy_operation_controller.h"

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/async_rw_lock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/finally.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NScheduler {

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

namespace {

TTagIdList GetFailReasonProfilingTags(EScheduleJobFailReason reason)
{
    static yhash<EScheduleJobFailReason, TTagId> tagId;

    auto it = tagId.find(reason);
    if (it == tagId.end()) {
        it = tagId.emplace(
            reason,
            TProfileManager::Get()->RegisterTag("reason", FormatEnum(reason))
        ).first;
    }
    return {it->second};
};

TTagId GetSlotIndexProfilingTag(int slotIndex)
{
    static yhash<int, TTagId> slotIndexToTagIdMap;

    auto it = slotIndexToTagIdMap.find(slotIndex);
    if (it == slotIndexToTagIdMap.end()) {
        it = slotIndexToTagIdMap.emplace(
            slotIndex,
            TProfileManager::Get()->RegisterTag("slot_index", ToString(slotIndex))
        ).first;
    }
    return it->second;
};

class TFairShareStrategyOperationState
    : public TIntrinsicRefCounted
{
public:
    using TTreeIdToPoolIdMap = yhash<TString, TString>;

    DEFINE_BYVAL_RO_PROPERTY(IOperationStrategyHost*, Host);
    DEFINE_BYVAL_RO_PROPERTY(TFairShareStrategyOperationControllerPtr, Controller);
    DEFINE_BYVAL_RW_PROPERTY(bool, Active);
    DEFINE_BYREF_RW_PROPERTY(TTreeIdToPoolIdMap, TreeIdToPoolIdMap);

public:
    TFairShareStrategyOperationState(IOperationStrategyHost* host)
        : Host_(host)
        , Controller_(New<TFairShareStrategyOperationController>(host))
        , Active_(false)
    { }

    TString GetPoolIdByTreeId(const TString& treeId) const
    {
        auto it = TreeIdToPoolIdMap_.find(treeId);
        YCHECK(it != TreeIdToPoolIdMap_.end());
        return it->second;
    }
};

using TFairShareStrategyOperationStatePtr = TIntrusivePtr<TFairShareStrategyOperationState>;

struct TOperationRegistrationUnregistrationResult
{
    std::vector<TOperationId> OperationsToActivate;
};

struct TPoolsUpdateResult
{
    TError Error;
    bool Updated;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

//! Thread affinity: any
struct IFairShareTreeSnapshot
    : public TIntrinsicRefCounted
{
    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) = 0;
    virtual void ProcessUpdatedJob(const TUpdatedJob& updatedJob) = 0;
    virtual void ProcessCompletedJob(const TCompletedJob& updatedJob) = 0;
    virtual bool HasOperation(const TOperationId& operationId) const = 0;
    virtual void ApplyJobMetricsDelta(const TOperationId& operationId, const TJobMetrics& jobMetricsDelta) = 0;
    virtual const TSchedulingTagFilter& GetNodesFilter() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareTreeSnapshot);

////////////////////////////////////////////////////////////////////////////////

class TFairShareTree
    : public TIntrinsicRefCounted
{
public:
    TFairShareTree(
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
        RootElement = New<TRootElement>(Host, config, GetPoolProfilingTag(RootPoolName), TreeId);
    }

    IFairShareTreeSnapshotPtr CreateSnapshot()
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        return New<TFairShareTreeSnapshot>(this, RootElementSnapshot);
    }

    TFuture<void> ValidateOperationStart(const IOperationStrategyHost* operation, const TString& poolId)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        return BIND(&TFairShareTree::DoValidateOperationStart, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operation, poolId);
    }

    void ValidateOperationCanBeRegistered(const IOperationStrategyHost* operation, const TString& poolId)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        ValidateOperationCountLimit(operation, poolId);
        ValidateEphemeralPoolLimit(operation, poolId);
    }

    TOperationRegistrationUnregistrationResult RegisterOperation(
        const TFairShareStrategyOperationStatePtr& state,
        const TStrategyOperationSpecPtr& spec,
        const TOperationStrategyRuntimeParamsPtr& runtimeParams)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto operationId = state->GetHost()->GetId();

        auto clonedSpec = CloneYsonSerializable(spec);
        auto optionsIt = spec->FairShareOptionsPerPoolTree.find(TreeId);
        if (optionsIt != spec->FairShareOptionsPerPoolTree.end()) {
            const auto& options = optionsIt->second;
            ReconfigureYsonSerializable(clonedSpec, ConvertToNode(options));
        }

        auto operationElement = New<TOperationElement>(
            Config,
            clonedSpec,
            runtimeParams,
            state->GetController(),
            ControllerConfig,
            Host,
            state->GetHost(),
            TreeId);

        int index = RegisterSchedulingTagFilter(TSchedulingTagFilter(clonedSpec->SchedulingTagFilter));
        operationElement->SetSchedulingTagFilterIndex(index);

        YCHECK(OperationIdToElement.insert(std::make_pair(operationId, operationElement)).second);

        const auto& userName = state->GetHost()->GetAuthenticatedUser();
        auto poolId = state->GetPoolIdByTreeId(TreeId);

        auto pool = FindPool(poolId);
        if (!pool) {
            pool = New<TPool>(
                Host,
                poolId,
                New<TPoolConfig>(),
                /* defaultConfigured */ true,
                Config,
                GetPoolProfilingTag(poolId),
                TreeId);

            pool->SetUserName(userName);
            UserToEphemeralPools[userName].insert(poolId);
            RegisterPool(pool);
        }
        if (!pool->GetParent()) {
            SetPoolDefaultParent(pool);
        }

        pool->IncreaseOperationCount(1);

        pool->AddChild(operationElement, false);
        pool->IncreaseResourceUsage(operationElement->GetResourceUsage());
        operationElement->SetParent(pool.Get());

        AssignOperationSlotIndex(state, poolId);

        LOG_DEBUG("Slot index assigned to operation (SlotIndex: %v, OperationId: %v)",
            state->GetHost()->GetSlotIndex(TreeId),
            operationId);

        TOperationRegistrationUnregistrationResult result;

        auto violatedPool = FindPoolViolatingMaxRunningOperationCount(pool.Get());
        if (violatedPool) {
            LOG_DEBUG("Max running operation count violated (OperationId: %v, Pool: %v, Limit: %v)",
                operationId,
                violatedPool->GetId(),
                violatedPool->GetMaxRunningOperationCount());
            WaitingOperationQueue.push_back(operationId);
        } else {
            AddOperationToPool(operationId);
            result.OperationsToActivate.push_back(operationId);
        }

        return result;
    }

    TOperationRegistrationUnregistrationResult UnregisterOperation(
        const TFairShareStrategyOperationStatePtr& state)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto operationId = state->GetHost()->GetId();
        auto operationElement = GetOperationElement(operationId);
        auto* pool = static_cast<TPool*>(operationElement->GetParent());

        UnregisterSchedulingTagFilter(operationElement->GetSchedulingTagFilterIndex());
        UnassignOperationPoolIndex(state, pool->GetId());

        auto finalResourceUsage = operationElement->Finalize();
        YCHECK(OperationIdToElement.erase(operationId) == 1);
        operationElement->SetAlive(false);
        pool->RemoveChild(operationElement);
        pool->IncreaseResourceUsage(-finalResourceUsage);
        pool->IncreaseOperationCount(-1);

        LOG_INFO("Operation removed from pool (OperationId: %v, Pool: %v)",
            operationId,
            pool->GetId());

        bool isPending = false;
        for (auto it = WaitingOperationQueue.begin(); it != WaitingOperationQueue.end(); ++it) {
            if (*it == operationId) {
                isPending = true;
                WaitingOperationQueue.erase(it);
                break;
            }
        }

        TOperationRegistrationUnregistrationResult result;

        if (!isPending) {
            pool->IncreaseRunningOperationCount(-1);
            TryActivateOperationsFromQueue(&result.OperationsToActivate);
        }

        if (pool->IsEmpty() && pool->IsDefaultConfigured()) {
            UnregisterPool(pool);
        }

        return result;
    }

    TPoolsUpdateResult UpdatePools(const INodePtr& poolsNode)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        if (LastPoolsNodeUpdate && AreNodesEqual(LastPoolsNodeUpdate, poolsNode)) {
            LOG_INFO("Pools are not changed, skipping update");
            return {LastPoolsNodeUpdateError, false};
        }

        LastPoolsNodeUpdate = poolsNode;

        std::vector<TError> errors;

        try {
            // Build the set of potential orphans.
            yhash_set<TString> orphanPoolIds;
            for (const auto& pair : Pools) {
                YCHECK(orphanPoolIds.insert(pair.first).second);
            }

            // Track ids appearing in various branches of the tree.
            yhash<TString, TYPath> poolIdToPath;

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

    void UpdateOperationRuntimeParams(const IOperationStrategyHost* operation, const TOperationStrategyRuntimeParamsPtr& runtimeParams)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = FindOperationElement(operation->GetId());
        if (!element) {
            return;
        }

        element->SetRuntimeParams(runtimeParams);
    }

    void UpdateConfig(const TFairShareStrategyTreeConfigPtr& config)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        Config = config;
        RootElement->UpdateTreeConfig(Config);
    }

    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        ControllerConfig = config;

        for (const auto& pair : OperationIdToElement) {
            const auto& element = pair.second;
            element->UpdateControllerConfig(config);
        }
    }

    void BuildOperationAttributes(const TOperationId& operationId, TFluentMap fluent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = GetOperationElement(operationId);
        auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
        fluent
            .Items(*serializedParams)
            .Item("pool").Value(element->GetParent()->GetId());
    }

    void BuildOperationProgress(const TOperationId& operationId, TFluentMap fluent)
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
            .Do(BIND(&TFairShareTree::BuildElementYson, Unretained(this), element));
    }

    void BuildBriefOperationProgress(const TOperationId& operationId, TFluentMap fluent)
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
            .Item("fair_share_ratio").Value(attributes.FairShareRatio);
    }

    void BuildUserToEphemeralPools(TFluentAny fluent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        fluent
            .DoMapFor(UserToEphemeralPools, [] (TFluentMap fluent, const auto& value) {
                fluent
                    .Item(value.first).Value(value.second);
            });
    }

    TString GetOperationLoggingProgress(const TOperationId& operationId)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = GetOperationElement(operationId);
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        return Format(
            "Scheduling info for tree %v = {Status: %v, DominantResource: %v, Demand: %.6lf, "
            "Usage: %.6lf, FairShare: %.6lf, Satisfaction: %.4lg, AdjustedMinShare: %.6lf, "
            "GuaranteedResourcesRatio: %.6lf, "
            "MaxPossibleUsage: %.6lf,  BestAllocation: %.6lf, "
            "Starving: %v, Weight: %v, "
            "PreemptableRunningJobs: %v, "
            "AggressivelyPreemptableRunningJobs: %v}",
            TreeId,
            element->GetStatus(),
            attributes.DominantResource,
            attributes.DemandRatio,
            element->GetResourceUsageRatio(),
            attributes.FairShareRatio,
            dynamicAttributes.SatisfactionRatio,
            attributes.AdjustedMinShareRatio,
            attributes.GuaranteedResourcesRatio,
            attributes.MaxPossibleUsageRatio,
            attributes.BestAllocationRatio,
            element->GetStarving(),
            element->GetWeight(),
            element->GetPreemptableJobCount(),
            element->GetAggressivelyPreemptableJobCount());
    }

    void BuildBriefSpec(const TOperationId& operationId, TFluentMap fluent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = GetOperationElement(operationId);
        fluent
            .Item("pool").Value(element->GetParent()->GetId());
    }

    // NB: This function is public for testing purposes.
    TError OnFairShareUpdateAt(TInstant now)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        TError error;

        // Run periodic update.
        PROFILE_AGGREGATED_TIMING(FairShareUpdateTimeCounter) {
            // The root element gets the whole cluster.
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

    void ProfileFairShare() const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        for (const auto& pair : Pools) {
            ProfileCompositeSchedulerElement(pair.second);
        }
        ProfileCompositeSchedulerElement(RootElement);
        if (Config->EnableOperationsProfiling) {
            for (const auto& pair : OperationIdToElement) {
                ProfileOperationElement(pair.second);
            }
        }
    }

    // NB: This function is public for testing purposes.
    void OnFairShareLoggingAt(TInstant now)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        PROFILE_AGGREGATED_TIMING(FairShareLogTimeCounter) {
            // Log pools information.
            Host->LogEventFluently(ELogEventType::FairShareInfo, now)
                .Item("tree_id").Value(TreeId)
                .Do(BIND(&TFairShareTree::BuildFairShareInfo, Unretained(this)));

            for (const auto& pair : OperationIdToElement) {
                const auto& operationId = pair.first;
                LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                    GetOperationLoggingProgress(operationId),
                    operationId);
            }
        }
    }

    // NB: This function is public for testing purposes.
    void OnFairShareEssentialLoggingAt(TInstant now)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        PROFILE_TIMING ("/fair_share_log_time") {
            // Log pools information.
            Host->LogEventFluently(ELogEventType::FairShareInfo, now)
                .Item("tree_id").Value(TreeId)
                .Do(BIND(&TFairShareTree::BuildEssentialFairShareInfo, Unretained(this)));

            for (const auto& pair : OperationIdToElement) {
                const auto& operationId = pair.first;
                LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                    GetOperationLoggingProgress(operationId),
                    operationId);
            }
        }
    }

    void RegisterJobs(const TOperationId& operationId, const std::vector<TJobPtr>& jobs)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = FindOperationElement(operationId);
        for (const auto& job : jobs) {
            element->OnJobStarted(job->GetId(), job->ResourceUsage());
        }
    }

    void BuildPoolsInformation(TFluentMap fluent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        fluent
            .Item("pools").DoMapFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                const auto& id = pair.first;
                const auto& pool = pair.second;
                const auto& config = pool->GetConfig();
                fluent
                    .Item(id).BeginMap()
                        .Item("mode").Value(config->Mode)
                        .Item("running_operation_count").Value(pool->RunningOperationCount())
                        .Item("operation_count").Value(pool->OperationCount())
                        .Item("max_running_operation_count").Value(pool->GetMaxRunningOperationCount())
                        .Item("max_operation_count").Value(pool->GetMaxOperationCount())
                        .Item("aggressive_starvation_enabled").Value(pool->IsAggressiveStarvationEnabled())
                        .Item("forbid_immediate_operations").Value(pool->AreImmediateOperationsFobidden())
                        .DoIf(config->Mode == ESchedulingMode::Fifo, [&] (TFluentMap fluent) {
                            fluent
                                .Item("fifo_sort_parameters").Value(config->FifoSortParameters);
                        })
                        .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                            fluent
                                .Item("parent").Value(pool->GetParent()->GetId());
                        })
                        .Do(BIND(&TFairShareTree::BuildElementYson, Unretained(this), pool))
                    .EndMap();
            });
    }

    void BuildStaticPoolsInformation(TFluentAny fluent)
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

    void BuildFairShareInfo(TFluentMap fluent)
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

    void BuildEssentialFairShareInfo(TFluentMap fluent)
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

    void ResetState()
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        LastPoolsNodeUpdate.Reset();
        LastPoolsNodeUpdateError = TError();
    }

    const TSchedulingTagFilter& GetNodesFilter() const
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        return Config->NodesFilter;
    }

private:
    TFairShareStrategyTreeConfigPtr Config;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig;
    ISchedulerStrategyHost* const Host;

    std::vector<IInvokerPtr> FeasibleInvokers;

    INodePtr LastPoolsNodeUpdate;
    TError LastPoolsNodeUpdateError;

    const TString TreeId;
    const TTagId TreeIdProfilingTag;

    const NLogging::TLogger Logger;

    using TPoolMap = yhash<TString, TPoolPtr>;
    TPoolMap Pools;

    yhash<TString, NProfiling::TTagId> PoolIdToProfilingTagId;

    yhash<TString, yhash_set<TString>> UserToEphemeralPools;

    yhash<TString, yhash_set<int>> PoolToSpareSlotIndices;
    yhash<TString, int> PoolToMinUnusedSlotIndex;

    using TOperationElementPtrByIdMap = yhash<TOperationId, TOperationElementPtr>;
    TOperationElementPtrByIdMap OperationIdToElement;

    std::list<TOperationId> WaitingOperationQueue;

    TReaderWriterSpinLock NodeIdToLastPreemptiveSchedulingTimeLock;
    yhash<TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime;

    std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilters;
    std::vector<int> FreeSchedulingTagFilterIndexes;
    struct TSchedulingTagFilterEntry
    {
        int Index;
        int Count;
    };
    yhash<TSchedulingTagFilter, TSchedulingTagFilterEntry> SchedulingTagFilterToIndexAndCount;

    TRootElementPtr RootElement;

    struct TRootElementSnapshot
        : public TIntrinsicRefCounted
    {
        TRootElementPtr RootElement;
        TOperationElementByIdMap OperationIdToElement;
        TFairShareStrategyTreeConfigPtr Config;

        TOperationElement* FindOperationElement(const TOperationId& operationId) const
        {
            auto it = OperationIdToElement.find(operationId);
            return it != OperationIdToElement.end() ? it->second : nullptr;
        }
    };

    typedef TIntrusivePtr<TRootElementSnapshot> TRootElementSnapshotPtr;
    TRootElementSnapshotPtr RootElementSnapshot;

    class TFairShareTreeSnapshot
        : public IFairShareTreeSnapshot
    {
    public:
        TFairShareTreeSnapshot(TFairShareTreePtr tree, TRootElementSnapshotPtr rootElementSnapshot)
            : Tree(std::move(tree))
            , RootElementSnapshot(std::move(rootElementSnapshot))
            , NodesFilter(Tree->GetNodesFilter())
        { }

        virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
        {
            return BIND(&TFairShareTree::DoScheduleJobs,
                Tree,
                schedulingContext,
                RootElementSnapshot)
                .AsyncVia(GetCurrentInvoker())
                .Run();
        }

        virtual void ProcessUpdatedJob(const TUpdatedJob& updatedJob)
        {
            auto* operationElement = RootElementSnapshot->FindOperationElement(updatedJob.OperationId);
            if (operationElement) {
                operationElement->IncreaseJobResourceUsage(updatedJob.JobId, updatedJob.Delta);
            }
        }

        virtual void ProcessCompletedJob(const TCompletedJob& completedJob) override
        {
            auto* operationElement = RootElementSnapshot->FindOperationElement(completedJob.OperationId);
            if (operationElement) {
                operationElement->OnJobFinished(completedJob.JobId);
            }
        }

        virtual void ApplyJobMetricsDelta(const TOperationId& operationId, const TJobMetrics& jobMetricsDelta) override
        {
            auto* operationElement = RootElementSnapshot->FindOperationElement(operationId);
            if (operationElement) {
                operationElement->ApplyJobMetricsDelta(jobMetricsDelta);
            }
        }

        virtual bool HasOperation(const TOperationId& operationId) const override
        {
            auto* operationElement = RootElementSnapshot->FindOperationElement(operationId);
            return operationElement != nullptr;
        }

        virtual const TSchedulingTagFilter& GetNodesFilter() const override
        {
            return NodesFilter;
        }

    private:
        const TIntrusivePtr<TFairShareTree> Tree;
        const TRootElementSnapshotPtr RootElementSnapshot;
        const TSchedulingTagFilter NodesFilter;
    };

    TDynamicAttributesList GlobalDynamicAttributes_;

    struct TProfilingCounters
    {
        TProfilingCounters(const TString& prefix, const TTagId& treeIdProfilingTag)
            : PrescheduleJobTimeCounter(prefix + "/preschedule_job_time", {treeIdProfilingTag})
            , TotalControllerScheduleJobTimeCounter(prefix + "/controller_schedule_job_time/total", {treeIdProfilingTag})
            , ExecControllerScheduleJobTimeCounter(prefix + "/controller_schedule_job_time/exec", {treeIdProfilingTag})
            , StrategyScheduleJobTimeCounter(prefix + "/strategy_schedule_job_time", {treeIdProfilingTag})
            , ScheduleJobCallCounter(prefix + "/schedule_job_count", {treeIdProfilingTag})
        {
            for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues())
            {
                auto tags = GetFailReasonProfilingTags(reason);
                tags.push_back(treeIdProfilingTag);

                ControllerScheduleJobFailCounter[reason] = TSimpleCounter(
                    prefix + "/controller_schedule_job_fail",
                    tags);
            }
        }

        TAggregateCounter PrescheduleJobTimeCounter;
        TAggregateCounter TotalControllerScheduleJobTimeCounter;
        TAggregateCounter ExecControllerScheduleJobTimeCounter;
        TAggregateCounter StrategyScheduleJobTimeCounter;
        TSimpleCounter ScheduleJobCallCounter;

        TEnumIndexedVector<TSimpleCounter, EScheduleJobFailReason> ControllerScheduleJobFailCounter;
    };

    TProfilingCounters NonPreemptiveProfilingCounters;
    TProfilingCounters PreemptiveProfilingCounters;

    TAggregateCounter FairShareUpdateTimeCounter;
    TAggregateCounter FairShareLogTimeCounter;
    TAggregateCounter AnalyzePreemptableJobsTimeCounter;

    TCpuInstant LastSchedulingInformationLoggedTime_ = 0;

    TDynamicAttributes GetGlobalDynamicAttributes(const TSchedulerElementPtr& element) const
    {
        int index = element->GetTreeIndex();
        if (index == UnassignedTreeIndex) {
            return TDynamicAttributes();
        } else {
            return GlobalDynamicAttributes_[index];
        }
    }

    void DoScheduleJobsWithoutPreemption(
        const TRootElementSnapshotPtr& rootElementSnapshot,
        TFairShareContext& context,
        const std::function<void(TProfilingCounters&, int, TDuration)> profileTimings,
        const std::function<void(const TStringBuf&)> logAndCleanSchedulingStatistics)
    {
        auto& rootElement = rootElementSnapshot->RootElement;

        {
            LOG_TRACE("Scheduling new jobs");

            bool prescheduleExecuted = false;
            TDuration prescheduleDuration;

            TWallTimer scheduleTimer;
            while (context.SchedulingContext->CanStartMoreJobs()) {
                if (!prescheduleExecuted) {
                    TWallTimer prescheduleTimer;
                    context.Initialize(rootElement->GetTreeSize(), RegisteredSchedulingTagFilters);
                    rootElement->PrescheduleJob(context, /*starvingOnly*/ false, /*aggressiveStarvationEnabled*/ false);
                    prescheduleDuration = prescheduleTimer.GetElapsedTime();
                    Profiler.Update(NonPreemptiveProfilingCounters.PrescheduleJobTimeCounter, DurationToCpuDuration(prescheduleDuration));
                    prescheduleExecuted = true;
                    context.PrescheduledCalled = true;
                }
                ++context.NonPreemptiveScheduleJobAttempts;
                if (!rootElement->ScheduleJob(context)) {
                    break;
                }
            }
            profileTimings(
                NonPreemptiveProfilingCounters,
                context.NonPreemptiveScheduleJobAttempts,
                scheduleTimer.GetElapsedTime() - prescheduleDuration - context.TotalScheduleJobDuration);

            if (context.NonPreemptiveScheduleJobAttempts > 0) {
                logAndCleanSchedulingStatistics(STRINGBUF("Non preemptive"));
            }
        }
    }

    void DoScheduleJobsWithPreemption(
        const TRootElementSnapshotPtr& rootElementSnapshot,
        TFairShareContext& context,
        const std::function<void(TProfilingCounters&, int, TDuration)>& profileTimings,
        const std::function<void(const TStringBuf&)>& logAndCleanSchedulingStatistics)
    {
        auto& rootElement = rootElementSnapshot->RootElement;
        auto& config = rootElementSnapshot->Config;

        if (!context.Initialized) {
            context.Initialize(rootElement->GetTreeSize(), RegisteredSchedulingTagFilters);
        }

        if (!context.PrescheduledCalled) {
            context.HasAggressivelyStarvingNodes = rootElement->HasAggressivelyStarvingNodes(context, false);
        }

        // Compute discount to node usage.
        LOG_TRACE("Looking for preemptable jobs");
        yhash_set<TCompositeSchedulerElementPtr> discountedPools;
        std::vector<TJobPtr> preemptableJobs;
        PROFILE_AGGREGATED_TIMING(AnalyzePreemptableJobsTimeCounter) {
            for (const auto& job : context.SchedulingContext->RunningJobs()) {
                auto* operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());
                if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                    LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                        job->GetId(),
                        job->GetOperationId());
                    continue;
                }

                if (!operationElement->IsPreemptionAllowed(context)) {
                    continue;
                }

                if (IsJobPreemptable(job, operationElement, context.HasAggressivelyStarvingNodes, config)) {
                    auto* parent = operationElement->GetParent();
                    while (parent) {
                        discountedPools.insert(parent);
                        context.DynamicAttributes(parent).ResourceUsageDiscount += job->ResourceUsage();
                        parent = parent->GetParent();
                    }
                    context.SchedulingContext->ResourceUsageDiscount() += job->ResourceUsage();
                    preemptableJobs.push_back(job);
                }
            }
        }

        context.ResourceUsageDiscount = context.SchedulingContext->ResourceUsageDiscount();

        int startedBeforePreemption = context.SchedulingContext->StartedJobs().size();

        // NB: Schedule at most one job with preemption.
        TJobPtr jobStartedUsingPreemption;
        {
            LOG_TRACE("Scheduling new jobs with preemption");

            // Clean data from previous profiling.
            context.TotalScheduleJobDuration = TDuration::Zero();
            context.ExecScheduleJobDuration = TDuration::Zero();
            std::fill(context.FailedScheduleJob.begin(), context.FailedScheduleJob.end(), 0);

            bool prescheduleExecuted = false;
            TDuration prescheduleDuration;

            TWallTimer timer;
            while (context.SchedulingContext->CanStartMoreJobs()) {
                if (!prescheduleExecuted) {
                    TWallTimer prescheduleTimer;
                    rootElement->PrescheduleJob(context, /*starvingOnly*/ true, /*aggressiveStarvationEnabled*/ false);
                    prescheduleDuration = prescheduleTimer.GetElapsedTime();
                    Profiler.Update(PreemptiveProfilingCounters.PrescheduleJobTimeCounter, DurationToCpuDuration(prescheduleDuration));
                    prescheduleExecuted = true;
                }

                ++context.PreemptiveScheduleJobAttempts;
                if (!rootElement->ScheduleJob(context)) {
                    break;
                }
                if (context.SchedulingContext->StartedJobs().size() > startedBeforePreemption) {
                    jobStartedUsingPreemption = context.SchedulingContext->StartedJobs().back();
                    break;
                }
            }
            profileTimings(
                PreemptiveProfilingCounters,
                context.PreemptiveScheduleJobAttempts,
                timer.GetElapsedTime() - prescheduleDuration - context.TotalScheduleJobDuration);
            if (context.PreemptiveScheduleJobAttempts > 0) {
                logAndCleanSchedulingStatistics(STRINGBUF("Preemptive"));
            }
        }

        int startedAfterPreemption = context.SchedulingContext->StartedJobs().size();

        context.ScheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

        // Reset discounts.
        context.SchedulingContext->ResourceUsageDiscount() = ZeroJobResources();
        for (const auto& pool : discountedPools) {
            context.DynamicAttributes(pool.Get()).ResourceUsageDiscount = ZeroJobResources();
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
                if (!Dominates(parent->ResourceLimits(), parent->GetResourceUsage())) {
                    return parent;
                }
                parent = parent->GetParent();
            }
            return nullptr;
        };

        auto findPoolWithViolatedLimits = [&] () -> TCompositeSchedulerElement* {
            for (const auto& job : context.SchedulingContext->StartedJobs()) {
                auto violatedPool = findPoolWithViolatedLimitsForJob(job);
                if (violatedPool) {
                    return violatedPool;
                }
            }
            return nullptr;
        };

        bool nodeLimitsViolated = true;
        bool poolsLimitsViolated = true;

        context.PreemptableJobCount = preemptableJobs.size();

        for (const auto& job : preemptableJobs) {
            auto* operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());
            if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                LOG_DEBUG("Dangling preemptable job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            // Update flags only if violation is not resolved yet to avoid costly computations.
            if (nodeLimitsViolated) {
                nodeLimitsViolated = !Dominates(context.SchedulingContext->ResourceLimits(), context.SchedulingContext->ResourceUsage());
            }
            if (!nodeLimitsViolated && poolsLimitsViolated) {
                poolsLimitsViolated = findPoolWithViolatedLimits() == nullptr;
            }

            if (!nodeLimitsViolated && !poolsLimitsViolated) {
                break;
            }

            if (nodeLimitsViolated) {
                if (jobStartedUsingPreemption) {
                    job->SetPreemptionReason(Format("Preempted to start job %v of operation %v",
                        jobStartedUsingPreemption->GetId(),
                        jobStartedUsingPreemption->GetOperationId()));
                } else {
                    job->SetPreemptionReason(Format("Node resource limits violated"));
                }
                PreemptJob(job, operationElement, context);
            }
            if (poolsLimitsViolated) {
                auto violatedPool = findPoolWithViolatedLimitsForJob(job);
                if (violatedPool) {
                    job->SetPreemptionReason(Format("Preempted due to violation of limits on pool %v",
                        violatedPool->GetId()));
                    PreemptJob(job, operationElement, context);
                }
            }
        }
    }

    void DoScheduleJobs(
        const ISchedulingContextPtr& schedulingContext,
        const TRootElementSnapshotPtr& rootElementSnapshot)
    {
        TFairShareContext context(schedulingContext);

        auto profileTimings = [&] (
            TProfilingCounters& counters,
            int scheduleJobCount,
            TDuration scheduleJobDurationWithoutControllers)
        {
            Profiler.Update(
                counters.StrategyScheduleJobTimeCounter,
                scheduleJobDurationWithoutControllers.MicroSeconds());

            Profiler.Update(
                counters.TotalControllerScheduleJobTimeCounter,
                context.TotalScheduleJobDuration.MicroSeconds());

            Profiler.Update(
                counters.ExecControllerScheduleJobTimeCounter,
                context.ExecScheduleJobDuration.MicroSeconds());

            Profiler.Increment(counters.ScheduleJobCallCounter, scheduleJobCount);

            for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
                Profiler.Increment(
                    counters.ControllerScheduleJobFailCounter[reason],
                    context.FailedScheduleJob[reason]);
            }
        };

        bool enableSchedulingInfoLogging = false;
        auto now = GetCpuInstant();
        if (LastSchedulingInformationLoggedTime_ + DurationToCpuDuration(Config->HeartbeatTreeSchedulingInfoLogBackoff) < now) {
            enableSchedulingInfoLogging = true;
            LastSchedulingInformationLoggedTime_ = now;
        }

        auto logAndCleanSchedulingStatistics = [&] (const TStringBuf& stageName) {
            if (!enableSchedulingInfoLogging) {
                return;
            }
            LOG_DEBUG("%v scheduling statistics (ActiveTreeSize: %v, ActiveOperationCount: %v, DeactivationReasons: %v, CanStartMoreJobs: %v)",
                stageName,
                context.ActiveTreeSize,
                context.ActiveOperationCount,
                context.DeactivationReasons,
                schedulingContext->CanStartMoreJobs());
            context.ActiveTreeSize = 0;
            context.ActiveOperationCount = 0;
            std::fill(context.DeactivationReasons.begin(), context.DeactivationReasons.end(), 0);
        };

        DoScheduleJobsWithoutPreemption(rootElementSnapshot, context, profileTimings, logAndCleanSchedulingStatistics);

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
                } else if (it->second + DurationToCpuDuration(Config->PreemptiveSchedulingBackoff) <= now) {
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
            DoScheduleJobsWithPreemption(rootElementSnapshot, context, profileTimings, logAndCleanSchedulingStatistics);
        } else {
            LOG_DEBUG("Skip preemptive scheduling");
        }

        LOG_DEBUG("Heartbeat info (StartedJobs: %v, PreemptedJobs: %v, "
            "JobsScheduledDuringPreemption: %v, PreemptableJobs: %v, PreemptableResources: %v, "
            "ControllerScheduleJobCount: %v, NonPreemptiveScheduleJobAttempts: %v, PreemptiveScheduleJobAttempts: %v, HasAggressivelyStarvingNodes: %v, Address: %v)",
            schedulingContext->StartedJobs().size(),
            schedulingContext->PreemptedJobs().size(),
            context.ScheduledDuringPreemption,
            context.PreemptableJobCount,
            FormatResources(context.ResourceUsageDiscount),
            context.ControllerScheduleJobCount,
            context.NonPreemptiveScheduleJobAttempts,
            context.PreemptiveScheduleJobAttempts,
            context.HasAggressivelyStarvingNodes,
            schedulingContext->GetNodeDescriptor().Address);
    }

    bool IsJobPreemptable(
        const TJobPtr& job,
        const TOperationElementPtr& element,
        bool aggressivePreemptionEnabled,
        const TFairShareStrategyTreeConfigPtr& config) const
    {
        int jobCount = element->GetRunningJobCount();
        if (jobCount <= config->MaxUnpreemptableRunningJobCount) {
            return false;
        }

        aggressivePreemptionEnabled = aggressivePreemptionEnabled && element->IsAggressiveStarvationPreemptionAllowed();

        double usageRatio = element->GetResourceUsageRatio();
        const auto& attributes = element->Attributes();
        auto threshold = aggressivePreemptionEnabled
            ? config->AggressivePreemptionSatisfactionThreshold
            : config->PreemptionSatisfactionThreshold;
        if (usageRatio < attributes.FairShareRatio * threshold) {
            return false;
        }

        if (!element->IsJobPreemptable(job->GetId(), aggressivePreemptionEnabled)) {
            return false;
        }

        return true;
    }

    void PreemptJob(
        const TJobPtr& job,
        const TOperationElementPtr& operationElement,
        TFairShareContext& context) const
    {
        context.SchedulingContext->ResourceUsage() -= job->ResourceUsage();
        operationElement->IncreaseJobResourceUsage(job->GetId(), -job->ResourceUsage());
        job->ResourceUsage() = ZeroJobResources();

        context.SchedulingContext->PreemptJob(job);
    }

    TCompositeSchedulerElement* FindPoolViolatingMaxRunningOperationCount(TCompositeSchedulerElement* pool)
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

    TCompositeSchedulerElementPtr FindPoolWithViolatedOperationCountLimit(const TCompositeSchedulerElementPtr& element)
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

    void AddOperationToPool(const TOperationId& operationId)
    {
        TForbidContextSwitchGuard contextSwitchGuard;

        const auto& operationElement = GetOperationElement(operationId);
        auto* parent = operationElement->GetParent();
        parent->EnableChild(operationElement);
        parent->IncreaseRunningOperationCount(1);

        LOG_INFO("Operation added to pool (OperationId: %v, Pool: %v)",
            operationId,
            parent->GetId());
    }

    void DoRegisterPool(const TPoolPtr& pool)
    {
        int index = RegisterSchedulingTagFilter(pool->GetSchedulingTagFilter());
        pool->SetSchedulingTagFilterIndex(index);
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        YCHECK(PoolToMinUnusedSlotIndex.insert(std::make_pair(pool->GetId(), 0)).second);
    }

    void RegisterPool(const TPoolPtr& pool)
    {
        DoRegisterPool(pool);

        LOG_INFO("Pool registered (Pool: %v)", pool->GetId());
    }

    void RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        DoRegisterPool(pool);

        pool->SetParent(parent.Get());
        parent->AddChild(pool);

        LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void ReconfigurePool(const TPoolPtr& pool, const TPoolConfigPtr& config)
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

    void UnregisterPool(const TPoolPtr& pool)
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

        LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    bool TryOccupyPoolSlotIndex(const TString& poolName, int slotIndex)
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

    void AssignOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName)
    {
        auto it = PoolToSpareSlotIndices.find(poolName);
        auto slotIndex = state->GetHost()->FindSlotIndex(TreeId);

        if (slotIndex) {
            // Revive case
            if (TryOccupyPoolSlotIndex(poolName, *slotIndex)) {
                return;
            } else {
                auto error = TError("Failed to assign slot index to operation during revive")
                    << TErrorAttribute("operation_id", state->GetHost()->GetId())
                    << TErrorAttribute("slot_index", *slotIndex);
                LOG_ERROR(error);
            }
        }

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
    }

    void UnassignOperationPoolIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName)
    {
        auto slotIndex = state->GetHost()->FindSlotIndex(TreeId);
        YCHECK(slotIndex);

        auto it = PoolToSpareSlotIndices.find(poolName);
        if (it == PoolToSpareSlotIndices.end()) {
            YCHECK(PoolToSpareSlotIndices.insert(std::make_pair(poolName, yhash_set<int>{*slotIndex})).second);
        } else {
            it->second.insert(*slotIndex);
        }
    }

    void TryActivateOperationsFromQueue(std::vector<TOperationId>* operationsToActivate)
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

    void BuildEssentialOperationProgress(const TOperationId& operationId, TFluentMap fluent)
    {
        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        fluent
            .Do(BIND(&TFairShareTree::BuildEssentialOperationElementYson, Unretained(this), element));
    }

    int RegisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
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

    void UnregisterSchedulingTagFilter(int index)
    {
        if (index == EmptySchedulingTagFilterIndex) {
            return;
        }
        UnregisterSchedulingTagFilter(RegisteredSchedulingTagFilters[index]);
    }

    void UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
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

    void SetPoolParent(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        if (pool->GetParent() == parent) {
            return;
        }

        auto* oldParent = pool->GetParent();
        if (oldParent) {
            oldParent->IncreaseResourceUsage(-pool->GetResourceUsage());
            oldParent->IncreaseOperationCount(-pool->OperationCount());
            oldParent->IncreaseRunningOperationCount(-pool->RunningOperationCount());
            oldParent->RemoveChild(pool);
        }

        pool->SetParent(parent.Get());
        if (parent) {
            parent->AddChild(pool);
            parent->IncreaseResourceUsage(pool->GetResourceUsage());
            parent->IncreaseOperationCount(pool->OperationCount());
            parent->IncreaseRunningOperationCount(pool->RunningOperationCount());

            LOG_INFO("Parent pool set (Pool: %v, Parent: %v)",
                pool->GetId(),
                parent->GetId());
        }
    }

    void SetPoolDefaultParent(const TPoolPtr& pool)
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

    TPoolPtr FindPool(const TString& id)
    {
        auto it = Pools.find(id);
        return it == Pools.end() ? nullptr : it->second;
    }

    TPoolPtr GetPool(const TString& id)
    {
        auto pool = FindPool(id);
        YCHECK(pool);
        return pool;
    }

    NProfiling::TTagId GetPoolProfilingTag(const TString& id)
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

    TOperationElementPtr FindOperationElement(const TOperationId& operationId)
    {
        auto it = OperationIdToElement.find(operationId);
        return it == OperationIdToElement.end() ? nullptr : it->second;
    }

    TOperationElementPtr GetOperationElement(const TOperationId& operationId)
    {
        auto element = FindOperationElement(operationId);
        YCHECK(element);
        return element;
    }

    TRootElementSnapshotPtr CreateRootElementSnapshot()
    {
        auto snapshot = New<TRootElementSnapshot>();
        snapshot->RootElement = RootElement->Clone();
        snapshot->RootElement->BuildOperationToElementMapping(&snapshot->OperationIdToElement);
        snapshot->Config = Config;
        return snapshot;
    }

    void BuildEssentialPoolsInformation(TFluentMap fluent)
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

    void BuildElementYson(const TSchedulerElementPtr& element, TFluentMap fluent)
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
            .Item("resource_usage").Value(element->GetResourceUsage())
            .Item("resource_limits").Value(element->ResourceLimits())
            .Item("dominant_resource").Value(attributes.DominantResource)
            .Item("weight").Value(element->GetWeight())
            .Item("min_share_ratio").Value(element->GetMinShareRatio())
            .Item("max_share_ratio").Value(element->GetMaxShareRatio())
            .Item("min_share_resources").Value(element->GetMinShareResources())
            .Item("adjusted_min_share_ratio").Value(attributes.AdjustedMinShareRatio)
            .Item("guaranteed_resources_ratio").Value(attributes.GuaranteedResourcesRatio)
            .Item("guaranteed_resources").Value(guaranteedResources)
            .Item("max_possible_usage_ratio").Value(attributes.MaxPossibleUsageRatio)
            .Item("usage_ratio").Value(element->GetResourceUsageRatio())
            .Item("demand_ratio").Value(attributes.DemandRatio)
            .Item("fair_share_ratio").Value(attributes.FairShareRatio)
            .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio)
            .Item("best_allocation_ratio").Value(attributes.BestAllocationRatio);
    }

    void BuildEssentialElementYson(const TSchedulerElementPtr& element, TFluentMap fluent, bool shouldPrintResourceUsage)
    {
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        fluent
            .Item("usage_ratio").Value(element->GetResourceUsageRatio())
            .Item("demand_ratio").Value(attributes.DemandRatio)
            .Item("fair_share_ratio").Value(attributes.FairShareRatio)
            .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio)
            .DoIf(shouldPrintResourceUsage, [&] (TFluentMap fluent) {
                fluent
                    .Item("resource_usage").Value(element->GetResourceUsage());
            });
    }

    void BuildEssentialPoolElementYson(const TSchedulerElementPtr& element, TFluentMap fluent)
    {
        BuildEssentialElementYson(element, fluent, false);
    }

    void BuildEssentialOperationElementYson(const TSchedulerElementPtr& element, TFluentMap fluent)
    {
        BuildEssentialElementYson(element, fluent, true);
    }

    TYPath GetPoolPath(const TCompositeSchedulerElementPtr& element)
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

    TCompositeSchedulerElementPtr GetDefaultParent()
    {
        auto defaultPool = FindPool(Config->DefaultParentPool);
        if (defaultPool) {
            return defaultPool;
        } else {
            return RootElement;
        }
    }

    void ValidateOperationCountLimit(const IOperationStrategyHost* operation, const TString& poolId)
    {
        TCompositeSchedulerElementPtr parentElement = FindPool(poolId);
        if (!parentElement) {
            parentElement = GetDefaultParent();
        }

        auto poolWithViolatedLimit = FindPoolWithViolatedOperationCountLimit(parentElement);
        if (poolWithViolatedLimit) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::TooManyOperations,
                "Limit for the number of concurrent operations %v for pool %Qv in tree %Qv has been reached",
                poolWithViolatedLimit->GetMaxOperationCount(),
                poolWithViolatedLimit->GetId(),
                TreeId);
        }
    }

    void ValidateEphemeralPoolLimit(const IOperationStrategyHost* operation, const TString& poolId)
    {
        auto pool = FindPool(poolId);
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

    void DoValidateOperationStart(const IOperationStrategyHost* operation, const TString& poolId)
    {
        ValidateOperationCountLimit(operation, poolId);
        ValidateEphemeralPoolLimit(operation, poolId);

        TCompositeSchedulerElementPtr immediateParentPool = FindPool(poolId);
        // NB: Check is not performed if operation is started in default or unknown pool.
        if (immediateParentPool && immediateParentPool->AreImmediateOperationsFobidden()) {
            THROW_ERROR_EXCEPTION(
                "Starting operations immediately in pool %Qv is forbidden",
                immediateParentPool->GetId());
        }

        if (!immediateParentPool) {
            immediateParentPool = GetDefaultParent();
        }

        auto poolPath = GetPoolPath(immediateParentPool);
        const auto& user = operation->GetAuthenticatedUser();

        Host->ValidatePoolPermission(poolPath, user, EPermission::Use);
    }

    void ProfileOperationElement(TOperationElementPtr element) const
    {
        auto poolTag = element->GetParent()->GetProfilingTag();
        auto slotIndexTag = GetSlotIndexProfilingTag(element->GetSlotIndex());

        ProfileSchedulerElement(element, "/operations", {poolTag, slotIndexTag, TreeIdProfilingTag});
    }

    void ProfileCompositeSchedulerElement(TCompositeSchedulerElementPtr element) const
    {
        auto tag = element->GetProfilingTag();
        ProfileSchedulerElement(element, "/pools", {tag, TreeIdProfilingTag});

        Profiler.Enqueue(
            "/running_operation_count",
            element->RunningOperationCount(),
            EMetricType::Gauge,
            {tag});
        Profiler.Enqueue(
            "/total_operation_count",
            element->OperationCount(),
            EMetricType::Gauge,
            {tag});
    }

    void ProfileSchedulerElement(TSchedulerElementPtr element, const TString& profilingPrefix, const TTagIdList& tags) const
    {
        Profiler.Enqueue(
            profilingPrefix + "/fair_share_ratio_x100000",
            static_cast<i64>(element->Attributes().FairShareRatio * 1e5),
            EMetricType::Gauge,
            tags);
        Profiler.Enqueue(
            profilingPrefix + "/usage_ratio_x100000",
            static_cast<i64>(element->GetResourceUsageRatio() * 1e5),
            EMetricType::Gauge,
            tags);
        Profiler.Enqueue(
            profilingPrefix + "/demand_ratio_x100000",
            static_cast<i64>(element->Attributes().DemandRatio * 1e5),
            EMetricType::Gauge,
            tags);
        Profiler.Enqueue(
            profilingPrefix + "/guaranteed_resource_ratio_x100000",
            static_cast<i64>(element->Attributes().GuaranteedResourcesRatio * 1e5),
            EMetricType::Gauge,
            tags);

        ProfileResources(
            Profiler,
            element->GetResourceUsage(),
            profilingPrefix + "/resource_usage",
            tags);
        ProfileResources(
            Profiler,
            element->ResourceLimits(),
            profilingPrefix + "/resource_limits",
            tags);
        ProfileResources(
            Profiler,
            element->ResourceDemand(),
            profilingPrefix + "/resource_demand",
            tags);

        element->GetJobMetrics().SendToProfiler(
            Profiler,
            profilingPrefix + "/metrics",
            tags);
    }
};

DEFINE_REFCOUNTED_TYPE(TFairShareTree)

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategy
    : public ISchedulerStrategy
{
public:
    TFairShareStrategy(
        TFairShareStrategyConfigPtr config,
        ISchedulerStrategyHost* host,
        const std::vector<IInvokerPtr>& feasibleInvokers)
        : Config(config)
        , Host(host)
        , FeasibleInvokers(feasibleInvokers)
        , Logger(SchedulerLogger)
        , LastProfilingTime_(TInstant::Zero())
    {
        FairShareUpdateExecutor_ = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TFairShareStrategy::OnFairShareUpdate, MakeWeak(this)),
            Config->FairShareUpdatePeriod);

        FairShareLoggingExecutor_ = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TFairShareStrategy::OnFairShareLogging, MakeWeak(this)),
            Config->FairShareLogPeriod);

        MinNeededJobResourcesUpdateExecutor_ = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TFairShareStrategy::OnMinNeededJobResourcesUpdate, MakeWeak(this)),
            Config->MinNeededResourcesUpdatePeriod);
    }

    virtual void OnMasterConnected() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareLoggingExecutor_->Start();
        FairShareUpdateExecutor_->Start();
        MinNeededJobResourcesUpdateExecutor_->Start();
    }

    virtual void OnMasterDisconnected() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareLoggingExecutor_->Stop();
        FairShareUpdateExecutor_->Stop();
        MinNeededJobResourcesUpdateExecutor_->Stop();

        {
            TWriterGuard guard(OperationIdToOperationStateLock_);
            OperationIdToOperationState_.clear();
        }

        IdToTree_.clear();

        DefaultTreeId_.Reset();

        {
            TWriterGuard guard(TreeIdToSnapshotLock_);
            TreeIdToSnapshot_.clear();
        }
    }

    void OnFairShareUpdate()
    {
        OnFairShareUpdateAt(TInstant::Now());
    }

    void OnMinNeededJobResourcesUpdate() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        LOG_INFO("Starting min needed job resources update");

        for (const auto& pair : OperationIdToOperationState_) {
            const auto& state = pair.second;
            if (state->GetHost()->IsSchedulable()) {
                state->GetController()->UpdateMinNeededJobResources();
            }
        }

        LOG_INFO("Min needed job resources successfully updated");
    }

    void OnFairShareLogging()
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        OnFairShareLoggingAt(TInstant::Now());
    }

    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = FindTreeSnapshotByNodeDescriptor(schedulingContext->GetNodeDescriptor());

        // Can happen if all trees are removed.
        if (!snapshot) {
            LOG_INFO("Node does not belong to any fair-share tree, scheduling skipped (Address: %v)",
                schedulingContext->GetNodeDescriptor().Address);
            return VoidFuture;
        }

        return snapshot->ScheduleJobs(schedulingContext);
    }

    virtual void RegisterOperation(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto spec = ParseSpec(operation, operation->GetSpec());
        auto state = New<TFairShareStrategyOperationState>(operation);
        state->TreeIdToPoolIdMap() = ParseOperationPools(operation);

        {
            TWriterGuard guard(OperationIdToOperationStateLock_);
            YCHECK(OperationIdToOperationState_.insert(
                std::make_pair(operation->GetId(), state)).second);
        }

        for (const auto& pair : state->TreeIdToPoolIdMap()) {
            const auto& tree = GetTree(pair.first);
            auto registrationResult = tree->RegisterOperation(state, spec, operation->GetRuntimeParams());
            ActivateOperations(registrationResult.OperationsToActivate);
        }
    }

    virtual void UnregisterOperation(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operation->GetId());
        for (const auto& pair : state->TreeIdToPoolIdMap()) {
            const auto& treeId = pair.first;
            auto unregistrationResult = GetTree(treeId)->UnregisterOperation(state);
            ActivateOperations(unregistrationResult.OperationsToActivate);
        }

        {
            TWriterGuard guard(OperationIdToOperationStateLock_);
            YCHECK(OperationIdToOperationState_.erase(operation->GetId()) == 1);
        }
    }

    virtual void UpdatePools(const INodePtr& poolsNode) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        LOG_INFO("Updating pool trees");

        if (poolsNode->GetType() != NYTree::ENodeType::Map) {
            auto error = TError("Pool trees node has invalid type")
                << TErrorAttribute("expected_type", NYTree::ENodeType::Map)
                << TErrorAttribute("actual_type", poolsNode->GetType());
            LOG_WARNING(error);
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
            return;
        }

        auto poolsMap = poolsNode->AsMap();

        std::vector<TError> errors;

        // Collect trees to add and remove.
        yhash_set<TString> treeIdsToAdd;
        yhash_set<TString> treeIdsToRemove;
        CollectTreesToAddAndRemove(poolsMap, &treeIdsToAdd, &treeIdsToRemove);

        // Populate trees map. New trees are not added to global map yet.
        auto idToTree = ConstructUpdatedTreeMap(
            poolsMap,
            treeIdsToAdd,
            treeIdsToRemove,
            &errors);

        // Check default tree pointer. It should point to some valid tree,
        // otherwise pool trees are not updated.
        auto defaultTreeId = poolsMap->Attributes().Find<TString>(DefaultTreeAttributeName);

        if (defaultTreeId && idToTree.find(*defaultTreeId) == idToTree.end()) {
            errors.emplace_back("Default tree is missing");
            auto error = TError("Error updating pool trees")
                << std::move(errors);
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
            return;
        }

        // Check that after adding or removing trees each node will belong exactly to one tree.
        if (!CheckTreesConfiguration(idToTree, &errors)) {
            auto error = TError("Error updating pool trees")
                << std::move(errors);
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
            return;
        }

        // Update configs and pools structure of all trees.
        int updatedTreeCount;
        UpdateTreesConfigs(poolsMap, idToTree, &errors, &updatedTreeCount);

        // Abort orphaned operations.
        AbortOrphanedOperations(treeIdsToRemove);

        // Updating default fair-share tree and global tree map.
        DefaultTreeId_ = defaultTreeId;
        std::swap(IdToTree_, idToTree);

        yhash<TString, IFairShareTreeSnapshotPtr> snapshots;
        for (const auto& pair : IdToTree_) {
            const auto& treeId = pair.first;
            const auto& tree = pair.second;
            YCHECK(snapshots.insert(std::make_pair(treeId, tree->CreateSnapshot())).second);
        }

        {
            TWriterGuard guard(TreeIdToSnapshotLock_);
            std::swap(TreeIdToSnapshot_, snapshots);
        }

        // Setting alerts.
        if (!errors.empty()) {
            auto error = TError("Error updating pool trees")
                << std::move(errors);
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
        } else {
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, TError());
            if (updatedTreeCount > 0 || treeIdsToRemove.size() > 0 || treeIdsToAdd.size() > 0) {
                Host->LogEventFluently(ELogEventType::PoolsInfo)
                    .Item("pools").DoMapFor(IdToTree_, [&] (TFluentMap fluent, const auto& value) {
                        const auto& treeId = value.first;
                        const auto& tree = value.second;
                        fluent
                            .Item(treeId).Do(BIND(&TFairShareTree::BuildStaticPoolsInformation, tree));
                    });
            }
            LOG_INFO("Pool trees updated");
        }
    }

    virtual void BuildOperationAttributes(const TOperationId& operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operationId);
        if (DefaultTreeId_ && state->TreeIdToPoolIdMap().find(*DefaultTreeId_)) {
            GetTree(*DefaultTreeId_)->BuildOperationAttributes(operationId, fluent);
        }
    }

    virtual void BuildOperationProgress(const TOperationId& operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        DoBuildOperationProgress(&TFairShareTree::BuildOperationProgress, operationId, fluent);
    }

    virtual void BuildBriefOperationProgress(const TOperationId& operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        DoBuildOperationProgress(&TFairShareTree::BuildBriefOperationProgress, operationId, fluent);
    }

    virtual void BuildBriefSpec(const TOperationId& operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operationId);
        const auto& pools = state->TreeIdToPoolIdMap();

        fluent
            .DoIf(DefaultTreeId_.HasValue(), BIND([&] (TFluentMap fluent) {
                auto it = pools.find(*DefaultTreeId_);
                if (it != pools.end()) {
                    fluent
                        .Item("pool").Value(it->second);
                }
            }))
            .Item("fair_share_info_per_pool_tree").DoMapFor(pools, [&] (TFluentMap fluent, const auto& value) {
                fluent
                    .Item(value.first).BeginMap()
                        .Item("pool").Value(value.second)
                    .EndMap();
            });
    }

    virtual void UpdateConfig(const TFairShareStrategyConfigPtr& config) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        Config = config;

        for (const auto& pair : IdToTree_) {
            const auto& tree = pair.second;
            tree->UpdateControllerConfig(config);
        }

        FairShareUpdateExecutor_->SetPeriod(Config->FairShareUpdatePeriod);
        FairShareLoggingExecutor_->SetPeriod(Config->FairShareLogPeriod);
        MinNeededJobResourcesUpdateExecutor_->SetPeriod(Config->MinNeededResourcesUpdatePeriod);
    }

    virtual void BuildOperationInfoForEventLog(const IOperationStrategyHost* operation, TFluentMap fluent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& operationState = GetOperationState(operation->GetId());
        const auto& pools = operationState->TreeIdToPoolIdMap();

        fluent
            .DoIf(DefaultTreeId_.HasValue(), [&] (TFluentMap fluent) {
                auto it = pools.find(*DefaultTreeId_);
                if (it != pools.end()) {
                    fluent
                        .Item("pool").Value(it->second);
                }
            });
    }

    virtual void UpdateOperationRuntimeParams(
        IOperationStrategyHost* operation,
        const TOperationStrategyRuntimeParamsPtr& runtimeParams) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operation->GetId());

        // TODO(asaitgalin): Support ability to specify runtime params
        // separately for each fair share tree.
        for (const auto& pair : state->TreeIdToPoolIdMap()) {
            const auto& treeId = pair.first;
            GetTree(treeId)->UpdateOperationRuntimeParams(operation, runtimeParams);
        }
    }

    virtual TString GetOperationLoggingProgress(const TOperationId& operationId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        std::vector<TString> progressParts;

        for (const auto& pair : IdToTree_) {
            const auto& tree = pair.second;
            progressParts.push_back(tree->GetOperationLoggingProgress(operationId));
        }

        return JoinToString(progressParts.begin(), progressParts.end(), STRINGBUF("; "));
    }

    virtual void BuildOrchid(TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        // TODO(ignat): stop using pools from here and remove this section (since it is also presented in fair_share_info subsection).
        if (DefaultTreeId_) {
            GetTree(*DefaultTreeId_)->BuildPoolsInformation(fluent);
        }


        yhash<TString, std::vector<TExecNodeDescriptor>> descriptorsPerPoolTree;
        for (const auto& pair : IdToTree_) {
            const auto& treeId = pair.first;
            descriptorsPerPoolTree.emplace(treeId, std::vector<TExecNodeDescriptor>{});
        }

        auto descriptors = Host->CalculateExecNodeDescriptors(TSchedulingTagFilter());
        for (const auto& descriptor : descriptors->Descriptors) {
            for (const auto& pair : IdToTree_) {
                const auto& treeId = pair.first;
                const auto& tree = pair.second;

                if (tree->GetNodesFilter().CanSchedule(descriptor.Tags)) {
                    descriptorsPerPoolTree[treeId].push_back(descriptor);
                    break;
                }
            }
        }

        fluent
            .DoIf(DefaultTreeId_.HasValue(), [&] (TFluentMap fluent) {
                fluent
                    // COMPAT(asaitgalin): Remove it when UI will use scheduling_info_per_pool_tree
                    .Item("fair_share_info").BeginMap()
                        .Do(BIND(&TFairShareTree::BuildFairShareInfo, GetTree(*DefaultTreeId_)))
                    .EndMap()
                    .Item("default_fair_share_tree").Value(*DefaultTreeId_);
            })
            .Item("scheduling_info_per_pool_tree").DoMapFor(IdToTree_, [&] (TFluentMap fluent, const auto& pair) {
                    const auto& treeId = pair.first;
                    const auto& tree = pair.second;

                    auto it = descriptorsPerPoolTree.find(treeId);
                    YCHECK(it != descriptorsPerPoolTree.end());

                    fluent
                        .Item(treeId).BeginMap()
                            .Do(BIND(&TFairShareStrategy::BuildTreeOrchid, tree, it->second))
                        .EndMap();
            });
    }

    virtual void ApplyJobMetricsDelta(const TOperationIdToOperationJobMetrics& operationIdToOperationJobMetrics) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TForbidContextSwitchGuard contextSwitchGuard;

        yhash<TString, IFairShareTreeSnapshotPtr> snapshots;
        {
            TReaderGuard guard(TreeIdToSnapshotLock_);
            snapshots = TreeIdToSnapshot_;
        }

        for (const auto& pair : operationIdToOperationJobMetrics) {
            const auto& operationId = pair.first;
            for (const auto& metrics : pair.second) {
                auto snapshotIt = snapshots.find(metrics.TreeId);
                if (snapshotIt == snapshots.end()) {
                    continue;
                }

                const auto& snapshot = snapshotIt->second;
                snapshot->ApplyJobMetricsDelta(operationId, metrics.Metrics);
            }
        }
    }

    virtual TFuture<void> ValidateOperationStart(const IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        return BIND(&TFairShareStrategy::DoValidateOperationStart, Unretained(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operation);
    }

    virtual void ValidateOperationCanBeRegistered(const IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto pools = ParseOperationPools(operation);

        for (const auto& pair : pools) {
            auto tree = GetTree(pair.first);
            tree->ValidateOperationCanBeRegistered(operation, pair.second);
        }
    }

    // NB: This function is public for testing purposes.
    virtual void OnFairShareUpdateAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        LOG_INFO("Starting fair share update");

        std::vector<TError> errors;

        for (const auto& pair : IdToTree_) {
            const auto& tree = pair.second;
            auto error = tree->OnFairShareUpdateAt(now);
            if (!error.IsOK()) {
                errors.push_back(error);
            }
        }

        yhash<TString, IFairShareTreeSnapshotPtr> snapshots;

        for (const auto& pair : IdToTree_) {
            const auto& treeId = pair.first;
            const auto& tree = pair.second;
            YCHECK(snapshots.insert(std::make_pair(treeId, tree->CreateSnapshot())).second);
        }

        {
            TWriterGuard guard(TreeIdToSnapshotLock_);
            std::swap(TreeIdToSnapshot_, snapshots);
        }

        if (LastProfilingTime_ + Config->FairShareProfilingPeriod < now) {
            LastProfilingTime_ = now;
            for (const auto& pair : IdToTree_) {
                const auto& tree = pair.second;
                tree->ProfileFairShare();
            }
        }

        if (!errors.empty()) {
            auto error = TError("Found pool configuration issues during fair share update")
                << std::move(errors);
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdateFairShare, error);
        } else {
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdateFairShare, TError());
        }

        LOG_INFO("Fair share successfully updated");
    }

    virtual void OnFairShareEssentialLoggingAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        for (const auto& pair : IdToTree_) {
            const auto& tree = pair.second;
            tree->OnFairShareEssentialLoggingAt(now);
        }
    }

    virtual void OnFairShareLoggingAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        for (const auto& pair : IdToTree_) {
            const auto& tree = pair.second;
            tree->OnFairShareLoggingAt(now);
        }
    }

    virtual void ProcessUpdatedAndCompletedJobs(
        std::vector<TUpdatedJob>* updatedJobs,
        std::vector<TCompletedJob>* completedJobs,
        std::vector<TJobId>* jobsToAbort) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        yhash<TString, IFairShareTreeSnapshotPtr> snapshots;
        {
            TReaderGuard guard(TreeIdToSnapshotLock_);
            snapshots = TreeIdToSnapshot_;
        }

        for (const auto& job : *updatedJobs) {
            auto snapshotIt = snapshots.find(job.TreeId);
            if (snapshotIt == snapshots.end()) {
                // Job is orphaned (does not belong to any tree), aborting it.
                jobsToAbort->push_back(job.JobId);
            } else {
                const auto& snapshot = snapshotIt->second;
                snapshot->ProcessUpdatedJob(job);
            }
        }
        updatedJobs->clear();

        std::vector<TCompletedJob> remainingCompletedJobs;
        for (const auto& job : *completedJobs) {
            auto snapshotIt = snapshots.find(job.TreeId);
            if (snapshotIt == snapshots.end()) {
                // Job is completed but tree does not exist, nothing to do.
                continue;
            }
            const auto& snapshot = snapshotIt->second;
            if (snapshot->HasOperation(job.OperationId)) {
                snapshot->ProcessCompletedJob(job);
            } else {
                // If operation is not yet in snapshot let's push it back to completed jobs.
                TReaderGuard guard(OperationIdToOperationStateLock_);
                if (OperationIdToOperationState_.find(job.OperationId) != OperationIdToOperationState_.end()) {
                    remainingCompletedJobs.push_back(job);
                }
            }
        }
        *completedJobs = remainingCompletedJobs;
    }

    virtual void RegisterJobs(const TOperationId& operationId, const std::vector<TJobPtr>& jobs) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        yhash<TString, std::vector<TJobPtr>> jobsByTreeId;

        for (const auto& job : jobs) {
            jobsByTreeId[job->GetTreeId()].push_back(job);
        }

        for (const auto& pair : jobsByTreeId) {
            auto tree = FindTree(pair.first);
            if (tree) {
                tree->RegisterJobs(operationId, pair.second);
            }
        }
    }

    virtual void OnOperationRunning(const TOperationId& operationId) override
    {
        const auto& state = GetOperationState(operationId);
        if (state->GetHost()->IsSchedulable()) {
            state->GetController()->UpdateMinNeededJobResources();
        }
    }

    virtual void ValidateNodeTags(const yhash_set<TString>& tags) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        // Trees this node falls into.
        std::vector<TString> trees;

        for (const auto& pair : IdToTree_) {
            const auto& treeId = pair.first;
            const auto& tree = pair.second;
            if (tree->GetNodesFilter().CanSchedule(tags)) {
                trees.push_back(treeId);
            }
        }

        if (trees.size() > 1) {
            THROW_ERROR_EXCEPTION("Node belongs to more than one fair-share tree")
                << TErrorAttribute("matched_trees", trees);
        }
    }

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* const Host;

    const std::vector<IInvokerPtr> FeasibleInvokers;

    mutable NLogging::TLogger Logger;

    TPeriodicExecutorPtr FairShareUpdateExecutor_;
    TPeriodicExecutorPtr FairShareLoggingExecutor_;
    TPeriodicExecutorPtr MinNeededJobResourcesUpdateExecutor_;

    TReaderWriterSpinLock OperationIdToOperationStateLock_;
    yhash<TOperationId, TFairShareStrategyOperationStatePtr> OperationIdToOperationState_;

    TInstant LastProfilingTime_;

    using TFairShareTreeMap = yhash<TString, TFairShareTreePtr>;
    TFairShareTreeMap IdToTree_;

    TNullable<TString> DefaultTreeId_;

    TReaderWriterSpinLock TreeIdToSnapshotLock_;
    yhash<TString, IFairShareTreeSnapshotPtr> TreeIdToSnapshot_;

    TStrategyOperationSpecPtr ParseSpec(const IOperationStrategyHost* operation, INodePtr specNode) const
    {
        try {
            return ConvertTo<TStrategyOperationSpecPtr>(specNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing strategy spec of operation")
                << ex;
        }
    }

    yhash<TString, TString> ParseOperationPools(const IOperationStrategyHost* operation) const
    {
        auto spec = ParseSpec(operation, operation->GetSpec());

        std::vector<TString> trees;

        // Skipping unknown trees.
        for (const auto& treeId : spec->PoolTrees) {
            if (FindTree(treeId)) {
                trees.push_back(treeId);
            }
        }

        if (trees.empty()) {
            if (!DefaultTreeId_) {
                THROW_ERROR_EXCEPTION("Failed to determine fair-share tree for operation since "
                    "valid pool trees are not specified and default fair-share tree is not configured");
            }

            auto it = spec->FairShareOptionsPerPoolTree.find(*DefaultTreeId_);
            if (it != spec->FairShareOptionsPerPoolTree.end()) {
                const auto& options = it->second;
                if (options->Pool) {
                    return {{*DefaultTreeId_, *options->Pool}};
                }
            }

            if (spec->Pool) {
                return {{*DefaultTreeId_, *spec->Pool}};
            }

            return {{*DefaultTreeId_, operation->GetAuthenticatedUser()}};
        }

        yhash<TString, TString> pools;

        for (const auto& treeId : trees) {
            auto optionsIt = spec->FairShareOptionsPerPoolTree.find(treeId);

            TNullable<TString> pool;
            if (optionsIt != spec->FairShareOptionsPerPoolTree.end()) {
                const auto& options = optionsIt->second;
                if (options->Pool) {
                    pool = options->Pool;
                }
            }

            if (pool) {
                pools.emplace(treeId, *pool);
            } else {
                if (spec->Pool) {
                    pools.emplace(treeId, *spec->Pool);
                } else {
                    pools.emplace(treeId, operation->GetAuthenticatedUser());
                }
            }
        }

        return pools;
    }

    void DoValidateOperationStart(const IOperationStrategyHost* operation)
    {
        if (IdToTree_.empty()) {
            THROW_ERROR_EXCEPTION("Scheduler strategy does not have configured fair-share trees");
        }

        auto spec = ParseSpec(operation, operation->GetSpec());
        auto pools = ParseOperationPools(operation);

        if (pools.size() > 1 && !spec->SchedulingTagFilter.IsEmpty()) {
            THROW_ERROR_EXCEPTION(
                "Scheduling tag filter cannot be specified for operations "
                "to be scheduled in multiple fair-share trees");
        }

        std::vector<TFuture<void>> futures;

        for (const auto& pair : pools) {
            auto tree = GetTree(pair.first);
            futures.push_back(tree->ValidateOperationStart(operation, pair.second));
        }

        WaitFor(Combine(futures))
            .ThrowOnError();
    }

    TFairShareStrategyOperationStatePtr GetOperationState(const TOperationId& operationId) const
    {
        auto it = OperationIdToOperationState_.find(operationId);
        YCHECK(it != OperationIdToOperationState_.end());
        return it->second;
    }

    TFairShareTreePtr FindTree(const TString& id) const
    {
        auto treeIt = IdToTree_.find(id);
        return treeIt != IdToTree_.end() ? treeIt->second : nullptr;
    }

    TFairShareTreePtr GetTree(const TString& id) const
    {
        auto tree = FindTree(id);
        YCHECK(tree);
        return tree;
    }

    IFairShareTreeSnapshotPtr FindTreeSnapshotByNodeDescriptor(const TExecNodeDescriptor& descriptor) const
    {
        IFairShareTreeSnapshotPtr result;

        TReaderGuard guard(TreeIdToSnapshotLock_);

        for (const auto& pair : TreeIdToSnapshot_) {
            const auto& snapshot = pair.second;
            if (snapshot->GetNodesFilter().CanSchedule(descriptor.Tags)) {
                YCHECK(!result);  // Only one snapshot should be found
                result = snapshot;
            }
        }

        return result;
    }

    void DoBuildOperationProgress(
        void (TFairShareTree::*method)(const TOperationId& operationId, TFluentMap fluent),
        const TOperationId& operationId,
        TFluentMap fluent)
    {
        const auto& state = GetOperationState(operationId);
        const auto& pools = state->TreeIdToPoolIdMap();

        fluent
            .DoIf(DefaultTreeId_ && pools.find(*DefaultTreeId_) != pools.end(),
                  BIND(method, GetTree(*DefaultTreeId_), operationId))
            .Item("fair_share_info_per_pool_tree")
                .DoMapFor(pools, [&] (TFluentMap fluent, const std::pair<TString, TString>& value) {
                    const auto& treeId = value.first;
                    fluent
                        .Item(treeId).BeginMap()
                            .Do(BIND(method, GetTree(treeId), operationId))
                        .EndMap();
                });
    }

    void ActivateOperations(const std::vector<TOperationId>& operationIds) const
    {
        for (const auto& operationId : operationIds) {
            const auto& state = GetOperationState(operationId);
            if (!state->GetActive()) {
                Host->ActivateOperation(operationId);
                state->SetActive(true);
            }
        }
    }

    void CollectTreesToAddAndRemove(
        const IMapNodePtr& poolsMap,
        yhash_set<TString>* treesToAdd,
        yhash_set<TString>* treesToRemove) const
    {
        for (const auto& key : poolsMap->GetKeys()) {
            if (IdToTree_.find(key) == IdToTree_.end()) {
                treesToAdd->insert(key);
            }
        }

        for (const auto& pair : IdToTree_) {
            const auto& treeId = pair.first;
            const auto& tree = pair.second;

            auto child = poolsMap->FindChild(treeId);
            if (!child) {
                treesToRemove->insert(treeId);
                continue;
            }

            // Nodes filter update is equivalent to remove-add operation.
            try {
                auto configMap = child->Attributes().ToMap();
                auto config = ConvertTo<TFairShareStrategyTreeConfigPtr>(configMap);

                if (config->NodesFilter != tree->GetNodesFilter()) {
                    treesToRemove->insert(treeId);
                    treesToAdd->insert(treeId);
                }
            } catch (const std::exception&) {
                // Do nothing, alert will be set later.
                continue;
            }
        }
    }

    TFairShareTreeMap ConstructUpdatedTreeMap(
        const IMapNodePtr& poolsMap,
        const yhash_set<TString>& treesToAdd,
        const yhash_set<TString>& treesToRemove,
        std::vector<TError>* errors) const
    {
        TFairShareTreeMap trees;

        for (const auto& treeId : treesToAdd) {
            TFairShareStrategyTreeConfigPtr treeConfig;
            try {
                auto configMap = poolsMap->GetChild(treeId)->Attributes().ToMap();
                treeConfig = ConvertTo<TFairShareStrategyTreeConfigPtr>(configMap);
            } catch (const std::exception& ex) {
                auto error = TError("Error parsing configuration of tree %Qv", treeId)
                    << ex;
                errors->push_back(error);
                LOG_WARNING(error);
                continue;
            }

            auto tree = New<TFairShareTree>(treeConfig, Config, Host, FeasibleInvokers, treeId);
            trees.emplace(treeId, tree);
        }

        for (const auto& pair : IdToTree_) {
            if (treesToRemove.find(pair.first) == treesToRemove.end()) {
                trees.insert(pair);
            }
        }

        return trees;
    }

    bool CheckTreesConfiguration(const TFairShareTreeMap& trees, std::vector<TError>* errors) const
    {
        yhash<NNodeTrackerClient::TNodeId, yhash_set<TString>> nodeIdToTreeSet;

        for (const auto& pair : trees) {
            const auto& treeId = pair.first;
            const auto& tree = pair.second;
            auto nodes = Host->GetExecNodeIds(tree->GetNodesFilter());

            for (const auto& node : nodes) {
                nodeIdToTreeSet[node].insert(treeId);
            }
        }

        for (const auto& pair : nodeIdToTreeSet) {
            const auto& nodeId = pair.first;
            const auto& trees  = pair.second;
            if (trees.size() > 1) {
                errors->emplace_back("Cannot update fair-share trees since there is node that "
                    "belongs to multiple trees (NodeId: %v, MatchedTrees: %v)",
                    nodeId,
                    trees);
                return false;
            }
        }

        return true;
    }

    void UpdateTreesConfigs(
        const IMapNodePtr& poolsMap,
        const TFairShareTreeMap& trees,
        std::vector<TError>* errors,
        int* updatedTreeCount) const
    {
        *updatedTreeCount = 0;

        for (const auto& pair : trees) {
            const auto& treeId = pair.first;
            const auto& tree = pair.second;

            auto child = poolsMap->GetChild(treeId);

            try {
                auto configMap = child->Attributes().ToMap();
                auto config = ConvertTo<TFairShareStrategyTreeConfigPtr>(configMap);
                tree->UpdateConfig(config);
            } catch (const std::exception& ex) {
                auto error = TError("Failed to configure tree %Qv, defaults will be used", treeId)
                    << ex;
                errors->push_back(error);
                continue;
            }

            auto updateResult = tree->UpdatePools(child);
            if (!updateResult.Error.IsOK()) {
                errors->push_back(updateResult.Error);
            }
            if (updateResult.Updated) {
                *updatedTreeCount = *updatedTreeCount + 1;
            }
        }
    }

    void AbortOrphanedOperations(const yhash_set<TString>& treesToRemove)
    {
        if (treesToRemove.empty()) {
            return;
        }

        yhash<TOperationId, yhash_set<TString>> operationIdToTreeSet;
        yhash<TString, yhash_set<TOperationId>> treeIdToOperationSet;

        for (const auto& pair : OperationIdToOperationState_) {
            const auto& operationId = pair.first;
            const auto& poolsMap = pair.second->TreeIdToPoolIdMap();

            for (const auto& treeAndPool : poolsMap) {
                const auto& treeId = treeAndPool.first;

                YCHECK(operationIdToTreeSet[operationId].insert(treeId).second);
                YCHECK(treeIdToOperationSet[treeId].insert(operationId).second);
            }
        }

        for (const auto& treeId : treesToRemove) {
            auto it = treeIdToOperationSet.find(treeId);

            // No operations are running in this tree.
            if (it == treeIdToOperationSet.end()) {
                continue;
            }

            // Unregister operations in removed tree and update their tree set.
            for (const auto& operationId : it->second) {
                const auto& state = GetOperationState(operationId);
                GetTree(treeId)->UnregisterOperation(state);
                YCHECK(state->TreeIdToPoolIdMap().erase(treeId) == 1);

                auto treeSetIt = operationIdToTreeSet.find(operationId);
                YCHECK(treeSetIt != operationIdToTreeSet.end());
                YCHECK(treeSetIt->second.erase(treeId) == 1);
            }
        }

        // Aborting orphaned operations.
        for (const auto& pair : operationIdToTreeSet) {
            const auto& operationId = pair.first;
            const auto& treeSet = pair.second;
            if (treeSet.empty()) {
                Host->AbortOperation(
                    operationId,
                    TError("No suitable fair-share trees to schedule operation"));
            }
        }
    }

    static void BuildTreeOrchid(
        const TFairShareTreePtr& tree,
        const std::vector<TExecNodeDescriptor>& descriptors,
        TFluentMap fluent)
    {
        TJobResources resources = ZeroJobResources();
        for (const auto& descriptor : descriptors) {
            resources += descriptor.ResourceLimits;
        }

        fluent
            .Item("user_to_ephemeral_pools").Do(BIND(&TFairShareTree::BuildUserToEphemeralPools, tree))
            .Item("fair_share_info").BeginMap()
                .Do(BIND(&TFairShareTree::BuildFairShareInfo, tree))
            .EndMap()
            .Item("resource_limits").Value(resources)
            .Item("node_count").Value(descriptors.size())
            .Item("node_addresses").BeginList()
                .DoFor(descriptors, [&] (TFluentList fluent, const auto& descriptor) {
                    fluent
                        .Item().Value(descriptor.Address);
                })
            .EndList();
    }
};

ISchedulerStrategyPtr CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host,
    const std::vector<IInvokerPtr>& feasibleInvokers)
{
    return New<TFairShareStrategy>(config, host, feasibleInvokers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

