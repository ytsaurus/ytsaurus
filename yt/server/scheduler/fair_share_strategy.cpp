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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;

static NProfiling::TAggregateCounter FairShareUpdateTimeCounter("/fair_share_update_time");
static NProfiling::TAggregateCounter FairShareLogTimeCounter("/fair_share_log_time");
static NProfiling::TAggregateCounter AnalyzePreemptableJobsTimeCounter("/analyze_preemptable_jobs_time");

////////////////////////////////////////////////////////////////////////////////

namespace {

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

struct TFairShareStrategyOperationState
    : public TIntrinsicRefCounted
{
public:
    TFairShareStrategyOperationState(const TOperationPtr& operation)
        : Host(operation.Get())
        , Controller(New<TFairShareStrategyOperationController>(operation.Get()))
    { }

    TFairShareStrategyOperationControllerPtr GetController() const
    {
        return Controller;
    }

    IOperationStrategyHost* GetHost() const
    {
        return Host;
    }

private:
    IOperationStrategyHost* Host;
    TFairShareStrategyOperationControllerPtr Controller;
};

using TFairShareStrategyOperationStatePtr = TIntrusivePtr<TFairShareStrategyOperationState>;

struct TOperationRegistrationUnregistrationResult
{
    std::vector<TOperationId> OperationsToActivate;
};

TStrategyOperationSpecPtr ParseSpec(const TOperationPtr& operation, INodePtr specNode)
{
    try {
        return ConvertTo<TStrategyOperationSpecPtr>(specNode);
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error parsing strategy spec of operation %v, defaults will be used",
            operation->GetId());
        return New<TStrategyOperationSpec>();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TFairShareTree
    : public TIntrinsicRefCounted
{
public:
    TFairShareTree(
        TFairShareStrategyTreeConfigPtr config,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        ISchedulerStrategyHost* host,
        const std::vector<IInvokerPtr>& feasibleInvokers)
        : Config(config)
        , ControllerConfig(controllerConfig)
        , Host(host)
        , FeasibleInvokers(feasibleInvokers)
        , NonPreemptiveProfilingCounters("/non_preemptive")
        , PreemptiveProfilingCounters("/preemptive")
    {
        RootElement = New<TRootElement>(Host, config, GetPoolProfilingTag(RootPoolName));
    }

    TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto asyncGuard = TAsyncLockReaderGuard::Acquire(&ScheduleJobsLock);

        auto rootElementSnapshot = GetRootSnapshot();

        auto jobScheduler =
            BIND(
                &TFairShareTree::DoScheduleJobs,
                MakeStrong(this),
                schedulingContext,
                rootElementSnapshot)
            .AsyncVia(GetCurrentInvoker());
        return asyncGuard.Apply(jobScheduler);
    }

    TFuture<void> ValidateOperationStart(const TOperationPtr& operation)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        return BIND(&TFairShareTree::DoValidateOperationStart, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operation);
    }

    void ValidateOperationCanBeRegistered(const TOperationPtr& operation)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        ValidateOperationCountLimit(operation);
        ValidateEphemeralPoolLimit(operation);
    }

    TOperationRegistrationUnregistrationResult RegisterOperation(
        const TFairShareStrategyOperationStatePtr& state,
        const TStrategyOperationSpecPtr& spec)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto operationId = state->GetHost()->GetId();
        auto params = BuildInitialRuntimeParams(spec);

        auto operationElement = New<TOperationElement>(
            Config,
            spec,
            params,
            state->GetController(),
            ControllerConfig,
            Host,
            state->GetHost());

        int index = RegisterSchedulingTagFilter(TSchedulingTagFilter(spec->SchedulingTagFilter));
        operationElement->SetSchedulingTagFilterIndex(index);

        {
            TWriterGuard guard(RegisteredOperationsSetLock);
            YCHECK(RegisteredOperationsSet.insert(operationId).second);
        }
        YCHECK(OperationIdToElement.insert(std::make_pair(operationId, operationElement)).second);

        const auto& userName = state->GetHost()->GetAuthenticatedUser();

        auto poolId = spec->Pool ? *spec->Pool : userName;
        auto pool = FindPool(poolId);
        if (!pool) {
            pool = New<TPool>(Host, poolId, New<TPoolConfig>(), /* defaultConfigured */ true, Config, GetPoolProfilingTag(poolId));
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
            state->GetHost()->GetSlotIndex(),
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
        {
            TWriterGuard guard(RegisteredOperationsSetLock);
            YCHECK(RegisteredOperationsSet.erase(operationId));
        }
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

    void ProcessUpdatedAndCompletedJobs(
        std::vector<TUpdatedJob>* updatedJobs,
        std::vector<TCompletedJob>* completedJobs)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto rootElementSnapshot = GetRootSnapshot();

        for (const auto& job : *updatedJobs) {
            auto* operationElement = rootElementSnapshot->FindOperationElement(job.OperationId);
            if (operationElement) {
                operationElement->IncreaseJobResourceUsage(job.JobId, job.Delta);
            }
        }
        updatedJobs->clear();

        std::vector<TCompletedJob> remainingCompletedJobs;
        for (const auto& job : *completedJobs) {
            auto* operationElement = rootElementSnapshot->FindOperationElement(job.OperationId);
            if (operationElement) {
                operationElement->OnJobFinished(job.JobId);
            } else {
                TReaderGuard guard(RegisteredOperationsSetLock);
                if (RegisteredOperationsSet.find(job.OperationId) != RegisteredOperationsSet.end()) {
                    remainingCompletedJobs.push_back(job);
                }
            }
        }
        *completedJobs = remainingCompletedJobs;
    }

    void ApplyJobMetricsDelta(const TOperationId& operationId, const TJobMetrics& jobMetricsDelta)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TRootElementSnapshotPtr rootElementSnapshot = GetRootSnapshot();

        auto* operationElement = rootElementSnapshot->FindOperationElement(operationId);
        if (operationElement) {
            operationElement->ApplyJobMetricsDelta(jobMetricsDelta);
        }
    }

    void UpdatePools(const INodePtr& poolsNode)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        if (LastPoolsNodeUpdate && AreNodesEqual(LastPoolsNodeUpdate, poolsNode)) {
            LOG_INFO("Pools are not changed, skipping update");
            return;
        }
        LastPoolsNodeUpdate = poolsNode;

        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&ScheduleJobsLock)).Value();

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
                            pool = New<TPool>(Host, childId, poolConfig, /* defaultConfigured */ false, Config, GetPoolProfilingTag(childId));
                            RegisterPool(pool, parent);
                        }
                        SetPoolParent(pool, parent);

                        if (parent->GetMode() == ESchedulingMode::Fifo) {
                            parent->SetMode(ESchedulingMode::FairShare);
                            errors.emplace_back(
                                TError(
                                    "Pool %Qv cannot have subpools since it is in %v mode",
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
            AssignRootElementSnapshot(CreateRootElementSnapshot());
        } catch (const std::exception& ex) {
            auto error = TError("Error updating pools")
                << ex;
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
            return;
        }

        if (!errors.empty()) {
            auto combinedError = TError("Found pool configuration issues");
            combinedError.InnerErrors() = std::move(errors);
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, combinedError);
        } else {
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, TError());
            Host->LogEventFluently(ELogEventType::PoolsInfo)
                .Item("pools").DoMapFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                    const auto& id = pair.first;
                    const auto& pool = pair.second;
                    fluent
                        .Item(id).Value(pool->GetConfig());
                });
            LOG_INFO("Pools updated");
        }
    }

    void UpdateOperationRuntimeParams(const TOperationPtr& operation, const INodePtr& update)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = FindOperationElement(operation->GetId());
        if (!element)
            return;

        NLogging::TLogger Logger(SchedulerLogger);
        Logger.AddTag("OperationId: %v", operation->GetId());

        try {
            auto newRuntimeParams = CloneYsonSerializable(element->GetRuntimeParams());
            if (ReconfigureYsonSerializable(newRuntimeParams, update)) {
                element->SetRuntimeParams(newRuntimeParams);
                LOG_INFO("Operation runtime parameters updated");
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing operation runtime parameters");
        }
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

    void BuildOperationAttributes(const TOperationId& operationId, IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = GetOperationElement(operationId);
        auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
        BuildYsonMapFluently(consumer)
            .Items(*serializedParams)
            .Item("pool").Value(element->GetParent()->GetId());
    }

    void BuildOperationInfoForEventLog(
        const TOperationPtr& operation,
        NYson::IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        BuildYsonMapFluently(consumer)
            .Item("pool").Value(GetOperationPoolName(operation));
    }

    void BuildOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto* parent = element->GetParent();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(parent->GetId())
            .Item("slot_index").Value(element->GetSlotIndex())
            .Item("start_time").Value(element->GetStartTime())
            .Item("preemptable_job_count").Value(element->GetPreemptableJobCount())
            .Item("aggressively_preemptable_job_count").Value(element->GetAggressivelyPreemptableJobCount())
            .Item("fifo_index").Value(element->Attributes().FifoIndex)
            .Do(BIND(&TFairShareTree::BuildElementYson, Unretained(this), element));
    }

    void BuildBriefOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto* parent = element->GetParent();
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(parent->GetId())
            .Item("fair_share_ratio").Value(attributes.FairShareRatio);
    }

    void BuildOrchid(IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        // TODO(ignat): stop using pools from here and remove this section (since it is also presented in fair_share_info subsection).
        BuildPoolsInformation(consumer);
        BuildYsonMapFluently(consumer)
            .Item("fair_share_info").BeginMap()
                .Do(BIND(&TFairShareTree::BuildFairShareInfo, Unretained(this)))
            .EndMap()
            .Item("user_to_ephemeral_pools").Value(UserToEphemeralPools);
    }

    TString GetOperationLoggingProgress(const TOperationId& operationId)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = GetOperationElement(operationId);
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        return Format(
            "Scheduling = {Status: %v, DominantResource: %v, Demand: %.6lf, "
            "Usage: %.6lf, FairShare: %.6lf, Satisfaction: %.4lg, AdjustedMinShare: %.6lf, "
            "GuaranteedResourcesRatio: %.6lf, "
            "MaxPossibleUsage: %.6lf,  BestAllocation: %.6lf, "
            "Starving: %v, Weight: %v, "
            "PreemptableRunningJobs: %v, "
            "AggressivelyPreemptableRunningJobs: %v}",
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

    void BuildBriefSpec(const TOperationId& operationId, IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = GetOperationElement(operationId);
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(element->GetParent()->GetId());
    }

    // NB: This function is public for testing purposes.
    void OnFairShareUpdateAt(TInstant now)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        LOG_INFO("Starting fair share update");

        // Run periodic update.
        PROFILE_AGGREGATED_TIMING (FairShareUpdateTimeCounter) {
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
            if (alerts.empty()) {
                Host->SetSchedulerAlert(ESchedulerAlertType::UpdateFairShare, TError());
            } else {
                auto error = TError("Found pool configuration issues during fair share update");
                error.InnerErrors() = std::move(alerts);
                Host->SetSchedulerAlert(ESchedulerAlertType::UpdateFairShare, error);
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

            AssignRootElementSnapshot(CreateRootElementSnapshot());
        }

        LOG_INFO("Fair share successfully updated");
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

        PROFILE_AGGREGATED_TIMING (FairShareLogTimeCounter) {
            // Log pools information.
            Host->LogEventFluently(ELogEventType::FairShareInfo, now)
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

    void BuildPoolsInformation(IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        BuildYsonMapFluently(consumer)
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

    void BuildFairShareInfo(IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        BuildYsonMapFluently(consumer)
            .Do(BIND(&TFairShareTree::BuildPoolsInformation, Unretained(this)))
            .Item("operations").DoMapFor(
                OperationIdToElement,
                [=] (TFluentMap fluent, const TOperationElementPtrByIdMap::value_type& pair) {
                    const auto& operationId = pair.first;
                    BuildYsonMapFluently(fluent)
                        .Item(ToString(operationId)).BeginMap()
                            .Do(BIND(&TFairShareTree::BuildOperationProgress, Unretained(this), operationId))
                        .EndMap();
                });
    }

    void BuildEssentialFairShareInfo(IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        BuildYsonMapFluently(consumer)
            .Do(BIND(&TFairShareTree::BuildEssentialPoolsInformation, Unretained(this)))
            .Item("operations").DoMapFor(
                OperationIdToElement,
                [=] (TFluentMap fluent, const TOperationElementPtrByIdMap::value_type& pair) {
                    const auto& operationId = pair.first;
                    BuildYsonMapFluently(fluent)
                        .Item(ToString(operationId)).BeginMap()
                            .Do(BIND(&TFairShareTree::BuildEssentialOperationProgress, Unretained(this), operationId))
                        .EndMap();
                });
    }

    void ResetState()
    {
        LastPoolsNodeUpdate.Reset();
    }

private:
    TFairShareStrategyTreeConfigPtr Config;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig;
    ISchedulerStrategyHost* const Host;

    std::vector<IInvokerPtr> FeasibleInvokers;

    INodePtr LastPoolsNodeUpdate;

    using TPoolMap = THashMap<TString, TPoolPtr>;
    TPoolMap Pools;

    THashMap<TString, NProfiling::TTagId> PoolIdToProfilingTagId;

    THashMap<TString, THashSet<TString>> UserToEphemeralPools;

    THashMap<TString, THashSet<int>> PoolToSpareSlotIndices;
    THashMap<TString, int> PoolToMinUnusedSlotIndex;

    using TOperationElementPtrByIdMap = THashMap<TOperationId, TOperationElementPtr>;
    TOperationElementPtrByIdMap OperationIdToElement;

    std::list<TOperationId> WaitingOperationQueue;
    
    TReaderWriterSpinLock RegisteredOperationsSetLock;
    THashSet<TOperationId> RegisteredOperationsSet;

    TReaderWriterSpinLock NodeIdToLastPreemptiveSchedulingTimeLock;
    THashMap<TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime;

    std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilters;
    std::vector<int> FreeSchedulingTagFilterIndexes;
    struct TSchedulingTagFilterEntry
    {
        int Index;
        int Count;
    };
    THashMap<TSchedulingTagFilter, TSchedulingTagFilterEntry> SchedulingTagFilterToIndexAndCount;

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

    TReaderWriterSpinLock RootElementSnapshotLock;
    TRootElementSnapshotPtr RootElementSnapshot;

    TAsyncReaderWriterLock ScheduleJobsLock;

    TDynamicAttributesList GlobalDynamicAttributes_;

    struct TProfilingCounters
    {
        TProfilingCounters(const TString& prefix)
            : PrescheduleJobTimeCounter(prefix + "/preschedule_job_time")
            , TotalControllerScheduleJobTimeCounter(prefix + "/controller_schedule_job_time/total")
            , ExecControllerScheduleJobTimeCounter(prefix + "/controller_schedule_job_time/exec")
            , StrategyScheduleJobTimeCounter(prefix + "/strategy_schedule_job_time")
            , ScheduleJobCallCounter(prefix + "/schedule_job_count")
        {
            for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues())
            {
                ControllerScheduleJobFailCounter[reason] = TSimpleCounter(
                    prefix + "/controller_schedule_job_fail",
                    GetFailReasonProfilingTags(reason));
            }
        }

        TAggregateCounter PrescheduleJobTimeCounter;
        TAggregateCounter TotalControllerScheduleJobTimeCounter;
        TAggregateCounter ExecControllerScheduleJobTimeCounter;
        TAggregateCounter StrategyScheduleJobTimeCounter;
        TAggregateCounter ScheduleJobCallCounter;

        TEnumIndexedVector<TSimpleCounter, EScheduleJobFailReason> ControllerScheduleJobFailCounter;
    };

    TProfilingCounters NonPreemptiveProfilingCounters;
    TProfilingCounters PreemptiveProfilingCounters;

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
                    context.InitializeStructures(rootElement->GetTreeSize(), RegisteredSchedulingTagFilters);
                    rootElement->PrescheduleJob(context, /*starvingOnly*/ false, /*aggressiveStarvationEnabled*/ false);
                    prescheduleDuration = prescheduleTimer.GetElapsedTime();
                    Profiler.Update(NonPreemptiveProfilingCounters.PrescheduleJobTimeCounter, DurationToCpuDuration(prescheduleDuration));
                    prescheduleExecuted = true;
                    context.PrescheduledCalled = true;
                }
                ++context.NonPreemptiveScheduleJobCount;
                if (!rootElement->ScheduleJob(context)) {
                    break;
                }
            }
            profileTimings(
                NonPreemptiveProfilingCounters,
                context.NonPreemptiveScheduleJobCount,
                scheduleTimer.GetElapsedTime() - prescheduleDuration - context.TotalScheduleJobDuration);

            if (context.NonPreemptiveScheduleJobCount > 0) {
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
            context.InitializeStructures(rootElement->GetTreeSize(), RegisteredSchedulingTagFilters);
        }

        if (!context.PrescheduledCalled) {
            context.HasAggressivelyStarvingNodes = rootElement->HasAggressivelyStarvingNodes(context, false);
        }

        // Compute discount to node usage.
        LOG_TRACE("Looking for preemptable jobs");
        THashSet<TCompositeSchedulerElementPtr> discountedPools;
        std::vector<TJobPtr> preemptableJobs;
        PROFILE_AGGREGATED_TIMING (AnalyzePreemptableJobsTimeCounter) {
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

                ++context.PreemptiveScheduleJobCount;
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
                context.PreemptiveScheduleJobCount,
                timer.GetElapsedTime() - prescheduleDuration - context.TotalScheduleJobDuration);
            if (context.PreemptiveScheduleJobCount > 0) {
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
        const TRootElementSnapshotPtr& rootElementSnapshot,
        const TIntrusivePtr<TAsyncLockReaderGuard>& /*guard*/)
    {
        auto context = TFairShareContext(schedulingContext);

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

            Profiler.Update(counters.ScheduleJobCallCounter, scheduleJobCount);

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
            for (auto& item : context.DeactivationReasons) {
                item = 0;
            }
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
            "NonPreemptiveScheduleJobCount: %v, PreemptiveScheduleJobCount: %v, HasAggressivelyStarvingNodes: %v, Address: %v)",
            schedulingContext->StartedJobs().size(),
            schedulingContext->PreemptedJobs().size(),
            context.ScheduledDuringPreemption,
            context.PreemptableJobCount,
            FormatResources(context.ResourceUsageDiscount),
            context.NonPreemptiveScheduleJobCount,
            context.PreemptiveScheduleJobCount,
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

    TOperationRuntimeParamsPtr BuildInitialRuntimeParams(const TStrategyOperationSpecPtr& spec)
    {
        auto params = New<TOperationRuntimeParams>();
        params->Weight = spec->Weight;
        params->ResourceLimits = spec->ResourceLimits;
        return params;
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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        DoRegisterPool(pool);

        LOG_INFO("Pool registered (Pool: %v)", pool->GetId());
    }

    void RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

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
        auto slotIndex = state->GetHost()->GetSlotIndex();

        if (slotIndex != -1) {
            // Revive case
            if (TryOccupyPoolSlotIndex(poolName, slotIndex)) {
                return;
            } else {
                auto error = TError("Failed to assign slot index to operation during revive")
                    << TErrorAttribute("operation_id", state->GetHost()->GetId())
                    << TErrorAttribute("slot_index", slotIndex);
                Host->SetOperationAlert(state->GetHost()->GetId(), EOperationAlertType::SlotIndexCollision, error);
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

        state->GetHost()->SetSlotIndex(slotIndex);
    }

    void UnassignOperationPoolIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName)
    {
        auto slotIndex = state->GetHost()->GetSlotIndex();

        auto it = PoolToSpareSlotIndices.find(poolName);
        if (it == PoolToSpareSlotIndices.end()) {
            YCHECK(PoolToSpareSlotIndices.insert(std::make_pair(poolName, THashSet<int>{slotIndex})).second);
        } else {
            it->second.insert(slotIndex);
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

    void BuildEssentialOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        BuildYsonMapFluently(consumer)
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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        if (pool->GetParent() == parent)
            return;

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

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
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

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

    void AssignRootElementSnapshot(TRootElementSnapshotPtr rootElementSnapshot)
    {
        // NB: Avoid destroying the cloned tree inside critical section.
        TWriterGuard guard(RootElementSnapshotLock);
        std::swap(RootElementSnapshot, rootElementSnapshot);
    }

    void BuildEssentialPoolsInformation(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("pools").DoMapFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                const auto& id = pair.first;
                const auto& pool = pair.second;
                fluent
                    .Item(id).BeginMap()
                        .Do(BIND(&TFairShareTree::BuildEssentialPoolElementYson, Unretained(this), pool))
                    .EndMap();
            });
    }

    void BuildElementYson(const TSchedulerElementPtr& element, IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        auto guaranteedResources = Host->GetMainNodesResourceLimits() * attributes.GuaranteedResourcesRatio;

        BuildYsonMapFluently(consumer)
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

    void BuildEssentialElementYson(const TSchedulerElementPtr& element, IYsonConsumer* consumer,
                                   bool shouldPrintResourceUsage)
    {
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        BuildYsonMapFluently(consumer)
            .Item("usage_ratio").Value(element->GetResourceUsageRatio())
            .Item("demand_ratio").Value(attributes.DemandRatio)
            .Item("fair_share_ratio").Value(attributes.FairShareRatio)
            .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio)
            .DoIf(shouldPrintResourceUsage, [&] (TFluentMap fluent) {
                fluent
                    .Item("resource_usage").Value(element->GetResourceUsage());
            });
    }

    void BuildEssentialPoolElementYson(const TSchedulerElementPtr& element, IYsonConsumer* consumer) {
        BuildEssentialElementYson(element, consumer, false);
    }

    void BuildEssentialOperationElementYson(const TSchedulerElementPtr& element, IYsonConsumer* consumer) {
        BuildEssentialElementYson(element, consumer, true);
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

        TYPath path;
        for (const auto& token : tokens) {
            path.append('/');
            path.append(NYPath::ToYPathLiteral(token));
        }
        return path;
    }

    TString GetOperationPoolName(const TOperationPtr& operation)
    {
        auto spec = ParseSpec(operation, operation->GetSpec());
        return spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
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

    TCompositeSchedulerElementPtr GetParentElement(const TOperationPtr& operation)
    {
        auto parentPool = FindPool(GetOperationPoolName(operation));
        return parentPool ? parentPool : GetDefaultParent();
    }

    void ValidateOperationCountLimit(const TOperationPtr& operation)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto parentElement = GetParentElement(operation);

        auto poolWithViolatedLimit = FindPoolWithViolatedOperationCountLimit(parentElement);
        if (poolWithViolatedLimit) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::TooManyOperations,
                "Limit for the number of concurrent operations %v for pool %Qv has been reached",
                poolWithViolatedLimit->GetMaxOperationCount(),
                poolWithViolatedLimit->GetId());
        }
    }

    void ValidateEphemeralPoolLimit(const TOperationPtr& operation)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto pool = FindPool(GetOperationPoolName(operation));
        if (pool) {
            return;
        }

        const auto& userName = operation->GetAuthenticatedUser();

        auto it = UserToEphemeralPools.find(userName);
        if (it == UserToEphemeralPools.end()) {
            return;
        }

        if (it->second.size() + 1 > Config->MaxEphemeralPoolsPerUser) {
            THROW_ERROR_EXCEPTION("Limit for number of ephemeral pools %v for user %v has been reached",
                Config->MaxEphemeralPoolsPerUser,
                userName);
        }
    }

    void DoValidateOperationStart(const TOperationPtr& operation)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        ValidateOperationCountLimit(operation);
        ValidateEphemeralPoolLimit(operation);

        auto immediateParentPool = FindPool(GetOperationPoolName(operation));
        // NB: Check is not performed if operation is started in default or unknown pool.
        if (immediateParentPool && immediateParentPool->AreImmediateOperationsFobidden()) {
            THROW_ERROR_EXCEPTION(
                "Starting operations immediately in pool %Qv is forbidden",
                immediateParentPool->GetId());
        }

        auto parentElement = GetParentElement(operation);
        auto poolPath = GetPoolPath(parentElement);
        const auto& user = operation->GetAuthenticatedUser();

        Host->ValidatePoolPermission(poolPath, user, EPermission::Use);
    }

    void ProfileOperationElement(TOperationElementPtr element) const
    {
        auto poolTag = element->GetParent()->GetProfilingTag();
        auto slotIndexTag = GetSlotIndexProfilingTag(element->GetSlotIndex());

        ProfileSchedulerElement(element, "/operations", {poolTag, slotIndexTag});
    }

    void ProfileCompositeSchedulerElement(TCompositeSchedulerElementPtr element) const
    {
        auto tag = element->GetProfilingTag();
        ProfileSchedulerElement(element, "/pools", {tag});

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

    TRootElementSnapshotPtr GetRootSnapshot() const
    {
        TReaderGuard guard(RootElementSnapshotLock);
        return RootElementSnapshot;
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
        , LastProfilingTime_(TInstant::Zero())
    {
        FairShareTree_ = New<TFairShareTree>(Config, Config, Host, FeasibleInvokers);

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

    virtual void StartPeriodicActivity() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareLoggingExecutor_->Start();
        FairShareUpdateExecutor_->Start();
        MinNeededJobResourcesUpdateExecutor_->Start();
    }

    virtual void ResetState() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareLoggingExecutor_->Stop();
        FairShareUpdateExecutor_->Stop();
        MinNeededJobResourcesUpdateExecutor_->Stop();

        // Do fair share update in order to rebuild trees snapshots
        // to drop references to old nodes.
        OnFairShareUpdate();

        FairShareTree_->ResetState();
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
                state->GetController()->InvokeMinNeededJobResourcesUpdate();
            }
        }

        LOG_INFO("Min needed job resources successfully updated");
    }

    void OnFairShareLogging()
    {
        OnFairShareLoggingAt(TInstant::Now());
    }

    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return FairShareTree_->ScheduleJobs(schedulingContext);
    }

    virtual void RegisterOperation(const TOperationPtr& operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto spec = ParseSpec(operation, operation->GetSpec());
        auto state = New<TFairShareStrategyOperationState>(operation);

        YCHECK(OperationIdToOperationState_.insert(
            std::make_pair(operation->GetId(), state)).second);

        auto registrationResult = FairShareTree_->RegisterOperation(state, spec);
        ActivateOperations(registrationResult.OperationsToActivate);
    }

    virtual void UnregisterOperation(const TOperationPtr& operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto state = GetOperationState(operation->GetId());
        auto unregistrationResult = FairShareTree_->UnregisterOperation(state);
        ActivateOperations(unregistrationResult.OperationsToActivate);
        YCHECK(OperationIdToOperationState_.erase(operation->GetId()) == 1);
    }

    virtual void UpdatePools(const INodePtr& poolsNode) override
    {
        FairShareTree_->UpdatePools(poolsNode);
    }

    virtual void BuildOperationAttributes(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->BuildOperationAttributes(operationId, consumer);
    }

    virtual void BuildOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->BuildOperationProgress(operationId, consumer);
    }

    virtual void BuildBriefOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->BuildBriefOperationProgress(operationId, consumer);
    }

    virtual void BuildBriefSpec(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->BuildBriefSpec(operationId, consumer);
    }

    virtual void UpdateConfig(const TFairShareStrategyConfigPtr& config) override
    {
        FairShareTree_->UpdateConfig(config);
        FairShareTree_->UpdateControllerConfig(config);

        FairShareUpdateExecutor_->SetPeriod(Config->FairShareUpdatePeriod);
        FairShareLoggingExecutor_->SetPeriod(Config->FairShareLogPeriod);
        MinNeededJobResourcesUpdateExecutor_->SetPeriod(Config->MinNeededResourcesUpdatePeriod);
    }

    virtual void BuildOperationInfoForEventLog(const TOperationPtr& operation, NYson::IYsonConsumer* consumer)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->BuildOperationInfoForEventLog(operation, consumer);
    }

    virtual void UpdateOperationRuntimeParams(
        const TOperationPtr& operation,
        const INodePtr& update) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->UpdateOperationRuntimeParams(operation, update);
    }

    virtual TString GetOperationLoggingProgress(const TOperationId& operationId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        return FairShareTree_->GetOperationLoggingProgress(operationId);
    }

    virtual void BuildOrchid(IYsonConsumer* consumer) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->BuildOrchid(consumer);
    }

    virtual void ApplyJobMetricsDelta(
        const TOperationId& operationId,
        const TJobMetrics& jobMetricsDelta) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        FairShareTree_->ApplyJobMetricsDelta(operationId, jobMetricsDelta);
    }

    virtual TFuture<void> ValidateOperationStart(const TOperationPtr& operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        return FairShareTree_->ValidateOperationStart(operation);
    }

    virtual void ValidateOperationCanBeRegistered(const TOperationPtr& operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->ValidateOperationCanBeRegistered(operation);
    }

    // NB: This function is public for testing purposes.
    virtual void OnFairShareUpdateAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->OnFairShareUpdateAt(now);

        if (LastProfilingTime_ + Config->FairShareProfilingPeriod < now) {
            LastProfilingTime_ = now;
            FairShareTree_->ProfileFairShare();
        }
    }

    virtual void OnFairShareEssentialLoggingAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->OnFairShareEssentialLoggingAt(now);
    }

    virtual void OnFairShareLoggingAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->OnFairShareLoggingAt(now);
    }

    virtual void ProcessUpdatedAndCompletedJobs(
        std::vector<TUpdatedJob>* updatedJobs,
        std::vector<TCompletedJob>* completedJobs) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        FairShareTree_->ProcessUpdatedAndCompletedJobs(updatedJobs, completedJobs);
    }

    virtual void RegisterJobs(const TOperationId& operationId, const std::vector<TJobPtr>& jobs) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareTree_->RegisterJobs(operationId, jobs);
    }

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* const Host;

    std::vector<IInvokerPtr> FeasibleInvokers;

    TPeriodicExecutorPtr FairShareUpdateExecutor_;
    TPeriodicExecutorPtr FairShareLoggingExecutor_;
    TPeriodicExecutorPtr MinNeededJobResourcesUpdateExecutor_;

    using TFairShareTreePtr = TIntrusivePtr<TFairShareTree>;
    TFairShareTreePtr FairShareTree_;

    THashMap<TOperationId, TFairShareStrategyOperationStatePtr> OperationIdToOperationState_;

    TInstant LastProfilingTime_;

    TFairShareStrategyOperationStatePtr GetOperationState(const TOperationId& operationId) const
    {
        auto it = OperationIdToOperationState_.find(operationId);
        YCHECK(it != OperationIdToOperationState_.end());
        return it->second;
    }

    void ActivateOperations(const std::vector<TOperationId>& operationIds) const
    {
        for (const auto& operationId : operationIds) {
            auto state = GetOperationState(operationId);
            state->GetController()->InvokeMinNeededJobResourcesUpdate();
            Host->ActivateOperation(operationId);
        }
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

