#include "fair_share_strategy.h"

#include "fair_share_tree.h"
#include "fair_share_tree_element.h"
#include "fair_share_tree_snapshot.h"
#include "persistent_scheduler_state.h"
#include "public.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "fair_share_strategy_operation_controller.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/helpers.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>
#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ytree/service_combiner.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategy
    : public ISchedulerStrategy
    , public IFairShareTreeHost
{
public:
    TFairShareStrategy(
        TFairShareStrategyConfigPtr config,
        ISchedulerStrategyHost* host,
        std::vector<IInvokerPtr> feasibleInvokers)
        : Config_(std::move(config))
        , Host_(host)
        , FeasibleInvokers_(std::move(feasibleInvokers))
        , Logger(StrategyLogger)
    {
        FairShareProfilingExecutor_ = New<TPeriodicExecutor>(
            Host_->GetFairShareProfilingInvoker(),
            BIND(&TFairShareStrategy::OnFairShareProfiling, MakeWeak(this)),
            Config_->FairShareProfilingPeriod);

        FairShareUpdateExecutor_ = New<TPeriodicExecutor>(
            Host_->GetControlInvoker(EControlQueue::FairShareStrategy),
            BIND(&TFairShareStrategy::OnFairShareUpdate, MakeWeak(this)),
            Config_->FairShareUpdatePeriod);

        FairShareLoggingExecutor_ = New<TPeriodicExecutor>(
            Host_->GetFairShareLoggingInvoker(),
            BIND(&TFairShareStrategy::OnFairShareLogging, MakeWeak(this)),
            Config_->FairShareLogPeriod);

        AccumulatedUsageLoggingExecutor_ = New<TPeriodicExecutor>(
            Host_->GetFairShareLoggingInvoker(),
            BIND(&TFairShareStrategy::OnLogAccumulatedUsage, MakeWeak(this)),
            Config_->AccumulatedUsageLogPeriod);

        MinNeededAllocationResourcesUpdateExecutor_ = New<TPeriodicExecutor>(
            Host_->GetControlInvoker(EControlQueue::FairShareStrategy),
            BIND(&TFairShareStrategy::OnMinNeededAllocationResourcesUpdate, MakeWeak(this)),
            Config_->MinNeededResourcesUpdatePeriod);

        ResourceMeteringExecutor_ = New<TPeriodicExecutor>(
            Host_->GetControlInvoker(EControlQueue::Metering),
            BIND(&TFairShareStrategy::OnBuildResourceMetering, MakeWeak(this)),
            Config_->ResourceMeteringPeriod);

        ResourceUsageUpdateExecutor_ = New<TPeriodicExecutor>(
            Host_->GetFairShareUpdateInvoker(),
            BIND(&TFairShareStrategy::OnUpdateResourceUsages, MakeWeak(this)),
            Config_->ResourceUsageSnapshotUpdatePeriod);

        SchedulerTreeAlertsUpdateExecutor_ = New<TPeriodicExecutor>(
            Host_->GetControlInvoker(EControlQueue::CommonPeriodicActivity),
            BIND(&TFairShareStrategy::UpdateSchedulerTreeAlerts, MakeWeak(this)),
            Config_->SchedulerTreeAlertsUpdatePeriod);

        EphemeralPoolNameRegex_.emplace(Config_->EphemeralPoolNameRegex);
    }

    void OnUpdateResourceUsages()
    {
        auto snapshot = TreeSetSnapshot_.Acquire();
        if (!snapshot) {
            return;
        }
        for (const auto& tree : snapshot->Trees()) {
            tree->UpdateResourceUsages();
        }
    }

    void OnMasterHandshake(const TMasterHandshakeResult& result) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        LastMeteringStatisticsUpdateTime_ = result.LastMeteringLogTime;
        ConnectionTime_ = TInstant::Now();
    }

    void OnMasterConnected() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        Connected_ = true;

        FairShareProfilingExecutor_->Start();
        FairShareUpdateExecutor_->Start();
        FairShareLoggingExecutor_->Start();
        AccumulatedUsageLoggingExecutor_->Start();
        MinNeededAllocationResourcesUpdateExecutor_->Start();
        ResourceMeteringExecutor_->Start();
        ResourceUsageUpdateExecutor_->Start();
        SchedulerTreeAlertsUpdateExecutor_->Start();
    }

    void OnMasterDisconnected() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        Connected_ = false;

        YT_UNUSED_FUTURE(FairShareProfilingExecutor_->Stop());
        YT_UNUSED_FUTURE(FairShareUpdateExecutor_->Stop());
        YT_UNUSED_FUTURE(YT_UNUSED_FUTURE(FairShareLoggingExecutor_->Stop()));
        YT_UNUSED_FUTURE(AccumulatedUsageLoggingExecutor_->Stop());
        YT_UNUSED_FUTURE(MinNeededAllocationResourcesUpdateExecutor_->Stop());
        YT_UNUSED_FUTURE(ResourceMeteringExecutor_->Stop());
        YT_UNUSED_FUTURE(ResourceUsageUpdateExecutor_->Stop());
        YT_UNUSED_FUTURE(SchedulerTreeAlertsUpdateExecutor_->Stop());

        OperationIdToOperationState_.clear();
        TreeSetSnapshot_.Reset();
        IdToTree_.clear();
        Initialized_ = false;
        DefaultTreeId_.reset();
        NodeIdToDescriptor_.clear();
        NodeAddresses_.clear();
        NodeIdsPerTree_.clear();
        NodeIdsWithoutTree_.clear();
        LastPoolTreesYson_ = {};
        LastTemplatePoolTreeConfigMapYson_ = {};
        TreeAlerts_ = {};
    }

    void OnFairShareProfiling()
    {
        OnFairShareProfilingAt(TInstant::Now());
    }

    void OnFairShareUpdate()
    {
        OnFairShareUpdateAt(TInstant::Now());
    }

    void OnMinNeededAllocationResourcesUpdate()
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_LOG_INFO("Starting min needed allocation resources update");

        for (const auto& [operationId, state] : OperationIdToOperationState_) {
            auto maybeUnschedulableReason = state->GetHost()->CheckUnschedulable();
            if (!maybeUnschedulableReason || maybeUnschedulableReason == EUnschedulableReason::NoPendingAllocations) {
                state->GetController()->UpdateMinNeededAllocationResources();
            }
        }

        YT_LOG_INFO("Min needed allocation resources successfully updated");
    }

    void OnFairShareLogging()
    {
        OnFairShareLoggingAt(TInstant::Now());
    }

    void OnLogAccumulatedUsage()
    {
        VERIFY_INVOKER_AFFINITY(Host_->GetFairShareLoggingInvoker());

        TForbidContextSwitchGuard contextSwitchGuard;

        auto snapshot = TreeSetSnapshot_.Acquire();
        if (!snapshot) {
            return;
        }
        for (const auto& tree : snapshot->Trees()) {
            tree->LogAccumulatedUsage();
        }
    }

    INodeHeartbeatStrategyProxyPtr CreateNodeHeartbeatStrategyProxy(
        TNodeId nodeId,
        const TString& address,
        const TBooleanFormulaTags& tags,
        TMatchingTreeCookie cookie) const override
    {
        auto [tree, newCookie] = FindTreeForNodeWithCookie(address, tags, cookie);
        return New<TNodeHeartbeatStrategyProxy>(nodeId, tree, newCookie);
    }

    void RegisterOperation(
        IOperationStrategyHost* operation,
        std::vector<TString>* unknownTreeIds,
        TPoolTreeControllerSettingsMap* poolTreeControllerSettingsMap) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_VERIFY(unknownTreeIds->empty());

        auto treeIdToPoolNameMap = GetOperationPools(operation->GetRuntimeParameters());
        for (const auto& [treeId, _] : treeIdToPoolNameMap) {
            if (!FindTree(treeId)) {
                unknownTreeIds->push_back(treeId);
            }
        }
        for (const auto& treeId : *unknownTreeIds) {
            treeIdToPoolNameMap.erase(treeId);
        }
        auto state = New<TFairShareStrategyOperationState>(operation, Config_, Host_->GetNodeShardInvokers().size());
        state->TreeIdToPoolNameMap() = std::move(treeIdToPoolNameMap);

        YT_VERIFY(OperationIdToOperationState_.insert(
            std::pair(operation->GetId(), state)).second);

        auto runtimeParameters = operation->GetRuntimeParameters();
        for (const auto& [treeName, poolName] : state->TreeIdToPoolNameMap()) {
            const auto& treeParams = GetOrCrash(runtimeParameters->SchedulingOptionsPerPoolTree, treeName);
            auto tree = GetTree(treeName);

            auto registrationResult = tree->RegisterOperation(state, operation->GetStrategySpecForTree(treeName), treeParams);

            poolTreeControllerSettingsMap->emplace(
                treeName,
                TPoolTreeControllerSettings{
                    .SchedulingTagFilter = tree->GetNodesFilter(),
                    .Tentative = GetSchedulingOptionsPerPoolTree(state->GetHost(), treeName)->Tentative,
                    .Probing = GetSchedulingOptionsPerPoolTree(state->GetHost(), treeName)->Probing,
                    .Offloading = GetSchedulingOptionsPerPoolTree(state->GetHost(), treeName)->Offloading,
                    .MainResource = tree->GetConfig()->MainResource,
                    .AllowIdleCpuPolicy = registrationResult.AllowIdleCpuPolicy,
                });
        }
    }

    void UnregisterOperation(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& state = GetOperationState(operation->GetId());
        for (const auto& [treeId, poolName] : state->TreeIdToPoolNameMap()) {
            DoUnregisterOperationFromTree(state, treeId);
        }

        EraseOrCrash(OperationIdToOperationState_, operation->GetId());
    }

    void UnregisterOperationFromTree(TOperationId operationId, const TString& treeId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& state = GetOperationState(operationId);

        YT_VERIFY(state->TreeIdToPoolNameMap().contains(treeId));

        DoUnregisterOperationFromTree(state, treeId);

        EraseOrCrash(state->TreeIdToPoolNameMap(), treeId);

        YT_LOG_INFO("Operation removed from a tree (OperationId: %v, TreeId: %v)", operationId, treeId);
    }

    void DoUnregisterOperationFromTree(const TFairShareStrategyOperationStatePtr& operationState, const TString& treeId)
    {
        auto tree = GetTree(treeId);
        tree->UnregisterOperation(operationState);
        tree->ProcessActivatableOperations();
    }

    void DisableOperation(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationId = operation->GetId();
        const auto& state = GetOperationState(operationId);
        for (const auto& [treeId, poolName] : state->TreeIdToPoolNameMap()) {
            if (auto tree = GetTree(treeId);
                tree->HasOperation(operationId))
            {
                tree->DisableOperation(state);
            }
        }
        state->SetEnabled(false);
    }

    void UpdatePoolTrees(const TYsonString& poolTreesYson) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (Config_->EnablePoolTreesConfigCache && poolTreesYson == LastPoolTreesYson_ && ConvertToYsonString(Config_->TemplatePoolTreeConfigMap) == LastTemplatePoolTreeConfigMapYson_) {
            YT_LOG_INFO("Pool trees and pools did not change, skipping update");
            return;
        }

        LastPoolTreesYson_ = {};
        LastTemplatePoolTreeConfigMapYson_ = {};

        auto templatePoolTreeConfigMap = Config_->TemplatePoolTreeConfigMap;

        INodePtr poolTreesNode;
        try {
            auto future =
                BIND([&] {
                    return ConvertToNode(poolTreesYson);
                })
                .AsyncVia(Host_->GetBackgroundInvoker())
                .Run();
            poolTreesNode = WaitFor(future)
                .ValueOrThrow();
        } catch (const std::exception& ex) {
            auto error = TError(EErrorCode::WatcherHandlerFailed, "Error parsing pool trees")
                << ex;
            THROW_ERROR(error);
        }

        std::vector<TOperationId> orphanedOperationIds;
        std::vector<TOperationId> changedOperationIds;
        TError error;
        {
            // No context switches allowed while fair share trees update.
            TForbidContextSwitchGuard contextSwitchGuard;

            YT_LOG_INFO("Updating pool trees");

            if (poolTreesNode->GetType() != NYTree::ENodeType::Map) {
                error = TError(EErrorCode::WatcherHandlerFailed, "Pool trees node has invalid type")
                    << TErrorAttribute("expected_type", NYTree::ENodeType::Map)
                    << TErrorAttribute("actual_type", poolTreesNode->GetType());
                THROW_ERROR(error);
            }

            auto poolsMap = poolTreesNode->AsMap();

            std::vector<TError> errors;

            // Collect trees to add and remove.
            THashSet<TString> treeIdsToAdd;
            THashSet<TString> treeIdsToRemove;
            THashSet<TString> treeIdsWithChangedFilter;
            THashMap<TString, TSchedulingTagFilter> treeIdToFilter;
            CollectTreeChanges(poolsMap, &treeIdsToAdd, &treeIdsToRemove, &treeIdsWithChangedFilter, &treeIdToFilter);

            YT_LOG_INFO("Pool trees collected to update (TreeIdsToAdd: %v, TreeIdsToRemove: %v, TreeIdsWithChangedFilter: %v)",
                treeIdsToAdd,
                treeIdsToRemove,
                treeIdsWithChangedFilter);

            // Populate trees map. New trees are not added to global map yet.
            auto idToTree = ConstructUpdatedTreeMap(
                poolsMap,
                treeIdsToAdd,
                treeIdsToRemove,
                templatePoolTreeConfigMap,
                &errors);

            // Check default tree pointer. It should point to some valid tree,
            // otherwise pool trees are not updated.
            auto defaultTreeId = poolsMap->Attributes().Find<TString>(DefaultTreeAttributeName);

            if (defaultTreeId && idToTree.find(*defaultTreeId) == idToTree.end()) {
                errors.emplace_back("Default tree is missing");
                error = TError(EErrorCode::WatcherHandlerFailed, "Error updating pool trees")
                    << std::move(errors);
                THROW_ERROR(error);
            }

            // Check that after adding or removing trees each node will belong exactly to one tree.
            // Check is skipped if trees configuration did not change.
            bool shouldCheckConfiguration = !treeIdsToAdd.empty() || !treeIdsToRemove.empty() || !treeIdsWithChangedFilter.empty();

            if (shouldCheckConfiguration && !CheckTreesConfiguration(treeIdToFilter, &errors)) {
                error = TError(EErrorCode::WatcherHandlerFailed, "Error updating pool trees")
                    << std::move(errors);
                THROW_ERROR(error);
            }

            // Update configs and pools structure of all trees.
            // NB: it updates already existing trees inplace.
            std::vector<TString> updatedTreeIds;
            UpdateTreesConfigs(
                poolsMap,
                idToTree,
                templatePoolTreeConfigMap,
                &errors,
                &updatedTreeIds);

            // Update node at scheduler.
            UpdateNodesOnChangedTrees(idToTree, treeIdsToAdd, treeIdsToRemove);

            // Remove trees from operation states.
            RemoveTreesFromOperationStates(treeIdsToRemove, &orphanedOperationIds, &changedOperationIds);

            // Updating default fair-share tree and global tree map.
            DefaultTreeId_ = defaultTreeId;
            std::swap(IdToTree_, idToTree);

            // Setting alerts.
            if (!errors.empty()) {
                error = TError(EErrorCode::WatcherHandlerFailed, "Error updating pool trees")
                    << std::move(errors);
            } else {
                if (!updatedTreeIds.empty() || !treeIdsToRemove.empty() || !treeIdsToAdd.empty()) {
                    Host_->LogEventFluently(&SchedulerEventLogger, ELogEventType::PoolsInfo)
                        .Item("pools").DoMapFor(IdToTree_, [&] (TFluentMap fluent, const auto& value) {
                            const auto& treeId = value.first;
                            const auto& tree = value.second;
                            fluent
                                .Item(treeId).Do(BIND(&IFairShareTree::BuildStaticPoolsInformation, tree));
                        });
                }
                YT_LOG_INFO("Pool trees updated");
            }
        }

        // Offload destroying pool trees node.
        Host_->GetBackgroundInvoker()->Invoke(BIND([poolTreesNode = std::move(poolTreesNode)] { }));

        // Invokes operation node flushes.
        FlushOperationNodes(changedOperationIds);

        // Perform abort of orphaned operations one by one.
        AbortOrphanedOperations(orphanedOperationIds);

        THROW_ERROR_EXCEPTION_IF_FAILED(error);

        LastPoolTreesYson_ = poolTreesYson;
        LastTemplatePoolTreeConfigMapYson_ = ConvertToYsonString(templatePoolTreeConfigMap);
    }

    TError UpdateUserToDefaultPoolMap(const THashMap<TString, TString>& userToDefaultPoolMap) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        std::vector<TError> errors;
        for (const auto& [_, tree] : IdToTree_) {
            auto error = tree->ValidateUserToDefaultPoolMap(userToDefaultPoolMap);
            if (!error.IsOK()) {
                errors.push_back(error);
            }
        }

        TError result;
        if (!errors.empty()) {
            result = TError("Error updating mapping from user to default parent pool")
                << std::move(errors);
        } else {
            for (const auto& [_, tree] : IdToTree_) {
                tree->ActualizeEphemeralPoolParents(userToDefaultPoolMap);
            }
        }

        Host_->SetSchedulerAlert(ESchedulerAlertType::UpdateUserToDefaultPoolMap, result);
        return result;
    }

    void BuildOperationProgress(TOperationId operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (!FindOperationState(operationId)) {
            return;
        }

        DoBuildOperationProgress(&IFairShareTree::BuildOperationProgress, operationId, fluent);
    }

    void BuildBriefOperationProgress(TOperationId operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (!FindOperationState(operationId)) {
            return;
        }

        DoBuildOperationProgress(&IFairShareTree::BuildBriefOperationProgress, operationId, fluent);
    }

    std::vector<std::pair<TOperationId, TError>> GetHungOperations() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        std::vector<std::pair<TOperationId, TError>> result;
        for (const auto& [operationId, operationState] : OperationIdToOperationState_) {
            if (operationState->TreeIdToPoolNameMap().empty()) {
                // This operation is orphaned and will be aborted.
                continue;
            }

            bool hasNonHungTree = false;
            TError operationError("Operation scheduling hung in all trees");

            for (const auto& treePoolPair : operationState->TreeIdToPoolNameMap()) {
                const auto& treeName = treePoolPair.first;
                auto error = GetTree(treeName)->CheckOperationIsHung(
                    operationId,
                    Config_->OperationHangupSafeTimeout,
                    Config_->OperationHangupMinScheduleAllocationAttempts,
                    Config_->OperationHangupDeactivationReasons,
                    Config_->OperationHangupDueToLimitingAncestorSafeTimeout);
                if (error.IsOK()) {
                    hasNonHungTree = true;
                    break;
                } else {
                    operationError.MutableInnerErrors()->push_back(error);
                }
            }

            if (!hasNonHungTree) {
                result.emplace_back(operationId, operationError);
            }
        }
        return result;
    }

    void UpdateConfig(const TFairShareStrategyConfigPtr& config) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        Config_ = config;

        for (const auto& [treeId, tree] : IdToTree_) {
            tree->UpdateControllerConfig(Config_);
        }

        for (const auto& [operationId, operationState] : OperationIdToOperationState_) {
            operationState->UpdateConfig(Config_);
        }

        FairShareProfilingExecutor_->SetPeriod(Config_->FairShareProfilingPeriod);
        FairShareUpdateExecutor_->SetPeriod(Config_->FairShareUpdatePeriod);
        FairShareLoggingExecutor_->SetPeriod(Config_->FairShareLogPeriod);
        AccumulatedUsageLoggingExecutor_->SetPeriod(Config_->AccumulatedUsageLogPeriod);
        MinNeededAllocationResourcesUpdateExecutor_->SetPeriod(Config_->MinNeededResourcesUpdatePeriod);
        ResourceMeteringExecutor_->SetPeriod(Config_->ResourceMeteringPeriod);
        ResourceUsageUpdateExecutor_->SetPeriod(Config_->ResourceUsageSnapshotUpdatePeriod);
        SchedulerTreeAlertsUpdateExecutor_->SetPeriod(Config_->SchedulerTreeAlertsUpdatePeriod);

        EphemeralPoolNameRegex_.emplace(Config_->EphemeralPoolNameRegex);
        if (!EphemeralPoolNameRegex_->ok()) {
            THROW_ERROR_EXCEPTION("Bad ephemeral pool name regular expression provided in scheduler config")
                << TErrorAttribute("regex", Config_->EphemeralPoolNameRegex);
        }
    }

    void BuildOperationInfoForEventLog(const IOperationStrategyHost* operation, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& operationState = GetOperationState(operation->GetId());
        const auto& pools = operationState->TreeIdToPoolNameMap();
        auto accumulatedUsagePerTree = ExtractAccumulatedUsageForLogging(operation->GetId());

        fluent
            .DoIf(DefaultTreeId_.operator bool(), [&] (TFluentMap fluent) {
                auto it = pools.find(*DefaultTreeId_);
                if (it != pools.end()) {
                    fluent
                        .Item("pool").Value(it->second.GetPool());
                }
            })
            .Item("scheduling_info_per_tree").DoMapFor(pools, [&] (TFluentMap fluent, const auto& pair) {
                const auto& [treeId, poolName] = pair;
                auto tree = GetTree(treeId);
                fluent
                    .Item(treeId).BeginMap()
                        .Do(std::bind(&IFairShareTree::BuildOperationAttributes, tree, operation->GetId(), std::placeholders::_1))
                    .EndMap();
            })
            .Item("accumulated_resource_usage_per_tree").Value(accumulatedUsagePerTree);
    }

    void ApplyOperationRuntimeParameters(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto state = GetOperationState(operation->GetId());
        const auto runtimeParameters = operation->GetRuntimeParameters();

        auto newPools = GetOperationPools(operation->GetRuntimeParameters());

        YT_VERIFY(newPools.size() == state->TreeIdToPoolNameMap().size());

        for (const auto& [treeId, oldPool] : state->TreeIdToPoolNameMap()) {
            const auto& newPool = GetOrCrash(newPools, treeId);
            auto tree = GetTree(treeId);
            if (oldPool.GetPool() != newPool.GetPool()) {
                tree->ChangeOperationPool(operation->GetId(), newPool, /*ensureRunning*/ true);
            }

            const auto& treeParams = GetOrCrash(runtimeParameters->SchedulingOptionsPerPoolTree, treeId);
            tree->UpdateOperationRuntimeParameters(operation->GetId(), treeParams);
        }
        state->TreeIdToPoolNameMap() = newPools;
    }

    void InitOperationRuntimeParameters(
        const TOperationRuntimeParametersPtr& runtimeParameters,
        const TOperationSpecBasePtr& spec,
        const TString& user,
        EOperationType operationType,
        TOperationId operationId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto poolTrees = ParsePoolTrees(spec, operationType);
        for (const auto& poolTreeDescription : poolTrees) {
            auto treeParams = New<TOperationFairShareTreeRuntimeParameters>();
            auto specIt = spec->SchedulingOptionsPerPoolTree.find(poolTreeDescription.Name);
            if (specIt != spec->SchedulingOptionsPerPoolTree.end()) {
                treeParams->Weight = specIt->second->Weight ? specIt->second->Weight : spec->Weight;
                treeParams->Pool = GetTree(poolTreeDescription.Name)->CreatePoolName(specIt->second->Pool ? specIt->second->Pool : spec->Pool, user);
                treeParams->ResourceLimits = specIt->second->ResourceLimits->IsNonTrivial() ? specIt->second->ResourceLimits : spec->ResourceLimits;
            } else {
                treeParams->Weight = spec->Weight;
                treeParams->Pool = GetTree(poolTreeDescription.Name)->CreatePoolName(spec->Pool, user);
                treeParams->ResourceLimits = spec->ResourceLimits;
            }
            treeParams->Tentative = poolTreeDescription.Tentative;
            treeParams->Probing = poolTreeDescription.Probing;
            EmplaceOrCrash(runtimeParameters->SchedulingOptionsPerPoolTree, poolTreeDescription.Name, std::move(treeParams));
        }

        for (const auto& [treeName, options] : runtimeParameters->SchedulingOptionsPerPoolTree) {
            const auto& offloadingSettings = GetTree(treeName)->GetOffloadingSettingsFor(options->Pool.GetSpecifiedPoolName());
            if (!offloadingSettings.empty() && !spec->SchedulingTagFilter.IsEmpty()) {
                YT_LOG_DEBUG("Ignoring offloading since operation has scheduling tag filter (SchedulingTagFilter: %v, OperationId: %v)",
                    spec->SchedulingTagFilter.GetFormula(),
                    operationId);
            } else {
                for (const auto& [offloadingPoolTreeName, offloadingPoolSettings] : offloadingSettings) {
                    if (runtimeParameters->SchedulingOptionsPerPoolTree.contains(offloadingPoolTreeName)) {
                        YT_LOG_DEBUG("Ignoring offloading pool since offloading pool tree is already used (OffloadingTreeId: %v, OffloadingPool: %v, OperationId: %v)",
                            offloadingPoolTreeName,
                            offloadingPoolSettings->Pool,
                            operationId);
                    } else {
                        auto tree = FindTree(offloadingPoolTreeName);
                        if (!tree) {
                            YT_LOG_DEBUG("Ignoring offloading pool since offloading pool tree does not exist (OffloadingTreeId: %v, OffloadingPool: %v, OperationId: %v)",
                                offloadingPoolTreeName,
                                offloadingPoolSettings->Pool,
                                operationId);
                        } else {
                            auto treeParams = New<TOperationFairShareTreeRuntimeParameters>();
                            treeParams->Weight = offloadingPoolSettings->Weight;
                            treeParams->Pool = tree->CreatePoolName(offloadingPoolSettings->Pool, user);
                            treeParams->Tentative = offloadingPoolSettings->Tentative;
                            treeParams->ResourceLimits = offloadingPoolSettings->ResourceLimits;
                            treeParams->Offloading = true;
                            EmplaceOrCrash(runtimeParameters->SchedulingOptionsPerPoolTree, offloadingPoolTreeName, std::move(treeParams));
                        }
                    }
                }
            }
        }
    }

    void UpdateRuntimeParameters(
        const TOperationRuntimeParametersPtr& origin,
        const TOperationRuntimeParametersUpdatePtr& update,
        const TString& user) override
    {
        YT_VERIFY(origin);

        for (auto& [poolTree, treeParams] : origin->SchedulingOptionsPerPoolTree) {
            std::optional<TString> newPoolName = update->Pool;
            auto treeUpdateIt = update->SchedulingOptionsPerPoolTree.find(poolTree);
            if (treeUpdateIt != update->SchedulingOptionsPerPoolTree.end()) {
                newPoolName = treeUpdateIt->second->Pool;
                treeParams = UpdateFairShareTreeRuntimeParameters(treeParams, treeUpdateIt->second);
            }

            // NB: root level attributes has higher priority.
            if (update->Weight) {
                treeParams->Weight = *update->Weight;
            }
            if (newPoolName) {
                treeParams->Pool = GetTree(poolTree)->CreatePoolName(*newPoolName, user);
            }
        }
    }

    TFuture<void> ValidateOperationRuntimeParameters(
        IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters,
        bool validatePools) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& state = GetOperationState(operation->GetId());

        for (const auto& [treeId, schedulingOptions] : runtimeParameters->SchedulingOptionsPerPoolTree) {
            auto poolTrees = state->TreeIdToPoolNameMap();
            if (poolTrees.find(treeId) == poolTrees.end()) {
                THROW_ERROR_EXCEPTION("Pool tree %Qv was not configured for this operation", treeId);
            }
        }

        if (validatePools) {
            return ValidateOperationPoolsCanBeUsed(operation, runtimeParameters);
        } else {
            return VoidFuture;
        }
    }

    void ValidatePoolLimitsOnPoolChange(
        IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto pools = GetOperationPools(runtimeParameters);
        for (const auto& [treeId, pool] : pools) {
            auto tree = GetTree(treeId);
            tree->ValidatePoolLimitsOnPoolChange(operation, pool);
        }
    }

    IYPathServicePtr GetOrchidService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto dynamicOrchidService = New<TCompositeMapService>();
        dynamicOrchidService->AddChild("pool_trees", New<TPoolTreeService>(this));
        return dynamicOrchidService;
    }

    void BuildOrchid(TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        // Snapshot list of treeIds.
        std::vector<TString> treeIds;
        treeIds.reserve(std::size(IdToTree_));
        for (auto [treeId, _] : IdToTree_) {
            treeIds.push_back(treeId);
        }

        fluent
            // COMPAT(ignat)
            .OptionalItem("default_fair_share_tree", DefaultTreeId_)
            .OptionalItem("default_pool_tree", DefaultTreeId_)
            .Item("last_metering_statistics_update_time").Value(LastMeteringStatisticsUpdateTime_)
            .Item("scheduling_info_per_pool_tree").DoMapFor(treeIds, [&] (TFluentMap fluent, const TString& treeId) {
                auto tree = FindTree(treeId);
                if (!tree) {
                    return;
                }

                fluent
                    .Item(treeId).BeginMap()
                        .Do(BIND(&TFairShareStrategy::BuildTreeOrchid, MakeStrong(this), tree))
                    .EndMap();
            });
    }

    void ApplyJobMetricsDelta(TOperationIdToOperationJobMetrics operationIdToOperationJobMetrics) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        TForbidContextSwitchGuard contextSwitchGuard;

        THashMap<TString, IFairShareTreePtr> idToTree;
        if (auto snapshot = TreeSetSnapshot_.Acquire()) {
            idToTree = snapshot->BuildIdToTreeMapping();
        } else {
            return;
        }

        THashMap<TString, THashMap<TOperationId, TJobMetrics>> treeIdToJobMetricDeltas;

        for (auto& [operationId, metricsPerTree] : operationIdToOperationJobMetrics) {
            for (auto& metrics : metricsPerTree) {
                auto treeIt = idToTree.find(metrics.TreeId);
                if (treeIt == idToTree.end()) {
                    continue;
                }

                const auto& state = GetOperationState(operationId);
                if (state->GetHost()->IsTreeErased(metrics.TreeId)) {
                    continue;
                }

                treeIdToJobMetricDeltas[metrics.TreeId].emplace(operationId, std::move(metrics.Metrics));
            }
        }

        for (auto& [treeId, jobMetricsPerOperation] : treeIdToJobMetricDeltas) {
            GetOrCrash(idToTree, treeId)->ApplyJobMetricsDelta(std::move(jobMetricsPerOperation));
        }
    }

    TFuture<void> ValidateOperationStart(const IOperationStrategyHost* operation) override
    {
        return ValidateOperationPoolsCanBeUsed(operation, operation->GetRuntimeParameters());
    }

    THashMap<TString, TError> GetPoolLimitViolations(
        const IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto pools = GetOperationPools(runtimeParameters);

        THashMap<TString, TError> result;

        for (const auto& [treeId, pool] : pools) {
            auto tree = GetTree(treeId);
            try {
                tree->ValidatePoolLimits(operation, pool);
            } catch (TErrorException& ex) {
                result.emplace(treeId, std::move(ex.Error()));
            }
        }

        return result;
    }

    void OnFairShareProfilingAt(TInstant /*now*/) override
    {
        VERIFY_INVOKER_AFFINITY(Host_->GetFairShareProfilingInvoker());

        TForbidContextSwitchGuard contextSwitchGuard;

        auto snapshot = TreeSetSnapshot_.Acquire();
        if (!snapshot) {
            return;
        }
        for (const auto& tree : snapshot->Trees()) {
            tree->ProfileFairShare();
        }
    }

    // NB: This function is public for testing purposes.
    void OnFairShareUpdateAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_LOG_INFO("Starting fair share update");

        std::vector<std::pair<TString, IFairShareTreePtr>> idToTree(IdToTree_.begin(), IdToTree_.end());
        std::sort(
            idToTree.begin(),
            idToTree.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.second->GetOperationCount() > rhs.second->GetOperationCount();
            });

        std::vector<TFuture<std::pair<IFairShareTreePtr, TError>>> futures;
        for (const auto& [treeId, tree] : idToTree) {
            futures.push_back(tree->OnFairShareUpdateAt(now));
        }

        auto resultsOrError = WaitFor(AllSucceeded(futures));
        if (!resultsOrError.IsOK()) {
            Host_->Disconnect(resultsOrError);
            return;
        }

        if (auto delay = Config_->StrategyTestingOptions->DelayInsideFairShareUpdate) {
            TDelayedExecutor::WaitForDuration(*delay);
        }

        std::vector<IFairShareTreePtr> snapshottedTrees;
        std::vector<TError> errors;

        const auto& results = resultsOrError.Value();
        for (const auto& [updatedTree, error] : results) {
            snapshottedTrees.push_back(updatedTree);
            if (!error.IsOK()) {
                errors.push_back(error);
            }
        }

        {
            // NB(eshcherbin): Make sure that snapshotted mapping in strategy and snapshots in trees are updated atomically.
            // This is necessary to maintain consistency between strategy and trees.
            TForbidContextSwitchGuard guard;

            std::sort(
                snapshottedTrees.begin(),
                snapshottedTrees.end(),
                [] (const auto& lhs, const auto& rhs) {
                    return lhs->GetId() < rhs->GetId();
                });

            TTreeSetTopology treeSetTopology;
            treeSetTopology.reserve(snapshottedTrees.size());
            for (const auto& tree : snapshottedTrees) {
                tree->FinishFairShareUpdate();
                treeSetTopology.emplace_back(tree->GetId(), tree->GetConfig()->NodesFilter);
            }

            if (treeSetTopology != TreeSetTopology_) {
                ++TreeSetTopologyVersion_;
                TreeSetTopology_ = treeSetTopology;
            }

            auto treeSetSnaphot = New<TFairShareTreeSetSnapshot>(
                std::move(snapshottedTrees),
                TreeSetTopologyVersion_);
            TreeSetSnapshot_.Store(treeSetSnaphot);

            YT_LOG_DEBUG("Stored updated fair share tree snapshots");
        }

        if (!errors.empty()) {
            auto error = TError("Found pool configuration issues during fair share update")
                << std::move(errors);
            Host_->SetSchedulerAlert(ESchedulerAlertType::UpdateFairShare, error);
        } else {
            Host_->SetSchedulerAlert(ESchedulerAlertType::UpdateFairShare, TError());
        }

        // TODO(eshcherbin): Decouple storing persistent state from fair share update?
        Host_->InvokeStoringStrategyState(BuildPersistentState());

        YT_LOG_INFO("Fair share successfully updated");
    }

    void OnFairShareEssentialLoggingAt(TInstant now) override
    {
        VERIFY_INVOKER_AFFINITY(Host_->GetFairShareLoggingInvoker());

        TForbidContextSwitchGuard contextSwitchGuard;

        auto snapshot = TreeSetSnapshot_.Acquire();
        if (!snapshot) {
            return;
        }
        for (const auto& tree : snapshot->Trees()) {
            tree->EssentialLogFairShareAt(now);
        }
    }

    void OnFairShareLoggingAt(TInstant now) override
    {
        VERIFY_INVOKER_AFFINITY(Host_->GetFairShareLoggingInvoker());

        TForbidContextSwitchGuard contextSwitchGuard;

        auto snapshot = TreeSetSnapshot_.Acquire();
        if (!snapshot) {
            return;
        }
        for (const auto& tree : snapshot->Trees()) {
            tree->LogFairShareAt(now);
        }
    }

    THashMap<TString, TResourceVolume> ExtractAccumulatedUsageForLogging(TOperationId operationId)
    {
        THashMap<TString, TResourceVolume> treeIdToUsage;
        const auto& state = GetOperationState(operationId);
        for (const auto& [treeId, _] : state->TreeIdToPoolNameMap()) {
            treeIdToUsage.emplace(treeId, GetTree(treeId)->ExtractAccumulatedUsageForLogging(operationId));
        }
        return treeIdToUsage;
    }

    void ProcessAllocationUpdates(
        const std::vector<TAllocationUpdate>& allocationUpdates,
        THashSet<TAllocationId>* allocationsToPostpone,
        THashMap<TAllocationId, EAbortReason>* allocationsToAbort) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(allocationsToPostpone->empty());
        YT_VERIFY(allocationsToAbort->empty());

        YT_LOG_DEBUG(
            "Processing allocation updates in strategy (UpdateCount: %v)",
            allocationUpdates.size());

        THashMap<TString, std::vector<TAllocationUpdate>> allocationUpdatesPerTree;
        for (const auto& allocationUpdate : allocationUpdates) {
            allocationUpdatesPerTree[allocationUpdate.TreeId].push_back(allocationUpdate);
        }

        THashMap<TString, IFairShareTreePtr> idToTree;
        if (auto snapshot = TreeSetSnapshot_.Acquire()) {
            idToTree = snapshot->BuildIdToTreeMapping();
        } else {
            return;
        }

        for (const auto& [treeId, treeAllocationUpdates] : allocationUpdatesPerTree) {
            auto it = idToTree.find(treeId);
            if (it == idToTree.end()) {
                for (const auto& allocationUpdate : treeAllocationUpdates) {
                    switch (allocationUpdate.Status) {
                        case EAllocationUpdateStatus::Running:
                            // Allocation is orphaned (does not belong to any tree), aborting it.
                            EmplaceOrCrash(*allocationsToAbort, allocationUpdate.AllocationId, EAbortReason::NonexistentPoolTree);
                            break;
                        case EAllocationUpdateStatus::Finished:
                            // Allocation is finished but tree does not exist, nothing to do.
                            YT_LOG_DEBUG(
                                "Dropping allocation update since pool tree is missing (OperationId: %v, AllocationId: %v)",
                                allocationUpdate.OperationId,
                                allocationUpdate.AllocationId);
                            break;
                        default:
                            YT_ABORT();
                    }
                }

                continue;
            }

            const auto& tree = it->second;
            tree->ProcessAllocationUpdates(treeAllocationUpdates, allocationsToPostpone, allocationsToAbort);
        }
    }

    void RegisterAllocationsFromRevivedOperation(TOperationId operationId, const std::vector<TAllocationPtr>& allocations) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        THashMap<TString, std::vector<TAllocationPtr>> allocationsByTreeId;
        for (const auto& allocation : allocations) {
            allocationsByTreeId[allocation->GetTreeId()].push_back(allocation);
        }

        for (auto&& [treeId, allocations] : allocationsByTreeId) {
            auto tree = FindTree(treeId);
            // NB: operation can be missing in tree since ban.
            if (tree && tree->HasOperation(operationId)) {
                tree->RegisterAllocationsFromRevivedOperation(operationId, std::move(allocations));
            } else {
                YT_LOG_INFO(
                    "Allocations are not registered in tree since operation is missing (OperationId: %v, TreeId: %v)",
                    operationId,
                    treeId);
            }
        }
    }

    void EnableOperation(IOperationStrategyHost* host) override
    {
        auto operationId = host->GetId();
        const auto& state = GetOperationState(operationId);
        state->SetEnabled(true);
        for (const auto& [treeId, poolName] : state->TreeIdToPoolNameMap()) {
            if (auto tree = GetTree(treeId);
                tree->HasRunningOperation(operationId))
            {
                tree->EnableOperation(state);
            }
        }
        auto maybeUnschedulableReason = host->CheckUnschedulable();
        if (!maybeUnschedulableReason || maybeUnschedulableReason == EUnschedulableReason::NoPendingAllocations) {
            state->GetController()->UpdateMinNeededAllocationResources();
        }
    }

    TFuture<void> RegisterOrUpdateNode(
        TNodeId nodeId,
        const TString& nodeAddress,
        const TBooleanFormulaTags& tags) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TFairShareStrategy::DoRegisterOrUpdateNode, MakeStrong(this))
            .AsyncVia(Host_->GetControlInvoker(EControlQueue::NodeTracker))
            .Run(nodeId, nodeAddress, tags);
    }

    void UnregisterNode(TNodeId nodeId, const TString& nodeAddress) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Host_->GetControlInvoker(EControlQueue::NodeTracker)->Invoke(
            BIND([this, this_ = MakeStrong(this), nodeId, nodeAddress] {
                // NOTE: If node is unregistered from node shard before it becomes online
                // then its id can be missing in the map.
                auto it = NodeIdToDescriptor_.find(nodeId);
                if (it == NodeIdToDescriptor_.end()) {
                    YT_LOG_WARNING("Node is not registered at strategy (Address: %v)", nodeAddress);

                    return;
                }

                auto treeId = it->second.TreeId;
                if (treeId) {
                    const auto& tree = GetOrCrash(IdToTree_, *treeId);
                    UnregisterNodeInTree(tree, nodeId);
                } else {
                    NodeIdsWithoutTree_.erase(nodeId);
                }

                EraseOrCrash(NodeAddresses_, nodeAddress);
                NodeIdToDescriptor_.erase(it);

                YT_LOG_INFO("Node was unregistered from strategy (Address: %v, TreeId: %v)", nodeAddress, treeId);
            }));
    }

    void RegisterNodeInTree(const IFairShareTreePtr& tree, TNodeId nodeId)
    {
        auto& treeNodeIds = GetOrCrash(NodeIdsPerTree_, tree->GetId());
        InsertOrCrash(treeNodeIds, nodeId);
        tree->RegisterNode(nodeId);
    }

    void UnregisterNodeInTree(const IFairShareTreePtr& tree, TNodeId nodeId)
    {
        auto& treeNodeIds = GetOrCrash(NodeIdsPerTree_, tree->GetId());
        EraseOrCrash(treeNodeIds, nodeId);
        tree->UnregisterNode(nodeId);
    }

    bool IsConnected() const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return Connected_;
    }

    void SetSchedulerTreeAlert(const TString& treeId, ESchedulerAlertType alertType, const TError& alert) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_VERIFY(IsSchedulerTreeAlertType(alertType));

        auto& alerts = TreeAlerts_[alertType];
        if (alert.IsOK()) {
            alerts.erase(treeId);
        } else {
            alerts[treeId] = alert;
        }
    }

    void UpdateSchedulerTreeAlerts()
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        for (const auto& [alertType, alertMessage] : GetSchedulerTreeAlertDescriptors()) {
            const auto& treeAlerts = TreeAlerts_[alertType];
            if (treeAlerts.empty()) {
                Host_->SetSchedulerAlert(alertType, TError());
                continue;
            }

            std::vector<TError> alerts;
            for (const auto& [treeId, alert] : treeAlerts) {
                alerts.push_back(alert
                    << TErrorAttribute("tree_id", treeId));
            }

            Host_->SetSchedulerAlert(
                alertType,
                TError(alertMessage)
                    << std::move(alerts));
        }
    }

    TErrorOr<TString> ChooseBestSingleTreeForOperation(TOperationId operationId, TJobResources newDemand, bool considerGuaranteesForSingleTree) override
    {
        THashMap<TString, IFairShareTreePtr> idToTree;
        {
            auto snapshot = TreeSetSnapshot_.Acquire();
            YT_VERIFY(snapshot);
            idToTree = snapshot->BuildIdToTreeMapping();
        }

        // NB(eshcherbin):
        // First, we ignore all trees in which the new operation is not marked running.
        // Then for every candidate pool we model the case if the new operation is assigned to it:
        // 1) We add the pool's current demand share and the operation's demand share to get the model demand share.
        // 2) We calculate reserveShare, defined as (estimatedGuaranteeShare - modelDemandShare).
        // Finally, we choose the pool with the maximum of MinComponent(reserveShare) over all trees.
        // More precisely, we compute MinComponent ignoring the resource types which are absent in the tree (e.g. GPU).
        TString bestTree;
        auto bestReserveRatio = std::numeric_limits<double>::lowest();
        std::vector<TString> emptyTrees;
        std::vector<TString> zeroGuaranteeTrees;
        for (const auto& [treeId, poolName] : GetOperationState(operationId)->TreeIdToPoolNameMap()) {
            YT_VERIFY(idToTree.contains(treeId));
            auto tree = idToTree[treeId];

            if (!tree->IsSnapshottedOperationRunningInTree(operationId)) {
                continue;
            }

            auto totalResourceLimits = tree->GetSnapshottedTotalResourceLimits();
            if (totalResourceLimits == TJobResources()) {
                emptyTrees.push_back(treeId);
                continue;
            }

            // If pool is not present in the tree (e.g. due to poor timings or if it is an ephemeral pool),
            // then its demand and guaranteed resources ratio are considered to be zero.
            TResourceVector currentDemandShare;
            TResourceVector estimatedGuaranteeShare;
            if (auto poolStateSnapshot = tree->GetMaybeStateSnapshotForPool(poolName.GetPool())) {
                currentDemandShare = poolStateSnapshot->DemandShare;
                estimatedGuaranteeShare = poolStateSnapshot->EstimatedGuaranteeShare;
            }

            if (Dominates(TResourceVector::SmallEpsilon(), estimatedGuaranteeShare) && considerGuaranteesForSingleTree) {
                zeroGuaranteeTrees.push_back(treeId);
                continue;
            }

            auto newDemandShare = TResourceVector::FromJobResources(newDemand, totalResourceLimits);
            auto modelDemandShare = newDemandShare + currentDemandShare;
            auto reserveShare = estimatedGuaranteeShare - modelDemandShare;

            // TODO(eshcherbin): Perhaps we need to add a configurable main resource for each tree and compare the shares of this resource.
            auto currentReserveRatio = std::numeric_limits<double>::max();
            #define XX(name, Name) \
                if (totalResourceLimits.Get##Name() > 0) { \
                    currentReserveRatio = std::min(currentReserveRatio, reserveShare[EJobResourceType::Name]); \
                }
            ITERATE_JOB_RESOURCES(XX)
            #undef XX

            // TODO(eshcherbin): This is rather verbose. Consider removing when well tested in production.
            YT_LOG_DEBUG(
                "Considering candidate single tree for operation ("
                "OperationId: %v, TreeId: %v, TotalResourceLimits: %v, "
                "NewDemandShare: %.6g, CurrentDemandShare: %.6g, ModelDemandShare: %.6g, "
                "EstimatedGuaranteeShare: %.6g, ReserveShare: %.6g, CurrentReserveRatio: %v)",
                operationId,
                treeId,
                FormatResources(totalResourceLimits),
                newDemandShare,
                currentDemandShare,
                modelDemandShare,
                estimatedGuaranteeShare,
                reserveShare,
                currentReserveRatio);

            if (currentReserveRatio > bestReserveRatio) {
                bestTree = treeId;
                bestReserveRatio = currentReserveRatio;
            }
        }

        if (!bestTree) {
            if (considerGuaranteesForSingleTree) {
                return TError("Found no best single non-empty tree for operation")
                    << TErrorAttribute("operation_id", operationId)
                    << TErrorAttribute("empty_candidate_trees", emptyTrees)
                    << TErrorAttribute("zero_guarantee_candidate_trees", zeroGuaranteeTrees);
            } else {
                YT_VERIFY(!emptyTrees.empty());

                bestTree = emptyTrees[0];

                YT_LOG_DEBUG(
                    "Found no best single non-empty tree for operation; choosing first found empty tree"
                    "(OperationId: %v, BestTree: %v, EmptyCandidateTrees: %v)",
                    operationId,
                    bestTree,
                    emptyTrees);
            }
        }

        YT_LOG_DEBUG("Chose best single tree for operation (OperationId: %v, BestTree: %v, BestReserveRatio: %v, EmptyCandidateTrees: %v)",
            operationId,
            bestTree,
            bestReserveRatio,
            emptyTrees);

        return bestTree;
    }

    TError OnOperationMaterialized(TOperationId operationId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto state = GetOperationState(operationId);
        std::vector<TError> multiTreeSchedulingErrors;
        for (const auto& [treeId, _] : state->TreeIdToPoolNameMap()) {
            auto tree = GetTree(treeId);
            tree->OnOperationMaterialized(operationId);

            if (auto error = tree->CheckOperationNecessaryResourceDemand(operationId); !error.IsOK()) {
                return error;
            }

            if (auto error = tree->CheckOperationSchedulingInSeveralTreesAllowed(operationId); !error.IsOK()) {
                multiTreeSchedulingErrors.push_back(TError("Scheduling in several trees is forbidden by %Qlv tree's configuration")
                    << std::move(error));
            }
        }

        if (!multiTreeSchedulingErrors.empty() && state->TreeIdToPoolNameMap().size() > 1) {
            std::vector<TString> treeIds;
            for (const auto& [treeId, _] : state->TreeIdToPoolNameMap()) {
                treeIds.push_back(treeId);
            }

            return TError("Scheduling in several trees is forbidden by some trees' configuration")
                << std::move(multiTreeSchedulingErrors)
                << TErrorAttribute("tree_ids", treeIds);
        }

        return TError();
    }

    void ScanPendingOperations() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        for (const auto& [_, tree] : IdToTree_) {
            tree->TryRunAllPendingOperations();
        }
    }

    TFuture<void> GetFullFairShareUpdateFinished() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return FairShareUpdateExecutor_->GetExecutedEvent();
    }

    void OnBuildResourceMetering()
    {
        DoBuildResourceMeteringAt(TInstant::Now());
    }

    TPersistentStrategyStatePtr BuildPersistentState()
    {
        auto result = New<TPersistentStrategyState>();
        for (const auto& [treeId, tree] : IdToTree_) {
            EmplaceOrCrash(result->TreeStates, treeId, tree->BuildPersistentState());
        }
        return result;
    }

    void BuildSchedulingAttributesForNode(
        TNodeId nodeId,
        const TString& nodeAddress,
        const TBooleanFormulaTags& nodeTags,
        TFluentMap fluent) const override
    {
        if (auto tree = FindTreeForNode(nodeAddress, nodeTags)) {
            fluent.Item("tree").Value(tree->GetId());
            tree->BuildSchedulingAttributesForNode(nodeId, fluent);
        } else {
            fluent.Item("tree").Entity();
        }
    }

    std::optional<TString> GetMaybeTreeIdForNode(TNodeId nodeId) const override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        return GetOrDefault(NodeIdToDescriptor_, nodeId).TreeId;
    }

private:
    class TPoolTreeService;
    friend class TPoolTreeService;

    TFairShareStrategyConfigPtr Config_;
    ISchedulerStrategyHost* const Host_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    const std::vector<IInvokerPtr> FeasibleInvokers_;

    mutable NLogging::TLogger Logger;

    TPeriodicExecutorPtr FairShareProfilingExecutor_;
    TPeriodicExecutorPtr FairShareUpdateExecutor_;
    TPeriodicExecutorPtr FairShareLoggingExecutor_;
    TPeriodicExecutorPtr AccumulatedUsageLoggingExecutor_;
    TPeriodicExecutorPtr MinNeededAllocationResourcesUpdateExecutor_;
    TPeriodicExecutorPtr ResourceMeteringExecutor_;
    TPeriodicExecutorPtr ResourceUsageUpdateExecutor_;
    TPeriodicExecutorPtr SchedulerTreeAlertsUpdateExecutor_;

    THashMap<TOperationId, TFairShareStrategyOperationStatePtr> OperationIdToOperationState_;

    using TFairShareTreeMap = THashMap<TString, IFairShareTreePtr>;
    TFairShareTreeMap IdToTree_;
    bool Initialized_ = false;

    std::optional<TString> DefaultTreeId_;

    // NB(eshcherbin): Note that these fair share tree mapping are only *snapshot* of actual mapping.
    // We should not expect that the set of trees or their structure in the snapshot are the same as
    // in the current |IdToTree_| map. Snapshots could be a little bit behind.
    TAtomicIntrusivePtr<TFairShareTreeSetSnapshot> TreeSetSnapshot_;

    // Topology describes set of trees and their node filters.
    using TTreeSetTopology = std::vector<std::pair<TString, TSchedulingTagFilter>>;
    TTreeSetTopology TreeSetTopology_;
    int TreeSetTopologyVersion_ = 0;

    TInstant ConnectionTime_;
    TInstant LastMeteringStatisticsUpdateTime_;
    TMeteringMap MeteringStatistics_;

    struct TStrategyExecNodeDescriptor
    {
        TBooleanFormulaTags Tags;
        TString Address;
        std::optional<TString> TreeId;
    };

    THashMap<TNodeId, TStrategyExecNodeDescriptor> NodeIdToDescriptor_;
    THashSet<TString> NodeAddresses_;
    THashMap<TString, TNodeIdSet> NodeIdsPerTree_;
    TNodeIdSet NodeIdsWithoutTree_;

    TYsonString LastPoolTreesYson_;
    TYsonString LastTemplatePoolTreeConfigMapYson_;

    TEnumIndexedArray<ESchedulerAlertType, THashMap<TString, TError>> TreeAlerts_;

    bool Connected_ = false;

    // NB: re2::RE2 does not have default constructor.
    std::optional<re2::RE2> EphemeralPoolNameRegex_;

    struct TPoolTreeDescription
    {
        TString Name;
        bool Tentative = false;
        bool Probing = false;
    };

    std::vector<TPoolTreeDescription> ParsePoolTrees(const TOperationSpecBasePtr& spec, EOperationType operationType) const
    {
        if (spec->PoolTrees) {
            for (const auto& treeId : *spec->PoolTrees) {
                if (!FindTree(treeId)) {
                    THROW_ERROR_EXCEPTION("Pool tree %Qv not found", treeId);
                }
            }
        }

        THashSet<TString> tentativePoolTrees;
        if (spec->TentativePoolTrees) {
            tentativePoolTrees = *spec->TentativePoolTrees;
        } else if (spec->UseDefaultTentativePoolTrees) {
            tentativePoolTrees = Config_->DefaultTentativePoolTrees;
        }

        if (!tentativePoolTrees.empty() && (!spec->PoolTrees || spec->PoolTrees->empty())) {
            THROW_ERROR_EXCEPTION("Regular pool trees must be explicitly specified for tentative pool trees to work properly");
        }

        for (const auto& tentativePoolTree : tentativePoolTrees) {
            if (spec->PoolTrees && spec->PoolTrees->contains(tentativePoolTree)) {
                THROW_ERROR_EXCEPTION("Regular and tentative pool trees must not intersect");
            }
        }

        std::vector<TPoolTreeDescription> result;
        if (spec->PoolTrees) {
            for (const auto& treeName : *spec->PoolTrees) {
                result.push_back(TPoolTreeDescription{ .Name = treeName });
            }
        } else {
            if (!DefaultTreeId_) {
                THROW_ERROR_EXCEPTION(
                    NScheduler::EErrorCode::PoolTreesAreUnspecified,
                    "Failed to determine fair-share tree for operation since "
                    "valid pool trees are not specified and default fair-share tree is not configured");
            }
            result.push_back(TPoolTreeDescription{ .Name = *DefaultTreeId_ });
        }

        if (result.empty()) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::PoolTreesAreUnspecified,
                "No pool trees are specified for operation");
        }

        // Data shuffling shouldn't be launched in tentative trees.
        for (const auto& treeId : tentativePoolTrees) {
            if (auto tree = FindTree(treeId)) {
                auto nonTentativeOperationTypesInTree = tree->GetConfig()->NonTentativeOperationTypes;
                const auto& noTentativePoolOperationTypes = nonTentativeOperationTypesInTree
                    ? *nonTentativeOperationTypesInTree
                    : Config_->OperationsWithoutTentativePoolTrees;
                if (noTentativePoolOperationTypes.find(operationType) == noTentativePoolOperationTypes.end()) {
                    result.push_back(TPoolTreeDescription{
                        .Name = treeId,
                        .Tentative = true
                    });
                }
            } else {
                if (!spec->TentativeTreeEligibility->IgnoreMissingPoolTrees) {
                    THROW_ERROR_EXCEPTION("Pool tree %Qv not found", treeId);
                }
            }
        }

        if (spec->ProbingPoolTree) {
            for (const auto& desc : result) {
                if (desc.Name == *spec->ProbingPoolTree) {
                    THROW_ERROR_EXCEPTION("Probing pool tree must not be in regular or tentative pool tree lists")
                        << TErrorAttribute("pool_tree", desc.Name)
                        << TErrorAttribute("is_tentative", desc.Tentative);
                }
            }

            if (auto tree = FindTree(spec->ProbingPoolTree.value())) {
                result.push_back(TPoolTreeDescription{
                    .Name = *spec->ProbingPoolTree,
                    .Probing = true
                });
            } else {
                THROW_ERROR_EXCEPTION("Probing pool tree %Qv not found", spec->ProbingPoolTree.value());
            }
        }

        return result;
    }

    int FindTreeIndexForNode(
        const std::vector<IFairShareTreePtr>& trees,
        const TString& nodeAddress,
        const TBooleanFormulaTags& nodeTags) const
    {
        bool hasMultipleMatchingTrees = false;
        int treeIndex = InvalidTreeIndex;
        for (int index = 0; index < std::ssize(trees); ++index) {
            const auto& tree = trees[index];
            if (!tree->GetSnapshottedConfig()->NodesFilter.CanSchedule(nodeTags)) {
                continue;
            }
            if (treeIndex != InvalidTreeIndex) {
                // Found second matching tree, skip scheduling.
                treeIndex = InvalidTreeIndex;
                hasMultipleMatchingTrees = true;
                break;
            }
            treeIndex = index;
        }

        if (treeIndex == InvalidTreeIndex) {
            if (hasMultipleMatchingTrees) {
                YT_LOG_INFO("Node belongs to multiple fair-share trees (Address: %v)",
                    nodeAddress);
            } else {
                YT_LOG_INFO("Node does not belong to any fair-share tree (Address: %v)",
                    nodeAddress);
            }
        }
        return treeIndex;
    }

    std::pair<IFairShareTreePtr, TMatchingTreeCookie> FindTreeForNodeWithCookie(
        const TString& nodeAddress,
        const TBooleanFormulaTags& nodeTags,
        TMatchingTreeCookie cookie) const
    {
        auto snapshot = TreeSetSnapshot_.Acquire();
        if (!snapshot) {
            return {nullptr, TMatchingTreeCookie{}};
        }

        const auto& trees = snapshot->Trees();
        if (cookie.TreeIndex != InvalidTreeIndex &&
            snapshot->GetTopologyVersion() == cookie.TreeSetTopologyVersion)
        {
            return {trees[cookie.TreeIndex], std::move(cookie)};
        }

        auto treeIndex = FindTreeIndexForNode(trees, nodeAddress, nodeTags);
        if (treeIndex == InvalidTreeIndex) {
            return {nullptr, TMatchingTreeCookie{}};
        } else {
            return {trees[treeIndex], TMatchingTreeCookie{snapshot->GetTopologyVersion(), treeIndex}};
        }
    }

    IFairShareTreePtr FindTreeForNode(const TString& nodeAddress, const TBooleanFormulaTags& nodeTags) const
    {
        auto snapshot = TreeSetSnapshot_.Acquire();
        if (!snapshot) {
            return nullptr;
        }

        const auto& trees = snapshot->Trees();
        auto treeIndex = FindTreeIndexForNode(trees, nodeAddress, nodeTags);
        return treeIndex == InvalidTreeIndex ? nullptr : trees[treeIndex];
    }

    TFuture<void> ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TOperationRuntimeParametersPtr& runtimeParameters)
    {
        if (IdToTree_.empty()) {
            THROW_ERROR_EXCEPTION("Scheduler strategy does not have configured fair-share trees");
        }

        auto spec = operation->GetStrategySpec();
        auto pools = GetOperationPools(runtimeParameters);

        if (pools.size() > 1 && !spec->SchedulingTagFilter.IsEmpty()) {
            THROW_ERROR_EXCEPTION(
                "Scheduling tag filter cannot be specified for operations "
                "to be scheduled in multiple fair-share trees");
        }

        std::vector<TFuture<void>> futures;

        for (const auto& [treeId, pool] : pools) {
            auto tree = GetTree(treeId);
            futures.push_back(tree->ValidateOperationPoolsCanBeUsed(operation, pool));
        }

        return AllSucceeded(futures);
    }

    const re2::RE2& GetEphemeralPoolNameRegex() const override
    {
        return *EphemeralPoolNameRegex_;
    }

    TFairShareStrategyOperationStatePtr FindOperationState(TOperationId operationId) const
    {
        auto it = OperationIdToOperationState_.find(operationId);
        if (it == OperationIdToOperationState_.end()) {
            return nullptr;
        }
        return it->second;
    }

    TFairShareStrategyOperationStatePtr GetOperationState(TOperationId operationId) const
    {
        return GetOrCrash(OperationIdToOperationState_, operationId);
    }

    IFairShareTreePtr FindTree(const TString& id) const
    {
        auto treeIt = IdToTree_.find(id);
        return treeIt != IdToTree_.end() ? treeIt->second : nullptr;
    }

    IFairShareTreePtr GetTree(const TString& id) const
    {
        auto tree = FindTree(id);
        YT_VERIFY(tree);
        return tree;
    }

    void DoBuildOperationProgress(
        void (IFairShareTree::*method)(TOperationId operationId, TFluentMap fluent) const,
        TOperationId operationId,
        TFluentMap fluent)
    {
        const auto& state = GetOperationState(operationId);
        const auto& pools = state->TreeIdToPoolNameMap();

        fluent
            .Item("scheduling_info_per_pool_tree")
                .DoMapFor(pools, [&] (TFluentMap fluent, const std::pair<TString, TPoolName>& value) {
                    const auto& treeId = value.first;
                    auto tree = GetTree(treeId);

                    fluent
                        .Item(treeId).BeginMap()
                            .Do(BIND(method, tree, operationId))
                        .EndMap();
                });
    }

    void OnOperationRunningInTree(IFairShareTree* tree, TOperationId operationId) const
    {
        YT_VERIFY(tree->HasRunningOperation(operationId));

        auto state = GetOperationState(operationId);
        Host_->MarkOperationAsRunningInStrategy(operationId);

        if (state->GetEnabled()) {
            tree->EnableOperation(state);
        }
    }

    TFairShareStrategyTreeConfigPtr ParsePoolTreeConfig(const INodePtr& poolTreeNode, const INodePtr& commonConfig) const
    {
        const auto& attributes = poolTreeNode->Attributes();
        auto ysonConfig = attributes.FindYson(TreeConfigAttributeName);

        if (!commonConfig) {
            return ysonConfig
                ? ConvertTo<TFairShareStrategyTreeConfigPtr>(ysonConfig)
                : ConvertTo<TFairShareStrategyTreeConfigPtr>(attributes.ToMap());
        }

        return ysonConfig
            ? ConvertTo<TFairShareStrategyTreeConfigPtr>(PatchNode(commonConfig, ConvertToNode(ysonConfig)))
            : ConvertTo<TFairShareStrategyTreeConfigPtr>(PatchNode(commonConfig, attributes.ToMap()));
    }

    TFairShareStrategyTreeConfigPtr BuildConfig(
        const IMapNodePtr& poolTreesMap,
        const THashMap<TString, TPoolTreesTemplateConfigPtr>& templatePoolTreeConfigMap,
        const TString& treeId) const
    {
        struct TPoolTreesTemplateConfigInfoView
        {
            TStringBuf name;
            const TPoolTreesTemplateConfig* config;
        };

        const auto& poolTreeAttributes = poolTreesMap->GetChildOrThrow(treeId);

        std::vector<TPoolTreesTemplateConfigInfoView> matchedTemplateConfigs;

        for (const auto& [name, value] : templatePoolTreeConfigMap) {
            if (value->Filter && NRe2::TRe2::FullMatch(treeId.data(), *value->Filter)) {
                matchedTemplateConfigs.push_back({name, value.Get()});
            }
        }

        if (matchedTemplateConfigs.empty()) {
            return ParsePoolTreeConfig(poolTreeAttributes, /*commonConfig*/ nullptr);
        }

        std::sort(
            std::begin(matchedTemplateConfigs),
            std::end(matchedTemplateConfigs),
            [] (const TPoolTreesTemplateConfigInfoView first, const TPoolTreesTemplateConfigInfoView second) {
                return first.config->Priority < second.config->Priority;
            });

        INodePtr compiledConfig = GetEphemeralNodeFactory()->CreateMap();
        for (const auto& config : matchedTemplateConfigs) {
            compiledConfig = PatchNode(compiledConfig, config.config->Config);
        }

        return ParsePoolTreeConfig(poolTreeAttributes, compiledConfig);
    }

    void CollectTreeChanges(
        const IMapNodePtr& poolsMap,
        THashSet<TString>* treesToAdd,
        THashSet<TString>* treesToRemove,
        THashSet<TString>* treeIdsWithChangedFilter,
        THashMap<TString, TSchedulingTagFilter>* treeIdToFilter) const
    {
        for (const auto& key : poolsMap->GetKeys()) {
            if (IdToTree_.find(key) == IdToTree_.end()) {
                treesToAdd->insert(key);
                try {
                    auto config = ParsePoolTreeConfig(poolsMap->FindChild(key), /*commonConfig*/ nullptr);
                    treeIdToFilter->emplace(key, config->NodesFilter);
                } catch (const std::exception&) {
                    // Do nothing, alert will be set later.
                    continue;
                }
            }
        }

        for (const auto& [treeId, tree] : IdToTree_) {
            auto child = poolsMap->FindChild(treeId);
            if (!child) {
                treesToRemove->insert(treeId);
                continue;
            }

            try {
                auto config = ParsePoolTreeConfig(child, /*commonConfig*/ nullptr);
                treeIdToFilter->emplace(treeId, config->NodesFilter);

                if (config->NodesFilter != tree->GetNodesFilter()) {
                    treeIdsWithChangedFilter->insert(treeId);
                }
            } catch (const std::exception&) {
                // Do nothing, alert will be set later.
                continue;
            }
        }
    }

    TFairShareTreeMap ConstructUpdatedTreeMap(
        const IMapNodePtr& poolTreesMap,
        const THashSet<TString>& treesToAdd,
        const THashSet<TString>& treesToRemove,
        const THashMap<TString, TPoolTreesTemplateConfigPtr>& templatePoolTreeConfigMap,
        std::vector<TError>* errors)
    {
        TFairShareTreeMap trees;

        for (const auto& treeId : treesToAdd) {
            TFairShareStrategyTreeConfigPtr treeConfig;
            try {
                treeConfig = BuildConfig(poolTreesMap, templatePoolTreeConfigMap, treeId);
            } catch (const std::exception& ex) {
                auto error = TError("Error parsing configuration of tree %Qv", treeId)
                    << ex;
                errors->push_back(error);
                YT_LOG_WARNING(error);
                continue;
            }

            auto tree = CreateFairShareTree(treeConfig, Config_, this, Host_, FeasibleInvokers_, treeId);
            tree->SubscribeOperationRunning(BIND_NO_PROPAGATE(
                &TFairShareStrategy::OnOperationRunningInTree,
                Unretained(this),
                Unretained(tree.Get())));

            trees.emplace(treeId, tree);
        }

        for (const auto& [treeId, tree] : IdToTree_) {
            if (treesToRemove.find(treeId) == treesToRemove.end()) {
                trees.emplace(treeId, tree);
            }
        }

        return trees;
    }

    bool CheckTreesConfiguration(const THashMap<TString, TSchedulingTagFilter>& treeIdToFilter, std::vector<TError>* errors) const
    {
        THashMap<TNodeId, std::vector<TString>> nodeIdToTreeSet;
        for (const auto& [nodeId, descriptor] : NodeIdToDescriptor_) {
            for (const auto& [treeId, filter] : treeIdToFilter) {
                if (filter.CanSchedule(descriptor.Tags)) {
                    nodeIdToTreeSet[nodeId].push_back(treeId);
                }
            }
        }

        for (const auto& [nodeId, treeIds] : nodeIdToTreeSet) {
            if (treeIds.size() > 1) {
                errors->emplace_back(
                    TError("Cannot update fair-share trees since there is node that belongs to multiple trees")
                        << TErrorAttribute("node_id", nodeId)
                        << TErrorAttribute("matched_trees", treeIds)
                        << TErrorAttribute("node_address", GetOrCrash(NodeIdToDescriptor_, nodeId).Address));
                return false;
            }
        }

        return true;
    }

    void UpdateTreesConfigs(
        const IMapNodePtr& poolTreesMap,
        const TFairShareTreeMap& trees,
        const THashMap<TString, TPoolTreesTemplateConfigPtr>& templatePoolTreeConfigMap,
        std::vector<TError>* errors,
        std::vector<TString>* updatedTreeIds) const
    {
        for (const auto& [treeId, tree] : trees) {
            auto child = poolTreesMap->GetChildOrThrow(treeId);

            bool treeConfigChanged = false;
            try {
                const auto& config = BuildConfig(poolTreesMap, templatePoolTreeConfigMap, treeId);
                treeConfigChanged = tree->UpdateConfig(config);
            } catch (const std::exception& ex) {
                auto error = TError("Failed to configure tree %Qv, defaults will be used", treeId)
                    << ex;
                errors->push_back(error);
                continue;
            }

            auto updateResult = tree->UpdatePools(child, treeConfigChanged);
            if (!updateResult.Error.IsOK()) {
                errors->push_back(updateResult.Error);
            }
            if (updateResult.Updated) {
                updatedTreeIds->push_back(treeId);
            }
        }
    }

    void RemoveTreesFromOperationStates(
        const THashSet<TString>& treesToRemove,
        std::vector<TOperationId>* orphanedOperationIds,
        std::vector<TOperationId>* operationsToFlush)
    {
        if (treesToRemove.empty()) {
            return;
        }

        THashMap<TOperationId, THashSet<TString>> operationIdToTreeSet;
        THashMap<TString, THashSet<TOperationId>> treeIdToOperationSet;

        for (const auto& [operationId, operationState] : OperationIdToOperationState_) {
            const auto& poolsMap = operationState->TreeIdToPoolNameMap();
            for (const auto& [treeId, pool] : poolsMap) {
                YT_VERIFY(operationIdToTreeSet[operationId].insert(treeId).second);
                YT_VERIFY(treeIdToOperationSet[treeId].insert(operationId).second);
            }
        }

        for (const auto& treeId : treesToRemove) {
            auto it = treeIdToOperationSet.find(treeId);

            // No operations are running in this tree.
            if (it == treeIdToOperationSet.end()) {
                continue;
            }

            // Unregister operations in removed tree and update their tree set.
            for (auto operationId : it->second) {
                const auto& state = GetOperationState(operationId);
                GetTree(treeId)->UnregisterOperation(state);
                EraseOrCrash(state->TreeIdToPoolNameMap(), treeId);

                auto& treeSet = GetOrCrash(operationIdToTreeSet, operationId);
                EraseOrCrash(treeSet, treeId);
            }
        }

        for (const auto& [operationId, treeSet] : operationIdToTreeSet) {
            const auto& state = GetOperationState(operationId);

            std::vector<TString> treeIdsToErase;
            for (auto [treeId, options] : state->GetHost()->GetRuntimeParameters()->SchedulingOptionsPerPoolTree) {
                if (treeSet.find(treeId) == treeSet.end()) {
                    treeIdsToErase.push_back(treeId);
                }
            }

            if (!treeIdsToErase.empty()) {
                YT_LOG_INFO("Removing operation from deleted trees (OperationId: %v, TreeIds: %v)",
                    operationId,
                    treeIdsToErase);

                state->GetHost()->EraseTrees(treeIdsToErase);
                operationsToFlush->push_back(operationId);
            }

            if (treeSet.empty()) {
                orphanedOperationIds->push_back(operationId);
            }
        }
    }

    void AbortOrphanedOperations(const std::vector<TOperationId>& operationIds)
    {
        for (auto operationId : operationIds) {
            if (OperationIdToOperationState_.find(operationId) != OperationIdToOperationState_.end()) {
                Host_->AbortOperation(
                    operationId,
                    TError("No suitable fair-share trees to schedule operation"));
            }
        }
    }

    void FlushOperationNodes(const std::vector<TOperationId>& operationIds)
    {
        for (auto operationId : operationIds) {
            Host_->FlushOperationNode(operationId);
        }
    }

    void DoRegisterOrUpdateNode(
        TNodeId nodeId,
        const TString& nodeAddress,
        const TBooleanFormulaTags& tags)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        std::vector<TString> treeIds;
        for (const auto& [treeId, tree] : IdToTree_) {
            if (tree->GetNodesFilter().CanSchedule(tags)) {
                treeIds.push_back(treeId);
            }
        }

        std::optional<TString> treeId;
        if (treeIds.size() == 0) {
            NodeIdsWithoutTree_.insert(nodeId);
        } else if (treeIds.size() == 1) {
            NodeIdsWithoutTree_.erase(nodeId);
            treeId = treeIds[0];
        } else {
            THROW_ERROR_EXCEPTION("Node belongs to more than one fair share tree")
                    << TErrorAttribute("matched_pool_trees", treeIds);
        }

        auto it = NodeIdToDescriptor_.find(nodeId);
        if (it == NodeIdToDescriptor_.end()) {
            THROW_ERROR_EXCEPTION_IF(NodeAddresses_.contains(nodeAddress),
                "Duplicate node address found (Address: %v, NewNodeId: %v)",
                nodeAddress,
                nodeId);

            EmplaceOrCrash(
                NodeIdToDescriptor_,
                nodeId,
                TStrategyExecNodeDescriptor{
                    .Tags = tags,
                    .Address = nodeAddress,
                    .TreeId = treeId,
                });
            NodeAddresses_.insert(nodeAddress);

            if (treeId) {
                const auto& tree = GetOrCrash(IdToTree_, *treeId);
                RegisterNodeInTree(tree, nodeId);
            }

            YT_LOG_INFO("Node was registered at strategy (NodeId: %v, Address: %v, Tags: %v, TreeId: %v)",
                nodeId,
                nodeAddress,
                tags,
                treeId);
        } else {
            auto& currentDescriptor = it->second;
            if (treeId != currentDescriptor.TreeId) {
                OnNodeChangedFairShareTree(IdToTree_, nodeId, treeId);
            }

            currentDescriptor.Tags = tags;
            currentDescriptor.Address = nodeAddress;

            YT_LOG_INFO("Node was updated at scheduler (NodeId: %v, Address: %v, Tags: %v, TreeId: %v)",
                nodeId,
                nodeAddress,
                tags,
                treeId);
        }

        ProcessNodesWithoutPoolTreeAlert();
    }

    void UpdateNodesOnChangedTrees(
        const TFairShareTreeMap& newIdToTree,
        const THashSet<TString> treeIdsToAdd,
        const THashSet<TString> treeIdsToRemove)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        for (const auto& treeId : treeIdsToAdd) {
            EmplaceOrCrash(NodeIdsPerTree_, treeId, TNodeIdSet{});
        }

        // NB(eshcherbin): |OnNodeChangedFairShareTree| requires both trees to be present in |NodeIdsPerTree_|.
        // This is why we add new trees before this cycle and remove old trees after it.
        for (const auto& [nodeId, descriptor] : NodeIdToDescriptor_) {
            std::optional<TString> newTreeId;
            for (const auto& [treeId, tree] : newIdToTree) {
                if (tree->GetNodesFilter().CanSchedule(descriptor.Tags)) {
                    YT_VERIFY(!newTreeId);
                    newTreeId = treeId;
                }
            }
            if (newTreeId) {
                NodeIdsWithoutTree_.erase(nodeId);
            } else {
                NodeIdsWithoutTree_.insert(nodeId);
            }
            if (newTreeId != descriptor.TreeId) {
                OnNodeChangedFairShareTree(newIdToTree, nodeId, newTreeId);
            }
        }

        for (const auto& treeId : treeIdsToRemove) {
            const auto& treeNodeIds = GetOrCrash(NodeIdsPerTree_, treeId);
            YT_VERIFY(treeNodeIds.empty());
            NodeIdsPerTree_.erase(treeId);

            for (const auto& [alertType, _] : GetSchedulerTreeAlertDescriptors()) {
                TreeAlerts_[alertType].erase(treeId);
            }
        }

        ProcessNodesWithoutPoolTreeAlert();
    }

    void OnNodeChangedFairShareTree(
        const TFairShareTreeMap& idToTree,
        TNodeId nodeId,
        const std::optional<TString>& newTreeId)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto it = NodeIdToDescriptor_.find(nodeId);
        YT_VERIFY(it != NodeIdToDescriptor_.end());

        auto& currentDescriptor = it->second;
        YT_VERIFY(newTreeId != currentDescriptor.TreeId);

        YT_LOG_INFO("Node has changed pool tree (NodeId: %v, Address: %v, OldTreeId: %v, NewTreeId: %v)",
            nodeId,
            currentDescriptor.Address,
            currentDescriptor.TreeId,
            newTreeId);

        if (auto oldTreeId = currentDescriptor.TreeId) {
            const auto& oldTree = GetOrCrash(IdToTree_, *oldTreeId);
            UnregisterNodeInTree(oldTree, nodeId);
        }
        if (newTreeId) {
            const auto& newTree = GetOrCrash(idToTree, *newTreeId);
            RegisterNodeInTree(newTree, nodeId);
        }

        currentDescriptor.TreeId = newTreeId;

        Host_->AbortAllocationsAtNode(nodeId, EAbortReason::NodeFairShareTreeChanged);
    }

    void ProcessNodesWithoutPoolTreeAlert()
    {
        if (NodeIdsWithoutTree_.empty()) {
            Host_->SetSchedulerAlert(ESchedulerAlertType::NodesWithoutPoolTree, TError());
            return;
        }

        std::vector<TString> nodeAddresses;
        int nodeCount = 0;
        bool truncated = false;
        for (auto nodeId : NodeIdsWithoutTree_) {
            ++nodeCount;
            if (nodeCount > MaxNodesWithoutPoolTreeToAlert) {
                truncated = true;
                break;
            }

            nodeAddresses.push_back(GetOrCrash(NodeIdToDescriptor_, nodeId).Address);
        }

        Host_->SetSchedulerAlert(
            ESchedulerAlertType::NodesWithoutPoolTree,
            TError("Found nodes that do not match any pool tree")
                << TErrorAttribute("node_addresses", nodeAddresses)
                << TErrorAttribute("truncated", truncated)
                << TErrorAttribute("node_count", NodeIdsWithoutTree_.size()));
    }

    void BuildTreeOrchid(
        const IFairShareTreePtr& tree,
        TFluentMap fluent)
    {
        const auto& nodeIds = NodeIdsPerTree_[tree->GetId()];
        fluent
            .Item("user_to_ephemeral_pools").Do(BIND(&IFairShareTree::BuildUserToEphemeralPoolsInDefaultPool, tree))
            .Item("config").Value(tree->GetConfig())
            .Item("resource_limits").Value(Host_->GetResourceLimits(tree->GetNodesFilter()))
            .Item("resource_usage").Value(Host_->GetResourceUsage(tree->GetNodesFilter()))
            .Item("node_count").Value(std::ssize(nodeIds))
            .Item("node_addresses").BeginList()
                .DoFor(nodeIds, [&] (TFluentList fluent, TNodeId nodeId) {
                    fluent
                        .Item().Value(GetOrCrash(NodeIdToDescriptor_, nodeId).Address);
                })
            .EndList()
            // This part is asynchronous.
            .Item("fair_share_info").BeginMap()
                .Do(BIND(&IFairShareTree::BuildFairShareInfo, tree))
            .EndMap();
    }

    virtual void DoBuildResourceMeteringAt(TInstant now)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers_);

        TMeteringMap newStatistics;

        auto snapshot = TreeSetSnapshot_.Acquire();
        if (!snapshot) {
            // It usually means that scheduler just started and tree snapshots have not been built yet.
            return;
        }

        for (const auto& tree : snapshot->Trees()) {
            TMeteringMap newStatisticsPerTree;
            THashMap<TString, TString> customMeteringTags;
            tree->BuildResourceMetering(&newStatisticsPerTree, &customMeteringTags);

            for (auto& [key, value] : newStatisticsPerTree) {
                auto it = MeteringStatistics_.find(key);
                // NB: we are going to have some accumulated values for metering.
                if (it != MeteringStatistics_.end()) {
                    TMeteringStatistics delta(
                        value.StrongGuaranteeResources(),
                        value.ResourceFlow(),
                        value.BurstGuaranteeResources(),
                        value.AllocatedResources(),
                        value.AccumulatedResourceUsage());
                    Host_->LogResourceMetering(
                        key,
                        delta,
                        customMeteringTags,
                        ConnectionTime_,
                        LastMeteringStatisticsUpdateTime_,
                        now);
                } else {
                    Host_->LogResourceMetering(
                        key,
                        value,
                        customMeteringTags,
                        ConnectionTime_,
                        LastMeteringStatisticsUpdateTime_,
                        now);
                }
                newStatistics.emplace(std::move(key), std::move(value));
            }
        }

        LastMeteringStatisticsUpdateTime_ = now;
        MeteringStatistics_.swap(newStatistics);

        try {
            WaitFor(Host_->UpdateLastMeteringLogTime(now))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to update last metering log time");
        }
    }

    void InitPersistentState(const TPersistentStrategyStatePtr& persistentStrategyState) override
    {
        YT_VERIFY(!Initialized_);
        YT_VERIFY(persistentStrategyState);

        YT_LOG_INFO("Initializing persistent strategy state %v",
            ConvertToYsonString(persistentStrategyState, EYsonFormat::Text));

        for (const auto& [treeId, tree] : IdToTree_) {
            auto stateIt = persistentStrategyState->TreeStates.find(treeId);
            auto treeState = stateIt != persistentStrategyState->TreeStates.end()
                ? stateIt->second
                : New<TPersistentTreeState>();
            tree->InitPersistentState(treeState);
        }

        for (const auto& [treeId, treeState] : persistentStrategyState->TreeStates) {
            YT_LOG_INFO_UNLESS(IdToTree_.contains(treeId),
                "Unknown pool tree, skipping its persistent state (TreeId: %v, PersistentState: %v)",
                treeId,
                ConvertToYsonString(treeState, EYsonFormat::Text));
        }

        Initialized_ = true;
    }

    class TPoolTreeService
        : public TVirtualMapBase
    {
    public:
        explicit TPoolTreeService(TIntrusivePtr<TFairShareStrategy> strategy)
            : Strategy_{std::move(strategy)}
        { }

    private:
        i64 GetSize() const final
        {
            VERIFY_INVOKERS_AFFINITY(Strategy_->FeasibleInvokers_);
            return std::ssize(Strategy_->IdToTree_);
        }

        std::vector<TString> GetKeys(const i64 limit) const final
        {
            VERIFY_INVOKERS_AFFINITY(Strategy_->FeasibleInvokers_);

            if (limit == 0) {
                return {};
            }

            std::vector<TString> keys;
            keys.reserve(std::min(limit, std::ssize(Strategy_->IdToTree_)));
            for (const auto& [id, tree] : Strategy_->IdToTree_) {
                keys.push_back(id);
                if (std::ssize(keys) == limit) {
                    break;
                }
            }

            return keys;
        }

        IYPathServicePtr FindItemService(const TStringBuf treeId) const final
        {
            VERIFY_INVOKERS_AFFINITY(Strategy_->FeasibleInvokers_);

            const auto treeIterator = Strategy_->IdToTree_.find(treeId);
            if (treeIterator == std::cend(Strategy_->IdToTree_)) {
                return nullptr;
            }

            const auto& tree = treeIterator->second;

            return tree->GetOrchidService();
        }

        const TIntrusivePtr<TFairShareStrategy> Strategy_;
    };

    class TNodeHeartbeatStrategyProxy
        : public INodeHeartbeatStrategyProxy
    {
    public:
        TNodeHeartbeatStrategyProxy(TNodeId nodeId, const IFairShareTreePtr& tree, TMatchingTreeCookie cookie)
            : NodeId_(nodeId)
            , Tree_(tree)
            , Cookie_(cookie)
        { }

        TFuture<void> ProcessSchedulingHeartbeat(
            const ISchedulingContextPtr& schedulingContext,
            bool skipScheduleAllocations) override
        {
            if (Tree_) {
                return Tree_->ProcessSchedulingHeartbeat(schedulingContext, skipScheduleAllocations);
            }
            return VoidFuture;
        }

        int GetSchedulingHeartbeatComplexity() const override
        {
            return Tree_ ? Tree_->GetSchedulingHeartbeatComplexity() : 0;
        }

        void BuildSchedulingAttributesString(
            TDelimitedStringBuilderWrapper& delimitedBuilder) const override
        {
            if (Tree_) {
                Tree_->BuildSchedulingAttributesStringForNode(NodeId_, delimitedBuilder);
            }
        }

        void BuildSchedulingAttributesStringForOngoingAllocations(
            const std::vector<TAllocationPtr>& allocations,
            TInstant now,
            TDelimitedStringBuilderWrapper& delimitedBuilder) const override
        {
            if (Tree_) {
                Tree_->BuildSchedulingAttributesStringForOngoingAllocations(allocations, now, delimitedBuilder);
            }
        }

        TMatchingTreeCookie GetMatchingTreeCookie() const override
        {
            return Cookie_;
        }

        bool HasMatchingTree() const override
        {
            return static_cast<bool>(Tree_);
        }

    private:
        TNodeId NodeId_;
        IFairShareTreePtr Tree_;
        TMatchingTreeCookie Cookie_;
    };
};

////////////////////////////////////////////////////////////////////////////////

ISchedulerStrategyPtr CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host,
    std::vector<IInvokerPtr> feasibleInvokers)
{
    return New<TFairShareStrategy>(std::move(config), host, std::move(feasibleInvokers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
