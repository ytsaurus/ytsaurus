#include "fair_share_strategy.h"
#include "fair_share_tree.h"
#include "fair_share_tree_element.h"
#include "fair_share_implementations.h"
#include "fair_share_tree_element_classic.h"
#include "public.h"
#include "scheduler_strategy.h"
#include "scheduler_tree.h"
#include "scheduling_context.h"
#include "fair_share_strategy_operation_controller.h"

#include <yt/server/lib/scheduler/config.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/client/security_client/acl.h>

#include <yt/core/concurrency/async_rw_lock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/algorithm_helpers.h>
#include <yt/core/misc/finally.h>
#include <yt/core/misc/atomic_object.h>

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
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

template <class TFairShareImpl>
class TFairShareStrategy
    : public ISchedulerStrategy
    , public ISchedulerTreeHost<TFairShareImpl>
{
public:
    using TTree = TFairShareTree<TFairShareImpl>;
    using TTreePtr = TIntrusivePtr<TTree>;

public:
    TFairShareStrategy(
        TFairShareStrategyConfigPtr config,
        ISchedulerStrategyHost* host,
        const std::vector<IInvokerPtr>& feasibleInvokers)
        : Config(config)
        , Host(host)
        , FeasibleInvokers(feasibleInvokers)
        , Logger(SchedulerLogger)
    {
        FairShareProfilingExecutor_ = New<TPeriodicExecutor>(
            Host->GetFairShareProfilingInvoker(),
            BIND(&TFairShareStrategy::OnFairShareProfiling, MakeWeak(this)),
            Config->FairShareProfilingPeriod);

        FairShareUpdateExecutor_ = New<TPeriodicExecutor>(
            Host->GetControlInvoker(EControlQueue::FairShareStrategy),
            BIND(&TFairShareStrategy::OnFairShareUpdate, MakeWeak(this)),
            Config->FairShareUpdatePeriod);

        FairShareLoggingExecutor_ = New<TPeriodicExecutor>(
            Host->GetControlInvoker(EControlQueue::FairShareStrategy),
            BIND(&TFairShareStrategy::OnFairShareLogging, MakeWeak(this)),
            Config->FairShareLogPeriod);

        MinNeededJobResourcesUpdateExecutor_ = New<TPeriodicExecutor>(
            Host->GetControlInvoker(EControlQueue::FairShareStrategy),
            BIND(&TFairShareStrategy::OnMinNeededJobResourcesUpdate, MakeWeak(this)),
            Config->MinNeededResourcesUpdatePeriod);
    }

    virtual void OnMasterConnected() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareProfilingExecutor_->Start();
        FairShareLoggingExecutor_->Start();
        FairShareUpdateExecutor_->Start();
        MinNeededJobResourcesUpdateExecutor_->Start();
    }

    virtual void OnMasterDisconnected() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        FairShareProfilingExecutor_->Stop();
        FairShareLoggingExecutor_->Stop();
        FairShareUpdateExecutor_->Stop();
        MinNeededJobResourcesUpdateExecutor_->Stop();

        OperationIdToOperationState_.clear();
        IdToTree_.clear();
        DefaultTreeId_.reset();
        TreeIdToSnapshot_.Store(THashMap<TString, IFairShareTreeSnapshotPtr>());
    }

    void OnFairShareProfiling()
    {
        OnFairShareProfilingAt(TInstant::Now());
    }

    void OnFairShareUpdate()
    {
        OnFairShareUpdateAt(TInstant::Now());
    }

    void OnMinNeededJobResourcesUpdate() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        YT_LOG_INFO("Starting min needed job resources update");

        for (const auto& [operationId, state] : OperationIdToOperationState_) {
            auto maybeUnschedulableReason = state->GetHost()->CheckUnschedulable();
            if (!maybeUnschedulableReason || maybeUnschedulableReason == EUnschedulableReason::NoPendingJobs) {
                state->GetController()->UpdateMinNeededJobResources();
            }
        }

        YT_LOG_INFO("Min needed job resources successfully updated");
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
        if (!snapshot) {
            return VoidFuture;
        }

        return snapshot->ScheduleJobs(schedulingContext);
    }

    virtual void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshot = FindTreeSnapshotByNodeDescriptor(schedulingContext->GetNodeDescriptor());
        if (snapshot) {
            snapshot->PreemptJobsGracefully(schedulingContext);
        }
    }

    virtual void RegisterOperation(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto spec = ParseSpec(operation);

        auto state = CreateFairShareStrategyOperationState(operation);

        YT_VERIFY(OperationIdToOperationState_.insert(
            std::make_pair(operation->GetId(), state)).second);

        auto runtimeParameters = operation->GetRuntimeParameters();

        for (const auto& [treeId, poolName] : state->TreeIdToPoolNameMap()) {
            const auto& treeParams = GetOrCrash(runtimeParameters->SchedulingOptionsPerPoolTree, treeId);
            GetTree(treeId)->RegisterOperation(state, spec, treeParams);
        }
    }

    virtual void UnregisterOperation(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operation->GetId());
        for (const auto& [treeId, poolName] : state->TreeIdToPoolNameMap()) {
            DoUnregisterOperationFromTree(state, treeId);
        }

        YT_VERIFY(OperationIdToOperationState_.erase(operation->GetId()) == 1);
    }

    virtual void UnregisterOperationFromTree(TOperationId operationId, const TString& treeId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operationId);
        if (!state->TreeIdToPoolNameMap().contains(treeId)) {
            YT_LOG_INFO("Operation to be removed from a tree was not found in that tree (OperationId: %v, TreeId: %v)",
                operationId,
                treeId);
            return;
        }

        DoUnregisterOperationFromTree(state, treeId);

        state->EraseTree(treeId);

        YT_LOG_INFO("Operation removed from a tree (OperationId: %v, TreeId: %v)", operationId, treeId);
    }

    void DoUnregisterOperationFromTree(const TFairShareStrategyOperationStatePtr& operationState, const TString& treeId)
    {
        auto tree = GetTree(treeId);
        tree->UnregisterOperation(operationState);
        tree->ProcessActivatableOperations();
    }

    virtual void DisableOperation(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto operationId = operation->GetId();
        const auto& state = GetOperationState(operationId);
        for (const auto& [treeId, poolName] : state->TreeIdToPoolNameMap()) {
            if (auto tree = GetTree(treeId);
                tree->HasRunningOperation(operationId))
            {
                tree->DisableOperation(state);
            }
        }
        state->SetEnabled(false);
    }

    virtual void UpdatePoolTrees(const INodePtr& poolTreesNode) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        std::vector<TOperationId> orphanedOperationIds;
        {
            // No context switches allowed while fair share trees update.
            TForbidContextSwitchGuard contextSwitchGuard;

            YT_LOG_INFO("Updating pool trees");

            if (poolTreesNode->GetType() != NYTree::ENodeType::Map) {
                auto error = TError("Pool trees node has invalid type")
                    << TErrorAttribute("expected_type", NYTree::ENodeType::Map)
                    << TErrorAttribute("actual_type", poolTreesNode->GetType());
                YT_LOG_WARNING(error);
                Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
                return;
            }

            auto poolsMap = poolTreesNode->AsMap();

            std::vector<TError> errors;

            // Collect trees to add and remove.
            THashSet<TString> treeIdsToAdd;
            THashSet<TString> treeIdsToRemove;
            THashSet<TString> treesWithChangedFilter;
            THashMap<TString, TSchedulingTagFilter> treeIdToFilter;
            CollectTreeChanges(poolsMap, &treeIdsToAdd, &treeIdsToRemove, &treesWithChangedFilter, &treeIdToFilter);

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
            // Check is skipped if trees configuration did not change.
            bool shouldCheckConfiguration = !treeIdsToAdd.empty() || !treeIdsToRemove.empty() || !treesWithChangedFilter.empty();

            if (shouldCheckConfiguration && !CheckTreesConfiguration(treeIdToFilter, &errors)) {
                auto error = TError("Error updating pool trees")
                    << std::move(errors);
                Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
                return;
            }

            // Update configs and pools structure of all trees.
            // NB: it updates already existing trees inplace.
            std::vector<TString> updatedTreeIds;
            UpdateTreesConfigs(poolsMap, idToTree, &errors, &updatedTreeIds);

            // Update node at scheduler.
            UpdateNodesOnChangedTrees(idToTree);

            // Abort orphaned operations.
            RemoveTreesFromOperationStates(treeIdsToRemove, &orphanedOperationIds);

            // Updating default fair-share tree and global tree map.
            DefaultTreeId_ = defaultTreeId;
            std::swap(IdToTree_, idToTree);

            // Setting alerts.
            if (!errors.empty()) {
                auto error = TError("Error updating pool trees")
                    << std::move(errors);
                Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
            } else {
                Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, TError());
                if (!updatedTreeIds.empty() || !treeIdsToRemove.empty() || !treeIdsToAdd.empty()) {
                    Host->LogEventFluently(ELogEventType::PoolsInfo)
                        .Item("pools").DoMapFor(IdToTree_, [&] (TFluentMap fluent, const auto& value) {
                            const auto& treeId = value.first;
                            const auto& tree = value.second;
                            fluent
                                .Item(treeId).Do(BIND(&TTree::BuildStaticPoolsInformation, tree));
                        });
                }
                YT_LOG_INFO("Pool trees updated");
            }
        }

        AbortOrphanedOperations(orphanedOperationIds);
    }

    virtual void BuildOperationAttributes(TOperationId operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operationId);
        const auto& pools = state->TreeIdToPoolNameMap();

        if (DefaultTreeId_ && pools.find(*DefaultTreeId_) != pools.end()) {
            GetTree(*DefaultTreeId_)->BuildOperationAttributes(operationId, fluent);
        }
    }

    virtual void BuildOperationProgress(TOperationId operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        if (!FindOperationState(operationId)) {
            return;
        }

        DoBuildOperationProgress(&TTree::BuildOperationProgress, operationId, fluent);
    }

    virtual void BuildBriefOperationProgress(TOperationId operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        if (!FindOperationState(operationId)) {
            return;
        }

        DoBuildOperationProgress(&TTree::BuildBriefOperationProgress, operationId, fluent);
    }

    virtual TPoolTreeControllerSettingsMap GetOperationPoolTreeControllerSettingsMap(TOperationId operationId) override
    {
        TPoolTreeControllerSettingsMap result;
        const auto& state = GetOperationState(operationId);
        for (const auto& [treeName, poolName] : state->TreeIdToPoolNameMap()) {
            auto tree = GetTree(treeName);
            result.emplace(
                treeName,
                TPoolTreeControllerSettings{
                    .SchedulingTagFilter = tree->GetNodesFilter(),
                    .Tentative = GetSchedulingOptionsPerPoolTree(state->GetHost(), treeName)->Tentative
                });
        }
        return result;
    }

    virtual std::vector<std::pair<TOperationId, TError>> GetUnschedulableOperations() override
    {
        std::vector<std::pair<TOperationId, TError>> result;
        for (const auto& operationStatePair : OperationIdToOperationState_) {
            auto operationId = operationStatePair.first;
            const auto& operationState = operationStatePair.second;

            if (operationState->TreeIdToPoolNameMap().empty()) {
                // This operation is orphaned and will be aborted.
                continue;
            }

            bool hasSchedulableTree = false;
            TError operationError("Operation is unschedulable in all trees");

            for (const auto& treePoolPair : operationState->TreeIdToPoolNameMap()) {
                const auto& treeName = treePoolPair.first;
                auto error = GetTree(treeName)->CheckOperationUnschedulable(
                    operationId,
                    Config->OperationUnschedulableSafeTimeout,
                    Config->OperationUnschedulableMinScheduleJobAttempts,
                    Config->OperationUnschedulableDeactiovationReasons);
                if (error.IsOK()) {
                    hasSchedulableTree = true;
                    break;
                } else {
                    operationError.InnerErrors().push_back(error);
                }
            }

            if (!hasSchedulableTree) {
                result.emplace_back(operationId, operationError);
            }
        }
        return result;
    }

    virtual void UpdateConfig(const TFairShareStrategyConfigPtr& config) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        Config = config;

        for (const auto& [treeId, tree] : IdToTree_) {
            tree->UpdateControllerConfig(config);
        }

        FairShareProfilingExecutor_->SetPeriod(Config->FairShareProfilingPeriod);
        FairShareUpdateExecutor_->SetPeriod(Config->FairShareUpdatePeriod);
        FairShareLoggingExecutor_->SetPeriod(Config->FairShareLogPeriod);
        MinNeededJobResourcesUpdateExecutor_->SetPeriod(Config->MinNeededResourcesUpdatePeriod);
    }

    virtual void BuildOperationInfoForEventLog(const IOperationStrategyHost* operation, TFluentMap fluent)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& operationState = GetOperationState(operation->GetId());
        const auto& pools = operationState->TreeIdToPoolNameMap();

        fluent
            .DoIf(DefaultTreeId_.operator bool(), [&] (TFluentMap fluent) {
                auto it = pools.find(*DefaultTreeId_);
                if (it != pools.end()) {
                    fluent
                        .Item("pool").Value(it->second.GetPool());
                }
            });
    }

    virtual void ApplyOperationRuntimeParameters(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto state = GetOperationState(operation->GetId());
        const auto runtimeParameters = operation->GetRuntimeParameters();

        auto newPools = GetOperationPools(operation->GetRuntimeParameters());

        YT_VERIFY(newPools.size() == state->TreeIdToPoolNameMap().size());

        // Tentative trees can be removed from state, we must apply these changes to new state.
        for (const auto& erasedTree : state->GetHost()->ErasedTrees()) {
            newPools.erase(erasedTree);
        }

        for (const auto& [treeId, oldPool] : state->TreeIdToPoolNameMap()) {
            const auto& newPool = GetOrCrash(newPools, treeId);
            auto tree = GetTree(treeId);
            if (oldPool.GetPool() != newPool.GetPool()) {
                tree->ChangeOperationPool(operation->GetId(), state, newPool);
            }

            const auto& treeParams = GetOrCrash(runtimeParameters->SchedulingOptionsPerPoolTree, treeId);
            tree->UpdateOperationRuntimeParameters(operation->GetId(), treeParams);
        }
        state->TreeIdToPoolNameMap() = newPools;
    }

    virtual void InitOperationRuntimeParameters(
        const TOperationRuntimeParametersPtr& runtimeParameters,
        const TOperationSpecBasePtr& spec,
        const TSerializableAccessControlList& baseAcl,
        const TString& user,
        EOperationType operationType) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        runtimeParameters->Acl = baseAcl;
        runtimeParameters->Acl.Entries.insert(
            runtimeParameters->Acl.Entries.end(),
            spec->Acl.Entries.begin(),
            spec->Acl.Entries.end());

        auto poolTrees = ParsePoolTrees(spec, operationType);
        for (const auto& poolTreeDescription : poolTrees) {
            auto treeParams = New<TOperationFairShareTreeRuntimeParameters>();
            auto specIt = spec->SchedulingOptionsPerPoolTree.find(poolTreeDescription.Name);
            if (specIt != spec->SchedulingOptionsPerPoolTree.end()) {
                treeParams->Weight = spec->Weight ? spec->Weight : specIt->second->Weight;
                treeParams->Pool = GetTree(poolTreeDescription.Name)->CreatePoolName(spec->Pool ? spec->Pool : specIt->second->Pool, user);
                treeParams->ResourceLimits = spec->ResourceLimits ? spec->ResourceLimits : specIt->second->ResourceLimits;
            } else {
                treeParams->Weight = spec->Weight;
                treeParams->Pool = GetTree(poolTreeDescription.Name)->CreatePoolName(spec->Pool, user);
                treeParams->ResourceLimits = spec->ResourceLimits;
            }
            treeParams->Tentative = poolTreeDescription.Tentative;
            YT_VERIFY(runtimeParameters->SchedulingOptionsPerPoolTree.emplace(poolTreeDescription.Name, std::move(treeParams)).second);
        }
    }

    virtual void ValidateOperationRuntimeParameters(
        IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters,
        bool validatePools) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operation->GetId());

        for (const auto& [treeId, schedulingOptions] : runtimeParameters->SchedulingOptionsPerPoolTree) {
            auto poolTrees = state->TreeIdToPoolNameMap();
            if (poolTrees.find(treeId) == poolTrees.end()) {
                THROW_ERROR_EXCEPTION("Pool tree %Qv was not configured for this operation", treeId);
            }
        }

        if (validatePools) {
            ValidateOperationPoolsCanBeUsed(operation, runtimeParameters);
            ValidateMaxRunningOperationsCountOnPoolChange(operation, runtimeParameters);

            auto poolLimitViolations = GetPoolLimitViolations(operation, runtimeParameters);
            if (!poolLimitViolations.empty()) {
                THROW_ERROR poolLimitViolations.begin()->second;
            }
        }
    }

    virtual void BuildOrchid(TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        THashMap<TString, std::vector<TExecNodeDescriptor>> descriptorsPerPoolTree;
        for (const auto& [treeId, poolTree] : IdToTree_) {
            descriptorsPerPoolTree.emplace(treeId, std::vector<TExecNodeDescriptor>{});
        }

        auto descriptors = Host->CalculateExecNodeDescriptors(TSchedulingTagFilter());
        for (const auto& idDescriptorPair : *descriptors) {
            const auto& descriptor = idDescriptorPair.second;
            if (!descriptor.Online) {
                continue;
            }
            for (const auto& idTreePair : IdToTree_) {
                const auto& treeId = idTreePair.first;
                const auto& tree = idTreePair.second;
                if (tree->GetNodesFilter().CanSchedule(descriptor.Tags)) {
                    descriptorsPerPoolTree[treeId].push_back(descriptor);
                    break;
                }
            }
        }

        fluent
            // COMAPT(ignat)
            .OptionalItem("default_fair_share_tree", DefaultTreeId_)
            .OptionalItem("default_pool_tree", DefaultTreeId_)
            .Item("scheduling_info_per_pool_tree").DoMapFor(IdToTree_, [&] (TFluentMap fluent, const auto& pair) {
                const auto& [treeId, tree] = pair;
                const auto& treeNodeDescriptor = GetOrCrash(descriptorsPerPoolTree, treeId);
                fluent
                    .Item(treeId).BeginMap()
                        .Do(BIND(&TFairShareStrategy::BuildTreeOrchid, tree, treeNodeDescriptor))
                    .EndMap();
            });
    }

    virtual void ApplyJobMetricsDelta(const TOperationIdToOperationJobMetrics& operationIdToOperationJobMetrics) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        TForbidContextSwitchGuard contextSwitchGuard;

        auto snapshots = TreeIdToSnapshot_.Load();

        for (const auto& [operationId, metricsPerTree] : operationIdToOperationJobMetrics) {
            for (const auto& metrics : metricsPerTree) {
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

        return BIND(&TFairShareStrategy::ValidateOperationPoolsCanBeUsed, Unretained(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operation, operation->GetRuntimeParameters());
    }

    virtual THashMap<TString, TError> GetPoolLimitViolations(
        const IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

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

    virtual void ValidateMaxRunningOperationsCountOnPoolChange(
        const IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto pools = GetOperationPools(runtimeParameters);

        for (const auto& [treeId, pool] : pools) {
            auto tree = GetTree(treeId);
            tree->ValidatePoolLimitsOnPoolChange(operation, pool);
        }
    }

    virtual void OnFairShareProfilingAt(TInstant now) override
    {
        VERIFY_INVOKER_AFFINITY(Host->GetFairShareProfilingInvoker());

        TForbidContextSwitchGuard contextSwitchGuard;

        auto snapshots = TreeIdToSnapshot_.Load();
        for (const auto& [treeId, treeSnapshot] : snapshots) {
            treeSnapshot->ProfileFairShare();
        }
    }

    // NB: This function is public for testing purposes.
    virtual void OnFairShareUpdateAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        YT_LOG_INFO("Starting fair share update");

        std::vector<std::pair<TString, TTreePtr>> idToTree(IdToTree_.begin(), IdToTree_.end());
        std::sort(
            idToTree.begin(),
            idToTree.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.second->GetOperationCount() > rhs.second->GetOperationCount();
            });

        std::vector<TFuture<std::tuple<TString, TError, IFairShareTreeSnapshotPtr>>> futures;
        for (const auto& [treeId, tree] : idToTree) {
            futures.push_back(tree->OnFairShareUpdateAt(now).Apply(BIND([treeId = treeId] (const std::pair<IFairShareTreeSnapshotPtr, TError>& pair) {
                const auto& [snapshot, error] = pair;
                return std::make_tuple(treeId, error, snapshot);
            })));
        }

        auto resultsOrError = WaitFor(Combine(futures));
        if (!resultsOrError.IsOK()) {
            Host->Disconnect(resultsOrError);
            return;
        }

        if (auto delay = Config->StrategyTestingOptions->DelayInsideFairShareUpdate) {
            TDelayedExecutor::WaitForDuration(*delay);
        }

        THashMap<TString, IFairShareTreeSnapshotPtr> snapshots;
        std::vector<TError> errors;

        const auto& results = resultsOrError.Value();
        for (const auto& [treeId, error, snapshot] : results) {
            snapshots.emplace(treeId, snapshot);
            if (!error.IsOK()) {
                errors.push_back(error);
            }
        }

        {
            // NB(eshcherbin): Make sure that snapshots in strategy and snapshots in trees are updated atomically.
            // This is necessary to maintain consistency between strategy and trees.
            TForbidContextSwitchGuard guard;

            TreeIdToSnapshot_.Exchange(std::move(snapshots));
            for (const auto& [_, tree] : idToTree) {
                tree->FinishFairShareUpdate();
            }
        }

        if (!errors.empty()) {
            auto error = TError("Found pool configuration issues during fair share update")
                << std::move(errors);
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdateFairShare, error);
        } else {
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdateFairShare, TError());
        }

        YT_LOG_INFO("Fair share successfully updated");
    }

    virtual void OnFairShareEssentialLoggingAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        for (const auto& [treeId, tree] : IdToTree_) {
            tree->OnFairShareEssentialLoggingAt(now);
        }
    }

    virtual void OnFairShareLoggingAt(TInstant now) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        for (const auto& [treeId, tree] : IdToTree_) {
            tree->OnFairShareLoggingAt(now);
        }
    }

    virtual void ProcessJobUpdates(
        const std::vector<TJobUpdate>& jobUpdates,
        std::vector<std::pair<TOperationId, TJobId>>* successfullyUpdatedJobs,
        std::vector<TJobId>* jobsToAbort) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_VERIFY(successfullyUpdatedJobs->empty());
        YT_VERIFY(jobsToAbort->empty());

        YT_LOG_DEBUG("Processing job updates in strategy (UpdateCount: %v)",
            jobUpdates.size());

        auto snapshots = TreeIdToSnapshot_.Load();

        THashSet<TJobId> jobsToPostpone;

        for (const auto& job : jobUpdates) {
            switch (job.Status) {
                case EJobUpdateStatus::Running: {
                    auto snapshotIt = snapshots.find(job.TreeId);
                    if (snapshotIt == snapshots.end()) {
                        // Job is orphaned (does not belong to any tree), aborting it.
                        jobsToAbort->push_back(job.JobId);
                    } else {
                        const auto& snapshot = snapshotIt->second;
                        snapshot->ProcessUpdatedJob(job.OperationId, job.JobId, job.Delta);
                    }
                    break;
                }
                case EJobUpdateStatus::Finished: {
                    auto snapshotIt = snapshots.find(job.TreeId);
                    if (snapshotIt == snapshots.end()) {
                        // Job is finished but tree does not exist, nothing to do.
                        YT_LOG_DEBUG("Skipping job update since pool tree is missing (OperationId: %v, JobId: %v)",
                            job.OperationId,
                            job.JobId);
                        continue;
                    }
                    const auto& snapshot = snapshotIt->second;
                    if (snapshot->HasOperation(job.OperationId)) {
                        snapshot->ProcessFinishedJob(job.OperationId, job.JobId);
                    } else if (snapshot->IsOperationDisabled(job.OperationId)) {
                        YT_LOG_DEBUG("Postpone job update since operation is disabled (OperationId: %v, JobId: %v)",
                            job.OperationId,
                            job.JobId);
                        jobsToPostpone.insert(job.JobId);
                    } else {
                        YT_LOG_DEBUG("Dropping finished job (OperationId: %v, JobId: %v)", job.OperationId, job.JobId);
                    }
                    break;
                }
                default:
                    YT_ABORT();
            }
        }

        for (const auto& job : jobUpdates) {
            if (!jobsToPostpone.contains(job.JobId)) {
                successfullyUpdatedJobs->push_back({job.OperationId, job.JobId});
            }
        }
    }

    virtual void RegisterJobsFromRevivedOperation(TOperationId operationId, const std::vector<TJobPtr>& jobs) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        THashMap<TString, std::vector<TJobPtr>> jobsByTreeId;

        for (const auto& job : jobs) {
            jobsByTreeId[job->GetTreeId()].push_back(job);
        }

        for (const auto& [treeId, jobs] : jobsByTreeId) {
            auto tree = FindTree(treeId);
            // NB: operation can be missing in tree since ban.
            if (tree && tree->HasOperation(operationId)) {
                tree->RegisterJobsFromRevivedOperation(operationId, jobs);
            }
        }
    }

    virtual void EnableOperation(IOperationStrategyHost* host) override
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
        if (!maybeUnschedulableReason || maybeUnschedulableReason == EUnschedulableReason::NoPendingJobs) {
            state->GetController()->UpdateMinNeededJobResources();
        }
    }

    virtual std::vector<TString> GetNodeTreeIds(const THashSet<TString>& tags) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        std::vector<TString> treeIds;

        for (const auto& [treeId, tree] : IdToTree_) {
            if (tree->GetNodesFilter().CanSchedule(tags)) {
                treeIds.push_back(treeId);
            }
        }

        return treeIds;
    }

    virtual TString ChooseBestSingleTreeForOperation(TOperationId operationId, TJobResources newDemand) override
    {
        auto snapshots = TreeIdToSnapshot_.Load();

        // NB(eshcherbin):
        // First, we ignore all trees in which the new operation is not marked running.
        // Then for every candidate pool we model the case if the new operation is assigned to it:
        // 1) We add the pool's current demand and the operation's demand to get the model demand.
        // 2) We calculate reserveRatio, defined as (guaranteedResourcesRatio - modelDemandRatio).
        // Finally, we choose the pool with the maximum reserveRatio.
        TString bestTree;
        auto bestReserveRatio = std::numeric_limits<double>::lowest();
        for (const auto& [treeId, poolName] : GetOperationState(operationId)->TreeIdToPoolNameMap()) {
            YT_VERIFY(snapshots.contains(treeId));
            auto snapshot = snapshots[treeId];

            if (!snapshot->IsOperationRunningInTree(operationId)) {
                continue;
            }

            auto totalResourceLimits = snapshot->GetTotalResourceLimits();
            TJobResources currentDemand;
            double guaranteedResourcesRatio = 0;

            // If pool is not present in the snapshot (e.g. due to poor timings or if it is an ephemeral pool),
            // then its demand and guaranteed resources ratio are considered to be zero.
            if (auto poolStateSnapshot = snapshot->GetMaybeStateSnapshotForPool(poolName.GetPool())) {
                currentDemand = poolStateSnapshot->ResourceDemand;
                guaranteedResourcesRatio = poolStateSnapshot->GuaranteedResourcesRatio;
            }

            auto modelDemand = newDemand + currentDemand;
            auto modelDemandRatio = GetMaxResourceRatio(modelDemand, totalResourceLimits);
            auto reserveRatio = guaranteedResourcesRatio - modelDemandRatio;

            // TODO(eshcherbin): This is rather verbose. Consider removing when well tested in production.
            YT_LOG_DEBUG(
                "Considering candidate single tree for operation (OperationId: %v, Tree: %v, "
                "NewDemand: %v, CurrentDemand: %v, ModelDemand: %v, TotalResourceLimits: %v, "
                "ModelDemandRatio: %v, GuaranteedResourcesRatio: %v, ReserveRatio: %v)",
                operationId,
                treeId,
                FormatResources(newDemand),
                FormatResources(currentDemand),
                FormatResources(modelDemand),
                FormatResources(totalResourceLimits),
                modelDemandRatio,
                guaranteedResourcesRatio,
                reserveRatio);

            if (reserveRatio > bestReserveRatio) {
                bestTree = treeId;
                bestReserveRatio = reserveRatio;
            }
        }

        YT_VERIFY(bestTree);
        YT_LOG_DEBUG("Chose best single tree for operation (OperationId: %v, BestTree: %v, BestReserveRatio: %v)",
            operationId,
            bestTree,
            bestReserveRatio);

        return bestTree;
    }

    virtual void ScanWaitingForPoolOperations() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        for (const auto& [_, tree] : IdToTree_) {
            tree->TryRunAllWaitingOperations();
        }
    }

    virtual TFuture<void> GetFullFairShareUpdateFinished() override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        return FairShareUpdateExecutor_->GetExecutedEvent();
    }

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* const Host;

    const std::vector<IInvokerPtr> FeasibleInvokers;

    mutable NLogging::TLogger Logger;

    TPeriodicExecutorPtr FairShareProfilingExecutor_;
    TPeriodicExecutorPtr FairShareUpdateExecutor_;
    TPeriodicExecutorPtr FairShareLoggingExecutor_;
    TPeriodicExecutorPtr MinNeededJobResourcesUpdateExecutor_;

    THashMap<TOperationId, TFairShareStrategyOperationStatePtr> OperationIdToOperationState_;

    using TFairShareTreeMap = THashMap<TString, TTreePtr>;
    TFairShareTreeMap IdToTree_;

    std::optional<TString> DefaultTreeId_;

    // NB(eshcherbin): Note that these fair share tree snapshots are only *snapshots*.
    // We should not expect that the set of trees or their structure in the snapshot are the same as
    // in the current |IdToTree_| map. Snapshots could be a little bit behind.
    TAtomicObject<THashMap<TString, IFairShareTreeSnapshotPtr>> TreeIdToSnapshot_;

    struct TPoolTreeDescription
    {
        TString Name;
        bool Tentative;
    };

    TStrategyOperationSpecPtr ParseSpec(const IOperationStrategyHost* operation) const
    {
        try {
            return ConvertTo<TStrategyOperationSpecPtr>(operation->GetSpecString());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing strategy spec of operation")
                << ex;
        }
    }

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
            tentativePoolTrees = Config->DefaultTentativePoolTrees;
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
                result.push_back(TPoolTreeDescription{
                    .Name = treeName,
                    .Tentative = false
                });
            }
        } else {
            if (!DefaultTreeId_) {
                THROW_ERROR_EXCEPTION(
                    NScheduler::EErrorCode::PoolTreesAreUnspecified,
                    "Failed to determine fair-share tree for operation since "
                    "valid pool trees are not specified and default fair-share tree is not configured");
            }
            result.push_back(TPoolTreeDescription{
                .Name = *DefaultTreeId_,
                .Tentative = false
            });
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
                    : Config->OperationsWithoutTentativePoolTrees;
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

        return result;
    }

    IFairShareTreeSnapshotPtr FindTreeSnapshotByNodeDescriptor(const TExecNodeDescriptor& descriptor) const
    {
        IFairShareTreeSnapshotPtr matchingSnapshot;

        auto snapshots = TreeIdToSnapshot_.Load();
        for (const auto& [treeId, snapshot] : snapshots) {
            if (snapshot->GetNodesFilter().CanSchedule(descriptor.Tags)) {
                if (matchingSnapshot) {
                    // Found second matching snapshot, skip scheduling.
                    YT_LOG_INFO("Node belong to multiple fair-share trees, scheduling skipped (Address: %v)",
                        descriptor.Address);
                    return nullptr;
                }
                matchingSnapshot = snapshot;
            }
        }

        if (!matchingSnapshot) {
            YT_LOG_INFO("Node does not belong to any fair-share tree, scheduling skipped (Address: %v)",
                descriptor.Address);
            return nullptr;
        }

        return matchingSnapshot;
    }

    void ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TOperationRuntimeParametersPtr& runtimeParameters)
    {
        if (IdToTree_.empty()) {
            THROW_ERROR_EXCEPTION("Scheduler strategy does not have configured fair-share trees");
        }

        auto spec = ParseSpec(operation);
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

        WaitFor(Combine(futures))
            .ThrowOnError();
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

    TTreePtr FindTree(const TString& id) const
    {
        auto treeIt = IdToTree_.find(id);
        return treeIt != IdToTree_.end() ? treeIt->second : nullptr;
    }

    TTreePtr GetTree(const TString& id) const
    {
        auto tree = FindTree(id);
        YT_VERIFY(tree);
        return tree;
    }

    void DoBuildOperationProgress(
        void (TTree::*method)(TOperationId operationId, TFluentMap fluent),
        TOperationId operationId,
        TFluentMap fluent)
    {
        const auto& state = GetOperationState(operationId);
        const auto& pools = state->TreeIdToPoolNameMap();

        fluent
            .Item("scheduling_info_per_pool_tree")
                .DoMapFor(pools, [&] (TFluentMap fluent, const std::pair<TString, TPoolName>& value) {
                    const auto& treeId = value.first;
                    fluent
                        .Item(treeId).BeginMap()
                            .Do(BIND(method, GetTree(treeId), operationId))
                        .EndMap();
                });
    }

    virtual void OnOperationReadyInTree(TOperationId operationId, TTree* tree) const override
    {
        YT_VERIFY(tree->HasRunningOperation(operationId));

        auto state = GetOperationState(operationId);
        if (!state->GetHost()->GetActivated()) {
            Host->ActivateOperation(operationId);
        } else if (state->GetEnabled()) {
            tree->EnableOperation(state);
        }
    }

    void CollectTreeChanges(
        const IMapNodePtr& poolsMap,
        THashSet<TString>* treesToAdd,
        THashSet<TString>* treesToRemove,
        THashSet<TString>* treesWithChangedFilter,
        THashMap<TString, TSchedulingTagFilter>* treeIdToFilter) const
    {
        for (const auto& key : poolsMap->GetKeys()) {
            if (IdToTree_.find(key) == IdToTree_.end()) {
                treesToAdd->insert(key);
                try {
                    auto child = poolsMap->FindChild(key);
                    auto configMap = child->Attributes().ToMap();
                    auto config = ConvertTo<TFairShareStrategyTreeConfigPtr>(configMap);
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
                auto configMap = child->Attributes().ToMap();
                auto config = ConvertTo<TFairShareStrategyTreeConfigPtr>(configMap);
                treeIdToFilter->emplace(treeId, config->NodesFilter);

                if (config->NodesFilter != tree->GetNodesFilter()) {
                    treesWithChangedFilter->insert(treeId);
                }
            } catch (const std::exception&) {
                // Do nothing, alert will be set later.
                continue;
            }
        }
    }

    TFairShareTreeMap ConstructUpdatedTreeMap(
        const IMapNodePtr& poolsMap,
        const THashSet<TString>& treesToAdd,
        const THashSet<TString>& treesToRemove,
        std::vector<TError>* errors)
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
                YT_LOG_WARNING(error);
                continue;
            }

            auto tree = New<TTree>(treeConfig, Config, Host, this, FeasibleInvokers, treeId);
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
        THashMap<NNodeTrackerClient::TNodeId, THashSet<TString>> nodeIdToTreeSet;

        for (const auto& [treeId, filter] : treeIdToFilter) {
            auto nodes = Host->GetExecNodeIds(filter);

            for (const auto& node : nodes) {
                nodeIdToTreeSet[node].insert(treeId);
            }
        }

        for (const auto& [nodeId, treeIds] : nodeIdToTreeSet) {
            if (treeIds.size() > 1) {
                errors->emplace_back(
                    TError("Cannot update fair-share trees since there is node that belongs to multiple trees")
                        << TErrorAttribute("node_id", nodeId)
                        << TErrorAttribute("matched_trees", treeIds)
                        << TErrorAttribute("node_address", Host->GetExecNodeAddress(nodeId)));
                return false;
            }
        }

        return true;
    }

    void UpdateTreesConfigs(
        const IMapNodePtr& poolsMap,
        const TFairShareTreeMap& trees,
        std::vector<TError>* errors,
        std::vector<TString>* updatedTreeIds) const
    {
        for (const auto& [treeId, tree] : trees) {
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
                updatedTreeIds->push_back(treeId);
            }
        }
    }

    void RemoveTreesFromOperationStates(const THashSet<TString>& treesToRemove, std::vector<TOperationId>* orphanedOperationIds)
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
                YT_VERIFY(state->TreeIdToPoolNameMap().erase(treeId) == 1);

                auto& treeSet = GetOrCrash(operationIdToTreeSet, operationId);
                YT_VERIFY(treeSet.erase(treeId) == 1);
            }
        }

        for (const auto& [operationId, treeSet] : operationIdToTreeSet) {
            if (treeSet.empty()) {
                orphanedOperationIds->push_back(operationId);
            }
        }
    }

    void AbortOrphanedOperations(const std::vector<TOperationId>& operationIds)
    {
        for (auto operationId : operationIds) {
            if (OperationIdToOperationState_.find(operationId) != OperationIdToOperationState_.end()) {
                Host->AbortOperation(
                    operationId,
                    TError("No suitable fair-share trees to schedule operation"));
            }
        }
    }

    void UpdateNodesOnChangedTrees(const TFairShareTreeMap& idToTree)
    {
        THashMap<TString, TSchedulingTagFilter> treeIdToFilter;
        for (const auto& [treeId, tree] : idToTree) {
            treeIdToFilter.emplace(treeId, tree->GetNodesFilter());
        }
        Host->UpdateNodesOnChangedTrees(treeIdToFilter);
    }

    static void BuildTreeOrchid(
        const TTreePtr& tree,
        const std::vector<TExecNodeDescriptor>& descriptors,
        TFluentMap fluent)
    {
        TJobResources resourceLimits;
        for (const auto& descriptor : descriptors) {
            resourceLimits += descriptor.ResourceLimits;
        }

        fluent
            .Item("user_to_ephemeral_pools").Do(BIND(&TTree::BuildUserToEphemeralPoolsInDefaultPool, tree))
            .Item("fair_share_info").BeginMap()
                .Do(BIND(&TTree::BuildFairShareInfo, tree))
            .EndMap()
            .Do(BIND(&TTree::BuildOrchid, tree))
            .Item("resource_limits").Value(resourceLimits)
            .Item("node_count").Value(descriptors.size())
            .Item("node_addresses").BeginList()
                .DoFor(descriptors, [&] (TFluentList fluent, const auto& descriptor) {
                    fluent
                        .Item().Value(descriptor.Address);
                })
            .EndList();
    }
};

template <class TFairShareImpl>
ISchedulerStrategyPtr CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host,
    const std::vector<IInvokerPtr>& feasibleInvokers)
{
    return New<TFairShareStrategy<TFairShareImpl>>(config, host, feasibleInvokers);
}

template ISchedulerStrategyPtr CreateFairShareStrategy<TClassicFairShareImpl>(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host,
    const std::vector<IInvokerPtr>& feasibleInvokers);

template ISchedulerStrategyPtr CreateFairShareStrategy<TVectorFairShareImpl>(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host,
    const std::vector<IInvokerPtr>& feasibleInvokers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
