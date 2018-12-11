#include "fair_share_strategy.h"
#include "fair_share_tree.h"
#include "fair_share_tree_element.h"
#include "public.h"
#include "config.h"
#include "profiler.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "fair_share_strategy_operation_controller.h"

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

        OperationIdToOperationState_.clear();
        IdToTree_.clear();

        DefaultTreeId_.reset();

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

        auto spec = ParseSpec(operation);
        auto state = New<TFairShareStrategyOperationState>(operation);
        state->TreeIdToPoolNameMap() = GetOperationPools(operation->GetRuntimeParameters());

        YCHECK(OperationIdToOperationState_.insert(
            std::make_pair(operation->GetId(), state)).second);

        auto runtimeParams = operation->GetRuntimeParameters();

        for (const auto& pair : state->TreeIdToPoolNameMap()) {
            const auto& treeId = pair.first;
            const auto& tree = GetTree(pair.first);

            auto paramsIt = runtimeParams->SchedulingOptionsPerPoolTree.find(treeId);
            YCHECK(paramsIt != runtimeParams->SchedulingOptionsPerPoolTree.end());

            if (tree->RegisterOperation(state, spec, paramsIt->second)) {
                ActivateOperations({operation->GetId()});
            }
        }
    }

    virtual void UnregisterOperation(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operation->GetId());
        for (const auto& pair : state->TreeIdToPoolNameMap()) {
            const auto& treeId = pair.first;
            DoUnregisterOperationFromTree(state, treeId);
        }

        YCHECK(OperationIdToOperationState_.erase(operation->GetId()) == 1);
    }

    virtual void UnregisterOperationFromTree(const TOperationId& operationId, const TString& treeId) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operationId);
        if (!state->TreeIdToPoolNameMap().contains(treeId)) {
            LOG_INFO("Operation to be removed from a tentative tree was not found in that tree (OperationId: %v, TreeId: %v)",
                operationId,
                treeId);
            return;
        }

        DoUnregisterOperationFromTree(state, treeId);

        state->EraseTree(treeId);

        LOG_INFO("Operation removed from a tentative tree (OperationId: %v, TreeId: %v)", operationId, treeId);
    }

    void DoUnregisterOperationFromTree(const TFairShareStrategyOperationStatePtr& operationState, const TString& treeId)
    {
        auto unregistrationResult = GetTree(treeId)->UnregisterOperation(operationState);
        ActivateOperations(unregistrationResult.OperationsToActivate);
    }

    virtual void DisableOperation(IOperationStrategyHost* operation) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operation->GetId());
        for (const auto& pair : state->TreeIdToPoolNameMap()) {
            const auto& treeId = pair.first;
            GetTree(treeId)->DisableOperation(state);
        }
    }

    virtual void UpdatePoolTrees(const INodePtr& poolTreesNode) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        LOG_INFO("Updating pool trees");

        if (poolTreesNode->GetType() != NYTree::ENodeType::Map) {
            auto error = TError("Pool trees node has invalid type")
                << TErrorAttribute("expected_type", NYTree::ENodeType::Map)
                << TErrorAttribute("actual_type", poolTreesNode->GetType());
            LOG_WARNING(error);
            Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
            return;
        }

        auto poolsMap = poolTreesNode->AsMap();

        std::vector<TError> errors;

        // Collect trees to add and remove.
        THashSet<TString> treeIdsToAdd;
        THashSet<TString> treeIdsToRemove;
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
        // Check is skipped if trees configuration did not change.
        bool skipTreesConfigurationCheck = treeIdsToAdd.empty() && treeIdsToRemove.empty();

        if (!skipTreesConfigurationCheck) {
            if (!CheckTreesConfiguration(idToTree, &errors)) {
                auto error = TError("Error updating pool trees")
                    << std::move(errors);
                Host->SetSchedulerAlert(ESchedulerAlertType::UpdatePools, error);
                return;
            }
        }

        // Update configs and pools structure of all trees.
        int updatedTreeCount;
        UpdateTreesConfigs(poolsMap, idToTree, &errors, &updatedTreeCount);

        // Abort orphaned operations.
        AbortOrphanedOperations(treeIdsToRemove);

        // Updating default fair-share tree and global tree map.
        DefaultTreeId_ = defaultTreeId;
        std::swap(IdToTree_, idToTree);

        THashMap<TString, IFairShareTreeSnapshotPtr> snapshots;
        for (const auto& pair : IdToTree_) {
            const auto& treeId = pair.first;
            const auto& tree = pair.second;
            YCHECK(snapshots.insert(std::make_pair(treeId, tree->CreateSnapshot())).second);
        }

        {
            TWriterGuard guard(TreeIdToSnapshotLock_);
            std::swap(TreeIdToSnapshot_, snapshots);
            ++SnapshotRevision_;
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
        const auto& pools = state->TreeIdToPoolNameMap();

        if (DefaultTreeId_ && pools.find(*DefaultTreeId_) != pools.end()) {
            GetTree(*DefaultTreeId_)->BuildOperationAttributes(operationId, fluent);
        }
    }

    virtual void BuildOperationProgress(const TOperationId& operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        if (!FindOperationState(operationId)) {
            return;
        }

        DoBuildOperationProgress(&TFairShareTree::BuildOperationProgress, operationId, fluent);
    }

    virtual void BuildBriefOperationProgress(const TOperationId& operationId, TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        if (!FindOperationState(operationId)) {
            return;
        }

        DoBuildOperationProgress(&TFairShareTree::BuildBriefOperationProgress, operationId, fluent);
    }

    virtual TPoolTreeToSchedulingTagFilter GetOperationPoolTreeToSchedulingTagFilter(const TOperationId& operationId) override
    {
        TPoolTreeToSchedulingTagFilter result;
        for (const auto& pair : GetOperationState(operationId)->TreeIdToPoolNameMap()) {
            const auto& treeName = pair.first;
            result.insert(std::make_pair(treeName, GetTree(treeName)->GetNodesFilter()));
        }
        return result;
    }

    virtual std::vector<std::pair<TOperationId, TError>> GetUnschedulableOperations() override
    {
        std::vector<std::pair<TOperationId, TError>> result;
        for (const auto& operationStatePair : OperationIdToOperationState_) {
            const auto& operationId = operationStatePair.first;
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
        const auto runtimeParams = operation->GetRuntimeParameters();

        auto newPools = GetOperationPools(operation->GetRuntimeParameters());

        YCHECK(newPools.size() == state->TreeIdToPoolNameMap().size());

        //tentative trees can be removed from state, we must apply these changes to new state
        for (const auto& erasedTree : state->ErasedTrees()) {
            newPools.erase(erasedTree);
        }

        for (const auto& pair : state->TreeIdToPoolNameMap()) {
            const auto& treeId = pair.first;
            const auto& oldPool = pair.second;

            auto newPoolIt = newPools.find(treeId);
            YCHECK(newPoolIt != newPools.end());

            if (oldPool.GetPool() != newPoolIt->second.GetPool()) {
                bool wasActive = GetTree(treeId)->ChangeOperationPool(operation->GetId(), state, newPoolIt->second);
                if (!wasActive) {
                    ActivateOperations({operation->GetId()});
                }
            }

            auto it = runtimeParams->SchedulingOptionsPerPoolTree.find(treeId);
            YCHECK(it != runtimeParams->SchedulingOptionsPerPoolTree.end());
            GetTree(treeId)->UpdateOperationRuntimeParameters(operation->GetId(), it->second);
        }
        state->TreeIdToPoolNameMap() = newPools;
    }

    virtual void InitOperationRuntimeParameters(
        const TOperationRuntimeParametersPtr& runtimeParameters,
        const TOperationSpecBasePtr& spec,
        const TString& user,
        EOperationType operationType) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto poolTrees = ParsePoolTrees(spec, operationType);
        runtimeParameters->Owners = spec->Owners;
        for (const auto& tree : poolTrees) {
            auto treeParams = New<TOperationFairShareTreeRuntimeParameters>();
            auto specIt = spec->SchedulingOptionsPerPoolTree.find(tree);
            if (specIt != spec->SchedulingOptionsPerPoolTree.end()) {
                treeParams->Weight = spec->Weight ? spec->Weight : specIt->second->Weight;
                treeParams->Pool = GetTree(tree)->CreatePoolName(spec->Pool ? spec->Pool : specIt->second->Pool, user);
                treeParams->ResourceLimits = spec->ResourceLimits ? spec->ResourceLimits : specIt->second->ResourceLimits;
            } else {
                treeParams->Weight = spec->Weight;
                treeParams->Pool = GetTree(tree)->CreatePoolName(spec->Pool, user);
                treeParams->ResourceLimits = spec->ResourceLimits;
            }
            YCHECK(runtimeParameters->SchedulingOptionsPerPoolTree.emplace(tree, std::move(treeParams)).second);
        }
    }

    virtual void ValidateOperationRuntimeParameters(
        IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParams) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        const auto& state = GetOperationState(operation->GetId());

        for (const auto& pair : runtimeParams->SchedulingOptionsPerPoolTree) {
            auto poolTrees = state->TreeIdToPoolNameMap();
            if (poolTrees.find(pair.first) == poolTrees.end()) {
                THROW_ERROR_EXCEPTION("Pool tree %Qv was not configured for this operation", pair.first);
            }
        }
//        TODO(renadeen): we shouldn't validate pools if user didn't change them
//        ValidateOperationPoolsCanBeUsed(operation, runtimeParams);
//        ValidatePoolLimits(operation, runtimeParams);
//        ValidateMaxRunningOperationsCountOnPoolChange(operation, runtimeParams);
    }

    virtual void BuildOrchid(TFluentMap fluent) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        // TODO(ignat): stop using pools from here and remove this section (since it is also presented in fair_share_info subsection).
        if (DefaultTreeId_) {
            GetTree(*DefaultTreeId_)->BuildPoolsInformation(fluent);
        }


        THashMap<TString, std::vector<TExecNodeDescriptor>> descriptorsPerPoolTree;
        for (const auto& pair : IdToTree_) {
            const auto& treeId = pair.first;
            descriptorsPerPoolTree.emplace(treeId, std::vector<TExecNodeDescriptor>{});
        }

        auto descriptors = Host->CalculateExecNodeDescriptors(TSchedulingTagFilter());
        for (const auto& idDescriptorPair : *descriptors) {
            const auto& descriptor = idDescriptorPair.second;
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
            .DoIf(DefaultTreeId_.operator bool(), [&] (TFluentMap fluent) {
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

        THashMap<TString, IFairShareTreeSnapshotPtr> snapshots;
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

        return BIND(&TFairShareStrategy::ValidateOperationPoolsCanBeUsed, Unretained(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operation, operation->GetRuntimeParameters());
    }

    virtual void ValidatePoolLimits(
        const IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto pools = GetOperationPools(runtimeParameters);

        for (const auto& pair : pools) {
            auto tree = GetTree(pair.first);
            tree->ValidatePoolLimits(operation, pair.second);
        }
    }

    virtual void ValidateMaxRunningOperationsCountOnPoolChange(
        const IOperationStrategyHost* operation,
        const TOperationRuntimeParametersPtr& runtimeParameters)
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        auto pools = GetOperationPools(runtimeParameters);

        for (const auto& pair : pools) {
            auto tree = GetTree(pair.first);
            tree->ValidatePoolLimitsOnPoolChange(operation, pair.second);
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

        THashMap<TString, IFairShareTreeSnapshotPtr> snapshots;

        for (const auto& pair : IdToTree_) {
            const auto& treeId = pair.first;
            const auto& tree = pair.second;
            YCHECK(snapshots.insert(std::make_pair(treeId, tree->CreateSnapshot())).second);
        }

        {
            TWriterGuard guard(TreeIdToSnapshotLock_);
            std::swap(TreeIdToSnapshot_, snapshots);
            ++SnapshotRevision_;
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

    virtual void ProcessJobUpdates(
        const std::vector<TJobUpdate>& jobUpdates,
        std::vector<std::pair<TOperationId, TJobId>>* successfullyUpdatedJobs,
        std::vector<TJobId>* jobsToAbort,
        int* snapshotRevision) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(successfullyUpdatedJobs->empty());
        YCHECK(jobsToAbort->empty());

        LOG_DEBUG("Processing job updates in strategy (UpdateCount: %v)",
            jobUpdates.size());

        THashMap<TString, IFairShareTreeSnapshotPtr> snapshots;
        {
            TReaderGuard guard(TreeIdToSnapshotLock_);
            snapshots = TreeIdToSnapshot_;
            *snapshotRevision = SnapshotRevision_;
        }

        THashSet<TJobId> jobsToSave;

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
                        continue;
                    }
                    const auto& snapshot = snapshotIt->second;
                    if (snapshot->HasOperation(job.OperationId)) {
                        snapshot->ProcessFinishedJob(job.OperationId, job.JobId);
                    } else if (!job.SnapshotRevision || *job.SnapshotRevision == *snapshotRevision) {
                        jobsToSave.insert(job.JobId);
                    }
                    break;
                }
                default:
                    Y_UNREACHABLE();
            }
        }

        for (const auto& job : jobUpdates) {
            if (!jobsToSave.contains(job.JobId)) {
                successfullyUpdatedJobs->push_back({job.OperationId, job.JobId});
            }
        }
    }

    virtual void RegisterJobs(const TOperationId& operationId, const std::vector<TJobPtr>& jobs) override
    {
        VERIFY_INVOKERS_AFFINITY(FeasibleInvokers);

        THashMap<TString, std::vector<TJobPtr>> jobsByTreeId;

        for (const auto& job : jobs) {
            jobsByTreeId[job->GetTreeId()].push_back(job);
        }

        for (const auto& pair : jobsByTreeId) {
            auto tree = FindTree(pair.first);
            // NB: operation can be missing in tree since ban.
            if (tree && tree->HasOperation(operationId)) {
                tree->RegisterJobs(operationId, pair.second);
            }
        }
    }

    virtual void EnableOperation(IOperationStrategyHost* host) override
    {
        const auto& operationId = host->GetId();
        const auto& state = GetOperationState(operationId);
        for (const auto& pair : state->TreeIdToPoolNameMap()) {
            const auto& treeId = pair.first;
            GetTree(treeId)->EnableOperation(state);
        }
        if (host->IsSchedulable()) {
            state->GetController()->UpdateMinNeededJobResources();
        }
    }

    virtual void ValidateNodeTags(const THashSet<TString>& tags) override
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

    THashMap<TOperationId, TFairShareStrategyOperationStatePtr> OperationIdToOperationState_;

    TInstant LastProfilingTime_;

    using TFairShareTreeMap = THashMap<TString, TFairShareTreePtr>;
    TFairShareTreeMap IdToTree_;

    std::optional<TString> DefaultTreeId_;

    TReaderWriterSpinLock TreeIdToSnapshotLock_;
    THashMap<TString, IFairShareTreeSnapshotPtr> TreeIdToSnapshot_;
    int SnapshotRevision_ = 0;

    TStrategyOperationSpecPtr ParseSpec(const IOperationStrategyHost* operation) const
    {
        try {
            return ConvertTo<TStrategyOperationSpecPtr>(operation->GetSpec());
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing strategy spec of operation")
                << ex;
        }
    }

    std::vector<TString> ParsePoolTrees(const TOperationSpecBasePtr& spec, EOperationType operationType) const
    {
        for (const auto& treeId : spec->PoolTrees) {
            if (!FindTree(treeId)) {
                THROW_ERROR_EXCEPTION("Pool tree %Qv not found", treeId);
            }
        }

        THashSet<TString> tentativePoolTrees;
        if (spec->TentativePoolTrees) {
            tentativePoolTrees = *spec->TentativePoolTrees;
        } else if (spec->UseDefaultTentativePoolTrees) {
            tentativePoolTrees = Config->DefaultTentativePoolTrees;
        }

        if (!tentativePoolTrees.empty() && spec->PoolTrees.empty()) {
            THROW_ERROR_EXCEPTION("Regular pool trees must be specified for tentative pool trees to work properly");
        }

        for (const auto& tentativePoolTree : tentativePoolTrees) {
            if (spec->PoolTrees.contains(tentativePoolTree)) {
                THROW_ERROR_EXCEPTION("Regular and tentative pool trees must not intersect");
            }
        }

        std::vector<TString> result(spec->PoolTrees.begin(), spec->PoolTrees.end());
        if (result.empty()) {
            if (!DefaultTreeId_) {
                THROW_ERROR_EXCEPTION("Failed to determine fair-share tree for operation since "
                    "valid pool trees are not specified and default fair-share tree is not configured");
            }
            result.push_back(*DefaultTreeId_);
        }

        // Data shuffling shouldn't be launched in tentative trees.
        const auto& noTentativePoolOperationTypes = Config->OperationsWithoutTentativePoolTrees;
        if (noTentativePoolOperationTypes.find(operationType) == noTentativePoolOperationTypes.end()) {
            std::vector<TString> presentedTentativePoolTrees;
            for (const auto& treeId : tentativePoolTrees) {
                if (FindTree(treeId)) {
                    presentedTentativePoolTrees.push_back(treeId);
                } else {
                    if (!spec->TentativeTreeEligibility->IgnoreMissingPoolTrees) {
                        THROW_ERROR_EXCEPTION("Pool tree %Qv not found", treeId);
                    }
                }
            }
            result.insert(result.end(), presentedTentativePoolTrees.begin(), presentedTentativePoolTrees.end());
        }

        return result;
    }

    THashMap<TString, TPoolName> GetOperationPools(const TOperationRuntimeParametersPtr& runtimeParams) const
    {
        THashMap<TString, TPoolName> pools;
        for (const auto& pair : runtimeParams->SchedulingOptionsPerPoolTree) {
            pools.emplace(pair.first, *pair.second->Pool);
        }
        return pools;
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

        for (const auto& pair : pools) {
            auto tree = GetTree(pair.first);
            futures.push_back(tree->ValidateOperationPoolsCanBeUsed(operation, pair.second));
        }

        WaitFor(Combine(futures))
            .ThrowOnError();
    }

    TFairShareStrategyOperationStatePtr FindOperationState(const TOperationId& operationId) const
    {
        auto it = OperationIdToOperationState_.find(operationId);
        if (it == OperationIdToOperationState_.end()) {
            return nullptr;
        }
        return it->second;
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
        THashSet<TString>* treesToAdd,
        THashSet<TString>* treesToRemove) const
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
        const THashSet<TString>& treesToAdd,
        const THashSet<TString>& treesToRemove,
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
        THashMap<NNodeTrackerClient::TNodeId, THashSet<TString>> nodeIdToTreeSet;

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
            const auto& treeIds  = pair.second;
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

    void AbortOrphanedOperations(const THashSet<TString>& treesToRemove)
    {
        if (treesToRemove.empty()) {
            return;
        }

        THashMap<TOperationId, THashSet<TString>> operationIdToTreeSet;
        THashMap<TString, THashSet<TOperationId>> treeIdToOperationSet;

        for (const auto& pair : OperationIdToOperationState_) {
            const auto& operationId = pair.first;
            const auto& poolsMap = pair.second->TreeIdToPoolNameMap();

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
                YCHECK(state->TreeIdToPoolNameMap().erase(treeId) == 1);

                auto treeSetIt = operationIdToTreeSet.find(operationId);
                YCHECK(treeSetIt != operationIdToTreeSet.end());
                YCHECK(treeSetIt->second.erase(treeId) == 1);
            }
        }

        // Aborting orphaned operations.
        for (const auto& pair : operationIdToTreeSet) {
            const auto& operationId = pair.first;
            const auto& treeSet = pair.second;
            bool isOperationExists = OperationIdToOperationState_.find(operationId) != OperationIdToOperationState_.end();
            if (treeSet.empty() && isOperationExists) {
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
        auto resourceLimits = ZeroJobResources();
        for (const auto& descriptor : descriptors) {
            resourceLimits += descriptor.ResourceLimits;
        }

        fluent
            .Item("user_to_ephemeral_pools").Do(BIND(&TFairShareTree::BuildUserToEphemeralPools, tree))
            .Item("fair_share_info").BeginMap()
                .Do(BIND(&TFairShareTree::BuildFairShareInfo, tree))
            .EndMap()
            .Do(BIND(&TFairShareTree::BuildOrchid, tree))
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

ISchedulerStrategyPtr CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host,
    const std::vector<IInvokerPtr>& feasibleInvokers)
{
    return New<TFairShareStrategy>(config, host, feasibleInvokers);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
