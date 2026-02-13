#include "pool_tree.h"

#include "pool_tree_element.h"
#include "pool_tree_snapshot.h"
#include "persistent_state.h"
#include "public.h"
#include "pools_config_parser.h"
#include "resource_tree.h"
#include "strategy.h"
#include "pool_tree_profile_manager.h"
#include "operation.h"
#include "operation_state.h"
#include "field_filter.h"
#include "helpers.h"

#include <yt/yt/server/scheduler/strategy/policy/gpu/scheduling_policy.h>

#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy.h>
#include <yt/yt/server/scheduler/strategy/policy/scheduling_policy_detail.h>

#include <yt/yt/server/scheduler/common/allocation.h>
#include <yt/yt/server/scheduler/common/helpers.h>

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>
#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>
#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/library/vector_hdrf/fair_share_update.h>
#include <yt/yt/library/vector_hdrf/serialize.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NScheduler::NStrategy {

using namespace NPolicy;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;

using NYT::ToProto;

using NVectorHdrf::TFairShareUpdateExecutor;
using NVectorHdrf::TFairShareUpdateOptions;
using NVectorHdrf::TFairShareUpdateContext;
using NVectorHdrf::SerializeDominant;

////////////////////////////////////////////////////////////////////////////////

struct TFairShareUpdateResult
{
    TJobResources ResourceUsage;
    TJobResources ResourceLimits;

    THashMap<TSchedulingTagFilter, TJobResources> ResourceLimitsByTagFilter;

    TNonOwningOperationElementMap EnabledOperationIdToElement;
    TNonOwningOperationElementMap DisabledOperationIdToElement;
    TNonOwningPoolElementMap PoolNameToElement;

    std::vector<TError> Errors;
};

////////////////////////////////////////////////////////////////////////////////

class TResourceDistributionAccumulator
{
public:
    TResourceDistributionAccumulator(
        bool accumulateForPools,
        bool accumulateForOperations)
        : AccumulateForPools_(accumulateForPools)
        , AccumulateForOperations_(accumulateForOperations)
        , LastLocalUpdateTime_(TInstant::Now())
    { }

    void Update(const TPoolTreeSnapshotPtr& treeSnapshot, const TResourceUsageSnapshotPtr& resourceUsageSnapshot)
    {
        auto now = TInstant::Now();
        auto updatePeriod = treeSnapshot->TreeConfig()->AccumulatedResourceDistributionUpdatePeriod;
        auto period = now - LastLocalUpdateTime_;

        if (AccumulateForPools_) {
            for (const auto& [poolName, resourceUsage] : resourceUsageSnapshot->PoolToResourceUsage) {
                LocalPoolToAccumulatedResourceUsage_[poolName] += TResourceVolume(resourceUsage, period);
            }
        }
        if (AccumulateForOperations_) {
            for (const auto& [operationId, operationElement] : treeSnapshot->EnabledOperationMap()) {
                auto fairResources = treeSnapshot->ResourceLimits() * operationElement->Attributes().FairShare.Total;
                auto resourceUsage = GetOrDefault(resourceUsageSnapshot->OperationIdToResourceUsage, operationId);

                LocalOperationIdToAccumulatedResourceDistribution_[operationId].AppendPeriod(fairResources, resourceUsage, period);
            }
        }

        if (LastUpdateTime_ + updatePeriod < now) {
            auto guard = Guard(Lock_);
            if (AccumulateForPools_) {
                for (const auto& [poolName, resourceVolume] : LocalPoolToAccumulatedResourceUsage_) {
                    PoolToAccumulatedResourceUsage_[poolName] += resourceVolume;
                }
            }
            if (AccumulateForOperations_) {
                for (const auto& [operationId, distribution] : LocalOperationIdToAccumulatedResourceDistribution_) {
                    OperationIdToAccumulatedResourceDistribution_[operationId] += distribution;
                }
            }
            LocalPoolToAccumulatedResourceUsage_.clear();
            LocalOperationIdToAccumulatedResourceDistribution_.clear();
            LastUpdateTime_ = now;
        }

        LastLocalUpdateTime_ = now;
    }

    THashMap<TString, TResourceVolume> ExtractPoolResourceUsages()
    {
        YT_VERIFY(AccumulateForPools_);

        auto guard = Guard(Lock_);
        auto result = std::move(PoolToAccumulatedResourceUsage_);
        PoolToAccumulatedResourceUsage_.clear();
        return result;
    }

    THashMap<TOperationId, TAccumulatedResourceDistribution> ExtractOperationResourceDistributionVolumes()
    {
        YT_VERIFY(AccumulateForOperations_);

        auto guard = Guard(Lock_);
        auto result = std::move(OperationIdToAccumulatedResourceDistribution_);
        OperationIdToAccumulatedResourceDistribution_.clear();
        return result;
    }

    TAccumulatedResourceDistribution ExtractOperationAccumulatedResourceDistribution(TOperationId operationId)
    {
        YT_VERIFY(AccumulateForOperations_);

        auto guard = Guard(Lock_);

        TAccumulatedResourceDistribution usage;
        auto it = OperationIdToAccumulatedResourceDistribution_.find(operationId);
        if (it != OperationIdToAccumulatedResourceDistribution_.end()) {
            usage = it->second;
        }
        OperationIdToAccumulatedResourceDistribution_.erase(it);
        return usage;
    }


private:
    const bool AccumulateForPools_;
    const bool AccumulateForOperations_;

    // This maps is updated regularly from some thread pool, no parallel updates are possible.
    THashMap<TString, TResourceVolume> LocalPoolToAccumulatedResourceUsage_;
    THashMap<TOperationId, TAccumulatedResourceDistribution> LocalOperationIdToAccumulatedResourceDistribution_;
    TInstant LastLocalUpdateTime_;

    // This maps is updated rarely and accessed from Control thread.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TString, TResourceVolume> PoolToAccumulatedResourceUsage_;
    THashMap<TOperationId, TAccumulatedResourceDistribution> OperationIdToAccumulatedResourceDistribution_;
    TInstant LastUpdateTime_;
};

////////////////////////////////////////////////////////////////////////////////

void TAccumulatedResourceDistribution::AppendPeriod(const TJobResources& fairResources, const TJobResources& usage, TDuration period)
{
    auto fairResourcesVolume = TResourceVolume(fairResources, period);
    FairResources_ += fairResourcesVolume;

    auto usageVolume = TResourceVolume(usage, period);
    Usage_ += usageVolume;

    UsageDeficit_ += Max(fairResourcesVolume - usageVolume, TResourceVolume());
}

TAccumulatedResourceDistribution& TAccumulatedResourceDistribution::operator+=(const TAccumulatedResourceDistribution& other)
{
    FairResources_ += other.FairResources_;
    Usage_ += other.Usage_;
    UsageDeficit_ += other.UsageDeficit_;
    return *this;
}

void Serialize(const TAccumulatedResourceDistribution& volume, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("fair_resources").Value(volume.FairResources())
            .Item("usage").Value(volume.Usage())
            .Item("usage_deficit").Value(volume.UsageDeficit())
        .EndMap();
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

static const auto EmptyListYsonString = BuildYsonStringFluently()
    .List(std::vector<TString>())
    .ToString();

////////////////////////////////////////////////////////////////////////////////

//! This class represents a pool tree.
//!
//! We maintain following entities:
//!
//!   * Actual tree, it contains the latest and consistent structure of pools and operations.
//!     This tree represented by fields #RootElement_, #OperationIdToElement_, #Pools_.
//!     Update of this tree performed in sequentual manner from #Control thread.
//!
//!   * Snapshot of the tree with scheduling attributes (fair share ratios, best leaf descendants et. c).
//!     It is built repeatedly from actual tree by taking snapshot and calculating scheduling attributes.
//!     Clones of this tree are used in heartbeats for scheduling. Also, element attributes from this tree
//!     are used in orchid, for logging and for profiling.
//!     This tree represented by #TreeSnapshot_.
//!     NB: Elements of this tree may be invalidated by #Alive flag in resource tree. In this case element cannot be safely used
//!     (corresponding operation or pool can be already deleted from all other scheduler structures).
//!
//!   * Resource tree, it is thread safe tree that maintain shared attributes of tree elements.
//!     More details can be find at #TResourceTree.
class TPoolTree
    : public IPoolTree
    , public IPoolTreeElementHost
    , public NPolicy::ISchedulingPolicyHost
{
public:
    using TPoolTreePtr = TIntrusivePtr<TPoolTree>;

    TPoolTree(
        TStrategyTreeConfigPtr config,
        TStrategyOperationControllerConfigPtr controllerConfig,
        IPoolTreeHost* host,
        IStrategyHost* strategyHost,
        const std::vector<IInvokerPtr>& feasibleInvokers,
        std::string treeId)
        : Config_(std::move(config))
        , ConfigNode_(ConvertToNode(Config_))
        , ControllerConfig_(std::move(controllerConfig))
        , TreeId_(std::move(treeId))
        , Logger(StrategyLogger().WithTag("TreeId: %v", TreeId_))
        , Host_(host)
        , StrategyHost_(strategyHost)
        , ResourceTree_(New<TResourceTree>(Config_, feasibleInvokers))
        , Profiler_(
            SchedulerProfiler()
                .WithGlobal()
                .WithProducerRemoveSupport()
                .WithRequiredTag("tree", TreeId_))
        , SchedulingPolicy_(CreateSchedulingPolicy(
            TreeId_,
            Logger,
            MakeWeak(this),
            Host_,
            StrategyHost_,
            Config_,
            Profiler_))
        , GpuSchedulingPolicy_(NPolicy::NGpu::CreateSchedulingPolicy(
            MakeWeak(this),
            StrategyHost_,
            TreeId_,
            Config_,
            Profiler_.WithPrefix("/gpu_policy")))
        , ProfileManager_(New<TPoolTreeProfileManager>(
            Profiler_,
            Config_->SparsifyFairShareProfiling,
            strategyHost->GetFairShareProfilingInvoker(),
            SchedulingPolicy_))
        , FeasibleInvokers_(feasibleInvokers)
        , FairSharePreUpdateTimer_(ProfileManager_->GetProfiler().Timer("/fair_share_preupdate_time"))
        , FairShareUpdateTimer_(ProfileManager_->GetProfiler().Timer("/fair_share_update_time"))
        , FairShareFluentLogTimer_(ProfileManager_->GetProfiler().Timer("/fair_share_fluent_log_time"))
        , FairShareTextLogTimer_(ProfileManager_->GetProfiler().Timer("/fair_share_text_log_time"))
        , AccumulatedPoolResourceUsageForMetering_(
            /*accumulateUsageForPools*/ true,
            /*accumulateUsageForOperations*/ false)
        , AccumulatedOperationsResourceDistributionForProfiling_(
            /*accumulateUsageForPools*/ false,
            /*accumulateUsageForOperations*/ true)
        , AccumulatedOperationsResourceDistributionForLogging_(
            /*accumulateUsageForPools*/ false,
            /*accumulateUsageForOperations*/ true)
    {
        RootElement_ = New<TPoolTreeRootElement>(StrategyHost_, this, Config_, TreeId_, Logger);

        ProfileManager_->RegisterPool(RootElement_);

        SchedulingPolicy_->Initialize();
        GpuSchedulingPolicy_->Initialize();

        YT_LOG_INFO("Pool tree created");
    }


    TStrategyTreeConfigPtr GetConfig() const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        return Config_;
    }

    TStrategyTreeConfigPtr GetSnapshottedConfig() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        return treeSnapshot->TreeConfig();
    }

    bool UpdateConfig(const TStrategyTreeConfigPtr& config) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        TError unrecognizedConfgOptionsError;
        if (config->EnableUnrecognizedAlert) {
            auto unrecognized = config->GetRecursiveUnrecognized();
            if (unrecognized && unrecognized->GetChildCount() > 0) {
                YT_LOG_WARNING("Pool tree config contains unrecognized options (Unrecognized: %v)",
                    ConvertToYsonString(unrecognized, EYsonFormat::Text));
                unrecognizedConfgOptionsError = TError("Pool tree config contains unrecognized options")
                    << TErrorAttribute("unrecognized", unrecognized);
            }
        }

        Host_->SetSchedulerTreeAlert(
            TreeId_,
            ESchedulerAlertType::UnrecognizedPoolTreeConfigOptions,
            unrecognizedConfgOptionsError);

        auto configNode = ConvertToNode(config);
        if (AreNodesEqual(configNode, ConfigNode_)) {
            // Offload destroying config node.
            StrategyHost_->GetBackgroundInvoker()->Invoke(BIND([configNode = std::move(configNode)] { }));
            return false;
        }

        auto oldConfig = Config_;

        Config_ = config;
        ConfigNode_ = std::move(configNode);
        RootElement_->UpdateTreeConfig(Config_);
        ResourceTree_->UpdateConfig(Config_);

        SchedulingPolicy_->UpdateConfig(Config_);
        GpuSchedulingPolicy_->UpdateConfig(Config_);

        auto pool = FindPool(Config_->DefaultParentPool);
        if (!pool && Config_->DefaultParentPool != RootPoolName) {
            auto error = TError("Default parent pool %Qv in tree %Qv is not registered", Config_->DefaultParentPool, TreeId_);
            Host_->SetSchedulerTreeAlert(TreeId_, ESchedulerAlertType::InvalidDefaultParentPool, error);
        } else if (pool && pool->GetMode() == ESchedulingMode::Fifo) {
            auto error = TError("Failed to use pool %Qv in tree %Qv as default parent pool because it has FIFO mode", Config_->DefaultParentPool, TreeId_);
            Host_->SetSchedulerTreeAlert(TreeId_, ESchedulerAlertType::InvalidDefaultParentPool, error);
        } else {
            Host_->SetSchedulerTreeAlert(TreeId_, ESchedulerAlertType::InvalidDefaultParentPool, TError());
        }

        YT_LOG_INFO("Tree has updated with new config");

        return true;
    }

    void UpdateControllerConfig(const TStrategyOperationControllerConfigPtr& config) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        ControllerConfig_ = config;

        for (const auto& [operationId, element] : OperationIdToElement_) {
            element->UpdateControllerConfig(config);
        }
    }

    const TSchedulingTagFilter& GetNodeTagFilter() const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        return Config_->NodeTagFilter;
    }

    // NB: This function is public for scheduler simulator.
    TFuture<std::pair<IPoolTreePtr, TError>> OnFairShareUpdateAt(TInstant now) override
    {
        return BIND(&TPoolTree::DoFairShareUpdateAt, MakeStrong(this), now)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    void FinishFairShareUpdate() override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_VERIFY(TreeSnapshotPrecommit_);

        auto oldTreeSnapshot = std::move(TreeSnapshot_);
        TreeSnapshot_ = std::move(TreeSnapshotPrecommit_);
        TreeSnapshotPrecommit_.Reset();

        AtomicTreeSnapshot_ = TreeSnapshot_;

        YT_LOG_DEBUG("Stored updated pool tree snapshot");

        // Offload destroying previous tree snapshot.
        StrategyHost_->GetBackgroundInvoker()->Invoke(BIND([oldTreeSnapshot = std::move(oldTreeSnapshot)] { }));
    }

    bool HasOperation(TOperationId operationId) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    TRegistrationResult RegisterOperation(
        const TStrategyOperationStatePtr& state,
        const TStrategyOperationSpecPtr& spec,
        const TOperationPoolTreeRuntimeParametersPtr& runtimeParameters,
        const TOperationOptionsPtr& operationOptions) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        TForbidContextSwitchGuard contextSwitchGuard;

        auto operationId = state->GetHost()->GetId();

        auto operationElement = New<TPoolTreeOperationElement>(
            Config_,
            spec,
            operationOptions,
            runtimeParameters,
            state->GetController(),
            ControllerConfig_,
            state,
            StrategyHost_,
            this,
            state->GetHost(),
            TreeId_,
            Logger);

        SchedulingPolicy_->RegisterOperation(operationElement.Get());
        GpuSchedulingPolicy_->RegisterOperation(operationElement.Get());

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

        YT_LOG_INFO("Operation element registered in tree (OperationId: %v, Pool: %v, MarkedAsRunning: %v)",
            operationId,
            poolName.ToString(),
            isRunningInPool);

        return TRegistrationResult{
            .AllowIdleCpuPolicy = operationElement->IsIdleCpuPolicyAllowed(),
        };
    }

    void UnregisterOperation(const TStrategyOperationStatePtr& state) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationId = state->GetHost()->GetId();
        auto operationElement = GetOperationElement(operationId);

        auto* pool = operationElement->GetMutableParent();

        // Profile finished operation.
        ProfileManager_->ProfileOperationUnregistration(pool, state->GetHost()->GetState());

        SchedulingPolicy_->DisableOperation(operationElement.Get(), /*markAsNonAlive*/ true);
        GpuSchedulingPolicy_->DisableOperation(operationElement.Get(), /*markAsNonAlive*/ true);
        operationElement->DetachParent();

        ReleaseOperationSlotIndex(state, pool->GetId());
        OnOperationRemovedFromPool(state, operationElement, pool);

        SchedulingPolicy_->UnregisterOperation(operationElement.Get());
        GpuSchedulingPolicy_->UnregisterOperation(operationElement.Get());

        EraseOrCrash(OperationIdToElement_, operationId);

        // Operation can be missing in these maps.
        OperationIdToActivationTime_.erase(operationId);
        OperationIdToFirstFoundLimitingAncestorTime_.erase(operationId);
    }

    void EnableOperation(const TStrategyOperationStatePtr& state) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationId = state->GetHost()->GetId();
        auto operationElement = GetOperationElement(operationId);

        operationElement->GetMutableParent()->EnableChild(operationElement);

        SchedulingPolicy_->EnableOperation(operationElement.Get());
        GpuSchedulingPolicy_->EnableOperation(operationElement.Get());
    }

    void DisableOperation(const TStrategyOperationStatePtr& state) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationElement = GetOperationElement(state->GetHost()->GetId());
        SchedulingPolicy_->DisableOperation(operationElement.Get(), /*markAsNonAlive*/ false);
        GpuSchedulingPolicy_->DisableOperation(operationElement.Get(), /*markAsNonAlive*/ false);

        operationElement->GetMutableParent()->DisableChild(operationElement);
    }

    void ChangeOperationPool(
        TOperationId operationId,
        const TPoolName& newPool,
        bool ensureRunning) override
    {
        auto element = FindOperationElement(operationId);
        if (!element) {
            THROW_ERROR_EXCEPTION("Operation element for operation %Qv not found", operationId);
        }

        ChangeOperationPool(element, newPool, ensureRunning);
    }

    void ChangeOperationPool(
        const TPoolTreeOperationElementPtr& element,
        const TPoolName& newPool,
        bool ensureRunning)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        bool operationWasRunning = element->IsOperationRunningInPool();

        auto state = element->GetStrategyOperationState();

        auto oldParent = element->GetMutableParent();
        auto newParent = GetOrCreatePool(newPool, state->GetHost()->GetAuthenticatedUser());

        ReleaseOperationSlotIndex(state, oldParent->GetId());

        int newSlotIndex = AllocateOperationSlotIndex(state, newParent->GetId());
        element->ChangeParent(newParent.Get(), newSlotIndex);
        state->GetHost()->SetSlotIndex(TreeId_, newSlotIndex);

        OnOperationRemovedFromPool(state, element, oldParent);
        bool isRunningInNewPool = OnOperationAddedToPool(state, element);
        if (ensureRunning) {
            YT_VERIFY(isRunningInNewPool);
        }

        if (!operationWasRunning && isRunningInNewPool) {
            OperationRunning_.Fire(element->GetOperationId());
        }
    }

    void UpdateOperationRuntimeParameters(
        TOperationId operationId,
        TSchedulingTagFilter schedulingTagFilter,
        const TOperationPoolTreeRuntimeParametersPtr& runtimeParameters) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (const auto& element = FindOperationElement(operationId)) {
            element->SetSchedulingTagFilter(std::move(schedulingTagFilter));
            element->SetRuntimeParameters(runtimeParameters);
        }
    }

    void RegisterAllocationsFromRevivedOperation(TOperationId operationId, std::vector<TAllocationPtr> allocations) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& element = FindOperationElement(operationId);
        SchedulingPolicy_->RegisterAllocationsFromRevivedOperation(element.Get(), std::move(allocations));
    }

    void RegisterNode(TNodeId nodeId, const std::string& nodeAddress) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        ++NodeCount_;
        NodeIdToAddress_.emplace(nodeId, nodeAddress);

        SchedulingPolicy_->RegisterNode(nodeId, nodeAddress);
        GpuSchedulingPolicy_->RegisterNode(nodeId, nodeAddress);
    }

    void UnregisterNode(TNodeId nodeId) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        --NodeCount_;
        NodeIdToAddress_.erase(nodeId);

        SchedulingPolicy_->UnregisterNode(nodeId);
        GpuSchedulingPolicy_->UnregisterNode(nodeId);
    }

    const std::string& GetId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return TreeId_;
    }

    TError CheckOperationIsStuck(
        TOperationId operationId,
        const TOperationStuckCheckOptionsPtr& options) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = FindOperationElementInSnapshot(operationId);
        if (!element) {
            return TError();
        }

        auto now = TInstant::Now();
        TInstant activationTime;
        {
            auto it = OperationIdToActivationTime_.find(operationId);
            if (!element->IsAlive()) {
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

        bool hasNeededResources = !element->GroupedNeededResources().empty();
        auto aggregatedMinNeededResources = element->AggregatedMinNeededAllocationResources();
        bool shouldCheckLimitingAncestor = hasNeededResources &&
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

                if (activationTime + options->LimitingAncestorSafeTimeout < now &&
                    firstFoundLimitingAncestorTime + options->LimitingAncestorSafeTimeout < now)
                {
                    const auto& resourceLimits = limitingAncestor->MaybeSpecifiedResourceLimits();
                    YT_VERIFY(resourceLimits);

                    std::vector<EJobResourceType> violatedResourceTypes;
                    #define XX(name, Name) \
                        if (aggregatedMinNeededResources.Get##Name() > resourceLimits->Get##Name()) { \
                            violatedResourceTypes.push_back(EJobResourceType::Name); \
                        }
                    ITERATE_JOB_RESOURCES(XX)
                    #undef XX

                    return TError(
                        "Operation has ancestor %Qv whose specified limits for resources %lv are too small to satisfy "
                        "operation's minimum allocation resource demand",
                        limitingAncestor->GetId(),
                        violatedResourceTypes)
                        << TErrorAttribute("safe_timeout", options->LimitingAncestorSafeTimeout)
                        << TErrorAttribute("resource_limits", *resourceLimits)
                        << TErrorAttribute("min_needed_resources", aggregatedMinNeededResources);
                }
            } else if (it != OperationIdToFirstFoundLimitingAncestorTime_.end()) {
                it->second = TInstant::Max();
            }
        }

        auto schedulingPolicyError = NPolicy::TSchedulingPolicy::CheckOperationIsStuck(
            GetTreeSnapshot(),
            element,
            now,
            activationTime,
            options);
        if (!schedulingPolicyError.IsOK()) {
            return schedulingPolicyError;
        }

        return TError();
    }

    void ProcessActivatableOperations() override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        while (!ActivatableOperationIds_.empty()) {
            auto operationId = ActivatableOperationIds_.back();
            ActivatableOperationIds_.pop_back();
            OperationRunning_.Fire(operationId);
        }
    }

    void TryRunAllPendingOperations() override
    {
        std::vector<TOperationId> readyOperationIds;
        std::vector<std::pair<TPoolTreeOperationElementPtr, TPoolTreeCompositeElement*>> stillPending;
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

    TPoolName CreatePoolName(const std::optional<TString>& poolFromSpec, const std::string& user) const override
    {
        // TODO(babenko): switch to std::string
        auto poolName = poolFromSpec.value_or(TString(user));

        auto pool = FindPool(poolName);
        if (pool && pool->GetConfig()->CreateEphemeralSubpools) {
            // TODO(babenko): switch to std::string
            return TPoolName(TString(user), poolName);
        }
        return TPoolName(poolName, std::nullopt);
    }

    const TOffloadingSettings& GetOffloadingSettingsFor(const TString& poolName, const std::string& user) const override
    {
        const TPoolTreeCompositeElement* pool = FindPool(poolName).Get();
        if (!pool) {
            pool = GetDefaultParentPoolForUser(user).Get();
        }

        while (!pool->IsRoot()) {
            const auto& offloadingSettings = pool->GetOffloadingSettings();
            if (!offloadingSettings.empty()) {
                return offloadingSettings;
            }
            pool = pool->GetParent();
        }

        return EmptyOffloadingSettings;
    }

    TPoolsUpdateResult UpdatePools(const INodePtr& poolsNode, bool forceUpdate) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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
                    auto pool = New<TPoolTreePoolElement>(
                        StrategyHost_,
                        this,
                        updatePoolAction.Name,
                        updatePoolAction.ObjectId,
                        updatePoolAction.PoolConfig,
                        /*defaultConfigured*/ false,
                        Config_,
                        TreeId_,
                        Logger);
                    const auto& parent = updatePoolAction.ParentName == RootPoolName
                        ? static_cast<TPoolTreeCompositeElementPtr>(RootElement_)
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

                        ApplyEphemeralSubpoolConfig(defaultParent, pool->GetConfig());
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
                    ReconfigurePool(pool, updatePoolAction.PoolConfig, updatePoolAction.ObjectId);
                    if (updatePoolAction.Type == EUpdatePoolActionType::Move) {
                        const auto& parent = updatePoolAction.ParentName == RootPoolName
                            ? static_cast<TPoolTreeCompositeElementPtr>(RootElement_)
                            : GetPool(updatePoolAction.ParentName);
                        pool->ChangeParent(parent.Get());
                    }
                    break;
                }
            }
        }

        std::vector<TPoolTreePoolElementPtr> staleEphemeralPools;
        for (const auto& [poolName, pool] : Pools_) {
            if (pool->IsDefaultConfigured() && pool->GetId().Contains(TPoolName::Delimiter) && !pool->GetParent()->IsEphemeralHub()) {
                staleEphemeralPools.push_back(pool);
            }
        }
        for (const auto& pool : staleEphemeralPools) {
            YT_LOG_INFO("Stale user ephemeral pool found, moving all its operations to parent pool (EphemeralPool: %v, ParentPool: %v)",
                pool->GetId(),
                pool->GetParent()->GetId());
            for (const auto& operation : pool->GetChildOperations()) {
                ChangeOperationPool(
                    operation->GetOperationId(),
                    TPoolName(pool->GetParent()->GetId(), /*parent*/ std::nullopt),
                    /*ensureRunning*/ false);
            }
        }

        LastPoolsNodeUpdateError_ = TError();

        return {LastPoolsNodeUpdateError_, true};
    }

    TError ValidateUserToDefaultPoolMap(const THashMap<std::string, TString>& userToDefaultPoolMap) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    void ValidatePoolLimits(const IOperation* operation, const TPoolName& poolName) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        ValidateOperationCountLimit(poolName, operation->GetAuthenticatedUser());
        ValidateEphemeralPoolLimit(operation, poolName);
    }

    void ValidatePoolLimitsOnPoolChange(const IOperation* operation, const TPoolName& newPoolName) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        ValidateEphemeralPoolLimit(operation, newPoolName);
        ValidateAllOperationCountsOnPoolChange(operation->GetId(), newPoolName);
    }

    TFuture<void> ValidateOperationPoolsCanBeUsed(const IOperation* operation, const TPoolName& poolName) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        return BIND(&TPoolTree::DoValidateOperationPoolsCanBeUsed, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operation, poolName);
    }

    TFuture<void> ValidateOperationPoolPermissions(TOperationId operationId, const std::string& user, NYTree::EPermissionSet permissions) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        return BIND(&TPoolTree::DoValidateOperationPoolPermissions, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operationId, user, permissions);
    }

    void EnsureOperationPoolExistence(const TString& poolName) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (!FindPool(poolName)) {
            THROW_ERROR_EXCEPTION(EErrorCode::OperationLaunchedInNonexistentPool, "Pool %Qv not found", poolName);
        }
    }

    TError CheckOperationJobResourceLimitsRestrictions(const TPoolTreeOperationElementPtr& element) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& initialGroupedNeededResources = element->GetInitialGroupedNeededResources();

        for (const auto& [taskName, allocationGroupResources] : initialGroupedNeededResources) {
            THashSet<EJobResourceType> resourcesWithViolatedRestrictions;
            Config_->MinJobResourceLimits->ForEachResource([&] (auto NVectorHdrf::TJobResourcesConfig::* resourceDataMember, EJobResourceType resourceType) {
                if (auto lowerBound = Config_->MinJobResourceLimits.Get()->*resourceDataMember) {
                    if (GetResource(allocationGroupResources.MinNeededResources, resourceType) < static_cast<double>(*lowerBound)) {
                        resourcesWithViolatedRestrictions.insert(resourceType);
                    }
                }
                if (auto upperBound = Config_->MaxJobResourceLimits.Get()->*resourceDataMember) {
                    if (GetResource(allocationGroupResources.MinNeededResources, resourceType) > static_cast<double>(*upperBound)) {
                        resourcesWithViolatedRestrictions.insert(resourceType);
                    }
                }
            });

            if (!resourcesWithViolatedRestrictions.empty()) {
                return TError(
                        EErrorCode::JobResourceLimitsRestrictionsViolated,
                        "Operation has jobs with resource demand that violates restrictions for resources %lv in tree %Qlv",
                        resourcesWithViolatedRestrictions,
                        GetId())
                    << TErrorAttribute("operation_id", element->GetOperationId())
                    << TErrorAttribute("task", taskName)
                    << TErrorAttribute("job_resource_demand", allocationGroupResources.MinNeededResources)
                    << TErrorAttribute("lower_bounds", Config_->MinJobResourceLimits)
                    << TErrorAttribute("upper_bounds", Config_->MaxJobResourceLimits);
            }
        }

        return TError();
    }

    TPersistentTreeStatePtr BuildPersistentState() const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto result = New<TPersistentTreeState>();
        for (const auto& [poolId, pool] : Pools_) {
            if (pool->GetIntegralGuaranteeType() != EIntegralGuaranteeType::None) {
                auto state = New<TPersistentPoolState>();
                state->AccumulatedResourceVolume = pool->IntegralResourcesState().AccumulatedVolume;
                result->PoolStates.emplace(poolId, std::move(state));
            }
        }

        result->SchedulingPolicyState = SchedulingPolicy_->BuildPersistentState();
        result->GpuSchedulingPolicyState = GpuSchedulingPolicy_->BuildPersistentState();

        return result;
    }

    void InitPersistentState(const TPersistentTreeStatePtr& persistentState) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        for (const auto& [poolName, poolState] : persistentState->PoolStates) {
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

        SchedulingPolicy_->InitPersistentState(persistentState->SchedulingPolicyState);
        GpuSchedulingPolicy_->InitPersistentState(persistentState->GpuSchedulingPolicyState);
    }

    TError OnOperationMaterialized(TOperationId operationId) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);
        auto error = SchedulingPolicy_->OnOperationMaterialized(element.Get());

        auto gpuPolicyError = GpuSchedulingPolicy_->OnOperationMaterialized(element.Get());
        YT_LOG_DEBUG_UNLESS(gpuPolicyError.IsOK(),
            gpuPolicyError,
            "Error occurred while processing materialized operation in GPU scheduling policy (OperationId: %v)",
            operationId);

        return error;
    }

    TError CheckOperationJobResourceLimitsRestrictions(TOperationId operationId, bool revivedFromSnapshot) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);

        // NB(eshcherbin): We don't apply this check to a revived operation so that
        // it doesn't fail in the middle of its progress.
        // TODO(eshcherbin): |revivedFromSnapshot| isn't 100% reliable because operation can be revived with clean start.
        if (const auto& error = CheckOperationJobResourceLimitsRestrictions(element);
            !error.IsOK() && !revivedFromSnapshot)
        {
            return error;
        }

        return {};
    }

    TError CheckOperationSchedulingInSeveralTreesAllowed(TOperationId operationId) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);
        return SchedulingPolicy_->CheckOperationSchedulingInSeveralTreesAllowed(element.Get());
    }

    std::vector<TString> GetAncestorPoolNames(const TPoolTreeOperationElement* element) const
    {
        std::vector<TString> result;
        const auto* current = element->GetParent();
        while (!current->IsRoot()) {
            result.push_back(current->GetId());
            current = current->GetParent();
        }
        std::reverse(result.begin(), result.end());
        return result;
    }

    void BuildOperationAttributes(TOperationId operationId, TFluentMap fluent) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = GetOperationElement(operationId);

        fluent
            .Item("pool").Value(element->GetParent()->GetId())
            .Item("ancestor_pools").Value(GetAncestorPoolNames(element.Get()));
    }

    void BuildOperationProgress(TOperationId operationId, TFluentMap fluent) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (auto treeSnapshot = GetTreeSnapshot()) {
            if (auto element = treeSnapshot->FindEnabledOperationElement(operationId)) {
                DoBuildOperationProgress(treeSnapshot, element, StrategyHost_, fluent);
            }
        }
    }

    void BuildBriefOperationProgress(TOperationId operationId, TFluentMap fluent) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto* parent = element->GetParent();
        const auto& attributes = element->Attributes();
        fluent
            .Item("pool").Value(parent->GetId())
            .Item("weight").Value(element->GetWeight())
            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            // Do we even need these values?? They are always zero because |element| is not taken from snapshot.
            .Item("fair_share_ratio").Value(MaxComponent(attributes.FairShare.Total))
            .Item("dominant_fair_share").Value(MaxComponent(attributes.FairShare.Total));
    }

    void BuildUserToEphemeralPoolsInDefaultPool(TFluentAny fluent) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        fluent
            .DoMapFor(UserToEphemeralPoolsInDefaultPool_, [] (TFluentMap fluent, const auto& pair) {
                const auto& [userName, ephemeralPools] = pair;
                fluent
                    .Item(userName).Value(ephemeralPools);
            });
    }

    void BuildStaticPoolsInformation(TFluentAny fluent) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        fluent
            .DoMapFor(Pools_, [&] (TFluentMap fluent, const auto& pair) {
                const auto& [poolName, pool] = pair;
                fluent
                    .Item(poolName).Value(pool->GetConfig());
            });
    }

    void BuildFairShareInfo(TFluentMap fluent) const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        Y_UNUSED(WaitFor(BIND(&TPoolTree::DoBuildFullFairShareInfo, MakeWeak(this), GetTreeSnapshot(), fluent)
            .AsyncVia(StrategyHost_->GetOrchidWorkerInvoker())
            .Run()));
    }

    static IYPathServicePtr FromProducer(
        TExtendedYsonProducer<const TFieldFilter&> producer)
    {
        return IYPathService::FromProducer(BIND(
            [producer{std::move(producer)}] (IYsonConsumer* consumer, const IAttributeDictionaryPtr& options) {
                TFieldFilter filter{options};
                producer.Run(consumer, filter);
            }));
    }

    IYPathServicePtr GetOrchidService() const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto dynamicOrchidService = New<TCompositeMapService>();

        dynamicOrchidService->AddChild("operations_by_pool", New<TOperationsByPoolOrchidService>(MakeStrong(this))
            ->Via(StrategyHost_->GetOrchidWorkerInvoker()));

        dynamicOrchidService->AddChild("pools", New<TPoolsOrchidService>(MakeStrong(this))
            ->Via(StrategyHost_->GetOrchidWorkerInvoker()));

        dynamicOrchidService->AddChild("child_pools_by_pool", New<TChildPoolsByPoolOrchidService>(MakeStrong(this))
            ->Via(StrategyHost_->GetOrchidWorkerInvoker()));

        dynamicOrchidService->AddChild("operations", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            const auto buildOperationInfo = [&] (TFluentMap fluent, const TPoolTreeOperationElement* const operation) {
                fluent
                    .Item(operation->GetId()).BeginMap()
                        .Do(BIND(
                            &TPoolTree::DoBuildOperationProgress,
                            ConstRef(treeSnapshot),
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
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).Value(treeSnapshot->TreeConfig());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("resource_usage", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).Value(treeSnapshot->ResourceUsage());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("resource_limits", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).Value(treeSnapshot->ResourceLimits());
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("node_count", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).Value(std::ssize(treeSnapshot->NodeAddresses()));
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        dynamicOrchidService->AddChild("node_addresses", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).Value(GetValues(treeSnapshot->NodeAddresses()));
        })))->Via(StrategyHost_->GetOrchidWorkerInvoker());

        // TODO(eshcherbin): Why not use tree snapshot here as well?
        dynamicOrchidService->AddChild("pool_count", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

            BuildYsonFluently(consumer).Value(GetPoolCount());
        })));

        dynamicOrchidService->AddChild("resource_distribution_info", IYPathService::FromProducer(BIND([this_ = MakeStrong(this), this] (IYsonConsumer* consumer) {
            auto treeSnapshot = GetTreeSnapshotForOrchid();

            BuildYsonFluently(consumer).BeginMap()
                .Do(BIND(&TPoolTreeRootElement::BuildResourceDistributionInfo, treeSnapshot->RootElement()))
            .EndMap();
        }))->Via(StrategyHost_->GetOrchidWorkerInvoker()));

        SchedulingPolicy_->PopulateOrchidService(dynamicOrchidService);
        GpuSchedulingPolicy_->PopulateOrchidService(dynamicOrchidService);

        return dynamicOrchidService;
    }

    TResourceTree* GetResourceTree() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ResourceTree_.Get();
    }

    TPoolTreeProfileManager* GetProfiler()
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ProfileManager_.Get();
    }

    void SetResourceUsageSnapshot(TResourceUsageSnapshotPtr snapshot)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (snapshot != nullptr) {
            ResourceUsageSnapshot_.Store(std::move(snapshot));
        } else {
            if (ResourceUsageSnapshot_.Acquire()) {
                ResourceUsageSnapshot_.Store(nullptr);
            }
        }
    }

    const TJobResourcesByTagFilter& GetResourceLimitsByTagFilter() const override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        static const TJobResourcesByTagFilter EmptyJobResourcesByTagFilter;

        auto snapshot = GetTreeSnapshot();
        return snapshot
            ? snapshot->ResourceLimitsByTagFilter()
            : EmptyJobResourcesByTagFilter;
    }

private:
    TStrategyTreeConfigPtr Config_;
    INodePtr ConfigNode_;

    TStrategyOperationControllerConfigPtr ControllerConfig_;

    const std::string TreeId_;
    const NLogging::TLogger Logger;

    IPoolTreeHost* const Host_;
    IStrategyHost* const StrategyHost_;

    TResourceTreePtr ResourceTree_;

    const NProfiling::TProfiler Profiler_;
    const NPolicy::ISchedulingPolicyPtr SchedulingPolicy_;
    const NPolicy::NGpu::ISchedulingPolicyPtr GpuSchedulingPolicy_;
    const TPoolTreeProfileManagerPtr ProfileManager_;

    const std::vector<IInvokerPtr> FeasibleInvokers_;


    INodePtr LastPoolsNodeUpdate_;
    TError LastPoolsNodeUpdateError_;

    TPoolElementMap Pools_;

    std::optional<TInstant> LastFairShareUpdateTime_;

    THashMap<std::string, THashSet<TString>> UserToEphemeralPoolsInDefaultPool_;

    THashMap<TString, THashSet<int>> PoolToSpareSlotIndices_;
    THashMap<TString, int> PoolToMinUnusedSlotIndex_;

    TOperationElementMap OperationIdToElement_;

    THashMap<TOperationId, TInstant> OperationIdToActivationTime_;
    THashMap<TOperationId, TInstant> OperationIdToFirstFoundLimitingAncestorTime_;

    TAtomicIntrusivePtr<TResourceUsageSnapshot> ResourceUsageSnapshot_;

    std::vector<TOperationId> ActivatableOperationIds_;

    TPoolTreeRootElementPtr RootElement_;

    // NB(eshcherbin): We have the set of nodes both in strategy and in tree allocation scheduler.
    // Here we only keep current node count to have it ready for snapshot.
    int NodeCount_ = 0;
    THashMap<TNodeId, std::string> NodeIdToAddress_;

    class TPoolsOrchidServiceBase
        : public TYPathServiceBase
        , public TSupportsList
        , public TSupportsExists
    {
    protected:
        explicit TPoolsOrchidServiceBase(TIntrusivePtr<const TPoolTree> tree)
            : PoolTree_(std::move(tree))
        { }


    private:
        TIntrusivePtr<const TPoolTree> PoolTree_;

        virtual IYPathServicePtr GetSelfServiceProducer(TPoolTreeSnapshotPtr&& poolTreeSnapshot) = 0;

        TResolveResult ResolveSelf(
            const TYPath& path,
            const IYPathServiceContextPtr& context) final
        {
            if (context->GetMethod() == "List") {
                auto typedContext = New<TCtxList>(context, NRpc::THandlerInvocationOptions{});
                if (!typedContext->DeserializeRequest()) {
                    THROW_ERROR_EXCEPTION("Error deserializing request");
                }

                const auto& request = typedContext->Request();
                if (!request.has_attributes() || NYT::FromProto<TAttributeFilter>(request.attributes()).IsEmpty()) {
                    return TResolveResultHere{path};
                }
            }

            return TResolveResultThere{
                GetSelfServiceProducer(PoolTree_->GetTreeSnapshotForOrchid()),
                path
            };
        }

        TResolveResult ResolveAttributes(
            const TYPath& path,
            const IYPathServiceContextPtr& context) final
        {
            return ResolveSelf("/@" + path, context);
        }

        virtual IYPathServicePtr GetRecursiveServiceProducer(TPoolTreeSnapshotPtr&& poolTreeSnapshot, const TString& poolName) = 0;

        TResolveResult ResolveRecursive(
            const TYPath& path,
            const IYPathServiceContextPtr& context) final
        {
            auto poolTreeSnapshot = PoolTree_->GetTreeSnapshotForOrchid();

            NYPath::TTokenizer tokenizer(path);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);

            const auto& poolName = tokenizer.GetLiteralValue();
            if (poolName != RootPoolName && !poolTreeSnapshot->PoolMap().contains(poolName)) {
                // TODO(omgronny): rewrite it properly
                if (context->GetMethod() == "Exists") {
                    return TResolveResultHere{path};
                }
                THROW_ERROR_EXCEPTION("Pool tree %Qv has no pool %Qv",
                    PoolTree_->TreeId_,
                    poolName);
            }

            // TODO(pogorelov): May be support limit here
            return TResolveResultThere{
                GetRecursiveServiceProducer(std::move(poolTreeSnapshot), poolName),
                NYPath::TYPath(tokenizer.GetSuffix())
            };
        }

        bool DoInvoke(const IYPathServiceContextPtr& context) final
        {
            DISPATCH_YPATH_SERVICE_METHOD(List);
            DISPATCH_YPATH_SERVICE_METHOD(Exists);
            return TYPathServiceBase::DoInvoke(context);
        }

        void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) final
        {
            i64 limit = request->has_limit()
                ? request->limit()
                : DefaultVirtualChildLimit;

            if (limit <= 0) {
                THROW_ERROR_EXCEPTION("Invalid limit value %v", limit);
            }

            auto poolTreeSnapshot = PoolTree_->GetTreeSnapshotForOrchid();

            bool incomplete = false;
            const auto& poolMap = poolTreeSnapshot->PoolMap();

            std::vector<TString> result;
            result.reserve(std::ssize(poolMap) + 1);
            result.push_back(RootPoolName);
            for (const auto& [name, _] : poolMap) {
                result.push_back(name);
            }

            // NB: We do not have many pools, so we can just sort all of it, without finding top min elements.
            std::sort(std::begin(result), std::end(result));
            if (std::ssize(result) > limit) {
                result.resize(limit);
                incomplete = true;
            }

            auto ysonString = BuildYsonStringFluently().BeginAttributes()
                    .DoIf(incomplete, [] (TFluentMap fluent) {
                        fluent.Item("incomplete").Value(true);
                    })
                .EndAttributes()
                .List(result);

            response->set_value(ToProto(ysonString));
            context->Reply();
        }

        void ListRecursive(const TYPath& /*path*/, TReqList* /*request*/, TRspList* /*response*/, const TCtxListPtr& /*context*/) final
        {
            YT_ABORT();
        }

        void ListAttribute(const TYPath& /*path*/, TReqList* /*request*/, TRspList* response, const TCtxListPtr& context) final
        {
            response->set_value(EmptyListYsonString);
            context->Reply();
        }
    };

    class TPoolsOrchidService
        : public TPoolsOrchidServiceBase
    {
    public:
        explicit TPoolsOrchidService(TIntrusivePtr<const TPoolTree> tree)
            : TPoolsOrchidServiceBase(std::move(tree))
        { }


    private:
        IYPathServicePtr GetSelfServiceProducer(TPoolTreeSnapshotPtr&& poolTreeSnapshot) override
        {
            return TPoolTree::FromProducer(BIND(
                [poolTreeSnapshot = std::move(poolTreeSnapshot)]
                (IYsonConsumer* consumer, const TFieldFilter& filter) mutable {
                    BuildYsonFluently(consumer).BeginMap()
                        .Do(
                            std::bind(
                                &TPoolTree::BuildPoolsInfo,
                                std::move(poolTreeSnapshot),
                                std::cref(filter),
                                std::placeholders::_1))
                    .EndMap();
                }));
        }

        IYPathServicePtr GetRecursiveServiceProducer(TPoolTreeSnapshotPtr&& poolTreeSnapshot, const TString& poolName) override
        {
            return TPoolTree::FromProducer(BIND(
                [poolTreeSnapshot = std::move(poolTreeSnapshot), poolName]
                (IYsonConsumer* consumer, const TFieldFilter& filter) {
                    BuildYsonFluently(consumer).BeginMap()
                        .Do([&] (TFluentMap fluent) {
                            if (poolName == RootPoolName) {
                                TPoolTree::BuildCompositeElementInfo(
                                    poolTreeSnapshot,
                                    poolTreeSnapshot->RootElement().Get(),
                                    filter,
                                    std::move(fluent));
                            } else {
                                auto* pool = GetOrCrash(poolTreeSnapshot->PoolMap(), poolName);
                                TPoolTree::BuildPoolInfo(
                                    poolTreeSnapshot,
                                    pool,
                                    filter,
                                    std::move(fluent));
                            }
                        })
                    .EndMap();
                }));
        }
    };

    class TChildPoolsByPoolOrchidService
        : public TPoolsOrchidServiceBase
    {
    public:
        explicit TChildPoolsByPoolOrchidService(TIntrusivePtr<const TPoolTree> tree)
            : TPoolsOrchidServiceBase{std::move(tree)}
        { }


    private:
        IYPathServicePtr GetSelfServiceProducer(TPoolTreeSnapshotPtr&& poolTreeSnapshot) override
        {
            return TPoolTree::FromProducer(BIND(
                [poolTreeSnapshot = std::move(poolTreeSnapshot)]
                (IYsonConsumer* consumer, const TFieldFilter& filter) mutable {
                    BuildYsonFluently(consumer).BeginMap()
                        .Do(
                            std::bind(
                                &TPoolTree::BuildChildPoolsByPoolInfos,
                                std::move(poolTreeSnapshot),
                                std::cref(filter),
                                std::placeholders::_1))
                    .EndMap();
                }));
        }

        IYPathServicePtr GetRecursiveServiceProducer(TPoolTreeSnapshotPtr&& poolTreeSnapshot, const TString& poolName) override
        {
            return TPoolTree::FromProducer(BIND(
                [poolTreeSnapshot = std::move(poolTreeSnapshot), poolName]
                (IYsonConsumer* consumer, const TFieldFilter& filter) {
                    BuildYsonFluently(consumer).BeginMap()
                        .Do(
                            std::bind(
                                &TPoolTree::BuildChildPoolsByPoolInfo,
                                std::move(poolTreeSnapshot),
                                std::cref(filter),
                                poolName,
                                std::placeholders::_1))
                    .EndMap();
                }));
        }
    };

    class TOperationsByPoolOrchidService
        : public TVirtualMapBase
    {
    public:
        explicit TOperationsByPoolOrchidService(TIntrusivePtr<const TPoolTree> tree)
            : PoolTree_{std::move(tree)}
        { }

        i64 GetSize() const final
        {
            YT_ASSERT_INVOKER_AFFINITY(PoolTree_->StrategyHost_->GetOrchidWorkerInvoker());

            auto poolTreeSnapshot = PoolTree_->GetTreeSnapshotForOrchid();

            return std::ssize(poolTreeSnapshot->PoolMap());
        }

        std::vector<std::string> GetKeys(const i64 limit) const final
        {
            YT_ASSERT_INVOKER_AFFINITY(PoolTree_->StrategyHost_->GetOrchidWorkerInvoker());

            if (!limit) {
                return {};
            }

            const auto poolTreeSnapshot = PoolTree_->GetTreeSnapshotForOrchid();

            std::vector<std::string> result;
            result.reserve(std::min(limit, std::ssize(poolTreeSnapshot->PoolMap())));

            for (const auto& [name, _] : poolTreeSnapshot->PoolMap()) {
                if (std::ssize(result) >= limit) {
                    break;
                }
                result.push_back(name);
            }

            return result;
        }

        IYPathServicePtr FindItemService(const std::string& poolName) const final
        {
            YT_ASSERT_INVOKER_AFFINITY(PoolTree_->StrategyHost_->GetOrchidWorkerInvoker());

            const auto poolTreeSnapshot = PoolTree_->GetTreeSnapshotForOrchid();

            const auto poolIterator = poolTreeSnapshot->PoolMap().find(poolName);
            if (poolIterator == std::cend(poolTreeSnapshot->PoolMap())) {
                return nullptr;
            }

            const auto& [_, element] = *poolIterator;
            const auto operations = element->GetChildOperations();

            auto operationsYson = BuildYsonStringFluently().BeginMap()
                    .Do([&] (TFluentMap fluent) {
                        for (const auto operation : operations) {
                            fluent
                                .Item(operation->GetId()).BeginMap()
                                    .Do(std::bind(
                                        &TPoolTree::DoBuildOperationProgress,
                                        std::cref(poolTreeSnapshot),
                                        operation,
                                        PoolTree_->StrategyHost_,
                                        std::placeholders::_1))
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
        TIntrusivePtr<const TPoolTree> PoolTree_;
    };

    friend class TOperationsByPoolOrchidService;

    // Thread affinity: Control.
    TPoolTreeSnapshotPtr TreeSnapshot_;
    // Thread affinity: any.
    TAtomicIntrusivePtr<TPoolTreeSnapshot> AtomicTreeSnapshot_;

    TPoolTreeSnapshotPtr TreeSnapshotPrecommit_;

    TEventTimer FairSharePreUpdateTimer_;
    TEventTimer FairShareUpdateTimer_;
    TEventTimer FairShareFluentLogTimer_;
    TEventTimer FairShareTextLogTimer_;

    // Used only in fair share logging invoker.
    mutable TTreeSnapshotId LastLoggedTreeSnapshotId_;

    mutable TResourceDistributionAccumulator AccumulatedPoolResourceUsageForMetering_;
    mutable TResourceDistributionAccumulator AccumulatedOperationsResourceDistributionForProfiling_;
    mutable TResourceDistributionAccumulator AccumulatedOperationsResourceDistributionForLogging_;

    void ThrowOrchidIsNotReady() const
    {
        THROW_ERROR_EXCEPTION("Pool tree orchid is not ready yet")
            << TErrorAttribute("tree_id", TreeId_);
    }

    TPoolTreeSnapshotPtr GetTreeSnapshot() const noexcept override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        return TreeSnapshot_;
    }

    TPoolTreeSnapshotPtr GetAtomicTreeSnapshot() const noexcept
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return AtomicTreeSnapshot_.Acquire();
    }

    TPoolTreeSnapshotPtr GetTreeSnapshotForOrchid() const
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();
        if (!treeSnapshot) {
            ThrowOrchidIsNotReady();
        }

        return treeSnapshot;
    }

    TFairSharePreUpdateContext CreatePreUpdateContext(TInstant now, const TSchedulingTagFilter& nodesFilter, const TJobResources& totalResourceLimits)
    {
        TFairSharePreUpdateContext result{
            .Now = now,
            .TotalResourceLimits = totalResourceLimits,
            .ResourceLimitsByTagFilter = {{nodesFilter, totalResourceLimits}},
        };

        return result;
    }

    std::pair<IPoolTreePtr, TError> DoFairShareUpdateAt(TInstant now)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_LOG_DEBUG("Preparing for pool tree fair share update");

        ResourceTree_->PerformPostponedActions();

        auto rootElement = RootElement_->Clone();
        {
            TEventTimerGuard timer(FairSharePreUpdateTimer_);
            rootElement->InitializeFairShareUpdate(now);
        }

        auto schedulingPolicyPostUpdateContext = SchedulingPolicy_->CreatePostUpdateContext(rootElement.Get());

        auto asyncUpdate =
            BIND([
                this,
                this_ = MakeStrong(this),
                now,
                treeId = TreeId_,
                config = Config_,
                &rootElement,
                &schedulingPolicyPostUpdateContext,
                lastFairShareUpdateTime = LastFairShareUpdateTime_
            ] () -> TFairShareUpdateResult {
                TFairShareUpdateResult fairShareUpdateResult;

                TForbidContextSwitchGuard contextSwitchGuard;
                {
                    TEventTimerGuard timer(FairShareUpdateTimer_);

                    YT_LOG_DEBUG("Pool tree fair share update started");

                    fairShareUpdateResult.ResourceUsage = StrategyHost_->GetResourceUsage(config->NodeTagFilter);
                    fairShareUpdateResult.ResourceLimits = StrategyHost_->GetResourceLimits(config->NodeTagFilter);

                    auto fairSharePreUpdateContext = CreatePreUpdateContext(now, config->NodeTagFilter, fairShareUpdateResult.ResourceLimits);
                    rootElement->PreUpdate(&fairSharePreUpdateContext);
                    fairShareUpdateResult.ResourceLimitsByTagFilter = std::move(fairSharePreUpdateContext.ResourceLimitsByTagFilter);

                    TFairShareUpdateContext fairShareUpdateContext(
                        TFairShareUpdateOptions{
                            .MainResource = config->MainResource,
                            .IntegralPoolCapacitySaturationPeriod = config->IntegralGuarantees->PoolCapacitySaturationPeriod,
                            .IntegralSmoothPeriod = config->IntegralGuarantees->SmoothPeriod,
                            .EnableStepFunctionForGangOperations = config->EnableStepFunctionForGangOperations,
                            .EnableImprovedFairShareByFitFactorComputation = config->EnableImprovedFairShareByFitFactorComputation,
                            .EnableImprovedFairShareByFitFactorComputationDistributionGap =
                                config->EnableImprovedFairShareByFitFactorComputationDistributionGap,
                        },
                        fairShareUpdateResult.ResourceLimits,
                        now,
                        lastFairShareUpdateTime);

                    TFairShareUpdateExecutor updateExecutor(
                        rootElement,
                        &fairShareUpdateContext,
                        /*loggingTag*/ Format("TreeId: %v", treeId));
                    updateExecutor.Run();
                    fairShareUpdateResult.Errors = std::move(fairShareUpdateContext.Errors);

                    TFairSharePostUpdateContext fairSharePostUpdateContext{
                        .TreeConfig = config,
                        .Now = now,
                    };
                    rootElement->PostUpdate(&fairSharePostUpdateContext);
                    rootElement->UpdateStarvationStatuses(now, config->EnablePoolStarvation);

                    SchedulingPolicy_->PostUpdate(&fairSharePostUpdateContext, &schedulingPolicyPostUpdateContext);

                    fairShareUpdateResult.EnabledOperationIdToElement = std::move(fairSharePostUpdateContext.EnabledOperationIdToElement);
                    fairShareUpdateResult.DisabledOperationIdToElement = std::move(fairSharePostUpdateContext.DisabledOperationIdToElement);
                    fairShareUpdateResult.PoolNameToElement = std::move(fairSharePostUpdateContext.PoolNameToElement);

                    YT_LOG_DEBUG(
                        "Pool tree fair share update finished "
                        "(TreeSize: %v, SchedulableElementCount: %v, UnschedulableReasons: %v)",
                        rootElement->GetTreeSize(),
                        rootElement->SchedulableElementCount(),
                        fairSharePostUpdateContext.UnschedulableReasons);
                }

                MaybeDelay(config->TestingOptions->DelayInsideFairShareUpdate);

                return fairShareUpdateResult;
            })
            .AsyncVia(StrategyHost_->GetFairShareUpdateInvoker())
            .Run();
        auto fairShareUpdateResult = WaitFor(asyncUpdate)
            .ValueOrThrow();

        YT_LOG_DEBUG("Processing pool tree fair share update result and creating a tree snapshot");

        TError error;
        if (!fairShareUpdateResult.Errors.empty()) {
            error = TError("Found pool configuration issues during fair share update in tree %Qv", TreeId_)
                << TErrorAttribute("pool_tree", TreeId_)
                << std::move(fairShareUpdateResult.Errors);
        }

        // Copy persistent attributes back to the original tree.
        for (const auto& [operationId, element] : fairShareUpdateResult.EnabledOperationIdToElement) {
            if (auto originalElement = FindOperationElement(operationId)) {
                originalElement->PersistentAttributes() = element->PersistentAttributes();
            }
        }
        for (const auto& [poolName, element] : fairShareUpdateResult.PoolNameToElement) {
            if (auto originalElement = FindPool(poolName)) {
                originalElement->PersistentAttributes() = element->PersistentAttributes();
            }
        }
        RootElement_->PersistentAttributes() = rootElement->PersistentAttributes();

        rootElement->MarkImmutable();

        auto treeSnapshotId = TTreeSnapshotId::Create();
        auto schedulingPolicyState = SchedulingPolicy_->CreateSnapshotState(&schedulingPolicyPostUpdateContext);
        auto treeSnapshot = New<TPoolTreeSnapshot>(
            treeSnapshotId,
            now,
            std::move(rootElement),
            std::move(fairShareUpdateResult.EnabledOperationIdToElement),
            std::move(fairShareUpdateResult.DisabledOperationIdToElement),
            std::move(fairShareUpdateResult.PoolNameToElement),
            Config_,
            ControllerConfig_,
            fairShareUpdateResult.ResourceUsage,
            fairShareUpdateResult.ResourceLimits,
            NodeIdToAddress_,
            std::move(schedulingPolicyState),
            std::move(fairShareUpdateResult.ResourceLimitsByTagFilter));

        SchedulingPolicy_->OnResourceUsageSnapshotUpdate(treeSnapshot, ResourceUsageSnapshot_.Acquire());

        YT_LOG_DEBUG("Pool tree snapshot created (TreeSnapshotId: %v)", treeSnapshotId);

        TreeSnapshotPrecommit_ = std::move(treeSnapshot);
        LastFairShareUpdateTime_ = now;

        return std::pair(MakeStrong(this), error);
    }

    void DoRegisterPool(const TPoolTreePoolElementPtr& pool)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        YT_VERIFY(Pools_.emplace(pool->GetId(), pool).second);
        YT_VERIFY(PoolToMinUnusedSlotIndex_.emplace(pool->GetId(), 0).second);

        ProfileManager_->RegisterPool(pool);
    }

    void RegisterPool(const TPoolTreePoolElementPtr& pool, const TPoolTreeCompositeElementPtr& parent)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        DoRegisterPool(pool);

        pool->AttachParent(parent.Get());

        YT_LOG_INFO("Pool registered (Pool: %v, Parent: %v, IsEphemeral: %v)",
            pool->GetId(),
            parent->GetId(),
            pool->IsDefaultConfigured());
    }

    void ReconfigurePool(
        const TPoolTreePoolElementPtr& pool,
        const TPoolConfigPtr& config,
        TGuid objectId)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        bool lightweightOperationsEnabledBefore = pool->GetEffectiveLightweightOperationsEnabled();

        pool->SetConfig(config);
        pool->SetObjectId(objectId);

        if (pool->GetConfig()->EphemeralSubpoolConfig.has_value()) {
            for (const auto& child : pool->EnabledChildren()) {
                auto* childPool = dynamic_cast<TPoolTreePoolElement*>(child.Get());

                if (childPool && childPool->IsDefaultConfigured()) {
                    ApplyEphemeralSubpoolConfig(pool, childPool->GetConfig());
                }
            }
        }

        if (lightweightOperationsEnabledBefore != pool->GetEffectiveLightweightOperationsEnabled()) {
            ReaccountLightweightRunningOperationsInPool(pool);
        }
    }

    void ReaccountLightweightRunningOperationsInPool(const TPoolTreePoolElementPtr& pool)
    {
        if (!pool->GetEffectiveLightweightOperationsEnabled()) {
            // We just increase the regular running operation count allowing overcommit.
            pool->IncreaseRunningOperationCount(pool->LightweightRunningOperationCount());
            pool->IncreaseLightweightRunningOperationCount(-pool->LightweightRunningOperationCount());

            return;
        }

        YT_VERIFY(pool->GetChildPoolCount() == 0);

        int lightweightRunningOperationCount = 0;
        std::vector<TPoolTreeOperationElement*> pendingLightweightOperations;
        for (auto* operation : pool->GetChildOperations()) {
            if (!operation->IsLightweightEligible()) {
                continue;
            }

            if (operation->IsOperationRunningInPool()) {
                ++lightweightRunningOperationCount;
            } else {
                pendingLightweightOperations.push_back(operation);
            }
        }

        // NB(eshcherbin): Pending operations that can become running after this change will be processed in |TryRunAllPendingOperations|.
        pool->IncreaseRunningOperationCount(-lightweightRunningOperationCount);
        pool->IncreaseLightweightRunningOperationCount(lightweightRunningOperationCount);

        for (auto* operation : pendingLightweightOperations) {
            auto operationId = operation->GetOperationId();
            if (auto blockedPoolName = operation->PendingByPool()) {
                if (auto blockedPool = FindPool(*blockedPoolName)) {
                    blockedPool->PendingOperationIds().remove(operationId);
                }
            }

            operation->MarkOperationRunningInPool();
            OperationRunning_.Fire(operationId);
        }
    }

    void UnregisterPool(const TPoolTreePoolElementPtr& pool)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto userName = pool->GetUserName();
        if (userName && pool->IsEphemeralInDefaultParentPool()) {
            EraseOrCrash(UserToEphemeralPoolsInDefaultPool_[*userName], pool->GetId());
        }

        EraseOrCrash(PoolToMinUnusedSlotIndex_, pool->GetId());

        // Pool may be not presented in this map.
        PoolToSpareSlotIndices_.erase(pool->GetId());

        ProfileManager_->UnregisterPool(pool);

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

    TPoolTreePoolElementPtr GetOrCreatePool(const TPoolName& poolName, std::string userName)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto pool = FindPool(poolName.GetPool());
        if (pool) {
            return pool;
        }

        // Create ephemeral pool.
        auto poolConfig = New<TPoolConfig>();

        TPoolTreeCompositeElement* parent = poolName.GetParentPool()
            ? GetPool(*poolName.GetParentPool()).Get()
            : GetDefaultParentPoolForUser(userName).Get();

        ApplyEphemeralSubpoolConfig(parent, poolConfig);

        pool = New<TPoolTreePoolElement>(
            StrategyHost_,
            this,
            poolName.GetPool(),
            TGuid(),
            poolConfig,
            /*defaultConfigured*/ true,
            Config_,
            TreeId_,
            Logger);

        if (!poolName.GetParentPool()) {
            pool->SetEphemeralInDefaultParentPool();
            UserToEphemeralPoolsInDefaultPool_[userName].insert(poolName.GetPool());
        }

        pool->SetUserName(userName);

        RegisterPool(pool, parent);
        return pool;
    }

    void ApplyEphemeralSubpoolConfig(const TPoolTreeCompositeElementPtr& parent, const TPoolConfigPtr& targetConfig)
    {
        if (parent->IsRoot()) {
            return;
        }

        auto* parentPool = dynamic_cast<TPoolTreePoolElement*>(parent.Get());
        YT_VERIFY(parentPool);
        auto maybeConfig = parentPool->GetConfig()->EphemeralSubpoolConfig;
        if (!maybeConfig.has_value()) {
            return;
        }

        const auto& source = *maybeConfig;
        targetConfig->Mode = source->Mode;
        targetConfig->MaxOperationCount = source->MaxOperationCount;
        targetConfig->MaxRunningOperationCount = source->MaxRunningOperationCount;
        targetConfig->ResourceLimits = source->ResourceLimits;
    }

    bool TryAllocatePoolSlotIndex(const TString& poolName, int slotIndex)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    int AllocateOperationSlotIndex(const TStrategyOperationStatePtr& state, const TString& poolName)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    void ReleaseOperationSlotIndex(const TStrategyOperationStatePtr& state, const TString& poolName)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override
    {
        SchedulingPolicy_->BuildElementLoggingStringAttributes(treeSnapshot, element, delimitedBuilder);
    }

    void OnOperationRemovedFromPool(
        const TStrategyOperationStatePtr& state,
        const TPoolTreeOperationElementPtr& element,
        const TPoolTreeCompositeElementPtr& parent)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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
        const TStrategyOperationStatePtr& state,
        const TPoolTreeOperationElementPtr& operationElement)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto violatedPool = FindPoolViolatingMaxRunningOperationCount(operationElement->GetMutableParent());
        if (operationElement->IsLightweight() || !violatedPool) {
            operationElement->MarkOperationRunningInPool();
            return true;
        }
        operationElement->MarkPendingBy(violatedPool);

        YT_UNUSED_FUTURE(StrategyHost_->SetOperationAlert(
            state->GetHost()->GetId(),
            EOperationAlertType::OperationPending,
            TError("Max running operation count violated")
                << TErrorAttribute("pool", violatedPool->GetId())
                << TErrorAttribute("limit", violatedPool->GetMaxRunningOperationCount())
                << TErrorAttribute("pool_tree", TreeId_)));

        return false;
    }

    void RemoveEmptyEphemeralPoolsRecursive(TPoolTreeCompositeElement* compositeElement)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (!compositeElement->IsRoot() && compositeElement->IsEmpty()) {
            TPoolTreePoolElementPtr parentPool = static_cast<TPoolTreePoolElement*>(compositeElement);
            if (parentPool->IsDefaultConfigured()) {
                UnregisterPool(parentPool);
                RemoveEmptyEphemeralPoolsRecursive(parentPool->GetMutableParent());
            }
        }
    }

    void CheckOperationsPendingByPool(TPoolTreeCompositeElement* pool)
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    TPoolTreeCompositeElement* FindPoolViolatingMaxRunningOperationCount(TPoolTreeCompositeElement* pool) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        while (pool) {
            if (pool->RunningOperationCount() >= pool->GetMaxRunningOperationCount()) {
                return pool;
            }
            pool = pool->GetMutableParent();
        }
        return nullptr;
    }

    const TPoolTreeCompositeElement* FindPoolWithViolatedOperationCountLimit(const TPoolTreeCompositeElementPtr& element) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        const TPoolTreeCompositeElement* current = element.Get();
        while (current) {
            if (current->OperationCount() >= current->GetMaxOperationCount()) {
                return current;
            }
            current = current->GetParent();
        }
        return nullptr;
    }

    // Finds the lowest ancestor of |element| whose resource limits are too small to satisfy |neededResources|.
    const TPoolTreeElement* FindAncestorWithInsufficientSpecifiedResourceLimits(
        const TPoolTreeElement* element,
        const TJobResources& neededResources) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        const TPoolTreeElement* current = element;
        while (current) {
            // NB(eshcherbin): We expect that |GetSpecifiedResourcesLimits| return infinite limits when no limits were specified.
            if (const auto& specifiedLimits = current->MaybeSpecifiedResourceLimits();
                specifiedLimits && !Dominates(*specifiedLimits, neededResources))
            {
                return current;
            }
            current = current->GetParent();
        }

        return nullptr;
    }

    TPoolTreeCompositeElementPtr GetDefaultParentPoolForUser(const std::string& userName) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto tryGetValidPool = [&] (const TString& poolName, const char* poolCaption, const std::string& loggedAttributes) -> TPoolTreeCompositeElementPtr {
            auto pool = FindPool(poolName);
            if (pool) {
                if (pool->GetMode() != ESchedulingMode::Fifo) {
                    return pool;
                } else {
                    YT_LOG_INFO("%v has FIFO mode and won't be used %v", poolCaption, loggedAttributes);
                }
            } else {
                YT_LOG_INFO("%v is not registered in tree %v", poolCaption, loggedAttributes);
            }
            return nullptr;
        };

        if (Config_->UseUserDefaultParentPoolMap) {
            const auto& userToDefaultPoolMap = StrategyHost_->GetUserDefaultParentPoolMap();
            auto it = userToDefaultPoolMap.find(userName);
            if (it != userToDefaultPoolMap.end()) {
                auto loggedAttributes = Format("(PoolName: %v, UserName: %v)", it->second, userName);
                if (auto pool = tryGetValidPool(it->second, "User default parent pool", loggedAttributes)) {
                    return pool;
                }
            }
        }

        auto loggedAttributes = Format("(PoolName: %v)", Config_->DefaultParentPool);
        if (auto pool = tryGetValidPool(Config_->DefaultParentPool, "Default parent pool", loggedAttributes)) {
            return pool;
        }

        YT_LOG_INFO("Using %v as default parent pool", RootPoolName);

        return RootElement_;
    }

    void ActualizeEphemeralPoolParents(const THashMap<std::string, TString>& userToDefaultPoolMap) override
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        if (!Config_->UseUserDefaultParentPoolMap) {
            return;
        }

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

    TPoolTreeCompositeElementPtr GetPoolOrParent(const TPoolName& poolName, const std::string& userName) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        TPoolTreeCompositeElementPtr pool = FindPool(poolName.GetPool());
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

    void ValidateAllOperationCountsOnPoolChange(TOperationId operationId, const TPoolName& newPoolName) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto operationElement = GetOperationElement(operationId);
        auto newPoolElement = GetPoolOrParent(newPoolName, operationElement->GetUserName());
        bool lightweightInNewPool = operationElement->IsLightweightEligible() && newPoolElement->GetEffectiveLightweightOperationsEnabled();
        for (const auto* currentPool : GetPoolsToValidateOperationCountsOnPoolChange(operationElement, newPoolElement)) {
            if (currentPool->OperationCount() >= currentPool->GetMaxOperationCount()) {
                THROW_ERROR_EXCEPTION("Max operation count of pool %Qv violated", currentPool->GetId());
            }

            if (!lightweightInNewPool && currentPool->RunningOperationCount() >= currentPool->GetMaxRunningOperationCount()) {
                THROW_ERROR_EXCEPTION("Max running operation count of pool %Qv violated", currentPool->GetId());
            }
        }
    }

    std::vector<const TPoolTreeCompositeElement*> GetPoolsToValidateOperationCountsOnPoolChange(
        const TPoolTreeOperationElementPtr& operationElement,
        const TPoolTreeCompositeElementPtr& newPoolElement) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        std::vector<const TPoolTreeCompositeElement*> poolsToValidate;
        const auto* pool = newPoolElement.Get();
        while (pool) {
            poolsToValidate.push_back(pool);
            pool = pool->GetParent();
        }

        if (!operationElement->IsOperationRunningInPool() || operationElement->IsLightweight()) {
            // If operation is pending or lightweight, it isn't counted as running in any pool, so we must check all new ancestors.
            return poolsToValidate;
        }

        // Otherwise, the operation is already counted as running in the common ancestors, so we can skip those pools validation.
        std::vector<const TPoolTreeCompositeElement*> oldPools;
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

    void ValidateOperationCountLimit(const TPoolName& poolName, const std::string& userName) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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

    void ValidateEphemeralPoolLimit(const IOperation* operation, const TPoolName& poolName) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

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
                THROW_ERROR_EXCEPTION(
                    "Cannot create new ephemeral pool %Qv as limit for number of ephemeral pools %v for user %Qv in tree %Qv has been reached; "
                    "previously created pools are [%v]",
                    poolName.GetPool(),
                    Config_->MaxEphemeralPoolsPerUser,
                    userName,
                    TreeId_,
                    JoinToString(it->second));
            }
        }
    }

    void ValidateSpecifiedResourceLimits(
        const IOperation* operation,
        const TPoolTreeCompositeElement* pool,
        const TJobResourcesConfigPtr& requiredLimitsConfig) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto requiredLimits = ToJobResources(requiredLimitsConfig, TJobResources::Infinite());

        YT_LOG_DEBUG("Validating operation resource limits (RequiredResourceLimits: %v, Pool: %v, OperationId: %v)",
            requiredLimits,
            pool->GetId(),
            operation->GetId());

        auto actualLimits = TJobResources::Infinite();
        const auto* current = pool;
        while (!current->IsRoot()) {
            if (const auto& specifiedLimits = current->ComputeMaybeSpecifiedResourceLimits()) {
                actualLimits = Min(actualLimits, *specifiedLimits);
            }

            if (Dominates(requiredLimits, actualLimits)) {
                return;
            }

            current = current->GetParent();
        }

        THROW_ERROR_EXCEPTION(
            "Operations of type %Qlv must have small enough specified resource limits in some of ancestor pools",
            operation->GetType())
            << TErrorAttribute("operation_id", operation->GetId())
            << TErrorAttribute("pool", pool->GetId())
            << TErrorAttribute("required_resource_limits", requiredLimitsConfig)
            << TErrorAttribute("tree_id", TreeId_);
    }

    void DoValidateOperationPoolsCanBeUsed(const IOperation* operation, const TPoolName& poolName) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        const TPoolTreeCompositeElement* pool = FindPool(poolName.GetPool()).Get();
        // NB: Check is not performed if operation is started in default or unknown pool.
        if (pool && pool->AreImmediateOperationsForbidden()) {
            THROW_ERROR_EXCEPTION("Starting operations immediately in pool %Qv is forbidden", poolName.GetPool());
        }

        if (!pool) {
            // Validate pool name only if pool does not exist.
            ValidatePoolName(poolName.GetSpecifiedPoolName(), Host_->GetEphemeralPoolNameRegex());

            pool = GetPoolOrParent(poolName, operation->GetAuthenticatedUser()).Get();
        }

        if (pool->IsDefaultConfigured()) {
            pool = pool->GetParent();
        }

        if (operation->GetStrategySpec()->IsGang && !pool->AreGangOperationsAllowed()) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::GangOperationsAllowedOnlyInFifoPools,
                "Starting gang operations in pool %Qv in tree %Qv is forbidden since it is not configured in FIFO mode",
                pool->GetId(),
                TreeId_);
        }

        StrategyHost_->ValidatePoolPermission(
            TreeId_,
            pool->GetObjectId(),
            pool->GetId(),
            operation->GetAuthenticatedUser(),
            EPermission::Use);
    }

    void DoValidateOperationPoolPermissions(TOperationId operationId, const std::string& user, NYTree::EPermissionSet permissions) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        MaybeDelay(Config_->TestingOptions->DelayInsidePoolPermissionsValidation, &Logger);

        auto operationElement = FindOperationElement(operationId);
        if (!operationElement) {
            // NB(eshcherbin): Operation has been unregistered concurrently.
            return;
        }

        auto* pool = operationElement->GetParent();
        while (pool->IsDefaultConfigured()) {
            pool = pool->GetParent();
        }

        for (auto permission : TEnumTraits<EPermission>::GetDomainValues()) {
            if (Any(permission & permissions)) {
                StrategyHost_->ValidatePoolPermission(
                    TreeId_,
                    pool->GetObjectId(),
                    pool->GetId(),
                    user,
                    permission);
            }
        }
    }

    int GetPoolCount() const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        return Pools_.size();
    }

    TPoolTreePoolElementPtr FindPool(const TString& id) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto it = Pools_.find(id);
        return it == Pools_.end() ? nullptr : it->second;
    }

    TPoolTreePoolElementPtr GetPool(const TString& id) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto pool = FindPool(id);
        YT_VERIFY(pool);
        return pool;
    }

    TPoolTreeOperationElementPtr FindOperationElement(TOperationId operationId) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto it = OperationIdToElement_.find(operationId);
        return it == OperationIdToElement_.end() ? nullptr : it->second;
    }

    TPoolTreeOperationElementPtr GetOperationElement(TOperationId operationId) const
    {
        YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

        auto element = FindOperationElement(operationId);
        YT_VERIFY(element);
        return element;
    }

    TPoolTreeOperationElement* FindOperationElementInSnapshot(TOperationId operationId) const
    {
        if (auto treeSnapshot = GetTreeSnapshot()) {
            return treeSnapshot->FindEnabledOperationElement(operationId);
        }
        return nullptr;
    }

    TFuture<void> ProcessSchedulingHeartbeat(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext, bool skipScheduleAllocations) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        StrategyHost_->GetControlInvoker(EControlQueue::Strategy)->Invoke(BIND(
            &NGpu::ISchedulingPolicy::UpdateNodeDescriptor,
            GpuSchedulingPolicy_,
            schedulingHeartbeatContext->GetNodeDescriptor()->Id,
            schedulingHeartbeatContext->GetNodeDescriptor()));

        if (auto traceContext = NTracing::TryGetCurrentTraceContext()) {
            traceContext->AddTag("tree", TreeId_);
        }

        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        auto processSchedulingHeartbeatFuture = BIND(
            &NPolicy::ISchedulingPolicy::ProcessSchedulingHeartbeat,
            SchedulingPolicy_,
            schedulingHeartbeatContext,
            treeSnapshot,
            skipScheduleAllocations)
            .AsyncVia(GetCurrentInvoker())
            .Run();

        return processSchedulingHeartbeatFuture
            .Apply(BIND(
                &TPoolTree::ApplyScheduledAndPreemptedResourcesDelta,
                MakeStrong(this),
                schedulingHeartbeatContext,
                treeSnapshot));
    }

    int GetSchedulingHeartbeatComplexity() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        return treeSnapshot->RootElement()->SchedulableElementCount();
    }

    void ProcessAllocationUpdates(
        const std::vector<TAllocationUpdate>& allocationUpdates,
        THashSet<TAllocationId>* allocationsToPostpone,
        THashMap<TAllocationId, EAbortReason>* allocationsToAbort) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        for (const auto& allocationUpdate : allocationUpdates) {
            std::optional<EAbortReason> maybeAbortReason;
            bool updateSuccessful = ProcessAllocationUpdate(treeSnapshot, allocationUpdate, &maybeAbortReason);

            if (!updateSuccessful) {
                YT_LOG_DEBUG(
                    "Postpone allocation update since operation is disabled or missing in snapshot (OperationId: %v, AllocationId: %v)",
                    allocationUpdate.OperationId,
                    allocationUpdate.AllocationId);

                allocationsToPostpone->insert(allocationUpdate.AllocationId);
            } else if (maybeAbortReason) {
                EmplaceOrCrash(*allocationsToAbort, allocationUpdate.AllocationId, *maybeAbortReason);
                // NB(eshcherbin): We want the node shard to send us an allocation finished update,
                // this is why we have to postpone the allocation here. This is very ad-hoc, but I hope it'll
                // soon be rewritten as a part of the new GPU scheduler. See: YT-15062.
                allocationsToPostpone->insert(allocationUpdate.AllocationId);
            }
        }
    }

    bool ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TAllocationUpdate& allocationUpdate,
        std::optional<EAbortReason>* maybeAbortReason)
    {
        auto* operationElement = treeSnapshot->FindEnabledOperationElement(allocationUpdate.OperationId);
        if (!operationElement) {
            return false;
        }

        if (allocationUpdate.Finished) {
            // NB: Should be filtered out on large clusters.
            YT_LOG_DEBUG(
                "Processing allocation finish (OperationId: %v, AllocationId: %v)",
                allocationUpdate.OperationId,
                allocationUpdate.AllocationId);

            return SchedulingPolicy_->ProcessFinishedAllocation(
                treeSnapshot,
                operationElement,
                allocationUpdate.AllocationId);
        }

        YT_VERIFY(allocationUpdate.ResourceUsageUpdated || allocationUpdate.ResetPreemptibleProgress);

        // NB: Should be filtered out on large clusters.
        YT_LOG_DEBUG(
            "Processing allocation update (OperationId: %v, AllocationId: %v, ResetPreemptibleProgress: %v, Resources: %v)",
            allocationUpdate.OperationId,
            allocationUpdate.AllocationId,
            allocationUpdate.ResetPreemptibleProgress,
            allocationUpdate.AllocationResources);

        return SchedulingPolicy_->ProcessAllocationUpdate(
            treeSnapshot,
            operationElement,
            allocationUpdate.AllocationId,
            allocationUpdate.AllocationResources,
            allocationUpdate.ResetPreemptibleProgress,
            allocationUpdate.AllocationDataCenter,
            allocationUpdate.AllocationInfinibandCluster,
            maybeAbortReason);
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
            &TPoolTreeProfileManager::ApplyJobMetricsDelta,
            ProfileManager_,
            treeSnapshot,
            Passed(std::move(jobMetricsPerOperation))));
    }

    void ApplyScheduledAndPreemptedResourcesDelta(
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (!treeSnapshot->TreeConfig()->EnableScheduledAndPreemptedResourcesProfiling) {
            return;
        }

        THashMap<std::optional<EAllocationSchedulingStage>, TOperationIdToJobResources> scheduledAllocationResources;
        TEnumIndexedArray<EAllocationPreemptionReason, TOperationIdToJobResources> preemptedAllocationResources;
        TEnumIndexedArray<EAllocationPreemptionReason, TOperationIdToJobResources> preemptedAllocationResourceTimes;
        TEnumIndexedArray<EAllocationPreemptionReason, TOperationIdToJobResources> improperlyPreemptedAllocationResources;

        for (const auto& [allocation, schedulingStage] : schedulingHeartbeatContext->StartedAllocations()) {
            TOperationId operationId = allocation->GetOperationId();
            const TJobResources& scheduledResourcesDelta = allocation->ResourceLimits();
            scheduledAllocationResources[schedulingStage][operationId] += scheduledResourcesDelta;
        }

        for (const auto& [allocation, _, preemptionReason] : schedulingHeartbeatContext->PreemptedAllocations()) {
            TOperationId operationId = allocation->GetOperationId();
            const TJobResources& preemptedResourcesDelta = allocation->ResourceLimits();
            preemptedAllocationResources[preemptionReason][operationId] += preemptedResourcesDelta;

            // NB(eshcherbin): This sensor for memory is easily overflown, so we decided not to compute it. See: YT-24236.
            {
                auto preemptedResourcesDeltaWithZeroMemory = preemptedResourcesDelta;
                preemptedResourcesDeltaWithZeroMemory.SetMemory(0);
                preemptedAllocationResourceTimes[preemptionReason][operationId] +=
                    preemptedResourcesDeltaWithZeroMemory *
                    static_cast<i64>(allocation->GetPreemptibleProgressDuration().Seconds());
            }

            if (allocation->GetPreemptedFor() && !allocation->GetPreemptedForProperlyStarvingOperation()) {
                improperlyPreemptedAllocationResources[preemptionReason][operationId] += preemptedResourcesDelta;
            }
        }

        StrategyHost_->GetFairShareProfilingInvoker()->Invoke(BIND(
            &TPoolTreeProfileManager::ApplyScheduledAndPreemptedResourcesDelta,
            ProfileManager_,
            treeSnapshot,
            Passed(std::move(scheduledAllocationResources)),
            Passed(std::move(preemptedAllocationResources)),
            Passed(std::move(preemptedAllocationResourceTimes)),
            Passed(std::move(improperlyPreemptedAllocationResources))));
    }

    TJobResources GetSnapshottedTotalResourceLimits() const override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        return treeSnapshot->ResourceLimits();
    }

    std::optional<TPoolTreeElementStateSnapshot> GetMaybeStateSnapshotForPool(const TString& poolId) const override
    {
        auto treeSnapshot = GetTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        if (auto* element = treeSnapshot->FindPool(poolId)) {
            return TPoolTreeElementStateSnapshot{
                element->Attributes().DemandShare,
                element->Attributes().EstimatedGuaranteeShare};
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
        rootElement->BuildResourceMetering(/*lowestMeteredAncestorKey*/ {}, accumulatedResourceUsageMap, meteringMap);

        *customMeteringTags = treeSnapshot->TreeConfig()->MeteringTags;
    }

    void BuildSchedulingAttributesStringForNode(TNodeId nodeId, TDelimitedStringBuilderWrapper& delimitedBuilder) const override
    {
        SchedulingPolicy_->BuildSchedulingAttributesStringForNode(nodeId, delimitedBuilder);
    }

    void BuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const override
    {
        SchedulingPolicy_->BuildSchedulingAttributesForNode(nodeId, fluent);
    }

    void BuildSchedulingAttributesStringForOngoingAllocations(
        const std::vector<TAllocationPtr>& allocations,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const override
    {
        SchedulingPolicy_->BuildSchedulingAttributesStringForOngoingAllocations(GetAtomicTreeSnapshot(), allocations, now, delimitedBuilder);
    }

    void ProfileFairShare() const override
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        ProfileManager_->ProfileTree(
            treeSnapshot,
            AccumulatedOperationsResourceDistributionForProfiling_.ExtractOperationResourceDistributionVolumes());
    }

    void LogFairShareAt(TInstant now) const override
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        auto treeSnapshotId = treeSnapshot->GetId();
        if (treeSnapshotId == LastLoggedTreeSnapshotId_) {
            YT_LOG_DEBUG("Skipping pool tree logging since the tree snapshot is the same as before (TreeSnapshotId: %v)",
                treeSnapshotId);

            return;
        }
        LastLoggedTreeSnapshotId_ = treeSnapshotId;

        {
            TEventTimerGuard timer(FairShareFluentLogTimer_);

            auto fairShareInfo = BuildSerializedFairShareInfo(
                treeSnapshot,
                treeSnapshot->TreeConfig()->MaxEventLogPoolBatchSize,
                treeSnapshot->TreeConfig()->MaxEventLogOperationBatchSize);
            auto logFairShareEventFluently = [&] {
                return StrategyHost_->LogFairShareEventFluently(now)
                    .Item(EventLogPoolTreeKey).Value(TreeId_)
                    .Item("tree_snapshot_id").Value(treeSnapshotId);
            };

            // NB(eshcherbin, YTADMIN-11230): First we log a single event with general pools info and resource distribution info.
            // Then we split all pools' and operations' info into several batches and log every batch in a separate event.
            logFairShareEventFluently()
                .Items(fairShareInfo.PoolCount)
                .Items(fairShareInfo.ResourceDistributionInfo);

            for (int batchIndex = 0; batchIndex < std::ssize(fairShareInfo.SplitPoolsInfo); ++batchIndex) {
                const auto& batch = fairShareInfo.SplitPoolsInfo[batchIndex];
                logFairShareEventFluently()
                    .Item("pools_batch_index").Value(batchIndex)
                    .Item("pools").BeginMap()
                        .Items(batch)
                    .EndMap();
            }

            for (int batchIndex = 0; batchIndex < std::ssize(fairShareInfo.SplitOperationsInfo); ++batchIndex) {
                const auto& batch = fairShareInfo.SplitOperationsInfo[batchIndex];
                logFairShareEventFluently()
                    .Item("operations_batch_index").Value(batchIndex)
                    .Item("operations").BeginMap()
                        .Items(batch)
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
        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        StrategyHost_->LogAccumulatedUsageEventFluently(TInstant::Now())
            .Item(EventLogPoolTreeKey).Value(TreeId_)
            .Item("pools").BeginMap()
                .Do(BIND(&TPoolTree::DoBuildPoolsStructureInfo, Unretained(this), ConstRef(treeSnapshot)))
            .EndMap()
            .Item("operations").BeginMap()
                .Do(BIND(&TPoolTree::DoBuildOperationsAccumulatedUsageInfo, Unretained(this), ConstRef(treeSnapshot)))
            .EndMap();
    }

    void EssentialLogFairShareAt(TInstant now) const override
    {
        auto treeSnapshot = GetAtomicTreeSnapshot();

        YT_VERIFY(treeSnapshot);

        {
            TEventTimerGuard timer(FairShareFluentLogTimer_);
            StrategyHost_->LogFairShareEventFluently(now)
                .Item(EventLogPoolTreeKey).Value(TreeId_)
                .Item("tree_snapshot_id").Value(treeSnapshot->GetId())
                .Do(BIND(&TPoolTree::DoBuildEssentialFairShareInfo, Unretained(this), ConstRef(treeSnapshot)));
        }

        {
            TEventTimerGuard timer(FairShareTextLogTimer_);
            LogPoolsInfo(treeSnapshot);
            LogOperationsInfo(treeSnapshot);
        }
    }

    void UpdateResourceUsages() override
    {
        YT_LOG_DEBUG("Building resource usage snapshot");

        auto treeSnapshot = GetAtomicTreeSnapshot();
        auto resourceUsageSnapshot = BuildResourceUsageSnapshot(treeSnapshot);

        YT_LOG_DEBUG("Updating accumulated resource usage");

        AccumulatedPoolResourceUsageForMetering_.Update(treeSnapshot, resourceUsageSnapshot);
        AccumulatedOperationsResourceDistributionForProfiling_.Update(treeSnapshot, resourceUsageSnapshot);
        AccumulatedOperationsResourceDistributionForLogging_.Update(treeSnapshot, resourceUsageSnapshot);

        YT_LOG_DEBUG("Updating resource usage snapshot");

        SchedulingPolicy_->OnResourceUsageSnapshotUpdate(treeSnapshot, resourceUsageSnapshot);
        SetResourceUsageSnapshot(std::move(resourceUsageSnapshot));
    }

    TAccumulatedResourceDistribution ExtractAccumulatedResourceDistributionForLogging(TOperationId operationId) override
    {
        // NB: We can loose some of usage, up to the AccumulatedResourceUsageUpdatePeriod duration.
        return AccumulatedOperationsResourceDistributionForLogging_.ExtractOperationAccumulatedResourceDistribution(operationId);
    }

    void LogOperationsInfo(const TPoolTreeSnapshotPtr& treeSnapshot) const
    {
        auto Logger = this->Logger().WithTag("TreeSnapshotId: %v", treeSnapshot->GetId());

        auto doLogOperationsInfo = [&] (const auto& operationIdToElement) {
            for (const auto& [operationId, element] : operationIdToElement) {
                // TODO(eshcherbin): Rethink format of fair share info log message.
                YT_LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                    element->GetLoggingString(treeSnapshot),
                    operationId);
            }
        };

        doLogOperationsInfo(treeSnapshot->EnabledOperationMap());
        doLogOperationsInfo(treeSnapshot->DisabledOperationMap());
    }

    void LogPoolsInfo(const TPoolTreeSnapshotPtr& treeSnapshot) const
    {
        auto Logger = this->Logger().WithTag("TreeSnapshotId: %v", treeSnapshot->GetId());

        for (const auto& [poolName, element] : treeSnapshot->PoolMap()) {
            YT_LOG_DEBUG("FairShareInfo: %v (Pool: %v)",
                element->GetLoggingString(treeSnapshot),
                poolName);
        }
    }

    void DoBuildFullFairShareInfo(const TPoolTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        YT_ASSERT_INVOKER_AFFINITY(StrategyHost_->GetOrchidWorkerInvoker());

        if (!treeSnapshot) {
            YT_LOG_DEBUG("Skipping construction of full fair share info, since snapshot is not constructed yet");
            return;
        }

        YT_LOG_DEBUG("Constructing full fair share info");

        auto fairShareInfo = BuildSerializedFairShareInfo(treeSnapshot);
        fluent
            .Items(fairShareInfo.PoolCount)
            .Item("pools").BeginMap()
                .DoFor(fairShareInfo.SplitPoolsInfo, [&] (TFluentMap fluent, const TYsonString& batch) {
                    fluent.Items(batch);
                })
            .EndMap()
            .Item("operations").BeginMap()
                .DoFor(fairShareInfo.SplitOperationsInfo, [&] (TFluentMap fluent, const TYsonString& batch) {
                    fluent.Items(batch);
                })
            .EndMap()
            .Items(fairShareInfo.ResourceDistributionInfo);
    }

    struct TSerializedFairShareInfo
    {
        TYsonString PoolCount;
        std::vector<TYsonString> SplitPoolsInfo;
        std::vector<TYsonString> SplitOperationsInfo;
        TYsonString ResourceDistributionInfo;
    };

    TSerializedFairShareInfo BuildSerializedFairShareInfo(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        int maxPoolBatchSize = std::numeric_limits<int>::max(),
        int maxOperationBatchSize = std::numeric_limits<int>::max()) const
    {
        YT_LOG_DEBUG("Started building serialized fair share info (MaxPoolBatchSize: %v, MaxOperationBatchSize: %v)",
            maxPoolBatchSize,
            maxOperationBatchSize);

        TSerializedFairShareInfo fairShareInfo;
        fairShareInfo.PoolCount = BuildYsonStringFluently<EYsonType::MapFragment>()
            .Item("pool_count").Value(std::ssize(treeSnapshot->PoolMap()))
            .Finish();
        fairShareInfo.ResourceDistributionInfo = BuildYsonStringFluently<EYsonType::MapFragment>()
            .Item("resource_distribution_info").BeginMap()
                .Do(std::bind(&TPoolTreeRootElement::BuildResourceDistributionInfo, treeSnapshot->RootElement(), std::placeholders::_1))
            .EndMap()
            .Finish();

        TYsonMapFragmentBatcher poolsConsumer(&fairShareInfo.SplitPoolsInfo, maxPoolBatchSize);
        BuildYsonMapFragmentFluently(&poolsConsumer)
            .Do(std::bind(&TPoolTree::BuildPoolsInfo, std::cref(treeSnapshot), TFieldFilter{}, std::placeholders::_1));
        poolsConsumer.Flush();

        auto buildOperationInfo = [&] (TFluentMap fluent, const TNonOwningOperationElementMap::value_type& pair) {
            const auto& [_, element] = pair;
            fluent
                .Item(element->GetId()).BeginMap()
                    .Do(std::bind(&TPoolTree::DoBuildOperationProgress, std::cref(treeSnapshot), element, StrategyHost_, std::placeholders::_1))
                .EndMap();
        };

        TYsonMapFragmentBatcher operationsConsumer(&fairShareInfo.SplitOperationsInfo, maxOperationBatchSize);
        BuildYsonMapFragmentFluently(&operationsConsumer)
            .DoFor(treeSnapshot->EnabledOperationMap(), buildOperationInfo)
            .DoFor(treeSnapshot->DisabledOperationMap(), buildOperationInfo);
        operationsConsumer.Flush();

        YT_LOG_DEBUG(
            "Finished building serialized fair share info "
            "(MaxPoolBatchSize: %v, PoolCount: %v, PoolBatchCount: %v, "
            "MaxOperationBatchSize: %v, OperationCount: %v, OperationBatchCount: %v)",
            maxPoolBatchSize,
            treeSnapshot->PoolMap().size() + 1,
            fairShareInfo.SplitPoolsInfo.size(),
            maxOperationBatchSize,
            treeSnapshot->EnabledOperationMap().size() + treeSnapshot->DisabledOperationMap().size(),
            fairShareInfo.SplitOperationsInfo.size());

        return fairShareInfo;
    }

    static void BuildCompositeElementInfo(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeCompositeElement* element,
        const TFieldFilter& filter,
        TFluentMap fluent)
    {
        const auto& attributes = element->Attributes();
        fluent
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "running_operation_count", element->RunningOperationCount())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "lightweight_running_operation_count", element->LightweightRunningOperationCount())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "schedulable_operation_count", element->SchedulableOperationCount())
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
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "aggressive_starvation_enabled", element->IsAggressiveStarvationEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "lightweight_operations_enabled", element->AreLightweightOperationsEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "effective_lightweight_operations_enabled", element->GetEffectiveLightweightOperationsEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "priority_strong_guarantee_adjustment_enabled", element->IsPriorityStrongGuaranteeAdjustmentEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "priority_strong_guarantee_adjustment_donorship_enabled", element->IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "gang_operations_allowed", element->AreGangOperationsAllowed())
            .Do(std::bind(&TPoolTree::DoBuildElementYson, std::cref(treeSnapshot), element, std::cref(filter), std::placeholders::_1));
    }

    static void BuildPoolInfo(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreePoolElement* pool,
        const TFieldFilter& filter,
        TFluentMap fluent)
    {
        fluent
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
                if (burstRatio > resourceFlowRatio + NVectorHdrf::LargeEpsilon) {
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
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "full_path", pool->GetFullPath(/*explicitOnly*/ false, /*withTreeId*/ false))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "child_pool_count", pool->GetChildPoolCount())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "redirect_to_cluster", pool->GetRedirectToCluster())
            .Do(std::bind(&TPoolTree::BuildCompositeElementInfo, std::cref(treeSnapshot), pool, std::cref(filter), std::placeholders::_1));
    }

    static void BuildPoolsInfo(const TPoolTreeSnapshotPtr& treeSnapshot, const TFieldFilter& filter, TFluentMap fluent)
    {
        const auto& poolMap = treeSnapshot->PoolMap();
        fluent
            .DoFor(poolMap, [&] (TFluentMap fluent, const TNonOwningPoolElementMap::value_type& pair) {
                const auto& [poolName, pool] = pair;
                fluent.Item(poolName)
                    .BeginMap()
                        .Do(std::bind(&TPoolTree::BuildPoolInfo, std::cref(treeSnapshot), pool, std::cref(filter), std::placeholders::_1))
                    .EndMap();
            })
            .Do(std::bind(&TPoolTree::DoBuildRootElementInfo, std::cref(treeSnapshot), std::cref(filter), std::placeholders::_1));
    }

    static void DoBuildRootElementInfo(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TFieldFilter& filter,
        TFluentMap fluent)
    {
        fluent
            .Item(RootPoolName).BeginMap()
                .Do(std::bind(
                    &TPoolTree::BuildCompositeElementInfo,
                    std::cref(treeSnapshot),
                    treeSnapshot->RootElement().Get(),
                    std::cref(filter),
                    std::placeholders::_1))
            .EndMap();
    }

    static void BuildChildPoolsByPoolInfo(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TFieldFilter& filter,
        const TString& parentPoolName,
        TFluentMap fluent)
    {
        auto* parentPool = [&] () -> TPoolTreeCompositeElement* {
            if (parentPoolName == RootPoolName) {
                return treeSnapshot->RootElement().Get();
            } else {
                return GetOrCrash(treeSnapshot->PoolMap(), parentPoolName);
            }
        }();

        fluent
            .Do(std::bind(&TPoolTree::DoBuildChildPoolsByPoolInfo,
                std::cref(treeSnapshot),
                std::cref(filter),
                parentPool,
                std::placeholders::_1));
    }

    static void BuildChildPoolsByPoolInfos(const TPoolTreeSnapshotPtr& treeSnapshot, const TFieldFilter& /*filter*/, TFluentMap fluent)
    {
        fluent
            .DoFor(treeSnapshot->PoolMap(), [&] (TFluentMap fluent, const TNonOwningPoolElementMap::value_type& pair) {
                const auto& [poolName, pool] = pair;
                fluent.Item(poolName).Entity();
            })
            .Item(RootPoolName).Entity();
    }

    static void DoBuildChildPoolsByPoolInfo(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TFieldFilter& filter,
        const TPoolTreeCompositeElement* parentPool,
        TFluentMap fluent)
    {
        fluent
            .DoFor(parentPool->EnabledChildren(), [&] (TFluentMap fluent, const TPoolTreeElementPtr& child) {
                if (!child->IsOperation()) {
                    fluent.Item(child->GetId())
                        .BeginMap().Do(
                            std::bind(&TPoolTree::BuildPoolInfo,
                            std::cref(treeSnapshot),
                            static_cast<TPoolTreePoolElement*>(child.Get()),
                            std::cref(filter),
                            std::placeholders::_1))
                        .EndMap();
                }
            });
    }

    void DoBuildPoolsStructureInfo(const TPoolTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        auto buildPoolInfo = [&] (const TPoolTreePoolElement* pool, TFluentMap fluent) {
            const auto& id = pool->GetId();
            fluent
                .Item(id).BeginMap()
                    .Item("abc").Value(pool->GetConfig()->Abc)
                    .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                        auto burstRatio = pool->GetSpecifiedBurstRatio();
                        auto resourceFlowRatio = pool->GetSpecifiedResourceFlowRatio();
                        fluent
                            .Item("parent").Value(pool->GetParent()->GetId())
                            .Item("strong_guarantee_resources").Value(pool->GetSpecifiedStrongGuaranteeResources())
                            .Item("burst_guarantee_resources").Value(pool->GetTotalResourceLimits() * burstRatio)
                            .Item("resource_flow").Value(pool->GetTotalResourceLimits() * resourceFlowRatio);
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

    void DoBuildOperationsAccumulatedUsageInfo(const TPoolTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        auto operationIdToAccumulatedResourceDistribution = AccumulatedOperationsResourceDistributionForLogging_.ExtractOperationResourceDistributionVolumes();

        auto buildOperationInfo = [&] (const TPoolTreeOperationElement* operation, TFluentMap fluent) {
            auto operationId = operation->GetOperationId();
            auto* parent = operation->GetParent();

            TAccumulatedResourceDistribution accumulatedResourceDistribution;
            {
                auto it = operationIdToAccumulatedResourceDistribution.find(operationId);
                if (it != operationIdToAccumulatedResourceDistribution.end()) {
                    accumulatedResourceDistribution = it->second;
                }
            }

            fluent
                .Item(operation->GetId()).BeginMap()
                    .Item("pool").Value(parent->GetId())
                    .Item("accumulated_resource_usage").Value(accumulatedResourceDistribution.Usage())
                    .Item("accumulated_resource_distribution").Value(accumulatedResourceDistribution)
                    .Item("user").Value(operation->GetUserName())
                    .Item("operation_type").Value(operation->GetOperationType())
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
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeOperationElement* element,
        IStrategyHost* const strategyHost,
        TFluentMap fluent)
    {
        auto* parent = element->GetParent();
        fluent
            .Item("pool").Value(parent->GetId())
            .Item("slot_index").Value(element->GetSlotIndex())
            .Item("start_time").Value(element->GetStartTime())
            .OptionalItem("fifo_index", element->Attributes().FifoIndex)
            .Item("grouped_needed_resources").Value(element->GroupedNeededResources())
            // COMPAT(eshcherbin)
            .Item("detailed_min_needed_job_resources").BeginList()
                .DoFor(element->GroupedNeededResources(), [&] (TFluentList fluent, const auto& pair) {
                    fluent.Item().Do([&] (TFluentAny fluent) {
                        strategyHost->SerializeResources(pair.second.MinNeededResources, fluent.GetConsumer());
                    });
                })
            .EndList()
            .Item("aggregated_min_needed_job_resources").Value(element->AggregatedMinNeededAllocationResources())
            .Item("tentative").Value(element->GetRuntimeParameters()->Tentative)
            .Item("probing").Value(element->GetRuntimeParameters()->Probing)
            .Item("offloading").Value(element->GetRuntimeParameters()->Offloading)
            .Item("starving_since").Value(element->GetStarvationStatus() != EStarvationStatus::NonStarving
                ? std::optional(element->GetLastNonStarvingTime())
                : std::nullopt)
            .Item("lightweight").Value(element->IsLightweight())
            .Item("is_gang").Value(element->IsGang())
            .Item("disk_request_media").DoListFor(element->DiskRequestMedia(), [&] (TFluentList fluent, int mediumIndex) {
                fluent.Item().Value(strategyHost->GetMediumNameByIndex(mediumIndex));
            })
            .Item("unschedulable_reason").Value(element->GetUnschedulableReason())
            .Item("allocation_preemption_timeout").Value(element->GetEffectiveAllocationPreemptionTimeout())
            .Item("allocation_graceful_preemption_timeout").Value(element->GetEffectiveAllocationGracefulPreemptionTimeout())
            .Item("user").Value(element->GetUserName())
            .Item("type").Value(element->GetOperationType())
            .Item("title").Value(element->GetTitle())
            .Do(BIND(&NPolicy::TSchedulingPolicy::BuildOperationProgress, ConstRef(treeSnapshot), Unretained(element), strategyHost))
            .Do(BIND(&TPoolTree::DoBuildElementYson, ConstRef(treeSnapshot), Unretained(element), TFieldFilter{}));
    }

    static void DoBuildElementYson(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeElement* element,
        const TFieldFilter& filter,
        TFluentMap fluent)
    {
        const auto& attributes = element->Attributes();
        const auto& persistentAttributes = element->PersistentAttributes();

        // TODO(eshcherbin): Rethink which fields should be here and which should be in |TPoolTreeElement::BuildYson|.
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
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_non_preemptible_resource_usage_threshold",
                element->EffectiveNonPreemptibleResourceUsageThresholdConfig())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_aggressive_starvation_enabled",
                element->GetEffectiveAggressiveStarvationEnabled())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
                filter,
                "effective_waiting_for_resources_on_node_timeout",
                element->GetEffectiveWaitingForResourcesOnNodeTimeout())
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
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_resource", attributes.DominantResource)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "resource_usage", element->GetResourceUsageAtUpdate())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "usage_share", attributes.UsageShare)
            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "usage_ratio", element->GetResourceDominantUsageShareAtUpdate())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_usage_share", element->GetResourceDominantUsageShareAtUpdate())

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "resource_demand", element->GetResourceDemand())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "demand_share", attributes.DemandShare)
            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "demand_ratio", MaxComponent(attributes.DemandShare))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_demand_share", MaxComponent(attributes.DemandShare))

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "resource_limits", element->GetResourceLimits())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "limits_share", attributes.LimitsShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "scheduling_tag_filter_resource_limits", element->GetSchedulingTagFilterResourceLimits())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "specified_resource_limits", element->GetSpecifiedResourceLimitsConfig())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "specified_resource_limits_overcommit_tolerance", element->SpecifiedResourceLimitsOvercommitTolerance())

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "limited_resource_demand", element->GetTotalResourceLimits() * element->LimitedDemandShare())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "limited_demand_share", element->LimitedDemandShare())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_limited_demand_share", MaxComponent(element->LimitedDemandShare()))

            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "min_share", attributes.StrongGuaranteeShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "strong_guarantee_share", attributes.StrongGuaranteeShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "strong_guarantee_share_by_tier", attributes.StrongGuaranteeShareByTier)
            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "min_share_resources", element->GetSpecifiedStrongGuaranteeResources())
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "strong_guarantee_resources", element->GetSpecifiedStrongGuaranteeResources())
            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "effective_min_share_resources", element->GetTotalResourceLimits() * attributes.StrongGuaranteeShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "effective_strong_guarantee_resources", element->GetTotalResourceLimits() * attributes.StrongGuaranteeShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "inferred_strong_guarantee_resources", attributes.InferredStrongGuaranteeResources)
            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "min_share_ratio", MaxComponent(attributes.StrongGuaranteeShare))

            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "fair_share_ratio", MaxComponent(attributes.FairShare.Total))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "detailed_fair_share", attributes.FairShare)
            .ITEM_DO_IF_SUITABLE_FOR_FILTER(
                filter,
                "detailed_dominant_fair_share",
                std::bind(&SerializeDominant, std::cref(attributes.FairShare), std::placeholders::_1))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "fair_resources", element->GetTotalResourceLimits() * attributes.FairShare.Total)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "detailed_promised_guarantee_fair_share", attributes.PromisedGuaranteeFairShare)
            .ITEM_DO_IF_SUITABLE_FOR_FILTER(
                filter,
                "detailed_dominant_promised_guarantee_fair_share",
                std::bind(&SerializeDominant, std::cref(attributes.PromisedGuaranteeFairShare), std::placeholders::_1))

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "estimated_guarantee_share", attributes.EstimatedGuaranteeShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "dominant_estimated_guarantee_share", MaxComponent(attributes.EstimatedGuaranteeShare))
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "estimated_guarantee_resources", element->GetTotalResourceLimits() * attributes.EstimatedGuaranteeShare)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "proposed_integral_share", attributes.ProposedIntegralShare)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "best_allocation_share", persistentAttributes.BestAllocationShare)

            .ITEM_OPTIONAL_VALUE_IF_SUITABLE_FOR_FILTER(filter, "fair_share_functions_statistics", element->GetFairShareFunctionsStatistics())

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "satisfaction_ratio", element->PostUpdateAttributes().SatisfactionRatio)
            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "local_satisfaction_ratio", element->PostUpdateAttributes().LocalSatisfactionRatio)

            .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "schedulable", element->IsSchedulable())
            .Do(BIND(&NPolicy::TSchedulingPolicy::BuildElementYson, ConstRef(treeSnapshot), Unretained(element), filter));
    }

    void DoBuildEssentialFairShareInfo(const TPoolTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        auto buildOperationsInfo = [&] (TFluentMap fluent, const TNonOwningOperationElementMap::value_type& pair) {
            const auto& [operationId, element] = pair;
            fluent
                .Item(ToString(operationId)).BeginMap()
                    .Do(BIND(&TPoolTree::DoBuildEssentialOperationProgress, Unretained(this), Unretained(element)))
                .EndMap();
        };

        fluent
            .Do(BIND(&TPoolTree::DoBuildEssentialPoolsInformation, Unretained(this), treeSnapshot))
            .Item("operations").BeginMap()
                .DoFor(treeSnapshot->EnabledOperationMap(), buildOperationsInfo)
                .DoFor(treeSnapshot->DisabledOperationMap(), buildOperationsInfo)
            .EndMap();
    }

    void DoBuildEssentialPoolsInformation(const TPoolTreeSnapshotPtr& treeSnapshot, TFluentMap fluent) const
    {
        const auto& poolMap = treeSnapshot->PoolMap();
        fluent
            .Item("pool_count").Value(std::ssize(poolMap))
            .Item("pools").DoMapFor(poolMap, [&] (TFluentMap fluent, const TNonOwningPoolElementMap::value_type& pair) {
                const auto& [poolName, pool] = pair;
                fluent
                    .Item(poolName).BeginMap()
                        .Do(BIND(&TPoolTree::DoBuildEssentialElementYson, Unretained(this), Unretained(pool)))
                    .EndMap();
            });
    }

    void DoBuildEssentialOperationProgress(const TPoolTreeOperationElement* element, TFluentMap fluent) const
    {
        fluent
            .Do(BIND(&TPoolTree::DoBuildEssentialElementYson, Unretained(this), Unretained(element)));
    }

    void DoBuildEssentialElementYson(const TPoolTreeElement* element, TFluentMap fluent) const
    {
        const auto& attributes = element->Attributes();

        fluent
            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .Item("usage_ratio").Value(element->GetResourceDominantUsageShareAtUpdate())
            .Item("dominant_usage_share").Value(element->GetResourceDominantUsageShareAtUpdate())
            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .Item("demand_ratio").Value(MaxComponent(attributes.DemandShare))
            .Item("dominant_demand_share").Value(MaxComponent(attributes.DemandShare))
            // COMPAT(eshcherbin, YT-24083): Deprecate old *_ratio and *_share terms.
            .Item("fair_share_ratio").Value(MaxComponent(attributes.FairShare.Total))
            .Item("dominant_fair_share").Value(MaxComponent(attributes.FairShare.Total))
            .Item("satisfaction_ratio").Value(element->PostUpdateAttributes().SatisfactionRatio)
            .Item("dominant_resource").Value(attributes.DominantResource)
            .DoIf(element->IsOperation(), [&] (TFluentMap fluent) {
                fluent
                    .Item("resource_usage").Value(element->GetResourceUsageAtUpdate());
            });
    }

    DEFINE_SIGNAL_OVERRIDE(void(TOperationId), OperationRunning);
};

////////////////////////////////////////////////////////////////////////////////

IPoolTreePtr CreatePoolTree(
    TStrategyTreeConfigPtr config,
    TStrategyOperationControllerConfigPtr controllerConfig,
    IPoolTreeHost* host,
    IStrategyHost* strategyHost,
    std::vector<IInvokerPtr> feasibleInvokers,
    std::string treeId)
{
    return New<TPoolTree>(
        std::move(config),
        std::move(controllerConfig),
        host,
        strategyHost,
        std::move(feasibleInvokers),
        std::move(treeId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
