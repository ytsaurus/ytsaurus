#include "fair_share_tree_profiling.h"

#include "fair_share_tree_allocation_scheduler.h"

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/misc/digest.h>

namespace NYT::NScheduler {

using namespace NProfiling;

using NVectorHdrf::ToJobResources;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto PoolKeepAlivePeriod = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeProfileManager::TFairShareTreeProfileManager(
    TProfiler profiler,
    bool sparsifyMetrics,
    const IInvokerPtr& profilingInvoker,
    TFairShareTreeAllocationSchedulerPtr treeScheduler)
    : Profiler_(std::move(profiler))
    , SparsifyMetrics_(sparsifyMetrics)
    , ProfilingInvoker_(profilingInvoker)
    , TreeScheduler_(std::move(treeScheduler))
    , NodeCountGauge_(Profiler_.Gauge("/node_count_per_tree"))
    , PoolCountGauge_(Profiler_.Gauge("/pools/pool_count"))
    , TotalElementCountGauge_(Profiler_.Gauge("/pools/total_element_count"))
    , DistributedResourcesBufferedProducer_(New<TBufferedProducer>())
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    Profiler_.AddProducer("", DistributedResourcesBufferedProducer_);
}

NProfiling::TProfiler TFairShareTreeProfileManager::GetProfiler() const
{
    return Profiler_;
}

void TFairShareTreeProfileManager::ProfileOperationUnregistration(const TSchedulerCompositeElement* pool, EOperationState state)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto guard = ReaderGuard(PoolNameToStateLock_);

    while (pool) {
        auto& counters = GetOrCrash(PoolNameToState_, pool->GetId()).UnregisterOperationCounters;
        if (IsOperationFinished(state)) {
            counters.FinishedCounters[state].Increment();
        } else {
            // Unregistration for running operation is considered as ban.
            counters.BannedCounter.Increment();
        }
        pool = pool->GetParent();
    }
}

void TFairShareTreeProfileManager::RegisterPool(const TSchedulerCompositeElementPtr& element)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    RegisterPoolProfiler(element->GetId());
}

void TFairShareTreeProfileManager::UnregisterPool(const TSchedulerCompositeElementPtr& element)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto guard = WriterGuard(PoolNameToStateLock_);

    GetOrCrash(PoolNameToState_, element->GetId()).RemoveTime = TInstant::Now();
}

void TFairShareTreeProfileManager::ProfileTree(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const THashMap<TOperationId, TResourceVolume>& operationIdToAccumulatedResourceUsageDelta)
{
    YT_ASSERT_INVOKER_AFFINITY(ProfilingInvoker_);

    NodeCountGauge_.Update(treeSnapshot->NodeCount());
    PoolCountGauge_.Update(treeSnapshot->PoolMap().size());
    TotalElementCountGauge_.Update(treeSnapshot->RootElement()->GetTreeSize());

    ProfileDistributedResources(treeSnapshot);

    // NB: We keep pool profiling entries in consistent with the main tree.
    PrepareOperationProfilingEntries(treeSnapshot);

    CleanupPoolProfilingEntries();

    ProfileOperations(treeSnapshot, operationIdToAccumulatedResourceUsageDelta);
    ProfilePools(treeSnapshot);
}

void TFairShareTreeProfileManager::PrepareOperationProfilingEntries(const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    for (auto [operationId, element] : treeSnapshot->EnabledOperationMap()) {
        auto slotIndex = element->GetSlotIndex();
        YT_VERIFY(slotIndex != UndefinedSlotIndex);
        auto parentPoolId = element->GetParent()->GetId();

        std::vector<TOperationUserProfilingTag> userProfilingTags;
        {
            bool enableByUserProfiling = treeSnapshot->TreeConfig()->EnableByUserProfiling;
            auto parent = element->GetParent();
            while (parent != nullptr) {
                bool enableProfiling = false;
                if (!parent->IsRoot()) {
                    const auto* pool = static_cast<const TSchedulerPoolElement*>(parent);
                    enableProfiling = pool->GetConfig()->EnableByUserProfiling.value_or(enableByUserProfiling);
                } else {
                    enableProfiling = enableByUserProfiling;
                }

                if (enableProfiling) {
                    userProfilingTags.push_back(TOperationUserProfilingTag{
                        .PoolId = parent->GetId(),
                        .UserName = element->GetUserName(),
                        .CustomTag = element->GetCustomProfilingTag(),
                    });

                }
                parent = parent->GetParent();
            }
        }

        auto& operationState = OperationIdToState_[operationId];
        if (!operationState.BufferedProducer ||
            operationState.SlotIndex != slotIndex ||
            operationState.ParentPoolId != parentPoolId ||
            operationState.UserProfilingTags != userProfilingTags)
        {
            operationState.SlotIndex = slotIndex;
            operationState.ParentPoolId = parentPoolId;
            operationState.UserProfilingTags = userProfilingTags;
            operationState.BufferedProducer = New<NProfiling::TBufferedProducer>();

            auto profiler = Profiler_
                .WithRequiredTag("pool", operationState.ParentPoolId, -1)
                .WithRequiredTag("slot_index", ToString(operationState.SlotIndex), -1);
            if (SparsifyMetrics_) {
                profiler = profiler.WithSparse();
            }
            profiler.AddProducer("/operations_by_slot", operationState.BufferedProducer);

            for (const auto& userProfilingTag : operationState.UserProfilingTags) {
                auto userProfiler = Profiler_
                    .WithTag("pool", userProfilingTag.PoolId, -1)
                    .WithRequiredTag("user_name", userProfilingTag.UserName, -1);
                if (SparsifyMetrics_) {
                    userProfiler = userProfiler.WithSparse();
                }

                if (userProfilingTag.CustomTag) {
                    userProfiler = userProfiler.WithTag("custom", *userProfilingTag.CustomTag, -1);
                }

                userProfiler.AddProducer("/operations_by_user", operationState.BufferedProducer);
            }
        }
    }

    std::vector<TOperationId> operationIdsToRemove;
    for (const auto& [operationId, _] : OperationIdToState_) {
        if (!treeSnapshot->EnabledOperationMap().contains(operationId)) {
            operationIdsToRemove.push_back(operationId);
        }
    }
    for (auto operationId : operationIdsToRemove) {
        OperationIdToState_.erase(operationId);
    }
}

void TFairShareTreeProfileManager::CleanupPoolProfilingEntries()
{
    auto now = TInstant::Now();

    auto guard = WriterGuard(PoolNameToStateLock_);

    std::vector<TString> poolNamesToRemove;
    for (const auto& [poolName, poolState] : PoolNameToState_) {
        if (poolState.RemoveTime && *poolState.RemoveTime + PoolKeepAlivePeriod < now) {
            poolNamesToRemove.push_back(poolName);
        }
    }

    for (const auto& poolName : poolNamesToRemove) {
        EraseOrCrash(PoolNameToState_, poolName);
        PoolNameToJobMetrics_.erase(poolName);
    }
}

void TFairShareTreeProfileManager::RegisterPoolProfiler(const TString& poolName)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    auto writerGuard = WriterGuard(PoolNameToStateLock_);

    if (auto it = PoolNameToState_.find(poolName);
        it != PoolNameToState_.end())
    {
        auto& poolState = it->second;
        poolState.RemoveTime = std::nullopt;
        return;
    }

    auto poolProfiler = Profiler_
        .WithRequiredTag("pool", poolName, -1);

    TUnregisterOperationCounters counters;
    counters.BannedCounter = poolProfiler.Counter("/pools/banned_operation_count");
    for (auto state : TEnumTraits<EOperationState>::GetDomainValues()) {
        if (IsOperationFinished(state)) {
            counters.FinishedCounters[state] = poolProfiler
                .WithTag("state", FormatEnum(state), -1)
                .Counter("/pools/finished_operation_count");
        }
    }

    auto it = EmplaceOrCrash(
        PoolNameToState_,
        poolName,
        TPoolState{
            .UnregisterOperationCounters = std::move(counters),
            .BufferedProducer = New<NProfiling::TBufferedProducer>(),
        });
    const auto& poolState = it->second;

    if (SparsifyMetrics_) {
        poolProfiler = poolProfiler.WithSparse();
    }
    poolProfiler.AddProducer("/pools", poolState.BufferedProducer);
}

void TFairShareTreeProfileManager::ProfileElement(
    ISensorWriter* writer,
    const TSchedulerElement* element,
    const TFairShareStrategyTreeConfigPtr& treeConfig)
{
    YT_ASSERT_INVOKER_AFFINITY(ProfilingInvoker_);

    const auto& attributes = element->Attributes();

    auto profileDominantFairShare = [&] (const TString& prefix, const NVectorHdrf::TDetailedFairShare& fairShare) {
        writer->AddGauge(prefix + "/strong_guarantee", MaxComponent(fairShare.StrongGuarantee));
        writer->AddGauge(prefix + "/integral_guarantee", MaxComponent(fairShare.IntegralGuarantee));
        writer->AddGauge(prefix + "/weight_proportional", MaxComponent(fairShare.WeightProportional));
        writer->AddGauge(prefix + "/total", MaxComponent(fairShare.Total));
    };

    writer->AddGauge("/dominant_usage_share", element->GetResourceDominantUsageShareAtUpdate());
    writer->AddGauge("/dominant_demand_share", MaxComponent(attributes.DemandShare));
    writer->AddGauge("/promised_dominant_fair_share", MaxComponent(attributes.PromisedFairShare));
    writer->AddGauge("/dominant_estimated_guarantee_share", MaxComponent(attributes.EstimatedGuaranteeShare));
    writer->AddGauge("/accumulated_volume_dominant_share", element->GetAccumulatedResourceRatioVolume());
    writer->AddGauge("/weight", element->GetWeight());
    profileDominantFairShare("/dominant_fair_share", attributes.FairShare);
    profileDominantFairShare("/dominant_promised_guarantee_fair_share", attributes.PromisedGuaranteeFairShare);
    writer->AddGauge("/dominant_limited_demand_share", MaxComponent(element->LimitedDemandShare()));

    if (element->PostUpdateAttributes().LocalSatisfactionRatio < InfiniteSatisfactionRatio) {
        writer->AddGauge("/local_satisfaction_ratio", element->PostUpdateAttributes().LocalSatisfactionRatio);
    }

    ProfileResources(writer, element->GetResourceUsageAtUpdate(), "/resource_usage");
    ProfileResources(writer, element->GetResourceLimits(), "/resource_limits");
    ProfileResources(writer, element->GetResourceDemand(), "/resource_demand");
    ProfileResources(writer, element->GetTotalResourceLimits() * element->LimitedDemandShare(), "/limited_resource_demand");
    ProfileResourcesConfig(writer, element->GetSpecifiedResourceLimitsConfig(), "/specified_resource_limits");

    for (const auto& [schedulingStage, scheduledResourcesMap] : ScheduledResourcesByStageMap_) {
        auto scheduledResourcesIt = scheduledResourcesMap.find(element->GetId());
        if (scheduledResourcesIt != scheduledResourcesMap.end()) {
            static_assert(CFormattable<std::optional<EAllocationSchedulingStage>>);
            TWithTagGuard guard(
                writer,
                "scheduling_stage",
                // TODO(babenko): migrate to std::string
                schedulingStage ? TString(FormatEnum(*schedulingStage)) : ToString(schedulingStage));
            ProfileResources(writer, scheduledResourcesIt->second, "/scheduled_job_resources", EMetricType::Counter);
        }
    }

    for (auto preemptionReason : TEnumTraits<EAllocationPreemptionReason>::GetDomainValues()) {
        auto preemptedResourcesIt = PreemptedResourcesByReasonMap_[preemptionReason].find(element->GetId());
        auto preemptedResourceTimesIt = PreemptedResourceTimesByReasonMap_[preemptionReason].find(element->GetId());
        auto improperlyPreemptedResourceIt = ImproperlyPreemptedResourcesByReasonMap_[preemptionReason].find(element->GetId());

        TWithTagGuard guard(writer, "preemption_reason", FormatEnum(preemptionReason));
        if (preemptedResourcesIt != PreemptedResourcesByReasonMap_[preemptionReason].end()) {
            ProfileResources(writer, preemptedResourcesIt->second, "/preempted_job_resources", EMetricType::Counter);
        }
        if (preemptedResourceTimesIt != PreemptedResourceTimesByReasonMap_[preemptionReason].end()) {
            ProfileResources(writer, preemptedResourceTimesIt->second, "/preempted_job_resource_times", EMetricType::Counter);
        }
        if (improperlyPreemptedResourceIt != ImproperlyPreemptedResourcesByReasonMap_[preemptionReason].end()) {
            ProfileResources(writer, improperlyPreemptedResourceIt->second, "/improperly_preempted_job_resources", EMetricType::Counter);
        }
    }

    bool enableVectorProfiling;
    if (element->IsOperation()) {
        enableVectorProfiling = treeConfig->EnableOperationsVectorProfiling;
    } else {
        enableVectorProfiling = treeConfig->EnablePoolsVectorProfiling;
    }

    if (enableVectorProfiling) {
        const auto& profiledResources = element->IsOperation()
            ? treeConfig->ProfiledOperationResources
            : treeConfig->ProfiledPoolResources;

        auto profileVectorFairShare = [&] (const TString& prefix, const NVectorHdrf::TDetailedFairShare& fairShare) {
            ProfileResourceVector(
                writer,
                profiledResources,
                fairShare.StrongGuarantee,
                prefix + "/strong_guarantee");
            ProfileResourceVector(
                writer,
                profiledResources,
                fairShare.IntegralGuarantee,
                prefix + "/integral_guarantee");
            ProfileResourceVector(
                writer,
                profiledResources,
                fairShare.WeightProportional,
                prefix + "/weight_proportional");
            ProfileResourceVector(
                writer,
                profiledResources,
                fairShare.Total,
                prefix + "/total");
        };

        profileVectorFairShare("/fair_share", attributes.FairShare);
        profileVectorFairShare("/promised_guarantee_fair_share", attributes.PromisedGuaranteeFairShare);

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.UsageShare,
            "/usage_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.DemandShare,
            "/demand_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.LimitsShare,
            "/limits_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            element->LimitedDemandShare(),
            "/limited_demand_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.StrongGuaranteeShare,
            "/strong_guarantee_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.ProposedIntegralShare,
            "/proposed_integral_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.PromisedFairShare,
            "/promised_fair_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.EstimatedGuaranteeShare,
            "/estimated_guarantee_share");
    }
}

void TFairShareTreeProfileManager::ProfileOperations(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const THashMap<TOperationId, TResourceVolume>& operationIdToAccumulatedResourceUsageDelta)
{
    YT_ASSERT_INVOKER_AFFINITY(ProfilingInvoker_);

    if (!treeSnapshot->TreeConfig()->EnableOperationsProfiling) {
        return;
    }

    for (auto [operationId, element] : treeSnapshot->EnabledOperationMap()) {
        TSensorBuffer buffer;
        ProfileElement(
            &buffer,
            element,
            treeSnapshot->TreeConfig());

        TreeScheduler_->ProfileOperation(element, treeSnapshot, &buffer);

        auto& operationState = GetOrCrash(OperationIdToState_, operationId);
        if (auto it = operationIdToAccumulatedResourceUsageDelta.find(operationId);
            it != operationIdToAccumulatedResourceUsageDelta.end())
        {
            operationState.AccumulatedResourceUsage += it->second;
        }

        ProfileResourceVolume(
            &buffer,
            operationState.AccumulatedResourceUsage,
            "/accumulated_resource_usage",
            EMetricType::Counter);

        operationState.JobMetrics.Profile(&buffer);

        operationState.BufferedProducer->Update(std::move(buffer));
    }
}

void TFairShareTreeProfileManager::ProfilePool(
    const TSchedulerCompositeElement* element,
    const TFairShareStrategyTreeConfigPtr& treeConfig,
    const NProfiling::TBufferedProducerPtr& producer)
{
    YT_ASSERT_INVOKER_AFFINITY(ProfilingInvoker_);

    TSensorBuffer buffer;
    ProfileElement(
        &buffer,
        element,
        treeConfig);

    buffer.AddGauge("/max_operation_count", element->GetMaxOperationCount());
    buffer.AddGauge("/max_running_operation_count", element->GetMaxRunningOperationCount());
    buffer.AddGauge("/running_operation_count", element->RunningOperationCount());
    buffer.AddGauge("/lightweight_running_operation_count", element->LightweightRunningOperationCount());
    buffer.AddGauge("/total_operation_count", element->OperationCount());
    buffer.AddGauge("/schedulable_element_count", element->SchedulableElementCount());
    buffer.AddGauge("/schedulable_pool_count", element->SchedulablePoolCount());
    buffer.AddGauge("/schedulable_operation_count", element->SchedulableOperationCount());

    ProfileResources(&buffer, element->GetSpecifiedStrongGuaranteeResources(), "/strong_guarantee_resources");
    ProfileResources(&buffer, element->Attributes().EffectiveStrongGuaranteeResources, "/effective_strong_guarantee_resources");

    auto integralGuaranteesConfig = element->GetIntegralGuaranteesConfig();
    if (integralGuaranteesConfig->GuaranteeType != EIntegralGuaranteeType::None) {
        ProfileResources(&buffer, ToJobResources(integralGuaranteesConfig->ResourceFlow, {}), "/resource_flow");
        ProfileResources(&buffer, ToJobResources(integralGuaranteesConfig->BurstGuaranteeResources, {}), "/burst_guarantee_resources");
    }

    if (integralGuaranteesConfig->ResourceFlow->IsNonTrivial()) {
        ProfileResourceVolume(
            &buffer,
            element->Attributes().AcceptedFreeVolume,
            "/accepted_free_volume");
        ProfileResourceVolume(
            &buffer,
            element->Attributes().VolumeOverflow,
            "/volume_overflow");
    }

    ProfileResourceVolume(
        &buffer,
        element->PersistentAttributes().IntegralResourcesState.AccumulatedVolume,
        "/accumulated_resource_volume");

    for (auto quantile : treeConfig->PerPoolSatisfactionProfilingQuantiles) {
        const auto& digest = element->PostUpdateAttributes().SatisfactionDigest;
        YT_ASSERT(digest);

        TWithTagGuard guard(&buffer, "quantile", ToString(quantile));
        buffer.AddGauge("/operation_satisfaction_distribution", digest->GetQuantile(quantile));
    }

    if (auto it = PoolNameToJobMetrics_.find(element->GetId());
        it != PoolNameToJobMetrics_.end())
    {
        it->second.Profile(&buffer);
    }

    producer->Update(std::move(buffer));
}

void TFairShareTreeProfileManager::ProfilePools(const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    YT_ASSERT_INVOKER_AFFINITY(ProfilingInvoker_);

    THashMap<TString, TPoolState> poolNameToState;
    {
        auto readerGuard = ReaderGuard(PoolNameToStateLock_);
        poolNameToState = PoolNameToState_;
    }

    auto findPoolBufferedProducer = [&poolNameToState] (const TString& poolName) -> NProfiling::TBufferedProducerPtr {
        auto it = poolNameToState.find(poolName);
        if (it == poolNameToState.end()) {
            return nullptr;
        }
        return it->second.BufferedProducer;
    };

    for (auto [poolName, element] : treeSnapshot->PoolMap()) {
        const auto& bufferedProducer = findPoolBufferedProducer(poolName);
        if (!bufferedProducer) {
            continue;
        }

        ProfilePool(
            element,
            treeSnapshot->TreeConfig(),
            bufferedProducer);
    }

    ProfilePool(
        treeSnapshot->RootElement().Get(),
        treeSnapshot->TreeConfig(),
        findPoolBufferedProducer(RootPoolName));
}

void TFairShareTreeProfileManager::ProfileDistributedResources(const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    TSensorBuffer buffer;

    auto info = treeSnapshot->RootElement()->GetResourceDistributionInfo();
    ProfileResources(&buffer, info.DistributedStrongGuaranteeResources, "/distributed_strong_guarantee_resources");
    ProfileResources(&buffer, info.DistributedResourceFlow, "/distributed_resource_flow");
    ProfileResources(&buffer, info.DistributedBurstGuaranteeResources, "/distributed_burst_guarantee_resources");
    ProfileResources(&buffer, info.DistributedResources, "/distributed_resources");
    ProfileResources(&buffer, info.UndistributedResources, "/undistributed_resources");
    ProfileResources(&buffer, info.UndistributedResourceFlow, "/undistributed_resource_flow");
    ProfileResources(&buffer, info.UndistributedBurstGuaranteeResources, "/undistributed_burst_guarantee_resources");

    DistributedResourcesBufferedProducer_->Update(std::move(buffer));
}

void TFairShareTreeProfileManager::ApplyJobMetricsDelta(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const THashMap<TOperationId, TJobMetrics>& jobMetricsPerOperation)
{
    YT_ASSERT_INVOKER_AFFINITY(ProfilingInvoker_);

    for (const auto& [operationId, jobMetricsDelta] : jobMetricsPerOperation) {
        const auto* operationElement = treeSnapshot->FindEnabledOperationElement(operationId);
        if (operationElement) {
            OperationIdToState_[operationId].JobMetrics += jobMetricsDelta;
        } else {
            operationElement = treeSnapshot->FindDisabledOperationElement(operationId);
            YT_VERIFY(operationElement);
            // NB: We add metrics to the parent pools for completeness purposes
            // and not add it to the operation since profiling is omitted for disabled operations.
        }

        const auto* currentElement = operationElement->GetParent();
        while (currentElement) {
            PoolNameToJobMetrics_[currentElement->GetId()] += jobMetricsDelta;
            currentElement = currentElement->GetParent();
        }
    }
}

void TFairShareTreeProfileManager::ApplyScheduledAndPreemptedResourcesDelta(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const THashMap<std::optional<EAllocationSchedulingStage>, TOperationIdToJobResources>& scheduledAllocationResources,
    const TEnumIndexedArray<EAllocationPreemptionReason, TOperationIdToJobResources>& preemptedAllocationResources,
    const TEnumIndexedArray<EAllocationPreemptionReason, TOperationIdToJobResources>& preemptedAllocationResourceTimes,
    const TEnumIndexedArray<EAllocationPreemptionReason, TOperationIdToJobResources>& improperlyPreemptedAllocationResources)
{
    YT_ASSERT_INVOKER_AFFINITY(ProfilingInvoker_);

    auto applyDeltas = [&] (const auto& operationIdToDeltas, auto& operationIdToValues) {
        for (const auto& [operationId, delta]: operationIdToDeltas) {
            const TSchedulerElement* currentElement = treeSnapshot->FindEnabledOperationElement(operationId);
            if (!currentElement) {
                currentElement = treeSnapshot->FindDisabledOperationElement(operationId);
            }

            currentElement = currentElement->GetParent();
            while (currentElement) {
                operationIdToValues[currentElement->GetId()] += delta;
                currentElement = currentElement->GetParent();
            }
        }
    };

    for (const auto& [schedulingStage, scheduledAllocationResourcesDeltas] : scheduledAllocationResources) {
        applyDeltas(scheduledAllocationResourcesDeltas, ScheduledResourcesByStageMap_[schedulingStage]);
    }

    for (auto preemptionReason : TEnumTraits<EAllocationPreemptionReason>::GetDomainValues()) {
        applyDeltas(preemptedAllocationResources[preemptionReason], PreemptedResourcesByReasonMap_[preemptionReason]);
        applyDeltas(preemptedAllocationResourceTimes[preemptionReason], PreemptedResourceTimesByReasonMap_[preemptionReason]);
        applyDeltas(improperlyPreemptedAllocationResources[preemptionReason], ImproperlyPreemptedResourcesByReasonMap_[preemptionReason]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
