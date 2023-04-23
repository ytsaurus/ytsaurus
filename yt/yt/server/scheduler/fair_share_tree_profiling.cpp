#include "fair_share_tree_profiling.h"

#include <yt/yt/ytlib/scheduler/helpers.h>

namespace NYT::NScheduler {

using namespace NProfiling;

using NVectorHdrf::ToJobResources;

////////////////////////////////////////////////////////////////////////////////

static constexpr TDuration PoolKeepAlivePeriod = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

bool TFairShareTreeProfileManager::TOperationUserProfilingTag::operator == (const TOperationUserProfilingTag& other) const
{
    return PoolId == other.PoolId && UserName == other.UserName && CustomTag == other.CustomTag;
}

bool TFairShareTreeProfileManager::TOperationUserProfilingTag::operator != (const TOperationUserProfilingTag& other) const
{
    return !(*this == other);
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeProfileManager::TFairShareTreeProfileManager(
    const TString& treeId,
    bool sparsifyMetrics,
    const IInvokerPtr& profilingInvoker)
    : Profiler_(
        SchedulerProfiler
            .WithGlobal()
            .WithProducerRemoveSupport()
            .WithRequiredTag("tree", treeId))
    , SparsifyMetrics_(sparsifyMetrics)
    , ProfilingInvoker_(profilingInvoker)
    , NodeCountGauge_(Profiler_.Gauge("/node_count_per_tree"))
    , PoolCountGauge_(Profiler_.Gauge("/pools/pool_count"))
    , TotalElementCountGauge_(Profiler_.Gauge("/pools/total_element_count"))
    , DistributedResourcesBufferedProducer_(New<TBufferedProducer>())
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Profiler_.AddProducer("", DistributedResourcesBufferedProducer_);
}

NProfiling::TProfiler TFairShareTreeProfileManager::GetProfiler() const
{
    return Profiler_;
}

void TFairShareTreeProfileManager::ProfileOperationUnregistration(const TSchedulerCompositeElement* pool, EOperationState state)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto guard = ReaderGuard(PoolNameToProfilingEntryLock_);

    while (pool) {
        auto& counters = GetOrCrash(PoolNameToProfilingEntry_, pool->GetId()).UnregisterOperationCounters;
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
    VERIFY_THREAD_AFFINITY(ControlThread);

    RegisterPoolProfiler(element->GetId());
}

void TFairShareTreeProfileManager::UnregisterPool(const TSchedulerCompositeElementPtr& element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto guard = WriterGuard(PoolNameToProfilingEntryLock_);

    GetOrCrash(PoolNameToProfilingEntry_, element->GetId()).RemoveTime = TInstant::Now();
}

void TFairShareTreeProfileManager::ProfileTree(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const THashMap<TOperationId, TResourceVolume>& operationIdToAccumulatedResourceUsageDelta)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

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

        bool createProfilers = false;
        auto it = OperationIdToProfilingEntry_.find(operationId);
        if (it == OperationIdToProfilingEntry_.end()) {
            auto insertResult = OperationIdToProfilingEntry_.emplace(
                operationId,
                TOperationProfilingEntry{
                    .SlotIndex = slotIndex,
                    .ParentPoolId = parentPoolId,
                    .UserProfilingTags = userProfilingTags,
                    .BufferedProducer = New<NProfiling::TBufferedProducer>()
                });
            YT_VERIFY(insertResult.second);
            it = insertResult.first;

            EmplaceOrCrash(OperationIdToAccumulatedResourceUsage_, operationId, TResourceVolume{});

            createProfilers = true;
        } else {
            auto& profilingEntry = it->second;
            if (profilingEntry.SlotIndex != slotIndex ||
                profilingEntry.ParentPoolId != parentPoolId ||
                profilingEntry.UserProfilingTags != userProfilingTags)
            {
                profilingEntry = TOperationProfilingEntry{
                    .SlotIndex = slotIndex,
                    .ParentPoolId = parentPoolId,
                    .UserProfilingTags = userProfilingTags,
                    .BufferedProducer = New<NProfiling::TBufferedProducer>()
                };
                createProfilers = true;
            }
        }

        const auto& profilingEntry = it->second;

        if (createProfilers) {
            auto profiler = Profiler_
                .WithRequiredTag("pool", profilingEntry.ParentPoolId, -1)
                .WithRequiredTag("slot_index", ToString(profilingEntry.SlotIndex), -1);
            if (SparsifyMetrics_) {
                profiler = profiler.WithSparse();
            }
            profiler.AddProducer("/operations_by_slot", profilingEntry.BufferedProducer);

            for (const auto& userProfilingTag : profilingEntry.UserProfilingTags) {
                auto userProfiler = Profiler_
                    .WithTag("pool", userProfilingTag.PoolId, -1)
                    .WithRequiredTag("user_name", userProfilingTag.UserName, -1);
                if (SparsifyMetrics_) {
                    userProfiler = userProfiler.WithSparse();
                }

                if (userProfilingTag.CustomTag) {
                    userProfiler = userProfiler.WithTag("custom", *userProfilingTag.CustomTag, -1);
                }

                userProfiler.AddProducer("/operations_by_user", profilingEntry.BufferedProducer);
            }
        }
    }

    std::vector<TOperationId> operationIdsToRemove;
    for (const auto& [operationId, entry] : OperationIdToProfilingEntry_) {
        if (!treeSnapshot->EnabledOperationMap().contains(operationId)) {
            operationIdsToRemove.push_back(operationId);
        }
    }
    for (auto operationId : operationIdsToRemove) {
        OperationIdToProfilingEntry_.erase(operationId);
        OperationIdToAccumulatedResourceUsage_.erase(operationId);
        JobMetricsMap_.erase(ToString(operationId));
    }
}

void TFairShareTreeProfileManager::CleanupPoolProfilingEntries()
{
    auto now = TInstant::Now();

    auto guard = WriterGuard(PoolNameToProfilingEntryLock_);

    std::vector<TString> poolNamesToRemove;
    for (const auto& [poolName, entry] : PoolNameToProfilingEntry_) {
        if (entry.RemoveTime && *entry.RemoveTime + PoolKeepAlivePeriod < now) {
            poolNamesToRemove.push_back(poolName);
        }
    }

    for (const auto& poolName : poolNamesToRemove) {
        EraseOrCrash(PoolNameToProfilingEntry_, poolName);
        JobMetricsMap_.erase(poolName);
    }
}

void TFairShareTreeProfileManager::RegisterPoolProfiler(const TString& poolName)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto writerGuard = WriterGuard(PoolNameToProfilingEntryLock_);

    auto it = PoolNameToProfilingEntry_.find(poolName);
    if (it != PoolNameToProfilingEntry_.end()) {
        auto& entry = it->second;
        entry.RemoveTime = std::nullopt;
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

    auto insertResult = PoolNameToProfilingEntry_.emplace(
        poolName,
        TPoolProfilingEntry{std::move(counters), /* RemoveTime */ std::nullopt, New<NProfiling::TBufferedProducer>()});
    YT_VERIFY(insertResult.second);

    const auto& entry = insertResult.first->second;

    if (SparsifyMetrics_) {
        poolProfiler = poolProfiler.WithSparse();
    }
    poolProfiler.AddProducer("/pools", entry.BufferedProducer);
}

void TFairShareTreeProfileManager::ProfileElement(
    ISensorWriter* writer,
    const TSchedulerElement* element,
    const TFairShareStrategyTreeConfigPtr& treeConfig)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

    const auto& attributes = element->Attributes();

    const auto& detailedFairShare = attributes.FairShare;
    writer->AddGauge("/dominant_usage_share", element->GetResourceDominantUsageShareAtUpdate());
    writer->AddGauge("/dominant_demand_share", MaxComponent(attributes.DemandShare));
    writer->AddGauge("/promised_dominant_fair_share", MaxComponent(attributes.PromisedFairShare));
    writer->AddGauge("/accumulated_volume_dominant_share", element->GetAccumulatedResourceRatioVolume());
    writer->AddGauge("/dominant_fair_share/strong_guarantee", MaxComponent(detailedFairShare.StrongGuarantee));
    writer->AddGauge("/dominant_fair_share/integral_guarantee", MaxComponent(detailedFairShare.IntegralGuarantee));
    writer->AddGauge("/dominant_fair_share/weight_proportional", MaxComponent(detailedFairShare.WeightProportional));
    writer->AddGauge("/dominant_fair_share/total", MaxComponent(detailedFairShare.Total));

    if (element->PostUpdateAttributes().LocalSatisfactionRatio < InfiniteSatisfactionRatio) {
        writer->AddGauge("/local_satisfaction_ratio", element->PostUpdateAttributes().LocalSatisfactionRatio);
    }

    ProfileResources(writer, element->GetResourceUsageAtUpdate(), "/resource_usage");
    ProfileResources(writer, element->GetResourceLimits(), "/resource_limits");
    ProfileResources(writer, element->GetResourceDemand(), "/resource_demand");
    ProfileResourcesConfig(writer, element->GetSpecifiedResourceLimitsConfig(), "/specified_resource_limits");

    auto jobMetricsIt = JobMetricsMap_.find(element->GetId());
    if (jobMetricsIt != JobMetricsMap_.end()) {
        jobMetricsIt->second.Profile(writer);
    }

    for (const auto& [schedulingStage, scheduledResourcesMap] : ScheduledResourcesByStageMap_) {
        auto scheduledResourcesIt = scheduledResourcesMap.find(element->GetId());
        if (scheduledResourcesIt != scheduledResourcesMap.end()) {
            TWithTagGuard guard(
                writer,
                "scheduling_stage",
                schedulingStage ? FormatEnum(*schedulingStage) : ToString(schedulingStage));
            ProfileResources(writer, scheduledResourcesIt->second, "/scheduled_job_resources", EMetricType::Counter);
        }
    }

    for (auto preemptionReason : TEnumTraits<EJobPreemptionReason>::GetDomainValues()) {
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

        ProfileResourceVector(
            writer,
            profiledResources,
            detailedFairShare.StrongGuarantee,
            "/fair_share/strong_guarantee");
        ProfileResourceVector(
            writer,
            profiledResources,
            detailedFairShare.IntegralGuarantee,
            "/fair_share/integral_guarantee");
        ProfileResourceVector(
            writer,
            profiledResources,
            detailedFairShare.WeightProportional,
            "/fair_share/weight_proportional");
        ProfileResourceVector(
            writer,
            profiledResources,
            detailedFairShare.Total,
            "/fair_share/total");

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

        if (!element->IsOperation()) {
            ProfileResourceVolume(
                writer,
                element->PersistentAttributes().IntegralResourcesState.AccumulatedVolume,
                "/accumulated_resource_volume");
        }
    }
}

void TFairShareTreeProfileManager::ProfileOperations(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const THashMap<TOperationId, TResourceVolume>& operationIdToAccumulatedResourceUsageDelta)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

    if (!treeSnapshot->TreeConfig()->EnableOperationsProfiling) {
        return;
    }

    for (auto [operationId, element] : treeSnapshot->EnabledOperationMap()) {
        TSensorBuffer buffer;
        ProfileElement(
            &buffer,
            element,
            treeSnapshot->TreeConfig());
        {
            if (auto itUsage = OperationIdToAccumulatedResourceUsage_.find(operationId);
                itUsage != OperationIdToAccumulatedResourceUsage_.end())
            {
                auto& accumulatedResourceUsageVolume = itUsage->second;
                if (auto itUsageDelta = operationIdToAccumulatedResourceUsageDelta.find(operationId);
                    itUsageDelta != operationIdToAccumulatedResourceUsageDelta.end())
                {
                    accumulatedResourceUsageVolume += itUsageDelta->second;
                }
                ProfileResourceVolume(&buffer, accumulatedResourceUsageVolume, "/accumulated_resource_usage", EMetricType::Counter);
            }
        }
        GetOrCrash(OperationIdToProfilingEntry_, operationId).BufferedProducer->Update(std::move(buffer));
    }
}

void TFairShareTreeProfileManager::ProfilePool(
    const TSchedulerCompositeElement* element,
    const TFairShareStrategyTreeConfigPtr& treeConfig,
    const NProfiling::TBufferedProducerPtr& producer)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

    TSensorBuffer buffer;
    ProfileElement(
        &buffer,
        element,
        treeConfig);
    buffer.AddGauge("/max_operation_count", element->GetMaxOperationCount());
    buffer.AddGauge("/max_running_operation_count", element->GetMaxRunningOperationCount());
    buffer.AddGauge("/running_operation_count", element->RunningOperationCount());
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

    producer->Update(std::move(buffer));
}

void TFairShareTreeProfileManager::ProfilePools(const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

    THashMap<TString, TPoolProfilingEntry> poolNameToProfilingEntry;
    {
        auto readerGuard = ReaderGuard(PoolNameToProfilingEntryLock_);
        poolNameToProfilingEntry = PoolNameToProfilingEntry_;
    }

    auto findPoolBufferedProducer = [&poolNameToProfilingEntry] (const TString& poolName) -> NProfiling::TBufferedProducerPtr {
        auto it = poolNameToProfilingEntry.find(poolName);
        if (it == poolNameToProfilingEntry.end()) {
            return nullptr;
        }
        return it->second.BufferedProducer;
    };

    for (auto [poolName, element] : treeSnapshot->PoolMap()) {
        const auto& entry = findPoolBufferedProducer(poolName);
        if (!entry) {
            continue;
        }

        ProfilePool(
            element,
            treeSnapshot->TreeConfig(),
            entry);
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
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

    for (const auto& [operationId, jobMetricsDelta] : jobMetricsPerOperation) {
        const TSchedulerElement* currentElement = treeSnapshot->FindEnabledOperationElement(operationId);
        if (!currentElement) {
            currentElement = treeSnapshot->FindDisabledOperationElement(operationId);
        }
        YT_VERIFY(currentElement);
        while (currentElement) {
            JobMetricsMap_[currentElement->GetId()] += jobMetricsDelta;
            currentElement = currentElement->GetParent();
        }
    }
}

void TFairShareTreeProfileManager::ApplyScheduledAndPreemptedResourcesDelta(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const THashMap<std::optional<EJobSchedulingStage>, TOperationIdToJobResources>& scheduledJobResources,
    const TEnumIndexedVector<EJobPreemptionReason, TOperationIdToJobResources>& preemptedJobResources,
    const TEnumIndexedVector<EJobPreemptionReason, TOperationIdToJobResources>& preemptedJobResourceTimes,
    const TEnumIndexedVector<EJobPreemptionReason, TOperationIdToJobResources>& improperlyPreemptedJobResources)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

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

    for (const auto& [schedulingStage, scheduledJobResourcesDeltas] : scheduledJobResources) {
        applyDeltas(scheduledJobResourcesDeltas, ScheduledResourcesByStageMap_[schedulingStage]);
    }

    for (auto preemptionReason : TEnumTraits<EJobPreemptionReason>::GetDomainValues()) {
        applyDeltas(preemptedJobResources[preemptionReason], PreemptedResourcesByReasonMap_[preemptionReason]);
        applyDeltas(preemptedJobResourceTimes[preemptionReason], PreemptedResourceTimesByReasonMap_[preemptionReason]);
        applyDeltas(improperlyPreemptedJobResources[preemptionReason], ImproperlyPreemptedResourcesByReasonMap_[preemptionReason]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
