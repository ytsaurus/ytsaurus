#include "fair_share_tree_profiling.h"

namespace NYT::NScheduler {

using namespace NProfiling;

using NFairShare::ToJobResources;

////////////////////////////////////////////////////////////////////////////////

static constexpr TDuration PoolKeepAlivePeriod = TDuration::Minutes(1);

////////////////////////////////////////////////////////////////////////////////

bool TFairShareTreeProfiler::TOperationUserProfilingTag::operator == (const TOperationUserProfilingTag& other) const
{
    return PoolId == other.PoolId && UserName == other.UserName && CustomTag == other.CustomTag;
}

bool TFairShareTreeProfiler::TOperationUserProfilingTag::operator != (const TOperationUserProfilingTag& other) const
{
    return !(*this == other);
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeProfiler::TFairShareTreeProfiler(
    const TString& treeId,
    const IInvokerPtr& profilingInvoker)
    : Registry_(
        SchedulerProfiler
            .WithGlobal()
            .WithRequiredTag("tree", treeId))
    , ProfilingInvoker_(profilingInvoker)
    , PoolCountGauge_(Registry_.Gauge("/pools/pool_count"))
    , TotalElementCountGauge_(Registry_.Gauge("/pools/total_element_count"))
    , SchedulableElementCountGauge_(Registry_.Gauge("/pools/schedulable_element_count"))
    , DistributedResourcesBufferedProducer_(New<TBufferedProducer>())
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Registry_.AddProducer("", DistributedResourcesBufferedProducer_);
}

NProfiling::TProfiler TFairShareTreeProfiler::GetRegistry() const
{
    return Registry_;
}

void TFairShareTreeProfiler::ProfileOperationUnregistration(const TSchedulerCompositeElement* pool, EOperationState state)
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

void TFairShareTreeProfiler::RegisterPool(const TSchedulerCompositeElementPtr& element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    RegisterPoolProfiler(element->GetId());
}

void TFairShareTreeProfiler::UnregisterPool(const TSchedulerCompositeElementPtr& element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto guard = WriterGuard(PoolNameToProfilingEntryLock_);

    GetOrCrash(PoolNameToProfilingEntry_, element->GetId()).RemoveTime = TInstant::Now();
}

void TFairShareTreeProfiler::ProfileElements(const TFairShareTreeSnapshotImplPtr& treeSnapshot)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

    PoolCountGauge_.Update(treeSnapshot->PoolMap().size());
    TotalElementCountGauge_.Update(treeSnapshot->RootElement()->GetTreeSize());
    SchedulableElementCountGauge_.Update(treeSnapshot->RootElement()->GetSchedulableElementCount());

    ProfileDistributedResources(treeSnapshot);

    // NB: We keep pool profiling entries in consistent with the main tree.
    PrepareOperationProfilingEntries(treeSnapshot);

    CleanupPoolProfilingEntries();

    ProfileOperations(treeSnapshot);
    ProfilePools(treeSnapshot);
}

void TFairShareTreeProfiler::PrepareOperationProfilingEntries(const TFairShareTreeSnapshotImplPtr& treeSnapshot)
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
            Registry_
                .WithRequiredTag("pool", profilingEntry.ParentPoolId, -1)
                .WithRequiredTag("slot_index", ToString(profilingEntry.SlotIndex), -1)
                .AddProducer("/operations_by_slot", profilingEntry.BufferedProducer);

            for (const auto& userProfilingTag : profilingEntry.UserProfilingTags) {
                auto userProfiler = Registry_
                    .WithTag("pool", userProfilingTag.PoolId, -1)
                    .WithRequiredTag("user_name", userProfilingTag.UserName, -1);

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
        JobMetricsMap_.erase(ToString(operationId));
    }
}

void TFairShareTreeProfiler::CleanupPoolProfilingEntries()
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
        YT_VERIFY(PoolNameToProfilingEntry_.erase(poolName));
        JobMetricsMap_.erase(poolName);
    }
}

void TFairShareTreeProfiler::RegisterPoolProfiler(const TString& poolName)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto writerGuard = WriterGuard(PoolNameToProfilingEntryLock_);

    auto it = PoolNameToProfilingEntry_.find(poolName);
    if (it != PoolNameToProfilingEntry_.end()) {
        auto& entry = it->second;
        entry.RemoveTime = std::nullopt;
        return;
    }

    auto poolRegistry = Registry_
        .WithRequiredTag("pool", poolName, -1);

    TUnregisterOperationCounters counters;
    counters.BannedCounter = poolRegistry.Counter("/pools/banned_operation_count");
    for (auto state : TEnumTraits<EOperationState>::GetDomainValues()) {
        if (IsOperationFinished(state)) {
            counters.FinishedCounters[state] = poolRegistry
                .WithTag("state", FormatEnum(state), -1)
                .Counter("/pools/finished_operation_count");
        }
    }

    auto insertResult = PoolNameToProfilingEntry_.emplace(
        poolName,
        TPoolProfilingEntry{std::move(counters), /* RemoveTime */ std::nullopt, New<NProfiling::TBufferedProducer>()});
    YT_VERIFY(insertResult.second);

    const auto& entry = insertResult.first->second;

    poolRegistry.AddProducer("/pools", entry.BufferedProducer);
}

void TFairShareTreeProfiler::ProfileElement(
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

    ProfileResources(writer, element->GetResourceUsageAtUpdate(), "/resource_usage");
    ProfileResources(writer, element->GetResourceLimits(), "/resource_limits");
    ProfileResources(writer, element->GetResourceDemand(), "/resource_demand");

    auto jobMetricsIt = JobMetricsMap_.find(element->GetId());
    if (jobMetricsIt != JobMetricsMap_.end()) {
        jobMetricsIt->second.Profile(writer);
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

void TFairShareTreeProfiler::ProfileOperations(const TFairShareTreeSnapshotImplPtr& treeSnapshot)
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
        GetOrCrash(OperationIdToProfilingEntry_, operationId).BufferedProducer->Update(std::move(buffer));
    }
}

void TFairShareTreeProfiler::ProfilePool(
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

    ProfileResources(&buffer, element->GetSpecifiedStrongGuaranteeResources(), "/strong_guarantee_resources");
    ProfileResources(&buffer, element->Attributes().EffectiveStrongGuaranteeResources, "/effective_strong_guarantee_resources");

    auto integralGuaranteesConfig = element->GetIntegralGuaranteesConfig();
    if (integralGuaranteesConfig->GuaranteeType != EIntegralGuaranteeType::None) {
        ProfileResources(&buffer, ToJobResources(integralGuaranteesConfig->ResourceFlow, {}), "/resource_flow");
        ProfileResources(&buffer, ToJobResources(integralGuaranteesConfig->BurstGuaranteeResources, {}), "/burst_guarantee_resources");
    }

    producer->Update(std::move(buffer));
}

void TFairShareTreeProfiler::ProfilePools(const TFairShareTreeSnapshotImplPtr& treeSnapshot)
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

void TFairShareTreeProfiler::ProfileDistributedResources(const TFairShareTreeSnapshotImplPtr& treeSnapshot)
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

void TFairShareTreeProfiler::ApplyJobMetricsDelta(
    const TFairShareTreeSnapshotImplPtr& treeSnapshot,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
