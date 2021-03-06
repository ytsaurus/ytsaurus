#include "fair_share_tree_profiling.h"

namespace NYT::NScheduler {

using namespace NProfiling;

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
{
    VERIFY_THREAD_AFFINITY(ControlThread);
}

NProfiling::TProfiler TFairShareTreeProfiler::GetRegistry() const
{
    return Registry_;
}

void TFairShareTreeProfiler::ProfileOperationUnregistration(const TCompositeSchedulerElement* pool, EOperationState state)
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

void TFairShareTreeProfiler::RegisterPool(const TCompositeSchedulerElementPtr& element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    RegisterPoolProfiler(element->GetId());
}

void TFairShareTreeProfiler::UnregisterPool(const TCompositeSchedulerElementPtr& element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
        
    auto guard = WriterGuard(PoolNameToProfilingEntryLock_);

    GetOrCrash(PoolNameToProfilingEntry_, element->GetId()).RemoveTime = TInstant::Now();
}

void TFairShareTreeProfiler::ProfileElements(const TFairShareTreeSnapshotImplPtr& treeSnapshot)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

    PoolCountGauge_.Update(treeSnapshot->PoolMap().size());

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
                    const auto* pool = static_cast<const TPool*>(parent);
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
        counters.FinishedCounters[state] = poolRegistry
            .WithTag("state", FormatEnum(state), -1)
            .Counter("/pools/finished_operation_count");
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
    const TFairShareStrategyTreeConfigPtr& treeConfig,
    bool profilingCompatibilityEnabled)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

    const auto& attributes = element->Attributes();

    const auto& detailedFairShare = attributes.FairShare;
    if (profilingCompatibilityEnabled) {
        writer->AddGauge("/fair_share_ratio_x100000", static_cast<i64>(MaxComponent(attributes.FairShare.Total) * 1e5));
        writer->AddGauge("/usage_ratio_x100000", static_cast<i64>(element->GetResourceDominantUsageShareAtUpdate() * 1e5));
        writer->AddGauge("/demand_ratio_x100000", static_cast<i64>(MaxComponent(attributes.DemandShare) * 1e5));
        writer->AddGauge("/unlimited_demand_fair_share_ratio_x100000", static_cast<i64>(MaxComponent(attributes.PromisedFairShare) * 1e5));
        writer->AddGauge("/accumulated_resource_ratio_volume_x100000", static_cast<i64>(element->GetAccumulatedResourceRatioVolume() * 1e5));
        writer->AddGauge("/min_share_guarantee_ratio_x100000", static_cast<i64>(MaxComponent(detailedFairShare.StrongGuarantee) * 1e5));
        writer->AddGauge("/integral_guarantee_ratio_x100000", static_cast<i64>(MaxComponent(detailedFairShare.IntegralGuarantee) * 1e5));
        writer->AddGauge("/weight_proportional_ratio_x100000", static_cast<i64>(MaxComponent(detailedFairShare.WeightProportional) * 1e5));
    } else {
        writer->AddGauge("/dominant_fair_share", MaxComponent(attributes.FairShare.Total));
        writer->AddGauge("/dominant_usage_share", element->GetResourceDominantUsageShareAtUpdate());
        writer->AddGauge("/dominant_demand_share", MaxComponent(attributes.DemandShare));
        writer->AddGauge("/promised_dominant_fair_share", MaxComponent(attributes.PromisedFairShare));
        writer->AddGauge("/accumulated_volume_dominant_share", element->GetAccumulatedResourceRatioVolume());
        writer->AddGauge("/dominant_fair_share/strong_guarantee", MaxComponent(detailedFairShare.StrongGuarantee));
        writer->AddGauge("/dominant_fair_share/integral_guarantee", MaxComponent(detailedFairShare.IntegralGuarantee));
        writer->AddGauge("/dominant_fair_share/weight_proportional", MaxComponent(detailedFairShare.WeightProportional));
        writer->AddGauge("/dominant_fair_share/total", MaxComponent(detailedFairShare.Total));
    }

    ProfileResources(writer, element->ResourceUsageAtUpdate(), "/resource_usage");
    ProfileResources(writer, element->ResourceLimits(), "/resource_limits");
    ProfileResources(writer, element->ResourceDemand(), "/resource_demand");

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
            "/fair_share/strong_guarantee",
            profilingCompatibilityEnabled);
        ProfileResourceVector(
            writer,
            profiledResources,
            detailedFairShare.IntegralGuarantee,
            "/fair_share/integral_guarantee",
            profilingCompatibilityEnabled);
        ProfileResourceVector(
            writer,
            profiledResources,
            detailedFairShare.WeightProportional,
            "/fair_share/weight_proportional",
            profilingCompatibilityEnabled);
        ProfileResourceVector(
            writer,
            profiledResources,
            detailedFairShare.Total,
            "/fair_share/total",
            profilingCompatibilityEnabled);

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.UsageShare,
            "/usage_share",
            profilingCompatibilityEnabled);

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.DemandShare,
            "/demand_share",
            profilingCompatibilityEnabled);

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.LimitsShare,
            "/limits_share",
            profilingCompatibilityEnabled);

        if (profilingCompatibilityEnabled) {
            ProfileResourceVector(
                writer,
                profiledResources,
                attributes.StrongGuaranteeShare,
                "/min_share",
                profilingCompatibilityEnabled);
        } else {
            ProfileResourceVector(
                writer,
                profiledResources,
                attributes.StrongGuaranteeShare,
                "/strong_guarantee_share",
                profilingCompatibilityEnabled);
        }

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.ProposedIntegralShare,
            "/proposed_integral_share",
            profilingCompatibilityEnabled);

        ProfileResourceVector(
            writer,
            profiledResources,
            attributes.PromisedFairShare,
            "/promised_fair_share",
            profilingCompatibilityEnabled);

        if (!element->IsOperation()) {
            ProfileResourceVolume(
                writer,
                element->GetAccumulatedResourceVolume(),
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
            treeSnapshot->TreeConfig(),
            treeSnapshot->GetCoreProfilingCompatibilityEnabled());
        GetOrCrash(OperationIdToProfilingEntry_, operationId).BufferedProducer->Update(std::move(buffer));
    }
}

void TFairShareTreeProfiler::ProfilePool(
    const TCompositeSchedulerElement* element,
    const TFairShareStrategyTreeConfigPtr& treeConfig,
    bool profilingCompatibilityEnabled,
    const NProfiling::TBufferedProducerPtr& producer)
{
    VERIFY_INVOKER_AFFINITY(ProfilingInvoker_);

    TSensorBuffer buffer;
    ProfileElement(
        &buffer,
        element,
        treeConfig,
        profilingCompatibilityEnabled);
    buffer.AddGauge("/max_operation_count", element->GetMaxOperationCount());
    buffer.AddGauge("/max_running_operation_count", element->GetMaxRunningOperationCount());
    buffer.AddGauge("/running_operation_count", element->RunningOperationCount());
    buffer.AddGauge("/total_operation_count", element->OperationCount());
    if (profilingCompatibilityEnabled) {
        ProfileResources(&buffer, element->GetSpecifiedStrongGuaranteeResources(), "/min_share_resources");
        ProfileResources(&buffer, element->Attributes().EffectiveStrongGuaranteeResources, "/effective_min_share_resources");
    } else {
        ProfileResources(&buffer, element->GetSpecifiedStrongGuaranteeResources(), "/strong_guarantee_resources");
        ProfileResources(&buffer, element->Attributes().EffectiveStrongGuaranteeResources, "/effective_strong_guarantee_resources");
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
            treeSnapshot->GetCoreProfilingCompatibilityEnabled(),
            entry);
    }

    ProfilePool(
        treeSnapshot->RootElement().Get(),
        treeSnapshot->TreeConfig(),
        treeSnapshot->GetCoreProfilingCompatibilityEnabled(),
        findPoolBufferedProducer(RootPoolName));
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
