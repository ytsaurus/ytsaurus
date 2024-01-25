#include "fair_share_tree_allocation_scheduler_operation_shared_state.h"

namespace NYT::NScheduler {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

namespace {

// NB(eshcherbin): Used in situations where counter is only incremented by a single thread
// in order to avoid "lock add" on x86.
template <class T>
inline void IncrementAtomicCounterUnsafely(std::atomic<T>* counter)
{
    counter->store(counter->load() + 1, std::memory_order::release);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

static const TString InvalidCustomProfilingTag("invalid");

////////////////////////////////////////////////////////////////////////////////

static const TJobResourcesConfigPtr EmptyAllocationResourcesConfig = New<TJobResourcesConfig>();

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeAllocationSchedulerOperationSharedState::TFairShareTreeAllocationSchedulerOperationSharedState(
    ISchedulerStrategyHost* strategyHost,
    int updatePreemptibleAllocationsListLoggingPeriod,
    const NLogging::TLogger& logger)
    : StrategyHost_(strategyHost)
    , UpdatePreemptibleAllocationsListLoggingPeriod_(updatePreemptibleAllocationsListLoggingPeriod)
    , Logger(logger)
{ }

TJobResources TFairShareTreeAllocationSchedulerOperationSharedState::Disable()
{
    YT_LOG_DEBUG("Operation element disabled in strategy");

    auto guard = WriterGuard(AllocationPropertiesMapLock_);

    Enabled_ = false;

    TJobResources resourceUsage;
    for (const auto& [allocationId, properties] : AllocationPropertiesMap_) {
        resourceUsage += properties.ResourceUsage;
    }

    TotalDiskQuota_ = {};
    RunningAllocationCount_ = 0;
    AllocationPropertiesMap_.clear();
    for (auto preemptionStatus : TEnumTraits<EAllocationPreemptionStatus>::GetDomainValues()) {
        AllocationsPerPreemptionStatus_[preemptionStatus].clear();
        ResourceUsagePerPreemptionStatus_[preemptionStatus] = {};
    }

    return resourceUsage;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::Enable()
{
    YT_LOG_DEBUG("Operation element enabled in strategy");

    auto guard = WriterGuard(AllocationPropertiesMapLock_);

    YT_VERIFY(!Enabled_);
    Enabled_ = true;
}

bool TFairShareTreeAllocationSchedulerOperationSharedState::IsEnabled()
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);
    return Enabled_;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::RecordPackingHeartbeat(
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TFairShareStrategyPackingConfigPtr& packingConfig)
{
    HeartbeatStatistics_.RecordHeartbeat(heartbeatSnapshot, packingConfig);
}

bool TFairShareTreeAllocationSchedulerOperationSharedState::CheckPacking(
    const TSchedulerOperationElement* operationElement,
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TJobResourcesWithQuota& allocationResources,
    const TJobResources& totalResourceLimits,
    const TFairShareStrategyPackingConfigPtr& packingConfig)
{
    return HeartbeatStatistics_.CheckPacking(
        operationElement,
        heartbeatSnapshot,
        allocationResources,
        totalResourceLimits,
        packingConfig);
}

TJobResources TFairShareTreeAllocationSchedulerOperationSharedState::SetAllocationResourceUsage(
    TAllocationId allocationId,
    const TJobResources& resources)
{
    auto guard = WriterGuard(AllocationPropertiesMapLock_);

    if (!Enabled_) {
        return {};
    }

    return SetAllocationResourceUsage(GetAllocationProperties(allocationId), resources);
}

TDiskQuota TFairShareTreeAllocationSchedulerOperationSharedState::GetTotalDiskQuota() const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);
    return TotalDiskQuota_;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::PublishFairShare(const TResourceVector& fairShare)
{
    FairShare_.Store(fairShare);
}

bool TFairShareTreeAllocationSchedulerOperationSharedState::OnAllocationStarted(
    TSchedulerOperationElement* operationElement,
    TAllocationId allocationId,
    const TJobResourcesWithQuota& resourceUsage,
    const TJobResources& precommitedResources,
    TControllerEpoch scheduleAllocationEpoch,
    bool force)
{
    YT_ELEMENT_LOG_DETAILED(operationElement, "Adding allocation to strategy (AllocationId: %v)", allocationId);

    if (!force && (!IsEnabled() || operationElement->GetControllerEpoch() != scheduleAllocationEpoch)) {
        return false;
    }

    AddAllocation(allocationId, resourceUsage);
    operationElement->CommitHierarchicalResourceUsage(resourceUsage, precommitedResources);
    UpdatePreemptibleAllocationsList(operationElement);

    return true;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::OnAllocationFinished(
    TSchedulerOperationElement* operationElement,
    TAllocationId allocationId)
{
    YT_ELEMENT_LOG_DETAILED(operationElement, "Removing allocation from strategy (AllocationId: %v)", allocationId);

    if (auto delta = RemoveAllocation(allocationId)) {
        operationElement->IncreaseHierarchicalResourceUsage(-(*delta));
        UpdatePreemptibleAllocationsList(operationElement);
    }
}

void TFairShareTreeAllocationSchedulerOperationSharedState::UpdatePreemptibleAllocationsList(const TSchedulerOperationElement* element)
{
    TWallTimer timer;

    int moveCount = 0;
    DoUpdatePreemptibleAllocationsList(element, &moveCount);

    auto elapsed = timer.GetElapsedTime();
    YT_LOG_DEBUG_IF(elapsed > element->TreeConfig()->UpdatePreemptibleListDurationLoggingThreshold,
        "Preemptible list update is too long (Duration: %v, MoveCount: %v)",
        elapsed.MilliSeconds(),
        moveCount);
}

void TFairShareTreeAllocationSchedulerOperationSharedState::DoUpdatePreemptibleAllocationsList(const TSchedulerOperationElement* element, int* moveCount)
{
    auto convertToShare = [&] (const TJobResources& allocationResources) -> TResourceVector {
        return TResourceVector::FromJobResources(allocationResources, element->GetTotalResourceLimits());
    };

    // NB(eshcherbin): It's possible to incorporate |resourceUsageBound| into |fairShareBound|,
    // but I think it's more explicit the current way.
    auto isUsageBelowBounds = [&] (
        const TJobResources& resourceUsage,
        const TJobResources& resourceUsageBound,
        const TResourceVector& fairShareBound) -> bool
    {
        return StrictlyDominates(resourceUsageBound, resourceUsage) ||
            element->IsStrictlyDominatesNonBlocked(fairShareBound, convertToShare(resourceUsage));
    };

    auto balanceLists = [&] (
        TAllocationIdList* left,
        TAllocationIdList* right,
        TJobResources resourceUsage,
        const TJobResources& resourceUsageBound,
        const TResourceVector& fairShareBound,
        const std::function<void(TAllocationProperties*)>& onMovedLeftToRight,
        const std::function<void(TAllocationProperties*)>& onMovedRightToLeft)
    {
        auto initialResourceUsage = resourceUsage;

        // Move from left to right and decrease |resourceUsage| until the next move causes
        // |operationElement->IsStrictlyDominatesNonBlocked(fairShareBound, getUsageShare(nextUsage))| to become true.
        // In particular, even if fair share is slightly less than it should be due to precision errors,
        // we expect no problems, because the allocation which crosses the fair share boundary belongs to the left list.
        while (!left->empty()) {
            auto allocationId = left->back();
            auto* allocationProperties = GetAllocationProperties(allocationId);

            auto nextUsage = resourceUsage - allocationProperties->ResourceUsage;
            if (isUsageBelowBounds(nextUsage, resourceUsageBound, fairShareBound)) {
                break;
            }

            left->pop_back();
            right->push_front(allocationId);
            allocationProperties->AllocationIdListIterator = right->begin();
            onMovedLeftToRight(allocationProperties);

            resourceUsage = nextUsage;
            ++(*moveCount);
        }

        // Move from right to left and increase |resourceUsage|.
        while (!right->empty() && isUsageBelowBounds(resourceUsage, resourceUsageBound, fairShareBound)) {
            auto allocationId = right->front();
            auto* allocationProperties = GetAllocationProperties(allocationId);

            right->pop_front();
            left->push_back(allocationId);
            allocationProperties->AllocationIdListIterator = --left->end();
            onMovedRightToLeft(allocationProperties);

            resourceUsage += allocationProperties->ResourceUsage;
            ++(*moveCount);
        }

        return resourceUsage - initialResourceUsage;
    };

    auto setPreemptible = [] (TAllocationProperties* properties) {
        properties->PreemptionStatus = EAllocationPreemptionStatus::Preemptible;
    };

    auto setAggressivelyPreemptible = [] (TAllocationProperties* properties) {
        properties->PreemptionStatus = EAllocationPreemptionStatus::AggressivelyPreemptible;
    };

    auto setNonPreemptible = [] (TAllocationProperties* properties) {
        properties->PreemptionStatus = EAllocationPreemptionStatus::NonPreemptible;
    };

    auto guard = WriterGuard(AllocationPropertiesMapLock_);

    bool enableLogging =
        (UpdatePreemptibleAllocationsListCount_.fetch_add(1) % UpdatePreemptibleAllocationsListLoggingPeriod_) == 0 ||
            element->AreDetailedLogsEnabled();

    auto fairShare = FairShare_.Load();
    auto preemptionSatisfactionThreshold = element->TreeConfig()->PreemptionSatisfactionThreshold;
    auto aggressivePreemptionSatisfactionThreshold = element->TreeConfig()->AggressivePreemptionSatisfactionThreshold;

    // NB: |element->EffectiveNonPreemptibleResourceUsageThresholdConfig()| can be null during revived allocations registration.
    // We don't care about this, because the next fair share update will bring correct config.
    const auto& nonPreemptibleResourceUsageConfig = element->EffectiveNonPreemptibleResourceUsageThresholdConfig()
        ? element->EffectiveNonPreemptibleResourceUsageThresholdConfig()
        : EmptyAllocationResourcesConfig;
    auto nonPreemptibleResourceUsageThreshold = nonPreemptibleResourceUsageConfig->IsNonTrivial()
        ? ToJobResources(nonPreemptibleResourceUsageConfig, TJobResources::Infinite())
        : TJobResources();

    YT_LOG_DEBUG_IF(
        enableLogging,
        "Update preemptible lists inputs (FairShare: %.6g, TotalResourceLimits: %v, NonPreemptibleResourceUsageConfig: %v, "
        "PreemptionSatisfactionThreshold: %v, AggressivePreemptionSatisfactionThreshold: %v)",
        fairShare,
        FormatResources(element->GetTotalResourceLimits()),
        FormatResourcesConfig(nonPreemptibleResourceUsageConfig),
        preemptionSatisfactionThreshold,
        aggressivePreemptionSatisfactionThreshold);

    // NB: We need 2 iterations since thresholds may change significantly such that we need
    // to move allocation from preemptible list to non-preemptible list through aggressively preemptible list.
    for (int iteration = 0; iteration < 2; ++iteration) {
        YT_LOG_DEBUG_IF(enableLogging,
            "Preemptible lists usage bounds before update "
            "(NonPreemptibleResourceUsage: %v, AggressivelyPreemptibleResourceUsage: %v, PreemptibleResourceUsage: %v, Iteration: %v)",
            FormatResources(ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::NonPreemptible]),
            FormatResources(ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::AggressivelyPreemptible]),
            FormatResources(ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::Preemptible]),
            iteration);

        {
            auto usageDelta = balanceLists(
                &AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::NonPreemptible],
                &AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::AggressivelyPreemptible],
                ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::NonPreemptible],
                nonPreemptibleResourceUsageThreshold,
                fairShare * aggressivePreemptionSatisfactionThreshold,
                setAggressivelyPreemptible,
                setNonPreemptible);
            ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::NonPreemptible] += usageDelta;
            ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::AggressivelyPreemptible] -= usageDelta;
        }

        {
            auto usageDelta = balanceLists(
                &AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::AggressivelyPreemptible],
                &AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::Preemptible],
                ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::NonPreemptible] +
                    ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::AggressivelyPreemptible],
                nonPreemptibleResourceUsageThreshold,
                Preemptible_ ? fairShare * preemptionSatisfactionThreshold : TResourceVector::Infinity(),
                setPreemptible,
                setAggressivelyPreemptible);
            ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::AggressivelyPreemptible] += usageDelta;
            ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::Preemptible] -= usageDelta;
        }
    }

    YT_LOG_DEBUG_IF(
        enableLogging,
        "Preemptible lists usage bounds after update "
        "(NonPreemptibleResourceUsage: %v, AggressivelyPreemptibleResourceUsage: %v, PreemptibleResourceUsage: %v)",
        FormatResources(ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::NonPreemptible]),
        FormatResources(ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::AggressivelyPreemptible]),
        FormatResources(ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::Preemptible]));
}

void TFairShareTreeAllocationSchedulerOperationSharedState::SetPreemptible(bool value)
{
    bool oldValue = Preemptible_;
    if (oldValue != value) {
        YT_LOG_DEBUG("Preemptible status changed (OldValue: %v, NewValue: %v)", oldValue, value);

        Preemptible_ = value;
    }
}

bool TFairShareTreeAllocationSchedulerOperationSharedState::GetPreemptible() const
{
    return Preemptible_;
}

bool TFairShareTreeAllocationSchedulerOperationSharedState::IsAllocationKnown(TAllocationId allocationId) const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    return AllocationPropertiesMap_.find(allocationId) != AllocationPropertiesMap_.end();
}

EAllocationPreemptionStatus TFairShareTreeAllocationSchedulerOperationSharedState::GetAllocationPreemptionStatus(TAllocationId allocationId) const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    if (!Enabled_) {
        return EAllocationPreemptionStatus::NonPreemptible;
    }

    return GetAllocationProperties(allocationId)->PreemptionStatus;
}

int TFairShareTreeAllocationSchedulerOperationSharedState::GetRunningAllocationCount() const
{
    return RunningAllocationCount_;
}

int TFairShareTreeAllocationSchedulerOperationSharedState::GetPreemptibleAllocationCount() const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    return AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::Preemptible].size();
}

int TFairShareTreeAllocationSchedulerOperationSharedState::GetAggressivelyPreemptibleAllocationCount() const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    return AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::AggressivelyPreemptible].size();
}

void TFairShareTreeAllocationSchedulerOperationSharedState::AddAllocation(
    TAllocationId allocationId,
    const TJobResourcesWithQuota& resourceUsage)
{
    auto guard = WriterGuard(AllocationPropertiesMapLock_);

    LastScheduleAllocationSuccessTime_ = TInstant::Now();

    auto& preemptibleAllocations = AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::Preemptible];
    preemptibleAllocations.push_back(allocationId);

    auto it = EmplaceOrCrash(
        AllocationPropertiesMap_,
        allocationId,
        TAllocationProperties{
            .PreemptionStatus = EAllocationPreemptionStatus::Preemptible,
            .AllocationIdListIterator = --preemptibleAllocations.end(),
            .ResourceUsage = {},
            .DiskQuota = resourceUsage.DiskQuota(),
        });

    ++RunningAllocationCount_;

    SetAllocationResourceUsage(&it->second, resourceUsage.ToJobResources());

    TotalDiskQuota_ += resourceUsage.DiskQuota();
}

std::optional<TJobResources> TFairShareTreeAllocationSchedulerOperationSharedState::RemoveAllocation(TAllocationId allocationId)
{
    auto guard = WriterGuard(AllocationPropertiesMapLock_);

    if (!Enabled_) {
        return std::nullopt;
    }

    auto it = GetIteratorOrCrash(AllocationPropertiesMap_, allocationId);

    auto* properties = &it->second;
    AllocationsPerPreemptionStatus_[properties->PreemptionStatus].erase(properties->AllocationIdListIterator);

    --RunningAllocationCount_;

    auto resourceUsage = properties->ResourceUsage;
    SetAllocationResourceUsage(properties, TJobResources());

    TotalDiskQuota_ -= properties->DiskQuota;

    AllocationPropertiesMap_.erase(it);

    return resourceUsage;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::UpdatePreemptionStatusStatistics(EOperationPreemptionStatus status)
{
    auto guard = Guard(PreemptionStatusStatisticsLock_);

    ++PreemptionStatusStatistics_[status];
}

TPreemptionStatusStatisticsVector TFairShareTreeAllocationSchedulerOperationSharedState::GetPreemptionStatusStatistics() const
{
    auto guard = Guard(PreemptionStatusStatisticsLock_);

    return PreemptionStatusStatistics_;
}

TAllocationPreemptionStatusMap TFairShareTreeAllocationSchedulerOperationSharedState::GetAllocationPreemptionStatusMap() const
{
    TAllocationPreemptionStatusMap allocationPreemptionStatuses;

    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    allocationPreemptionStatuses.reserve(AllocationPropertiesMap_.size());
    for (const auto& [allocationId, properties] : AllocationPropertiesMap_) {
        EmplaceOrCrash(allocationPreemptionStatuses, allocationId, properties.PreemptionStatus);
    }

    return allocationPreemptionStatuses;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::OnMinNeededResourcesUnsatisfied(
    const ISchedulingContextPtr& schedulingContext,
    const TJobResources& availableResources,
    const TJobResources& minNeededResources)
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];
#define XX(name, Name) \
        if (availableResources.Get##Name() < minNeededResources.Get##Name()) { \
            IncrementAtomicCounterUnsafely(&shard.MinNeededResourcesUnsatisfiedCount[EJobResourceType::Name]); \
        }
    ITERATE_JOB_RESOURCES(XX)
#undef XX
}

TEnumIndexedArray<EJobResourceType, int> TFairShareTreeAllocationSchedulerOperationSharedState::GetMinNeededResourcesUnsatisfiedCount()
{
    UpdateDiagnosticCounters();

    return MinNeededResourcesUnsatisfiedCount_;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::OnOperationDeactivated(const ISchedulingContextPtr& schedulingContext, EDeactivationReason reason)
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];
    IncrementAtomicCounterUnsafely(&shard.DeactivationReasons[reason]);
    IncrementAtomicCounterUnsafely(&shard.DeactivationReasonsFromLastNonStarvingTime[reason]);
}

TEnumIndexedArray<EDeactivationReason, int> TFairShareTreeAllocationSchedulerOperationSharedState::GetDeactivationReasons()
{
    UpdateDiagnosticCounters();

    return DeactivationReasons_;
}

TEnumIndexedArray<EDeactivationReason, int> TFairShareTreeAllocationSchedulerOperationSharedState::GetDeactivationReasonsFromLastNonStarvingTime()
{
    UpdateDiagnosticCounters();

    return DeactivationReasonsFromLastNonStarvingTime_;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::IncrementOperationScheduleAllocationAttemptCount(const ISchedulingContextPtr& schedulingContext)
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];

    IncrementAtomicCounterUnsafely(&shard.ScheduleAllocationAttemptCount);
}

int TFairShareTreeAllocationSchedulerOperationSharedState::GetOperationScheduleAllocationAttemptCount()
{
    UpdateDiagnosticCounters();

    return ScheduleAllocationAttemptCount_;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::ProcessUpdatedStarvationStatus(EStarvationStatus status)
{
    if (StarvationStatusAtLastUpdate_ == EStarvationStatus::NonStarving && status != EStarvationStatus::NonStarving) {
        std::fill(DeactivationReasonsFromLastNonStarvingTime_.begin(), DeactivationReasonsFromLastNonStarvingTime_.end(), 0);

        int shardId = 0;
        for (const auto& invoker : StrategyHost_->GetNodeShardInvokers()) {
            invoker->Invoke(BIND([this, this_=MakeStrong(this), shardId] {
                auto& shard = StateShards_[shardId];
                for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
                    shard.DeactivationReasonsFromLastNonStarvingTime[reason].store(0, std::memory_order::release);
                }
            }));

            ++shardId;
        }
    }

    StarvationStatusAtLastUpdate_ = status;
}

void TFairShareTreeAllocationSchedulerOperationSharedState::UpdateDiagnosticCounters()
{
    auto now = TInstant::Now();
    if (now < LastDiagnosticCountersUpdateTime_ + UpdateStateShardsBackoff_) {
        return;
    }

    std::fill(DeactivationReasons_.begin(), DeactivationReasons_.end(), 0);
    std::fill(DeactivationReasonsFromLastNonStarvingTime_.begin(), DeactivationReasonsFromLastNonStarvingTime_.end(), 0);
    std::fill(MinNeededResourcesUnsatisfiedCount_.begin(), MinNeededResourcesUnsatisfiedCount_.end(), 0);
    i64 scheduleAllocationAttemptCount = 0;

    for (int shardId = 0; shardId < std::ssize(StrategyHost_->GetNodeShardInvokers()); ++shardId) {
        auto& shard = StateShards_[shardId];
        for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
            DeactivationReasons_[reason] += shard.DeactivationReasons[reason].load();
            DeactivationReasonsFromLastNonStarvingTime_[reason] +=
                shard.DeactivationReasonsFromLastNonStarvingTime[reason].load();
        }
        for (auto resource : TEnumTraits<EJobResourceType>::GetDomainValues()) {
            MinNeededResourcesUnsatisfiedCount_[resource] +=
                shard.MinNeededResourcesUnsatisfiedCount[resource].load();
        }
        scheduleAllocationAttemptCount += shard.ScheduleAllocationAttemptCount;
    }

    ScheduleAllocationAttemptCount_ = scheduleAllocationAttemptCount;
    LastDiagnosticCountersUpdateTime_ = now;
}

TInstant TFairShareTreeAllocationSchedulerOperationSharedState::GetLastScheduleAllocationSuccessTime() const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    return LastScheduleAllocationSuccessTime_;
}

std::optional<TString> TSchedulerOperationElement::GetCustomProfilingTag() const
{
    auto tagName = Spec_->CustomProfilingTag;
    if (!tagName) {
        return {};
    }

    if (!GetParent()) {
        return {};
    }

    THashSet<TString> allowedProfilingTags;
    const auto* parent = GetParent();
    while (parent) {
        for (const auto& tag : parent->GetAllowedProfilingTags()) {
            allowedProfilingTags.insert(tag);
        }
        parent = parent->GetParent();
    }

    if (allowedProfilingTags.find(*tagName) == allowedProfilingTags.end() ||
        (TreeConfig_->CustomProfilingTagFilter &&
            NRe2::TRe2::FullMatch(NRe2::StringPiece(*tagName), *TreeConfig_->CustomProfilingTagFilter)))
    {
        tagName = InvalidCustomProfilingTag;
    }

    return tagName;
}

TJobResources TFairShareTreeAllocationSchedulerOperationSharedState::SetAllocationResourceUsage(
    TAllocationProperties* properties,
    const TJobResources& resources)
{
    auto delta = resources - properties->ResourceUsage;
    properties->ResourceUsage = resources;
    ResourceUsagePerPreemptionStatus_[properties->PreemptionStatus] += delta;

    return delta;
}

TFairShareTreeAllocationSchedulerOperationSharedState::TAllocationProperties*
TFairShareTreeAllocationSchedulerOperationSharedState::GetAllocationProperties(TAllocationId allocationId)
{
    auto it = AllocationPropertiesMap_.find(allocationId);
    YT_ASSERT(it != AllocationPropertiesMap_.end());
    return &it->second;
}

const TFairShareTreeAllocationSchedulerOperationSharedState::TAllocationProperties*
TFairShareTreeAllocationSchedulerOperationSharedState::GetAllocationProperties(TAllocationId allocationId) const
{
    auto it = AllocationPropertiesMap_.find(allocationId);
    YT_ASSERT(it != AllocationPropertiesMap_.end());
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
