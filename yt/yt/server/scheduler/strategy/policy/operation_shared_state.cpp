#include "operation_shared_state.h"

#include "scheduling_heartbeat_context.h"

namespace NYT::NScheduler::NStrategy::NPolicy {

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

static const TJobResourcesConfigPtr EmptyAllocationResourcesConfig = New<TJobResourcesConfig>();

////////////////////////////////////////////////////////////////////////////////

TOperationSharedState::TOperationSharedState(
    IStrategyHost* strategyHost,
    int updatePreemptibleAllocationsListLoggingPeriod,
    const NLogging::TLogger& logger)
    : StrategyHost_(strategyHost)
    , UpdatePreemptibleAllocationsListLoggingPeriod_(updatePreemptibleAllocationsListLoggingPeriod)
    , Logger(logger)
{ }

TJobResources TOperationSharedState::Disable()
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

void TOperationSharedState::Enable()
{
    YT_LOG_DEBUG("Operation element enabled in strategy");

    auto guard = WriterGuard(AllocationPropertiesMapLock_);

    YT_VERIFY(!Enabled_);
    Enabled_ = true;
}

bool TOperationSharedState::IsEnabled()
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);
    return Enabled_;
}

void TOperationSharedState::RecordPackingHeartbeat(
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TStrategyPackingConfigPtr& packingConfig)
{
    HeartbeatStatistics_.RecordHeartbeat(heartbeatSnapshot, packingConfig);
}

bool TOperationSharedState::CheckPacking(
    const TPoolTreeOperationElement* operationElement,
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TJobResourcesWithQuota& allocationResources,
    const TJobResources& totalResourceLimits,
    const TStrategyPackingConfigPtr& packingConfig)
{
    return HeartbeatStatistics_.CheckPacking(
        operationElement,
        heartbeatSnapshot,
        allocationResources,
        totalResourceLimits,
        packingConfig);
}

bool TOperationSharedState::ProcessAllocationUpdate(
    TPoolTreeOperationElement* operationElement,
    TAllocationId allocationId,
    const TJobResources& resources,
    bool resetPreemptibleProgress)
{
    if (!IsEnabled()) {
        return false;
    }

    auto delta = [&] {
        auto guard = WriterGuard(AllocationPropertiesMapLock_);

        return SetAllocationResourceUsage(
            GetAllocationProperties(allocationId),
            resources);
    }();

    if (delta != TJobResources()) {
        operationElement->IncreaseHierarchicalResourceUsage(delta);
    }

    if (resetPreemptibleProgress) {
        ResetAllocationPreemptibleProgress(operationElement, allocationId);
    }

    if (delta != TJobResources() || resetPreemptibleProgress) {
        UpdatePreemptibleAllocationsList(operationElement);
    }

    return true;
}

TDiskQuota TOperationSharedState::GetTotalDiskQuota() const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);
    return TotalDiskQuota_;
}

void TOperationSharedState::PublishFairShare(const TResourceVector& fairShare)
{
    FairShare_.Store(fairShare);
}

bool TOperationSharedState::OnAllocationStarted(
    TPoolTreeOperationElement* operationElement,
    TAllocationId allocationId,
    const TJobResourcesWithQuota& resourceUsage,
    const TJobResources& precommittedResources,
    TControllerEpoch scheduleAllocationEpoch,
    bool force)
{
    YT_ELEMENT_LOG_DETAILED(operationElement, "Adding allocation to strategy (AllocationId: %v)", allocationId);

    if (!force && (!IsEnabled() || operationElement->GetControllerEpoch() != scheduleAllocationEpoch)) {
        return false;
    }

    AddAllocation(allocationId, resourceUsage);
    operationElement->CommitHierarchicalResourceUsage(resourceUsage, precommittedResources);

    UpdatePreemptibleAllocationsList(operationElement);

    return true;
}

bool TOperationSharedState::OnAllocationFinished(
    TPoolTreeOperationElement* operationElement,
    TAllocationId allocationId)
{
    YT_ELEMENT_LOG_DETAILED(operationElement, "Removing allocation from strategy (AllocationId: %v)", allocationId);

    if (auto delta = RemoveAllocation(allocationId)) {
        operationElement->IncreaseHierarchicalResourceUsage(-(*delta));
        UpdatePreemptibleAllocationsList(operationElement);
        return true;
    }

    return false;
}

void TOperationSharedState::ResetAllocationPreemptibleProgress(
    TPoolTreeOperationElement* operationElement,
    TAllocationId allocationId)
{
    YT_ELEMENT_LOG_DETAILED(operationElement, "Resetting preemptible allocation progress (AllocationId: %v)", allocationId);

    auto guard = WriterGuard(AllocationPropertiesMapLock_);

    auto* properties = GetAllocationProperties(allocationId);
    auto& listToInsert = AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::Preemptible];
    listToInsert.splice(
        end(listToInsert),
        AllocationsPerPreemptionStatus_[properties->PreemptionStatus],
        properties->AllocationIdListIterator);

    properties->AllocationIdListIterator = --end(listToInsert);

    ResourceUsagePerPreemptionStatus_[properties->PreemptionStatus] -= properties->ResourceUsage;
    ResourceUsagePerPreemptionStatus_[EAllocationPreemptionStatus::Preemptible] += properties->ResourceUsage;
    properties->PreemptionStatus = EAllocationPreemptionStatus::Preemptible;
}

void TOperationSharedState::UpdatePreemptibleAllocationsList(
    const TPoolTreeOperationElement* element)
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

void TOperationSharedState::DoUpdatePreemptibleAllocationsList(const TPoolTreeOperationElement* element, int* moveCount)
{
    auto convertToShare = [&] (const TJobResources& allocationResources) -> TResourceVector {
        return TResourceVector::FromJobResources(allocationResources, element->GetTotalResourceLimits());
    };

    auto isUsageNearBound = [&] (
        const TJobResources& resourceUsage,
        const TResourceVector& fairShareBound) -> bool
    {
        auto usageShare = convertToShare(resourceUsage);
        return TResourceVector::Near(fairShareBound, usageShare, NVectorHdrf::Epsilon);
    };

    // NB(eshcherbin): It's possible to incorporate |resourceUsageBound| into |fairShareBound|,
    // but I think it's more explicit the current way.
    auto isUsageBelowBounds = [&] (
        const TJobResources& resourceUsage,
        const TJobResources& resourceUsageBound,
        const TResourceVector& fairShareBound) -> bool
    {
        auto usageShare = convertToShare(resourceUsage);

        // TODO(yaishenka): Reconsider how IsStrictlyDominatesNonBlocked function works.
        return StrictlyDominates(resourceUsageBound, resourceUsage) ||
            element->IsStrictlyDominatesNonBlocked(fairShareBound, usageShare);
    };

    auto balanceLists = [&] (
        TAllocationIdList* left,
        TAllocationIdList* right,
        TJobResources resourceUsage,
        const TJobResources& resourceUsageBound,
        const TResourceVector& fairShareBound,
        const CInvocable<void(TAllocationProperties*)> auto& onMovedLeftToRight,
        const CInvocable<void(TAllocationProperties*)> auto& onMovedRightToLeft,
        bool moveAllocationsWithOnBoundaryUsage = false)
    {
        auto initialResourceUsage = resourceUsage;

        // Move from left to right and decrease |resourceUsage| until the next move causes
        // |operationElement->IsStrictlyDominatesNonBlocked(fairShareBound, getUsageShare(nextUsage))| to become true.
        // In particular, even if fair share is slightly less than it should be due to precision errors,
        // we expect no problems, because the allocation which crosses the fair share boundary belongs to the left list.
        // NB(yaishenka): With moveAllocationsWithOnBoundaryUsage option allocations that cross the boundry will be moved.
        // Allocations with usage ~ boundry will be considered less preemptible due to near check in comparison func.
        // We expect no problems with precision errors, because we explicity check for |near(fairShareBound, resourceUsage)| case.
        while (!left->empty()) {
            auto allocationId = left->back();
            auto* allocationProperties = GetAllocationProperties(allocationId);

            auto nextUsage = resourceUsage - allocationProperties->ResourceUsage;

            const auto& resourceUsageToCheck = moveAllocationsWithOnBoundaryUsage
                ? resourceUsage
                : nextUsage;

            bool isBelowBounds = isUsageBelowBounds(
                resourceUsageToCheck,
                resourceUsageBound,
                fairShareBound);

            if (isBelowBounds) {
                break;
            }

            // NB(yaishenka): with new flag we should not move allocation with fairShareBound ~= usageShareToCheck.
            if (moveAllocationsWithOnBoundaryUsage && isUsageNearBound(resourceUsageToCheck, fairShareBound)) {
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
        while (!right->empty()) {
            auto allocationId = right->front();
            auto* allocationProperties = GetAllocationProperties(allocationId);

            auto nextUsage = resourceUsage + allocationProperties->ResourceUsage;

            const auto& resourceUsageToCheck = moveAllocationsWithOnBoundaryUsage
                ? nextUsage
                : resourceUsage;

            bool belowBounds = isUsageBelowBounds(
                resourceUsageToCheck,
                resourceUsageBound,
                fairShareBound);

            // NB(yaishenka) with new flag we should move allocation with fairShareBound ~= usageShareToCheck.
            if (!belowBounds) {
                if (!moveAllocationsWithOnBoundaryUsage || !isUsageNearBound(resourceUsageToCheck, fairShareBound)) {
                    break;
                }
            }

            right->pop_front();
            left->push_back(allocationId);
            allocationProperties->AllocationIdListIterator = --left->end();
            onMovedRightToLeft(allocationProperties);

            resourceUsage = nextUsage;
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
        YT_LOG_DEBUG_IF(
            enableLogging,
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
                setNonPreemptible,
                element->TreeConfig()->ConsiderAllocationOnFairShareBoundPreemptible);
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
                setAggressivelyPreemptible,
                element->TreeConfig()->ConsiderAllocationOnFairShareBoundPreemptible);
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

void TOperationSharedState::SetPreemptible(bool value)
{
    bool oldValue = Preemptible_;
    if (oldValue != value) {
        YT_LOG_DEBUG("Preemptible status changed (OldValue: %v, NewValue: %v)", oldValue, value);

        Preemptible_ = value;
    }
}

bool TOperationSharedState::GetPreemptible() const
{
    return Preemptible_;
}

bool TOperationSharedState::IsAllocationKnown(TAllocationId allocationId) const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    return AllocationPropertiesMap_.find(allocationId) != AllocationPropertiesMap_.end();
}

EAllocationPreemptionStatus TOperationSharedState::GetAllocationPreemptionStatus(TAllocationId allocationId) const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    if (!Enabled_) {
        return EAllocationPreemptionStatus::NonPreemptible;
    }

    return GetAllocationProperties(allocationId)->PreemptionStatus;
}

int TOperationSharedState::GetRunningAllocationCount() const
{
    return RunningAllocationCount_;
}

int TOperationSharedState::GetPreemptibleAllocationCount() const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    return AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::Preemptible].size();
}

int TOperationSharedState::GetAggressivelyPreemptibleAllocationCount() const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    return AllocationsPerPreemptionStatus_[EAllocationPreemptionStatus::AggressivelyPreemptible].size();
}

void TOperationSharedState::AddAllocation(
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

std::optional<TJobResources> TOperationSharedState::RemoveAllocation(TAllocationId allocationId)
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

void TOperationSharedState::UpdatePreemptionStatusStatistics(EOperationPreemptionStatus status)
{
    auto guard = Guard(PreemptionStatusStatisticsLock_);

    ++PreemptionStatusStatistics_[status];
}

TPreemptionStatusStatisticsVector TOperationSharedState::GetPreemptionStatusStatistics() const
{
    auto guard = Guard(PreemptionStatusStatisticsLock_);

    return PreemptionStatusStatistics_;
}

TAllocationPreemptionStatusMap TOperationSharedState::GetAllocationPreemptionStatusMap() const
{
    TAllocationPreemptionStatusMap allocationPreemptionStatuses;

    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    allocationPreemptionStatuses.reserve(AllocationPropertiesMap_.size());
    for (const auto& [allocationId, properties] : AllocationPropertiesMap_) {
        EmplaceOrCrash(allocationPreemptionStatuses, allocationId, properties.PreemptionStatus);
    }

    return allocationPreemptionStatuses;
}

void TOperationSharedState::OnMinNeededResourcesUnsatisfied(
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
    const TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>& unsatisfiedResources)
{
    auto& shard = StateShards_[schedulingHeartbeatContext->GetNodeShardId()];
    for (auto resourceType : TEnumTraitsImpl<EJobResourceWithDiskQuotaType>::GetDomainValues()) {
        if (unsatisfiedResources[resourceType]) {
            IncrementAtomicCounterUnsafely(&shard.MinNeededResourcesUnsatisfiedCount[resourceType]);
        }
    }
}

TEnumIndexedArray<EJobResourceWithDiskQuotaType, int> TOperationSharedState::GetMinNeededResourcesWithDiskQuotaUnsatisfiedCount()
{
    UpdateDiagnosticCounters();

    return MinNeededResourcesWithDiskQuotaUnsatisfiedCount_;
}

void TOperationSharedState::OnOperationDeactivated(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext, EDeactivationReason reason)
{
    auto& shard = StateShards_[schedulingHeartbeatContext->GetNodeShardId()];
    IncrementAtomicCounterUnsafely(&shard.DeactivationReasons[reason]);
    IncrementAtomicCounterUnsafely(&shard.DeactivationReasonsFromLastNonStarvingTime[reason]);
}

TEnumIndexedArray<EDeactivationReason, int> TOperationSharedState::GetDeactivationReasons()
{
    UpdateDiagnosticCounters();

    return DeactivationReasons_;
}

TEnumIndexedArray<EDeactivationReason, int> TOperationSharedState::GetDeactivationReasonsFromLastNonStarvingTime()
{
    UpdateDiagnosticCounters();

    return DeactivationReasonsFromLastNonStarvingTime_;
}

void TOperationSharedState::IncrementOperationScheduleAllocationAttemptCount(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext)
{
    auto& shard = StateShards_[schedulingHeartbeatContext->GetNodeShardId()];

    IncrementAtomicCounterUnsafely(&shard.ScheduleAllocationAttemptCount);
}

int TOperationSharedState::GetOperationScheduleAllocationAttemptCount()
{
    UpdateDiagnosticCounters();

    return ScheduleAllocationAttemptCount_;
}

void TOperationSharedState::ProcessUpdatedStarvationStatus(EStarvationStatus status)
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

void TOperationSharedState::UpdateDiagnosticCounters()
{
    auto now = TInstant::Now();
    if (now < LastDiagnosticCountersUpdateTime_ + UpdateStateShardsBackoff_) {
        return;
    }

    std::fill(DeactivationReasons_.begin(), DeactivationReasons_.end(), 0);
    std::fill(DeactivationReasonsFromLastNonStarvingTime_.begin(), DeactivationReasonsFromLastNonStarvingTime_.end(), 0);
    std::fill(MinNeededResourcesWithDiskQuotaUnsatisfiedCount_.begin(), MinNeededResourcesWithDiskQuotaUnsatisfiedCount_.end(), 0);
    i64 scheduleAllocationAttemptCount = 0;

    for (int shardId = 0; shardId < std::ssize(StrategyHost_->GetNodeShardInvokers()); ++shardId) {
        auto& shard = StateShards_[shardId];
        for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
            DeactivationReasons_[reason] += shard.DeactivationReasons[reason].load();
            DeactivationReasonsFromLastNonStarvingTime_[reason] +=
                shard.DeactivationReasonsFromLastNonStarvingTime[reason].load();
        }
        for (auto resource : TEnumTraits<EJobResourceWithDiskQuotaType>::GetDomainValues()) {
            MinNeededResourcesWithDiskQuotaUnsatisfiedCount_[resource] +=
                shard.MinNeededResourcesUnsatisfiedCount[resource].load();
        }
        scheduleAllocationAttemptCount += shard.ScheduleAllocationAttemptCount;
    }

    ScheduleAllocationAttemptCount_ = scheduleAllocationAttemptCount;
    LastDiagnosticCountersUpdateTime_ = now;
}

TInstant TOperationSharedState::GetLastScheduleAllocationSuccessTime() const
{
    auto guard = ReaderGuard(AllocationPropertiesMapLock_);

    return LastScheduleAllocationSuccessTime_;
}

TJobResources TOperationSharedState::SetAllocationResourceUsage(
    TAllocationProperties* properties,
    const TJobResources& resources)
{
    YT_ASSERT_WRITER_SPINLOCK_AFFINITY(AllocationPropertiesMapLock_);

    auto delta = resources - properties->ResourceUsage;
    properties->ResourceUsage = resources;
    ResourceUsagePerPreemptionStatus_[properties->PreemptionStatus] += delta;

    return delta;
}

TOperationSharedState::TAllocationProperties*
TOperationSharedState::GetAllocationProperties(TAllocationId allocationId)
{
    auto it = AllocationPropertiesMap_.find(allocationId);
    YT_ASSERT(it != AllocationPropertiesMap_.end());
    return &it->second;
}

const TOperationSharedState::TAllocationProperties*
TOperationSharedState::GetAllocationProperties(TAllocationId allocationId) const
{
    auto it = AllocationPropertiesMap_.find(allocationId);
    YT_ASSERT(it != AllocationPropertiesMap_.end());
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
