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

static const TJobResourcesConfigPtr EmptyJobResourcesConfig = New<TJobResourcesConfig>();

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeJobSchedulerOperationSharedState::TFairShareTreeJobSchedulerOperationSharedState(
    ISchedulerStrategyHost* strategyHost,
    int updatePreemptibleJobsListLoggingPeriod,
    const NLogging::TLogger& logger)
    : StrategyHost_(strategyHost)
    , UpdatePreemptibleJobsListLoggingPeriod_(updatePreemptibleJobsListLoggingPeriod)
    , Logger(logger)
{ }

TJobResources TFairShareTreeJobSchedulerOperationSharedState::Disable()
{
    YT_LOG_DEBUG("Operation element disabled in strategy");

    auto guard = WriterGuard(JobPropertiesMapLock_);

    Enabled_ = false;

    TJobResources resourceUsage;
    for (const auto& [jobId, properties] : JobPropertiesMap_) {
        resourceUsage += properties.ResourceUsage;
    }

    TotalDiskQuota_ = {};
    RunningJobCount_ = 0;
    JobPropertiesMap_.clear();
    for (auto preemptionStatus : TEnumTraits<EJobPreemptionStatus>::GetDomainValues()) {
        JobsPerPreemptionStatus_[preemptionStatus].clear();
        ResourceUsagePerPreemptionStatus_[preemptionStatus] = {};
    }

    return resourceUsage;
}

void TFairShareTreeJobSchedulerOperationSharedState::Enable()
{
    YT_LOG_DEBUG("Operation element enabled in strategy");

    auto guard = WriterGuard(JobPropertiesMapLock_);

    YT_VERIFY(!Enabled_);
    Enabled_ = true;
}

bool TFairShareTreeJobSchedulerOperationSharedState::IsEnabled()
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);
    return Enabled_;
}

void TFairShareTreeJobSchedulerOperationSharedState::RecordPackingHeartbeat(
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TFairShareStrategyPackingConfigPtr& packingConfig)
{
    HeartbeatStatistics_.RecordHeartbeat(heartbeatSnapshot, packingConfig);
}

bool TFairShareTreeJobSchedulerOperationSharedState::CheckPacking(
    const TSchedulerOperationElement* operationElement,
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TJobResourcesWithQuota& jobResources,
    const TJobResources& totalResourceLimits,
    const TFairShareStrategyPackingConfigPtr& packingConfig)
{
    return HeartbeatStatistics_.CheckPacking(
        operationElement,
        heartbeatSnapshot,
        jobResources,
        totalResourceLimits,
        packingConfig);
}

TJobResources TFairShareTreeJobSchedulerOperationSharedState::SetJobResourceUsage(
    TJobId jobId,
    const TJobResources& resources)
{
    auto guard = WriterGuard(JobPropertiesMapLock_);

    if (!Enabled_) {
        return {};
    }

    return SetJobResourceUsage(GetJobProperties(jobId), resources);
}

TDiskQuota TFairShareTreeJobSchedulerOperationSharedState::GetTotalDiskQuota() const
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);
    return TotalDiskQuota_;
}

void TFairShareTreeJobSchedulerOperationSharedState::PublishFairShare(const TResourceVector& fairShare)
{
    FairShare_.Store(fairShare);
}

bool TFairShareTreeJobSchedulerOperationSharedState::OnJobStarted(
    TSchedulerOperationElement* operationElement,
    TJobId jobId,
    const TJobResourcesWithQuota& resourceUsage,
    const TJobResources& precommitedResources,
    TControllerEpoch scheduleJobEpoch,
    bool force)
{
    YT_ELEMENT_LOG_DETAILED(operationElement, "Adding job to strategy (JobId: %v)", jobId);

    if (!force && (!IsEnabled() || operationElement->GetControllerEpoch() != scheduleJobEpoch)) {
        return false;
    }

    AddJob(jobId, resourceUsage);
    operationElement->CommitHierarchicalResourceUsage(resourceUsage, precommitedResources);
    UpdatePreemptibleJobsList(operationElement);

    return true;
}

void TFairShareTreeJobSchedulerOperationSharedState::OnJobFinished(TSchedulerOperationElement* operationElement, TJobId jobId)
{
    YT_ELEMENT_LOG_DETAILED(operationElement, "Removing job from strategy (JobId: %v)", jobId);

    if (auto delta = RemoveJob(jobId)) {
        operationElement->IncreaseHierarchicalResourceUsage(-(*delta));
        UpdatePreemptibleJobsList(operationElement);
    }
}

void TFairShareTreeJobSchedulerOperationSharedState::UpdatePreemptibleJobsList(const TSchedulerOperationElement* element)
{
    TWallTimer timer;

    int moveCount = 0;
    DoUpdatePreemptibleJobsList(element, &moveCount);

    auto elapsed = timer.GetElapsedTime();
    YT_LOG_DEBUG_IF(elapsed > element->TreeConfig()->UpdatePreemptibleListDurationLoggingThreshold,
        "Preemptible list update is too long (Duration: %v, MoveCount: %v)",
        elapsed.MilliSeconds(),
        moveCount);
}

void TFairShareTreeJobSchedulerOperationSharedState::DoUpdatePreemptibleJobsList(const TSchedulerOperationElement* element, int* moveCount)
{
    auto convertToShare = [&] (const TJobResources& jobResources) -> TResourceVector {
        return TResourceVector::FromJobResources(jobResources, element->GetTotalResourceLimits());
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
        TJobIdList* left,
        TJobIdList* right,
        TJobResources resourceUsage,
        const TJobResources& resourceUsageBound,
        const TResourceVector& fairShareBound,
        const std::function<void(TJobProperties*)>& onMovedLeftToRight,
        const std::function<void(TJobProperties*)>& onMovedRightToLeft)
    {
        auto initialResourceUsage = resourceUsage;

        // Move from left to right and decrease |resourceUsage| until the next move causes
        // |operationElement->IsStrictlyDominatesNonBlocked(fairShareBound, getUsageShare(nextUsage))| to become true.
        // In particular, even if fair share is slightly less than it should be due to precision errors,
        // we expect no problems, because the job which crosses the fair share boundary belongs to the left list.
        while (!left->empty()) {
            auto jobId = left->back();
            auto* jobProperties = GetJobProperties(jobId);

            auto nextUsage = resourceUsage - jobProperties->ResourceUsage;
            if (isUsageBelowBounds(nextUsage, resourceUsageBound, fairShareBound)) {
                break;
            }

            left->pop_back();
            right->push_front(jobId);
            jobProperties->JobIdListIterator = right->begin();
            onMovedLeftToRight(jobProperties);

            resourceUsage = nextUsage;
            ++(*moveCount);
        }

        // Move from right to left and increase |resourceUsage|.
        while (!right->empty() && isUsageBelowBounds(resourceUsage, resourceUsageBound, fairShareBound)) {
            auto jobId = right->front();
            auto* jobProperties = GetJobProperties(jobId);

            right->pop_front();
            left->push_back(jobId);
            jobProperties->JobIdListIterator = --left->end();
            onMovedRightToLeft(jobProperties);

            resourceUsage += jobProperties->ResourceUsage;
            ++(*moveCount);
        }

        return resourceUsage - initialResourceUsage;
    };

    auto setPreemptible = [] (TJobProperties* properties) {
        properties->PreemptionStatus = EJobPreemptionStatus::Preemptible;
    };

    auto setAggressivelyPreemptible = [] (TJobProperties* properties) {
        properties->PreemptionStatus = EJobPreemptionStatus::AggressivelyPreemptible;
    };

    auto setNonPreemptible = [] (TJobProperties* properties) {
        properties->PreemptionStatus = EJobPreemptionStatus::NonPreemptible;
    };

    auto guard = WriterGuard(JobPropertiesMapLock_);

    bool enableLogging =
        (UpdatePreemptibleJobsListCount_.fetch_add(1) % UpdatePreemptibleJobsListLoggingPeriod_) == 0 ||
            element->AreDetailedLogsEnabled();

    auto fairShare = FairShare_.Load();
    auto preemptionSatisfactionThreshold = element->TreeConfig()->PreemptionSatisfactionThreshold;
    auto aggressivePreemptionSatisfactionThreshold = element->TreeConfig()->AggressivePreemptionSatisfactionThreshold;

    // NB: |element->EffectiveNonPreemptibleResourceUsageThresholdConfig()| can be null during revived jobs registration.
    // We don't care about this, because the next fair share update will bring correct config.
    const auto& nonPreemptibleResourceUsageConfig = element->EffectiveNonPreemptibleResourceUsageThresholdConfig()
        ? element->EffectiveNonPreemptibleResourceUsageThresholdConfig()
        : EmptyJobResourcesConfig;
    auto nonPreemptibleResourceUsageThreshold = nonPreemptibleResourceUsageConfig->IsNonTrivial()
        ? ToJobResources(nonPreemptibleResourceUsageConfig, TJobResources::Infinite())
        : TJobResources();

    YT_LOG_DEBUG_IF(enableLogging,
        "Update preemptible lists inputs (FairShare: %.6g, TotalResourceLimits: %v, NonPreemptibleResourceUsageConfig: %v, "
        "PreemptionSatisfactionThreshold: %v, AggressivePreemptionSatisfactionThreshold: %v)",
        fairShare,
        FormatResources(element->GetTotalResourceLimits()),
        FormatResourcesConfig(nonPreemptibleResourceUsageConfig),
        preemptionSatisfactionThreshold,
        aggressivePreemptionSatisfactionThreshold);

    // NB: We need 2 iterations since thresholds may change significantly such that we need
    // to move job from preemptible list to non-preemptible list through aggressively preemptible list.
    for (int iteration = 0; iteration < 2; ++iteration) {
        YT_LOG_DEBUG_IF(enableLogging,
            "Preemptible lists usage bounds before update "
            "(NonPreemptibleResourceUsage: %v, AggressivelyPreemptibleResourceUsage: %v, PreemptibleResourceUsage: %v, Iteration: %v)",
            FormatResources(ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::NonPreemptible]),
            FormatResources(ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::AggressivelyPreemptible]),
            FormatResources(ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::Preemptible]),
            iteration);

        {
            auto usageDelta = balanceLists(
                &JobsPerPreemptionStatus_[EJobPreemptionStatus::NonPreemptible],
                &JobsPerPreemptionStatus_[EJobPreemptionStatus::AggressivelyPreemptible],
                ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::NonPreemptible],
                nonPreemptibleResourceUsageThreshold,
                fairShare * aggressivePreemptionSatisfactionThreshold,
                setAggressivelyPreemptible,
                setNonPreemptible);
            ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::NonPreemptible] += usageDelta;
            ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::AggressivelyPreemptible] -= usageDelta;
        }

        {
            auto usageDelta = balanceLists(
                &JobsPerPreemptionStatus_[EJobPreemptionStatus::AggressivelyPreemptible],
                &JobsPerPreemptionStatus_[EJobPreemptionStatus::Preemptible],
                ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::NonPreemptible] +
                    ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::AggressivelyPreemptible],
                nonPreemptibleResourceUsageThreshold,
                Preemptible_ ? fairShare * preemptionSatisfactionThreshold : TResourceVector::Infinity(),
                setPreemptible,
                setAggressivelyPreemptible);
            ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::AggressivelyPreemptible] += usageDelta;
            ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::Preemptible] -= usageDelta;
        }
    }

    YT_LOG_DEBUG_IF(enableLogging,
        "Preemptible lists usage bounds after update "
        "(NonPreemptibleResourceUsage: %v, AggressivelyPreemptibleResourceUsage: %v, PreemptibleResourceUsage: %v)",
        FormatResources(ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::NonPreemptible]),
        FormatResources(ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::AggressivelyPreemptible]),
        FormatResources(ResourceUsagePerPreemptionStatus_[EJobPreemptionStatus::Preemptible]));
}

void TFairShareTreeJobSchedulerOperationSharedState::SetPreemptible(bool value)
{
    bool oldValue = Preemptible_;
    if (oldValue != value) {
        YT_LOG_DEBUG("Preemptible status changed (OldValue: %v, NewValue: %v)", oldValue, value);

        Preemptible_ = value;
    }
}

bool TFairShareTreeJobSchedulerOperationSharedState::GetPreemptible() const
{
    return Preemptible_;
}

bool TFairShareTreeJobSchedulerOperationSharedState::IsJobKnown(TJobId jobId) const
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    return JobPropertiesMap_.find(jobId) != JobPropertiesMap_.end();
}

EJobPreemptionStatus TFairShareTreeJobSchedulerOperationSharedState::GetJobPreemptionStatus(TJobId jobId) const
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    if (!Enabled_) {
        return EJobPreemptionStatus::NonPreemptible;
    }

    return GetJobProperties(jobId)->PreemptionStatus;
}

int TFairShareTreeJobSchedulerOperationSharedState::GetRunningJobCount() const
{
    return RunningJobCount_;
}

int TFairShareTreeJobSchedulerOperationSharedState::GetPreemptibleJobCount() const
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    return JobsPerPreemptionStatus_[EJobPreemptionStatus::Preemptible].size();
}

int TFairShareTreeJobSchedulerOperationSharedState::GetAggressivelyPreemptibleJobCount() const
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    return JobsPerPreemptionStatus_[EJobPreemptionStatus::AggressivelyPreemptible].size();
}

void TFairShareTreeJobSchedulerOperationSharedState::AddJob(TJobId jobId, const TJobResourcesWithQuota& resourceUsage)
{
    auto guard = WriterGuard(JobPropertiesMapLock_);

    LastScheduleJobSuccessTime_ = TInstant::Now();

    auto& preemptibleJobs = JobsPerPreemptionStatus_[EJobPreemptionStatus::Preemptible];
    preemptibleJobs.push_back(jobId);

    auto it = EmplaceOrCrash(
        JobPropertiesMap_,
        jobId,
        TJobProperties{
            .PreemptionStatus = EJobPreemptionStatus::Preemptible,
            .JobIdListIterator = --preemptibleJobs.end(),
            .ResourceUsage = {},
            .DiskQuota = resourceUsage.DiskQuota(),
        });

    ++RunningJobCount_;

    SetJobResourceUsage(&it->second, resourceUsage.ToJobResources());

    TotalDiskQuota_ += resourceUsage.DiskQuota();
}

std::optional<TJobResources> TFairShareTreeJobSchedulerOperationSharedState::RemoveJob(TJobId jobId)
{
    auto guard = WriterGuard(JobPropertiesMapLock_);

    if (!Enabled_) {
        return std::nullopt;
    }

    auto it = JobPropertiesMap_.find(jobId);
    YT_VERIFY(it != JobPropertiesMap_.end());

    auto* properties = &it->second;
    JobsPerPreemptionStatus_[properties->PreemptionStatus].erase(properties->JobIdListIterator);

    --RunningJobCount_;

    auto resourceUsage = properties->ResourceUsage;
    SetJobResourceUsage(properties, TJobResources());

    TotalDiskQuota_ -= properties->DiskQuota;

    JobPropertiesMap_.erase(it);

    return resourceUsage;
}

void TFairShareTreeJobSchedulerOperationSharedState::UpdatePreemptionStatusStatistics(EOperationPreemptionStatus status)
{
    auto guard = Guard(PreemptionStatusStatisticsLock_);

    ++PreemptionStatusStatistics_[status];
}

TPreemptionStatusStatisticsVector TFairShareTreeJobSchedulerOperationSharedState::GetPreemptionStatusStatistics() const
{
    auto guard = Guard(PreemptionStatusStatisticsLock_);

    return PreemptionStatusStatistics_;
}

TJobPreemptionStatusMap TFairShareTreeJobSchedulerOperationSharedState::GetJobPreemptionStatusMap() const
{
    TJobPreemptionStatusMap jobPreemptionStatuses;

    auto guard = ReaderGuard(JobPropertiesMapLock_);

    jobPreemptionStatuses.reserve(JobPropertiesMap_.size());
    for (const auto& [jobId, properties] : JobPropertiesMap_) {
        YT_VERIFY(jobPreemptionStatuses.emplace(jobId, properties.PreemptionStatus).second);
    }

    return jobPreemptionStatuses;
}

void TFairShareTreeJobSchedulerOperationSharedState::OnMinNeededResourcesUnsatisfied(
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

TEnumIndexedVector<EJobResourceType, int> TFairShareTreeJobSchedulerOperationSharedState::GetMinNeededResourcesUnsatisfiedCount()
{
    UpdateDiagnosticCounters();

    return MinNeededResourcesUnsatisfiedCount_;
}

void TFairShareTreeJobSchedulerOperationSharedState::OnOperationDeactivated(const ISchedulingContextPtr& schedulingContext, EDeactivationReason reason)
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];
    IncrementAtomicCounterUnsafely(&shard.DeactivationReasons[reason]);
    IncrementAtomicCounterUnsafely(&shard.DeactivationReasonsFromLastNonStarvingTime[reason]);
}

TEnumIndexedVector<EDeactivationReason, int> TFairShareTreeJobSchedulerOperationSharedState::GetDeactivationReasons()
{
    UpdateDiagnosticCounters();

    return DeactivationReasons_;
}

TEnumIndexedVector<EDeactivationReason, int> TFairShareTreeJobSchedulerOperationSharedState::GetDeactivationReasonsFromLastNonStarvingTime()
{
    UpdateDiagnosticCounters();

    return DeactivationReasonsFromLastNonStarvingTime_;
}

void TFairShareTreeJobSchedulerOperationSharedState::IncrementOperationScheduleJobAttemptCount(const ISchedulingContextPtr& schedulingContext)
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];

    IncrementAtomicCounterUnsafely(&shard.ScheduleJobAttemptCount);
}

int TFairShareTreeJobSchedulerOperationSharedState::GetOperationScheduleJobAttemptCount()
{
    UpdateDiagnosticCounters();

    return ScheduleJobAttemptCount_;
}

void TFairShareTreeJobSchedulerOperationSharedState::ProcessUpdatedStarvationStatus(EStarvationStatus status)
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

void TFairShareTreeJobSchedulerOperationSharedState::UpdateDiagnosticCounters()
{
    auto now = TInstant::Now();
    if (now < LastDiagnosticCountersUpdateTime_ + UpdateStateShardsBackoff_) {
        return;
    }

    std::fill(DeactivationReasons_.begin(), DeactivationReasons_.end(), 0);
    std::fill(DeactivationReasonsFromLastNonStarvingTime_.begin(), DeactivationReasonsFromLastNonStarvingTime_.end(), 0);
    std::fill(MinNeededResourcesUnsatisfiedCount_.begin(), MinNeededResourcesUnsatisfiedCount_.end(), 0);
    i64 scheduleJobAttemptCount = 0;

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
        scheduleJobAttemptCount += shard.ScheduleJobAttemptCount;
    }

    ScheduleJobAttemptCount_ = scheduleJobAttemptCount;
    LastDiagnosticCountersUpdateTime_ = now;
}

TInstant TFairShareTreeJobSchedulerOperationSharedState::GetLastScheduleJobSuccessTime() const
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    return LastScheduleJobSuccessTime_;
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

TJobResources TFairShareTreeJobSchedulerOperationSharedState::SetJobResourceUsage(
    TJobProperties* properties,
    const TJobResources& resources)
{
    auto delta = resources - properties->ResourceUsage;
    properties->ResourceUsage = resources;
    ResourceUsagePerPreemptionStatus_[properties->PreemptionStatus] += delta;

    return delta;
}

TFairShareTreeJobSchedulerOperationSharedState::TJobProperties* TFairShareTreeJobSchedulerOperationSharedState::GetJobProperties(TJobId jobId)
{
    auto it = JobPropertiesMap_.find(jobId);
    YT_ASSERT(it != JobPropertiesMap_.end());
    return &it->second;
}

const TFairShareTreeJobSchedulerOperationSharedState::TJobProperties* TFairShareTreeJobSchedulerOperationSharedState::GetJobProperties(TJobId jobId) const
{
    auto it = JobPropertiesMap_.find(jobId);
    YT_ASSERT(it != JobPropertiesMap_.end());
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
