#include "fair_share_tree_job_scheduler_operation_shared_state.h"
#include "fair_share_tree_element.h"

namespace NYT::NScheduler {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const TString InvalidCustomProfilingTag("invalid");

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
    TotalResourceUsage_ = {};
    NonPreemptibleResourceUsage_ = {};
    AggressivelyPreemptibleResourceUsage_ = {};
    RunningJobCount_ = 0;
    PreemptibleJobs_.clear();
    AggressivelyPreemptibleJobs_.clear();
    NonPreemptibleJobs_.clear();
    JobPropertiesMap_.clear();

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

bool TFairShareTreeJobSchedulerOperationSharedState::OnJobStarted(
    TSchedulerOperationElement* operationElement,
    TJobId jobId,
    const TJobResourcesWithQuota& resourceUsage,
    const TJobResources& precommitedResources,
    int scheduleJobEpoch,
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
    auto getUsageShare = [&] (const TJobResources& resourceUsage) -> TResourceVector {
        return TResourceVector::FromJobResources(resourceUsage, element->GetTotalResourceLimits());
    };

    auto balanceLists = [&] (
        TJobIdList* left,
        TJobIdList* right,
        TJobResources resourceUsage,
        const TResourceVector& fairShareBound,
        const std::function<void(TJobProperties*)>& onMovedLeftToRight,
        const std::function<void(TJobProperties*)>& onMovedRightToLeft)
    {
        // Move from left to right and decrease |resourceUsage| until the next move causes
        // |operationElement->IsStrictlyDominatesNonBlocked(fairShareBound, getUsageShare(nextUsage))| to become true.
        // In particular, even if fair share is slightly less than it should be due to precision errors,
        // we expect no problems, because the job which crosses the fair share boundary belongs to the left list.
        while (!left->empty()) {
            auto jobId = left->back();
            auto* jobProperties = GetJobProperties(jobId);

            auto nextUsage = resourceUsage - jobProperties->ResourceUsage;
            if (element->IsStrictlyDominatesNonBlocked(fairShareBound, getUsageShare(nextUsage))) {
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
        while (!right->empty() && element->IsStrictlyDominatesNonBlocked(fairShareBound, getUsageShare(resourceUsage))) {
            auto jobId = right->front();
            auto* jobProperties = GetJobProperties(jobId);

            right->pop_front();
            left->push_back(jobId);
            jobProperties->JobIdListIterator = --left->end();
            onMovedRightToLeft(jobProperties);

            resourceUsage += jobProperties->ResourceUsage;
            ++(*moveCount);
        }

        return resourceUsage;
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

    auto fairShare = element->GetFairShare();
    auto preemptionSatisfactionThreshold = element->TreeConfig()->PreemptionSatisfactionThreshold;
    auto aggressivePreemptionSatisfactionThreshold = element->TreeConfig()->AggressivePreemptionSatisfactionThreshold;

    YT_LOG_DEBUG_IF(enableLogging,
        "Update preemptible lists inputs (FairShare: %.6g, TotalResourceLimits: %v, "
        "PreemptionSatisfactionThreshold: %v, AggressivePreemptionSatisfactionThreshold: %v)",
        fairShare,
        FormatResources(element->GetTotalResourceLimits()),
        preemptionSatisfactionThreshold,
        aggressivePreemptionSatisfactionThreshold);

    // NB: We need 2 iterations since thresholds may change significantly such that we need
    // to move job from preemptible list to non-preemptible list through aggressively preemptible list.
    for (int iteration = 0; iteration < 2; ++iteration) {
        YT_LOG_DEBUG_IF(enableLogging,
            "Preemptible lists usage bounds before update "
            "(NonPreemptibleResourceUsage: %v, AggressivelyPreemptibleResourceUsage: %v, PreemtableResourceUsage: %v, Iteration: %v)",
            FormatResources(NonPreemptibleResourceUsage_),
            FormatResources(AggressivelyPreemptibleResourceUsage_),
            FormatResources(TotalResourceUsage_ - NonPreemptibleResourceUsage_ - AggressivelyPreemptibleResourceUsage_),
            iteration);

        auto startNonPreemptibleAndAggressivelyPreemptibleResourceUsage_ = NonPreemptibleResourceUsage_ + AggressivelyPreemptibleResourceUsage_;

        NonPreemptibleResourceUsage_ = balanceLists(
            &NonPreemptibleJobs_,
            &AggressivelyPreemptibleJobs_,
            NonPreemptibleResourceUsage_,
            fairShare * aggressivePreemptionSatisfactionThreshold,
            setAggressivelyPreemptible,
            setNonPreemptible);

        auto nonpreemptibleAndAggressivelyPreemptibleResourceUsage_ = balanceLists(
            &AggressivelyPreemptibleJobs_,
            &PreemptibleJobs_,
            startNonPreemptibleAndAggressivelyPreemptibleResourceUsage_,
            Preemptible_ ? fairShare * preemptionSatisfactionThreshold : TResourceVector::Infinity(),
            setPreemptible,
            setAggressivelyPreemptible);

        AggressivelyPreemptibleResourceUsage_ = nonpreemptibleAndAggressivelyPreemptibleResourceUsage_ - NonPreemptibleResourceUsage_;
    }

    YT_LOG_DEBUG_IF(enableLogging,
        "Preemptible lists usage bounds after update "
        "(NonPreemptibleResourceUsage: %v, AggressivelyPreemptibleResourceUsage: %v, PreemtableResourceUsage: %v)",
        FormatResources(NonPreemptibleResourceUsage_),
        FormatResources(AggressivelyPreemptibleResourceUsage_),
        FormatResources(TotalResourceUsage_ - NonPreemptibleResourceUsage_ - AggressivelyPreemptibleResourceUsage_));
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

    return PreemptibleJobs_.size();
}

int TFairShareTreeJobSchedulerOperationSharedState::GetAggressivelyPreemptibleJobCount() const
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    return AggressivelyPreemptibleJobs_.size();
}

void TFairShareTreeJobSchedulerOperationSharedState::AddJob(TJobId jobId, const TJobResourcesWithQuota& resourceUsage)
{
    auto guard = WriterGuard(JobPropertiesMapLock_);

    LastScheduleJobSuccessTime_ = TInstant::Now();

    PreemptibleJobs_.push_back(jobId);

    auto it = JobPropertiesMap_.emplace(
        jobId,
        TJobProperties{
            .PreemptionStatus = EJobPreemptionStatus::Preemptible,
            .JobIdListIterator = --PreemptibleJobs_.end(),
            .ResourceUsage = {}});
    YT_VERIFY(it.second);

    ++RunningJobCount_;

    SetJobResourceUsage(&it.first->second, resourceUsage.ToJobResources());

    TotalDiskQuota_ += resourceUsage.GetDiskQuota();
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
    switch (properties->PreemptionStatus) {
        case EJobPreemptionStatus::Preemptible:
            PreemptibleJobs_.erase(properties->JobIdListIterator);
            break;
        case EJobPreemptionStatus::AggressivelyPreemptible:
            AggressivelyPreemptibleJobs_.erase(properties->JobIdListIterator);
            break;
        case EJobPreemptionStatus::NonPreemptible:
            NonPreemptibleJobs_.erase(properties->JobIdListIterator);
            break;
        default:
            YT_ABORT();
    }

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
            ++shard.MinNeededResourcesUnsatisfiedCountLocal[EJobResourceType::Name]; \
        }
    ITERATE_JOB_RESOURCES(XX)
#undef XX
}

TEnumIndexedVector<EJobResourceType, int> TFairShareTreeJobSchedulerOperationSharedState::GetMinNeededResourcesUnsatisfiedCount()
{
    UpdateShardState();

    TEnumIndexedVector<EJobResourceType, int> result;
    for (const auto& shard : StateShards_) {
        for (auto resource : TEnumTraits<EJobResourceType>::GetDomainValues()) {
            result[resource] += shard.MinNeededResourcesUnsatisfiedCount[resource].load();
        }
    }
    return result;
}

void TFairShareTreeJobSchedulerOperationSharedState::OnOperationDeactivated(const ISchedulingContextPtr& schedulingContext, EDeactivationReason reason)
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];
    ++shard.DeactivationReasonsLocal[reason];
    ++shard.DeactivationReasonsFromLastNonStarvingTimeLocal[reason];
}

TEnumIndexedVector<EDeactivationReason, int> TFairShareTreeJobSchedulerOperationSharedState::GetDeactivationReasons()
{
    UpdateShardState();

    TEnumIndexedVector<EDeactivationReason, int> result;
    for (const auto& shard : StateShards_) {
        for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
            result[reason] += shard.DeactivationReasons[reason].load();
        }
    }
    return result;
}

TEnumIndexedVector<EDeactivationReason, int> TFairShareTreeJobSchedulerOperationSharedState::GetDeactivationReasonsFromLastNonStarvingTime()
{
    UpdateShardState();

    TEnumIndexedVector<EDeactivationReason, int> result;
    for (const auto& shard : StateShards_) {
        for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
            result[reason] += shard.DeactivationReasonsFromLastNonStarvingTime[reason].load();
        }
    }
    return result;
}

void TFairShareTreeJobSchedulerOperationSharedState::ResetDeactivationReasonsFromLastNonStarvingTime()
{
    int index = 0;
    for (const auto& invoker : StrategyHost_->GetNodeShardInvokers()) {
        invoker->Invoke(BIND([this, this_=MakeStrong(this), index] {
            auto& shard = StateShards_[index];
            for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
                shard.DeactivationReasonsFromLastNonStarvingTime[reason].store(0);
                shard.DeactivationReasonsFromLastNonStarvingTimeLocal[reason] = 0;
            }
        }));
        ++index;
    }
}

void TFairShareTreeJobSchedulerOperationSharedState::UpdateShardState()
{
    auto now = TInstant::Now();
    if (now < LastStateShardsUpdateTime_ + UpdateStateShardsBackoff_) {
        return;
    }
    int index = 0;
    for (const auto& invoker : StrategyHost_->GetNodeShardInvokers()) {
        invoker->Invoke(BIND([this, this_=MakeStrong(this), index] {
            auto& shard = StateShards_[index];
            for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
                shard.DeactivationReasonsFromLastNonStarvingTime[reason].store(
                    shard.DeactivationReasonsFromLastNonStarvingTimeLocal[reason]);
                shard.DeactivationReasons[reason].store(shard.DeactivationReasonsLocal[reason]);
            }
            for (auto resource : TEnumTraits<EJobResourceType>::GetDomainValues()) {
                shard.MinNeededResourcesUnsatisfiedCount[resource].store(
                    shard.MinNeededResourcesUnsatisfiedCountLocal[resource]);
            }
        }));
        ++index;
    }
    LastStateShardsUpdateTime_ = now;
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
    TotalResourceUsage_ += delta;
    switch (properties->PreemptionStatus) {
        case EJobPreemptionStatus::Preemptible:
            // Do nothing.
            break;
        case EJobPreemptionStatus::AggressivelyPreemptible:
            AggressivelyPreemptibleResourceUsage_ += delta;
            break;
        case EJobPreemptionStatus::NonPreemptible:
            NonPreemptibleResourceUsage_ += delta;
            break;
        default:
            YT_ABORT();
    }

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