#include "fair_share_tree_element.h"

#include "fair_share_tree.h"
#include "helpers.h"
#include "piecewise_linear_function_helpers.h"
#include "resource_tree.h"
#include "resource_tree_element.h"
#include "scheduling_context.h"

#include "operation_log.h"

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/historic_usage_aggregator.h>

#include <yt/core/profiling/timing.h>

#include <util/generic/ymath.h>

namespace NYT::NScheduler::NVectorScheduler {

using namespace NConcurrency;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;

using NProfiling::CpuDurationToDuration;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = SchedulerProfiler;

////////////////////////////////////////////////////////////////////////////////

static const TString MissingCustomProfilingTag("missing");

////////////////////////////////////////////////////////////////////////////////

namespace {

TTagId GetCustomProfilingTag(const TString& tagName)
{
    static THashMap<TString, TTagId> tagNameToTagIdMap;

    auto it = tagNameToTagIdMap.find(tagName);
    if (it == tagNameToTagIdMap.end()) {
        it = tagNameToTagIdMap.emplace(
            tagName,
            TProfileManager::Get()->RegisterTag("custom", tagName)
        ).first;
    }
    return it->second;
}

TJobResources ToJobResources(const TResourceLimitsConfigPtr& config, TJobResources defaultValue)
{
    if (config->UserSlots) {
        defaultValue.SetUserSlots(*config->UserSlots);
    }
    if (config->Cpu) {
        defaultValue.SetCpu(*config->Cpu);
    }
    if (config->Network) {
        defaultValue.SetNetwork(*config->Network);
    }
    if (config->Memory) {
        defaultValue.SetMemory(*config->Memory);
    }
    if (config->Gpu) {
        defaultValue.SetGpu(*config->Gpu);
    }
    return defaultValue;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TFairShareSchedulingStage::TFairShareSchedulingStage(TString loggingName, TScheduleJobsProfilingCounters profilingCounters)
    : LoggingName(std::move(loggingName))
    , ProfilingCounters(std::move(profilingCounters))
{ }

////////////////////////////////////////////////////////////////////////////////

TFairShareContext::TFairShareContext(
    ISchedulingContextPtr schedulingContext,
    bool enableSchedulingInfoLogging,
    const NLogging::TLogger& logger)
    : SchedulingContext(std::move(schedulingContext))
    , EnableSchedulingInfoLogging(enableSchedulingInfoLogging)
    , Logger(logger)
{ }

void TFairShareContext::Initialize(int treeSize, const std::vector<TSchedulingTagFilter>& registeredSchedulingTagFilters)
{
    YT_VERIFY(!Initialized);

    Initialized = true;

    DynamicAttributesList.resize(treeSize);
    CanSchedule.reserve(registeredSchedulingTagFilters.size());
    for (const auto& filter : registeredSchedulingTagFilters) {
        CanSchedule.push_back(SchedulingContext->CanSchedule(filter));
    }
}

TDynamicAttributes& TFairShareContext::DynamicAttributesFor(const TSchedulerElement* element)
{
    int index = element->GetTreeIndex();
    YT_VERIFY(index != UnassignedTreeIndex && index < DynamicAttributesList.size());
    return DynamicAttributesList[index];
}

const TDynamicAttributes& TFairShareContext::DynamicAttributesFor(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_VERIFY(index != UnassignedTreeIndex && index < DynamicAttributesList.size());
    return DynamicAttributesList[index];
}

TFairShareContext::TStageState::TStageState(TFairShareSchedulingStage* schedulingStage)
    : SchedulingStage(schedulingStage)
{ }

void TFairShareContext::StartStage(TFairShareSchedulingStage* schedulingStage)
{
    YT_VERIFY(!StageState);
    StageState.emplace(TStageState(schedulingStage));
}

void TFairShareContext::ProfileStageTimingsAndLogStatistics()
{
    YT_VERIFY(StageState);

    ProfileStageTimings();

    if (StageState->ScheduleJobAttemptCount > 0 && EnableSchedulingInfoLogging) {
        LogStageStatistics();
    }
}

void TFairShareContext::FinishStage()
{
    YT_VERIFY(StageState);
    StageState = std::nullopt;
}

void TFairShareContext::ProfileStageTimings()
{
    YT_VERIFY(StageState);

    auto* profilingCounters = &StageState->SchedulingStage->ProfilingCounters;

    Profiler.Update(
        profilingCounters->PrescheduleJobTime,
        StageState->PrescheduleDuration.MicroSeconds());

    auto strategyScheduleJobDuration = StageState->TotalDuration
        - StageState->PrescheduleDuration
        - StageState->TotalScheduleJobDuration;
    Profiler.Update(profilingCounters->StrategyScheduleJobTime, strategyScheduleJobDuration.MicroSeconds());

    Profiler.Update(
        profilingCounters->TotalControllerScheduleJobTime,
        StageState->TotalScheduleJobDuration.MicroSeconds());

    Profiler.Update(
        profilingCounters->ExecControllerScheduleJobTime,
        StageState->ExecScheduleJobDuration.MicroSeconds());

    Profiler.Update(
        profilingCounters->PackingRecordHeartbeatTime,
        StageState->PackingRecordHeartbeatDuration.MicroSeconds());

    Profiler.Update(
        profilingCounters->PackingCheckTime,
        StageState->PackingCheckDuration.MicroSeconds());

    Profiler.Increment(profilingCounters->ScheduleJobAttemptCount, StageState->ScheduleJobAttemptCount);
    Profiler.Increment(profilingCounters->ScheduleJobFailureCount, StageState->ScheduleJobFailureCount);

    for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
        Profiler.Increment(
            profilingCounters->ControllerScheduleJobFail[reason],
            StageState->FailedScheduleJob[reason]);
    }
}

void TFairShareContext::LogStageStatistics()
{
    YT_VERIFY(StageState);

    YT_LOG_DEBUG("%v scheduling statistics (ActiveTreeSize: %v, ActiveOperationCount: %v, DeactivationReasons: %v, CanStartMoreJobs: %v, Address: %v)",
        StageState->SchedulingStage->LoggingName,
        StageState->ActiveTreeSize,
        StageState->ActiveOperationCount,
        StageState->DeactivationReasons,
        SchedulingContext->CanStartMoreJobs(),
        SchedulingContext->GetNodeDescriptor().Address);
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerElementFixedState::TSchedulerElementFixedState(
    ISchedulerStrategyHost* host,
    IFairShareTreeHost* treeHost,
    TFairShareStrategyTreeConfigPtr treeConfig,
    TString treeId)
    : Host_(host)
    , TreeHost_(treeHost)
    , TreeConfig_(std::move(treeConfig))
    , TotalResourceLimits_(host->GetResourceLimits(TreeConfig_->NodesFilter))
    , TreeId_(std::move(treeId))
{ }

////////////////////////////////////////////////////////////////////////////////

void TSchedulerElement::MarkUnmutable()
{
    Mutable_ = false;
}

int TSchedulerElement::EnumerateElements(int startIndex, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    TreeIndex_ = startIndex++;
    context->ElementIndexes[GetId()] = TreeIndex_;
    return startIndex;
}

void TSchedulerElement::UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    YT_VERIFY(Mutable_);

    TreeConfig_ = config;
}

void TSchedulerElement::PreUpdateBottomUp(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    TotalResourceLimits_ = context->TotalResourceLimits;
    // NB: ResourceLimits must be computed after TotalResourceLimits.
    ResourceLimits_ = ComputeResourceLimits();
    ResourceTreeElement_->SetResourceLimits(GetSpecifiedResourceLimits());
}

void TSchedulerElement::UpdateBottomUp(TDynamicAttributesList* dynamicAttributesList, TUpdateFairShareContext* )
{
    YT_VERIFY(Mutable_);

    LimitsShare_ = TResourceVector::FromDouble(MinComponent(TResourceVector::FromJobResources(
        Min(ResourceLimits_, TotalResourceLimits_),
        TotalResourceLimits_,
        /* zeroDivByZero */ 1.0,
        /* oneDivByZero */ 1.0)));
    YT_VERIFY(Dominates(TResourceVector::Ones(), LimitsShare_));
    YT_VERIFY(Dominates(LimitsShare_, TResourceVector::Zero()));

    UpdateAttributes();
}

void TSchedulerElement::UpdatePreemption(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    Attributes_.AdjustedMinShare = TResourceVector::Min(Attributes_.RecursiveMinShare, Attributes_.FairShare);

    if (Parent_) {
        Attributes_.AdjustedFairShareStarvationTolerance = std::min(
            GetFairShareStarvationTolerance(),
            Parent_->AdjustedFairShareStarvationToleranceLimit());

        Attributes_.AdjustedMinSharePreemptionTimeout = std::max(
            GetMinSharePreemptionTimeout(),
            Parent_->AdjustedMinSharePreemptionTimeoutLimit());

        Attributes_.AdjustedFairSharePreemptionTimeout = std::max(
            GetFairSharePreemptionTimeout(),
            Parent_->AdjustedFairSharePreemptionTimeoutLimit());
    }
}

void TSchedulerElement::UpdateDynamicAttributes(
    TDynamicAttributesList* dynamicAttributesList,
    TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    (*dynamicAttributesList)[GetTreeIndex()].Active = true;
    UpdateDynamicAttributes(dynamicAttributesList);
}

void TSchedulerElement::UpdateDynamicAttributes(TDynamicAttributesList* dynamicAttributesList)
{
    auto& attributes = (*dynamicAttributesList)[GetTreeIndex()];
    YT_VERIFY(attributes.Active);
    attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio();
    attributes.Active = IsAlive();
}

void TSchedulerElement::PrescheduleJob(TFairShareContext* context, bool /*starvingOnly*/, bool /*aggressiveStarvationEnabled*/)
{
    UpdateDynamicAttributes(&context->DynamicAttributesList);
}

void TSchedulerElement::UpdateAttributes()
{
    YT_VERIFY(Mutable_);

    auto maxPossibleResourceUsage = Min(TotalResourceLimits_, MaxPossibleResourceUsage_);

    if (ResourceUsageAtUpdate_ == TJobResources()) {
        Attributes_.DominantResource = GetDominantResource(ResourceDemand_, TotalResourceLimits_);
    } else {
        Attributes_.DominantResource = GetDominantResource(ResourceUsageAtUpdate_, TotalResourceLimits_);
    }

    Attributes_.UsageShare = TResourceVector::FromJobResources(ResourceUsageAtUpdate_, TotalResourceLimits_, 0, 1);
    Attributes_.DemandShare = TResourceVector::FromJobResources(ResourceDemand_, TotalResourceLimits_, 0, 1);
    YT_VERIFY(Dominates(Attributes_.DemandShare, Attributes_.UsageShare));

    // TODO(HDRFV): Rethink how and why MaxPossibleUsageShare is computed and used.
    auto possibleUsage = ComputePossibleResourceUsage(maxPossibleResourceUsage);
    Attributes_.MaxPossibleUsageShare = TResourceVector::Min(
        TResourceVector::FromJobResources(possibleUsage, TotalResourceLimits_, 0, 1),
        GetMaxShare());
}

const TSchedulingTagFilter& TSchedulerElement::GetSchedulingTagFilter() const
{
    return EmptySchedulingTagFilter;
}

bool TSchedulerElement::IsRoot() const
{
    return false;
}

bool TSchedulerElement::IsOperation() const
{
    return false;
}

TString TSchedulerElement::GetLoggingAttributesString(const TDynamicAttributes& dynamicAttributes) const
{
    return Format(
        "Status: %v, "
        "DominantResource: %v, "
        "DemandShare: %.6v, "
        "UsageShare: %.6v, "
        "FairShare: %.6v, "
        "Satisfaction: %.4lg, "
        "AdjustedMinShare: %.6v, "
        "GuaranteedResourcesShare: %.6v, "
        "MaxPossibleUsageShare: %.6v,  "
        "Starving: %v, "
        "Weight: %v",
        GetStatus(),
        Attributes_.DominantResource,
        Attributes_.DemandShare,
        GetResourceUsageShare(),
        Attributes_.FairShare,
        dynamicAttributes.SatisfactionRatio,
        Attributes_.AdjustedMinShare,
        Attributes_.GuaranteedResourcesShare,
        Attributes_.MaxPossibleUsageShare,
        GetStarving(),
        GetWeight());
}

TString TSchedulerElement::GetLoggingString(const TDynamicAttributes& dynamicAttributes) const
{
    return Format("Scheduling info for tree %Qv = {%v}", GetTreeId(), GetLoggingAttributesString(dynamicAttributes));
}

bool TSchedulerElement::IsActive(const TDynamicAttributesList& dynamicAttributesList) const
{
    return dynamicAttributesList[GetTreeIndex()].Active;
}

double TSchedulerElement::GetWeight() const
{
    auto specifiedWeight = GetSpecifiedWeight();

    if (auto parent = GetParent();
        parent && parent->IsInferringChildrenWeightsFromHistoricUsageEnabled())
    {
        // TODO(eshcherbin): Make the method of calculating weights from historic usage configurable.
        auto multiplier = Exp2(-1.0 * PersistentAttributes_.HistoricUsageAggregator.GetHistoricUsage());
        auto weight = specifiedWeight ? *specifiedWeight : 1.0;
        return weight * multiplier;
    }

    if (specifiedWeight) {
        return *specifiedWeight;
    }

    if (!TreeConfig_->InferWeightFromMinShareRatioMultiplier) {
        return 1.0;
    }
    double minShareRatio = MaxComponent(Attributes().RecursiveMinShare);

    if (minShareRatio < RatioComputationPrecision) {
        return 1.0;
    }

    double parentMinShareRatio = 1.0;
    if (GetParent()) {
        parentMinShareRatio = MaxComponent(GetParent()->Attributes().RecursiveMinShare);
    }

    if (parentMinShareRatio < RatioComputationPrecision) {
        return 1.0;
    }

    return minShareRatio * (*TreeConfig_->InferWeightFromMinShareRatioMultiplier) / parentMinShareRatio;
}

TCompositeSchedulerElement* TSchedulerElement::GetMutableParent()
{
    return Parent_;
}

const TCompositeSchedulerElement* TSchedulerElement::GetParent() const
{
    return Parent_;
}

TInstant TSchedulerElement::GetStartTime() const
{
    return StartTime_;
}

int TSchedulerElement::GetPendingJobCount() const
{
    return PendingJobCount_;
}

ESchedulableStatus TSchedulerElement::GetStatus() const
{
    return ESchedulableStatus::Normal;
}

bool TSchedulerElement::GetStarving() const
{
    return PersistentAttributes_.Starving;
}

void TSchedulerElement::SetStarving(bool starving)
{
    YT_VERIFY(Mutable_);

    PersistentAttributes_.Starving = starving;
}

TJobResources TSchedulerElement::GetInstantResourceUsage() const
{
    auto resourceUsage = ResourceTreeElement_->GetResourceUsage();
    if (resourceUsage.GetUserSlots() > 0 && resourceUsage.GetMemory() == 0) {
        YT_LOG_WARNING("Found usage of schedulable element %Qv with non-zero user slots and zero memory",
            GetId());
    }
    return resourceUsage;
}

TJobMetrics TSchedulerElement::GetJobMetrics() const
{
    return ResourceTreeElement_->GetJobMetrics();
}

double TSchedulerElement::GetMaxShareRatio() const
{
    return MaxComponent(GetMaxShare());
}

TResourceVector TSchedulerElement::GetResourceUsageShare() const
{
    return TResourceVector::FromJobResources(ResourceTreeElement_->GetResourceUsage(), TotalResourceLimits_, 0, 1);
}

double TSchedulerElement::GetResourceUsageRatio() const
{
    return MaxComponent(GetResourceUsageShare());
}

TResourceVector TSchedulerElement::GetResourceUsageShareWithPrecommit() const
{
    return TResourceVector::FromJobResources(
        ResourceTreeElement_->GetResourceUsageWithPrecommit(), TotalResourceLimits_, 0, 1);
}

TString TSchedulerElement::GetTreeId() const
{
    return TreeId_;
}

bool TSchedulerElement::CheckDemand(const TJobResources& delta, const TFairShareContext& context)
{
    return ResourceTreeElement_->CheckDemand(delta, ResourceDemand(), context.DynamicAttributesFor(this).ResourceUsageDiscount);
}

TJobResources TSchedulerElement::GetLocalAvailableResourceDemand(const TFairShareContext& context) const
{
    return ComputeAvailableResources(
        ResourceDemand(),
        ResourceTreeElement_->GetResourceUsageWithPrecommit(),
        context.DynamicAttributesFor(this).ResourceUsageDiscount);
}

TJobResources TSchedulerElement::GetLocalAvailableResourceLimits(const TFairShareContext& context) const
{
    return ComputeAvailableResources(
        ResourceLimits_,
        ResourceTreeElement_->GetResourceUsageWithPrecommit(),
        context.DynamicAttributesFor(this).ResourceUsageDiscount);
}

void TSchedulerElement::IncreaseHierarchicalResourceUsage(const TJobResources& delta)
{
    TreeHost_->GetResourceTree()->IncreaseHierarchicalResourceUsage(ResourceTreeElement_, delta);
}

TSchedulerElement::TSchedulerElement(
    ISchedulerStrategyHost* host,
    IFairShareTreeHost* treeHost,
    TFairShareStrategyTreeConfigPtr treeConfig,
    TString treeId,
    const NLogging::TLogger& logger)
    : TSchedulerElementFixedState(host, treeHost, std::move(treeConfig), std::move(treeId))
    , ResourceTreeElement_(New<TResourceTreeElement>())
    , Logger(logger)
{ }

TSchedulerElement::TSchedulerElement(
    const TSchedulerElement& other,
    TCompositeSchedulerElement* clonedParent)
    : TSchedulerElementFixedState(other)
    , ResourceTreeElement_(other.ResourceTreeElement_)
    , Logger(other.Logger)
{
    Parent_ = clonedParent;
    Cloned_ = true;
}

ISchedulerStrategyHost* TSchedulerElement::GetHost() const
{
    YT_VERIFY(Mutable_);

    return Host_;
}

IFairShareTreeHost* TSchedulerElement::GetTreeHost() const
{
    return TreeHost_;
}

double TSchedulerElement::ComputeLocalSatisfactionRatio() const
{
    const TResourceVector& fairShare = Attributes_.FairShare;
    TResourceVector usageShare = GetResourceUsageShare();

    // Starvation is disabled for operations in FIFO pool.
    if (Attributes_.FifoIndex >= 0) {
        return InfiniteSatisfactionRatio;
    }

    // Check if the element is over-satisfied.
    if (TResourceVector::Any(usageShare, fairShare, [] (double usage, double fair) { return usage > fair; })) {
        double satisfactionRatio = MaxComponent(
            Div(usageShare, fairShare, /* zeroDivByZero */ 0, /* oneDivByZero */ InfiniteSatisfactionRatio));
        YT_VERIFY(satisfactionRatio > 1);
        return satisfactionRatio;
    }

    double satisfactionRatio = 0.0;

    if (AreAllResourcesBlocked()) {
        // NB(antonkikh): Using MaxComponent would lead to satisfaction ratio being non-monotonous.
        satisfactionRatio = MinComponent(Div(usageShare, fairShare, 1, 1));
    } else {
        satisfactionRatio = 0;
        for (auto resourceType : TEnumTraits<EJobResourceType>::GetDomainValues()) {
            if (!IsResourceBlocked(resourceType) && fairShare[resourceType] != 0) {
                satisfactionRatio = std::max(satisfactionRatio, usageShare[resourceType] / fairShare[resourceType]);
            }
        }
    }

    YT_VERIFY(satisfactionRatio <= 1.0);
    return satisfactionRatio;
}

bool TSchedulerElement::IsResourceBlocked(EJobResourceType resource) const
{
    return Attributes_.DemandShare[resource] == Attributes_.FairShare[resource];
}

bool TSchedulerElement::AreAllResourcesBlocked() const
{
    return Attributes_.DemandShare == Attributes_.FairShare;
}

// Returns true either if there are non-blocked resources and for any such resource |r|: |lhs[r] > rhs[r]|
// or if all resources are blocked and there is at least one resource |r|: |lhs[r] > rhs[r]|.
// Note that this relation is neither reflective nor irreflective and cannot be used for sorting.
//
// This relation is monotonous in several aspects:
// * First argument monotonicity:
//      If |Dominates(vec2, vec1)| and |IsStrictlyDominatesNonBlocked(vec1, rhs)|,
//      then |IsStrictlyDominatesNonBlocked(vec2, rhs)|.
// * Second argument monotonicity:
//      If |Dominates(vec1, vec2)| and |IsStrictlyDominatesNonBlocked(lhs, vec1)|,
//      then |IsStrictlyDominatesNonBlocked(lsh, vec2)|.
// * Blocked resources monotonicity:
//      If |IsStrictlyDominatesNonBlocked(vec, rhs)| and the set of blocked resources increases,
//      then |IsStrictlyDominatesNonBlocked(vec, rhs)|.
// These properties are important for sensible scheduling.
bool TSchedulerElement::IsStrictlyDominatesNonBlocked(const TResourceVector& lhs, const TResourceVector& rhs) const
{
    if (AreAllResourcesBlocked()) {
        return TResourceVector::Any(lhs, rhs, [] (double x, double y) { return x > y; });
    }

    for (int i = 0; i < TResourceVector::Size; i++) {
        if (!IsResourceBlocked(TResourceVector::GetResourceTypeById(i)) && lhs[i] <= rhs[i]) {
            return false;
        }
    }

    return true;
}

ESchedulableStatus TSchedulerElement::GetStatus(double tolerance) const
{
    TResourceVector usageShare = GetResourceUsageShare();

    if (IsStrictlyDominatesNonBlocked(Attributes_.FairShare * tolerance, usageShare)) {
        return ESchedulableStatus::BelowFairShare;
    }

    return ESchedulableStatus::Normal;
}

void TSchedulerElement::CheckForStarvationImpl(
    TDuration minSharePreemptionTimeout,
    TDuration fairSharePreemptionTimeout,
    TInstant now)
{
    YT_VERIFY(Mutable_);

    auto updateStarving = [&] (const TDuration timeout)
    {
        if (!PersistentAttributes_.BelowFairShareSince) {
            PersistentAttributes_.BelowFairShareSince = now;
        } else if (*PersistentAttributes_.BelowFairShareSince < now - timeout) {
            SetStarving(true);
        }
    };

    auto status = GetStatus();
    switch (status) {
        case ESchedulableStatus::BelowFairShare:
            updateStarving(fairSharePreemptionTimeout);
            break;

        case ESchedulableStatus::Normal:
            PersistentAttributes_.BelowFairShareSince = std::nullopt;
            SetStarving(false);
            break;

        default:
            YT_ABORT();
    }
}

void TSchedulerElement::SetOperationAlert(
    TOperationId operationId,
    EOperationAlertType alertType,
    const TError& alert,
    std::optional<TDuration> timeout)
{
    Host_->SetOperationAlert(operationId, alertType, alert, timeout);
}


TJobResources TSchedulerElement::ComputeResourceLimits() const
{
    return Min(GetSpecifiedResourceLimits(), ComputeTotalResourcesOnSuitableNodes());
}

TJobResources TSchedulerElement::ComputeTotalResourcesOnSuitableNodes() const
{
    // Shortcut: if the scheduling tag filter is empty then we just use the resource limits for
    // the tree's nodes filter, which were computed earlier in PreUpdateBottomUp.
    if (GetSchedulingTagFilter() == EmptySchedulingTagFilter) {
        return TotalResourceLimits_ * GetMaxShare();
    }

    auto connectionTime = InstantToCpuInstant(Host_->GetConnectionTime());
    auto delay = DurationToCpuDuration(TreeConfig_->TotalResourceLimitsConsiderDelay);
    if (GetCpuInstant() < connectionTime + delay) {
        // Return infinity during the cluster startup.
        return TJobResources::Infinite();
    } else {
        return GetHost()->GetResourceLimits(TreeConfig_->NodesFilter & GetSchedulingTagFilter()) * GetMaxShare();
    }
}

TJobResources TSchedulerElement::GetTotalResourceLimits() const
{
    return TotalResourceLimits_;
}

TResourceVector TSchedulerElement::GetVectorSuggestion(double suggestion) const
{
    // TODO(ignat): move this YT_VERIFY to another place.
    YT_VERIFY(Dominates(LimitsShare_, Attributes().RecursiveMinShare));
    TResourceVector vectorSuggestion = TResourceVector::FromDouble(suggestion);
    vectorSuggestion = TResourceVector::Max(vectorSuggestion, Attributes().RecursiveMinShare);
    vectorSuggestion = TResourceVector::Min(vectorSuggestion, LimitsShare_);
    return vectorSuggestion;
}

void TSchedulerElement::PrepareUpdateFairShare(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    {
        TWallTimer timer;
        PrepareFairShareByFitFactor(context);
        context->PrepareFairShareByFitFactorTotalTime += timer.GetElapsedCpuTime();
    }
    YT_VERIFY(FairShareByFitFactor_.has_value());
    NDetail::VerifyNondecreasing(*FairShareByFitFactor_, Logger);
    YT_VERIFY(FairShareByFitFactor_->IsTrimmed());

    {
        TWallTimer timer;
        PrepareMaxFitFactorBySuggestion(context);
        context->PrepareMaxFitFactorBySuggestionTotalTime += timer.GetElapsedCpuTime();
    }
    YT_VERIFY(MaxFitFactorBySuggestion_.has_value());
    YT_VERIFY(MaxFitFactorBySuggestion_->LeftFunctionBound() == 0);
    YT_VERIFY(MaxFitFactorBySuggestion_->RightFunctionBound() == 1);
    NDetail::VerifyNondecreasing(*MaxFitFactorBySuggestion_, Logger);
    YT_VERIFY(MaxFitFactorBySuggestion_->IsTrimmed());

    {
        TWallTimer timer;
        FairShareBySuggestion_ = FairShareByFitFactor_->Compose(*MaxFitFactorBySuggestion_);
        context->ComposeTotalTime += timer.GetElapsedCpuTime();
    }
    YT_VERIFY(FairShareBySuggestion_.has_value());
    YT_VERIFY(FairShareBySuggestion_->LeftFunctionBound() == 0);
    YT_VERIFY(FairShareBySuggestion_->RightFunctionBound() == 1);
    NDetail::VerifyNondecreasing(*FairShareBySuggestion_, Logger);
    YT_VERIFY(FairShareBySuggestion_->IsTrimmed());

    {
        TWallTimer timer;
        // TODO(HDRFV, eshcherbin): Extract 1e-15 as a non-magic constant.
        *FairShareBySuggestion_ = NDetail::CompressFunction(*FairShareBySuggestion_, 1e-15);
        context->CompressFunctionTotalTime += timer.GetElapsedCpuTime();
    }
    NDetail::VerifyNondecreasing(*FairShareBySuggestion_, Logger);

    auto sampleFairShareBySuggestion = [&] (double suggestion) -> TResourceVector {
        const TResourceVector suggestedVector = GetVectorSuggestion(suggestion);

        double maxFitFactor;
        if (Dominates(suggestedVector, FairShareByFitFactor_->ValueAt(0))) {
            maxFitFactor = FloatingPointInverseLowerBound(
                /* lo */ 0,
                /* hi */ FairShareByFitFactor_->RightFunctionBound(),
                /* predicate */ [&] (double mid) {
                    return Dominates(suggestedVector, FairShareByFitFactor_->ValueAt(mid));
                });
        } else {
            maxFitFactor = 0;
        }

        return FairShareByFitFactor_->ValueAt(maxFitFactor);
    };

    // TODO(ignat): Fix randomized checks.
    // TODO(ignat): This function is not continuous
    // FairShareBySuggestion_->DoRandomizedCheckContinuous(sampleFairShareBySuggestion, Logger, 20, NLogging::ELogLevel::Fatal);
    std::ignore = sampleFairShareBySuggestion;
}

void TSchedulerElement::PrepareMaxFitFactorBySuggestion(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(FairShareByFitFactor_);

    std::vector<TScalarPiecewiseLinearFunction> mffForComponents;  // Mff stands for "MaxFitFactor".

    for (int r = 0; r < ResourceCount; r++) {
        // Fsbff stands for "FairShareByFitFactor".
        auto fsbffComponent = NDetail::ExtractComponent(r, *FairShareByFitFactor_);
        YT_VERIFY(fsbffComponent.IsTrimmed());

        double limit = std::min(LimitsShare_[r], fsbffComponent.RightFunctionValue());

        double guarantee = Attributes().RecursiveMinShare[r];
        guarantee = std::max(guarantee, fsbffComponent.LeftFunctionValue());
        guarantee = std::min(guarantee, limit);

        auto mffForComponent = std::move(fsbffComponent)
            .Transpose()
            .Narrow(guarantee, limit)
            .TrimLeft()
            .Extend(/* newLeftBound */ 0, /* newRightBound */ 1.0)
            .Trim();
        mffForComponents.push_back(std::move(mffForComponent));
    }

    {
        TWallTimer timer;
        MaxFitFactorBySuggestion_ = PointwiseMin(mffForComponents);
        context->PointwiseMinTotalTime += timer.GetElapsedCpuTime();
    }

    TResourceVector precisionAdjustedRecursiveMinShare = FairShareByFitFactor_->ValueAt(0);
    YT_VERIFY(Dominates(
        Attributes().RecursiveMinShare + TResourceVector::Epsilon(),
        precisionAdjustedRecursiveMinShare));

    auto sampleMaxFitFactor = [&] (double suggestion) -> double {
        const auto suggestedVector = TResourceVector::Max(
            GetVectorSuggestion(suggestion),
            precisionAdjustedRecursiveMinShare);

        return FloatingPointInverseLowerBound(
            /* lo */ 0,
            /* hi */ FairShareByFitFactor_->RightFunctionBound(),
            /* predicate */ [&] (double mid) {
                return Dominates(suggestedVector, FairShareByFitFactor_->ValueAt(mid));
            });
    };

    auto errorHandler = [&] (const auto& /* sample */, double arg) {
        auto mffSegment = MaxFitFactorBySuggestion_->SegmentAt(arg);

        // We are checking the function as if it is continuous.
        // The chance of hitting a discontinuity point by randomized check is close to zero.
        if (mffSegment.IsVertical()) {
            return;
        }

        auto expectedFitFactor = sampleMaxFitFactor(arg);
        auto actualFitFactor = mffSegment.ValueAt(arg);

        auto expectedFairShare = FairShareByFitFactor_->ValueAt(expectedFitFactor);
        auto actualFairShare = FairShareByFitFactor_->ValueAt(actualFitFactor);

        YT_LOG_FATAL(
            "Invalid MaxFitFactorBySuggestio: "
            "Arg: %.16v, "
            "FitFactorDiff: %.16v,"
            "ExpectedFitFactor: %.16v, "
            "ActualFitFactor: %.16v, "
            "FairShareDiff: %.16v, "
            "ExpectedFairShare: %.16v, "
            "ActualFairShare: %.16v, "
            "FitFactorSegmentBounds: {%.16v, %.16v}, "
            "FitFactorSegmentValues: {%.16v, %.16v}",
            arg,
            expectedFitFactor - actualFitFactor,
            expectedFitFactor,
            actualFitFactor,
            expectedFairShare - actualFairShare,
            expectedFairShare,
            actualFairShare,
            mffSegment.LeftBound(), mffSegment.RightBound(),
            mffSegment.LeftValue(), mffSegment.RightValue());
    };

    // TODO(ignat): Fix randomized checks.
    // MaxFitFactorBySuggestion_->DoRandomizedCheckContinuous(sampleMaxFitFactor, 20, errorHandler);
    std::ignore = sampleMaxFitFactor;
    std::ignore = errorHandler;
}

////////////////////////////////////////////////////////////////////////////////

TCompositeSchedulerElement::TCompositeSchedulerElement(
    ISchedulerStrategyHost* host,
    IFairShareTreeHost* treeHost,
    TFairShareStrategyTreeConfigPtr treeConfig,
    NProfiling::TTagId profilingTag,
    const TString& treeId,
    const NLogging::TLogger& logger)
    : TSchedulerElement(host, treeHost, std::move(treeConfig), treeId, logger)
    , ProfilingTag_(profilingTag)
{ }

TCompositeSchedulerElement::TCompositeSchedulerElement(
    const TCompositeSchedulerElement& other,
    TCompositeSchedulerElement* clonedParent)
    : TSchedulerElement(other, clonedParent)
    , TCompositeSchedulerElementFixedState(other)
    , ProfilingTag_(other.ProfilingTag_)
{
    auto cloneChildren = [&] (
        const std::vector<TSchedulerElementPtr>& list,
        THashMap<TSchedulerElementPtr, int>* clonedMap,
        std::vector<TSchedulerElementPtr>* clonedList)
    {
        for (const auto& child : list) {
            auto childClone = child->Clone(this);
            clonedList->push_back(childClone);
            YT_VERIFY(clonedMap->emplace(childClone, clonedList->size() - 1).second);
        }
    };
    cloneChildren(other.EnabledChildren_, &EnabledChildToIndex_, &EnabledChildren_);
    cloneChildren(other.DisabledChildren_, &DisabledChildToIndex_, &DisabledChildren_);
}

void TCompositeSchedulerElement::MarkUnmutable()
{
    TSchedulerElement::MarkUnmutable();
    for (const auto& child : EnabledChildren_) {
        child->MarkUnmutable();
    }
    for (const auto& child : DisabledChildren_) {
        child->MarkUnmutable();
    }
}

int TCompositeSchedulerElement::EnumerateElements(int startIndex, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    startIndex = TSchedulerElement::EnumerateElements(startIndex, context);
    for (const auto& child : EnabledChildren_) {
        startIndex = child->EnumerateElements(startIndex, context);
    }
    return startIndex;
}

void TCompositeSchedulerElement::DisableNonAliveElements()
{
    std::vector<TSchedulerElementPtr> childrenToDisable;
    for (const auto& child : EnabledChildren_) {
        if (!child->IsAlive()) {
            childrenToDisable.push_back(child);
        }
    }
    for (const auto& child : childrenToDisable) {
        DisableChild(child);
    }
    for (const auto& child : EnabledChildren_) {
        child->DisableNonAliveElements();
    }
}

void TCompositeSchedulerElement::UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    YT_VERIFY(Mutable_);

    TSchedulerElement::UpdateTreeConfig(config);

    auto updateChildrenConfig = [&config] (TChildList& list) {
        for (const auto& child : list) {
            child->UpdateTreeConfig(config);
        }
    };

    updateChildrenConfig(EnabledChildren_);
    updateChildrenConfig(DisabledChildren_);
}

void TCompositeSchedulerElement::PreUpdateBottomUp(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    ResourceUsageAtUpdate_ = {};
    ResourceDemand_ = {};

    for (const auto& child : EnabledChildren_) {
        child->PreUpdateBottomUp(context);

        ResourceUsageAtUpdate_ += child->ResourceUsageAtUpdate();
        ResourceDemand_ += child->ResourceDemand();
    }

    TSchedulerElement::PreUpdateBottomUp(context);
}

void TCompositeSchedulerElement::UpdateBottomUp(TDynamicAttributesList* dynamicAttributesList, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    PendingJobCount_ = 0;

    SchedulableChildren_.clear();
    TJobResources maxPossibleChildrenResourceUsage;
    for (const auto& child : EnabledChildren_) {
        child->UpdateBottomUp(dynamicAttributesList, context);

        if (IsInferringChildrenWeightsFromHistoricUsageEnabled()) {
            // NB(eshcherbin): This is a lazy parameters update so it has to be done every time.
            child->PersistentAttributes_.HistoricUsageAggregator.UpdateParameters(
                GetHistoricUsageAggregationParameters());

            // TODO(eshcherbin): should we use vectors instead of ratios?
            auto usageRatio = MaxComponent(child->GetResourceUsageShare());
            child->PersistentAttributes_.HistoricUsageAggregator.UpdateAt(context->Now, usageRatio);
        }

        if (child->IsSchedulable()) {
            SchedulableChildren_.push_back(child);
        }

        PendingJobCount_ += child->GetPendingJobCount();
        maxPossibleChildrenResourceUsage += child->MaxPossibleResourceUsage();
    }

    MaxPossibleResourceUsage_ = Min(maxPossibleChildrenResourceUsage, ResourceLimits_);
    TSchedulerElement::UpdateBottomUp(dynamicAttributesList, context);
}

void TCompositeSchedulerElement::UpdatePreemption(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);
    TSchedulerElement::UpdatePreemption(context);

    if (Parent_) {
        AdjustedFairShareStarvationToleranceLimit_ = std::min(
            GetFairShareStarvationToleranceLimit(),
            Parent_->AdjustedFairShareStarvationToleranceLimit());

        AdjustedMinSharePreemptionTimeoutLimit_ = std::max(
            GetMinSharePreemptionTimeoutLimit(),
            Parent_->AdjustedMinSharePreemptionTimeoutLimit());

        AdjustedFairSharePreemptionTimeoutLimit_ = std::max(
            GetFairSharePreemptionTimeoutLimit(),
            Parent_->AdjustedFairSharePreemptionTimeoutLimit());
    }

    // Propagate updates to children.
    for (const auto& child : EnabledChildren_) {
        child->UpdatePreemption(context);
    }
}

void TCompositeSchedulerElement::UpdateDynamicAttributes(
    TDynamicAttributesList* dynamicAttributesList,
    TUpdateFairShareContext* context)
{
    for (const auto& child : EnabledChildren_) {
        child->UpdateDynamicAttributes(dynamicAttributesList, context);
    }

    TSchedulerElement::UpdateDynamicAttributes(dynamicAttributesList, context);
}

TJobResources TCompositeSchedulerElement::ComputePossibleResourceUsage(TJobResources limit) const
{
    TJobResources additionalUsage;

    for (const auto& child : EnabledChildren_) {
        auto childUsage = child->ComputePossibleResourceUsage(limit);
        limit -= childUsage;
        additionalUsage += childUsage;
    }

    return additionalUsage;
}

double TCompositeSchedulerElement::GetFairShareStarvationToleranceLimit() const
{
    return 1.0;
}

TDuration TCompositeSchedulerElement::GetMinSharePreemptionTimeoutLimit() const
{
    return TDuration::Zero();
}

TDuration TCompositeSchedulerElement::GetFairSharePreemptionTimeoutLimit() const
{
    return TDuration::Zero();
}

void TCompositeSchedulerElement::UpdateDynamicAttributes(TDynamicAttributesList* dynamicAttributesList)
{
    YT_VERIFY(IsActive(*dynamicAttributesList));
    auto& attributes = (*dynamicAttributesList)[GetTreeIndex()];

    if (!IsAlive()) {
        attributes.Active = false;
        return;
    }

    // Satisfaction ratio of a composite element is a minimum of satisfaction ratios of its children.
    attributes.SatisfactionRatio = InfiniteSatisfactionRatio;

    // Declare the element passive if all children are passive.
    attributes.Active = false;
    attributes.BestLeafDescendant = nullptr;

    while (auto bestChild = GetBestActiveChild(*dynamicAttributesList)) {
        const auto& bestChildAttributes = (*dynamicAttributesList)[bestChild->GetTreeIndex()];
        auto childBestLeafDescendant = bestChildAttributes.BestLeafDescendant;
        if (!childBestLeafDescendant->IsAlive()) {
            bestChild->UpdateDynamicAttributes(dynamicAttributesList);
            if (!bestChildAttributes.Active) {
                continue;
            }
            childBestLeafDescendant = bestChildAttributes.BestLeafDescendant;
        }

        attributes.SatisfactionRatio = bestChildAttributes.SatisfactionRatio;
        attributes.BestLeafDescendant = childBestLeafDescendant;
        attributes.Active = true;
        break;
    }
}

void TCompositeSchedulerElement::BuildElementMapping(TRawOperationElementMap* enabledOperationMap, TRawOperationElementMap* disabledOperationMap, TRawPoolMap* poolMap)
{
    for (const auto& child : EnabledChildren_) {
        child->BuildElementMapping(enabledOperationMap, disabledOperationMap, poolMap);
    }
    for (const auto& child : DisabledChildren_) {
        if (child->IsOperation()) {
            child->BuildElementMapping(enabledOperationMap, disabledOperationMap, poolMap);
        }
    }
}

void TCompositeSchedulerElement::IncreaseOperationCount(int delta)
{
    OperationCount_ += delta;

    auto parent = GetMutableParent();
    while (parent) {
        parent->OperationCount() += delta;
        parent = parent->GetMutableParent();
    }
}

void TCompositeSchedulerElement::IncreaseRunningOperationCount(int delta)
{
    RunningOperationCount_ += delta;

    auto parent = GetMutableParent();
    while (parent) {
        parent->RunningOperationCount() += delta;
        parent = parent->GetMutableParent();
    }
}

void TCompositeSchedulerElement::PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled)
{
    auto& attributes = context->DynamicAttributesFor(this);

    if (!IsAlive()) {
        ++context->StageState->DeactivationReasons[EDeactivationReason::IsNotAlive];
        attributes.Active = false;
        return;
    }

    if (TreeConfig_->EnableSchedulingTags &&
        SchedulingTagFilterIndex_ != EmptySchedulingTagFilterIndex &&
        !context->CanSchedule[SchedulingTagFilterIndex_])
    {
        ++context->StageState->DeactivationReasons[EDeactivationReason::UnmatchedSchedulingTag];
        attributes.Active = false;
        return;
    }

    attributes.Active = true;

    auto starving = PersistentAttributes_.Starving;
    aggressiveStarvationEnabled = aggressiveStarvationEnabled || IsAggressiveStarvationEnabled();
    if (starving && aggressiveStarvationEnabled) {
        context->SchedulingStatistics.HasAggressivelyStarvingElements = true;
    }

    // If pool is starving, any child will do.
    bool starvingOnlyForChildren = starving ? false : starvingOnly;
    for (const auto& child : SchedulableChildren_) {
        child->PrescheduleJob(context, starvingOnlyForChildren, aggressiveStarvationEnabled);
    }

    TSchedulerElement::PrescheduleJob(context, starvingOnly, aggressiveStarvationEnabled);

    if (attributes.Active) {
        ++context->StageState->ActiveTreeSize;
    }
}

bool TCompositeSchedulerElement::IsSchedulable() const
{
    return !SchedulableChildren_.empty();
}

bool TCompositeSchedulerElement::HasAggressivelyStarvingElements(TFairShareContext* context, bool aggressiveStarvationEnabled) const
{
    // TODO(ignat): eliminate copy/paste
    aggressiveStarvationEnabled = aggressiveStarvationEnabled || IsAggressiveStarvationEnabled();
    if (PersistentAttributes_.Starving && aggressiveStarvationEnabled) {
        return true;
    }

    for (const auto& child : EnabledChildren_) {
        if (child->HasAggressivelyStarvingElements(context, aggressiveStarvationEnabled)) {
            return true;
        }
    }

    return false;
}

TFairShareScheduleJobResult TCompositeSchedulerElement::ScheduleJob(TFairShareContext* context, bool ignorePacking)
{
    auto& attributes = context->DynamicAttributesFor(this);
    if (!attributes.Active) {
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    auto bestLeafDescendant = attributes.BestLeafDescendant;
    if (!bestLeafDescendant->IsAlive()) {
        UpdateDynamicAttributes(&context->DynamicAttributesList);
        if (!attributes.Active) {
            return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
        }
        bestLeafDescendant = attributes.BestLeafDescendant;
    }

    auto childResult = bestLeafDescendant->ScheduleJob(context, ignorePacking);
    return TFairShareScheduleJobResult(/* finished */ false, /* scheduled */ childResult.Scheduled);
}

bool TCompositeSchedulerElement::IsExplicit() const
{
    return false;
}

bool TCompositeSchedulerElement::IsAggressiveStarvationEnabled() const
{
    return false;
}

bool TCompositeSchedulerElement::IsAggressiveStarvationPreemptionAllowed() const
{
    return true;
}

void TCompositeSchedulerElement::AddChild(TSchedulerElement* child, bool enabled)
{
    YT_VERIFY(Mutable_);

    if (enabled) {
        child->PersistentAttributes_ = TPersistentAttributes();
    }

    auto& map = enabled ? EnabledChildToIndex_ : DisabledChildToIndex_;
    auto& list = enabled ? EnabledChildren_ : DisabledChildren_;
    AddChild(&map, &list, child);
}

void TCompositeSchedulerElement::EnableChild(const TSchedulerElementPtr& child)
{
    YT_VERIFY(Mutable_);

    child->PersistentAttributes_ = TPersistentAttributes();

    RemoveChild(&DisabledChildToIndex_, &DisabledChildren_, child);
    AddChild(&EnabledChildToIndex_, &EnabledChildren_, child);
}

void TCompositeSchedulerElement::DisableChild(const TSchedulerElementPtr& child)
{
    YT_VERIFY(Mutable_);

    if (EnabledChildToIndex_.find(child) == EnabledChildToIndex_.end()) {
        return;
    }

    RemoveChild(&EnabledChildToIndex_, &EnabledChildren_, child);
    AddChild(&DisabledChildToIndex_, &DisabledChildren_, child);
}

void TCompositeSchedulerElement::RemoveChild(TSchedulerElement* child)
{
    YT_VERIFY(Mutable_);

    bool enabled = ContainsChild(EnabledChildToIndex_, child);
    auto& map = enabled ? EnabledChildToIndex_ : DisabledChildToIndex_;
    auto& list = enabled ? EnabledChildren_ : DisabledChildren_;
    RemoveChild(&map, &list, child);
}

bool TCompositeSchedulerElement::IsEnabledChild(TSchedulerElement* child)
{
    return ContainsChild(EnabledChildToIndex_, child);
}

bool TCompositeSchedulerElement::IsEmpty() const
{
    return EnabledChildren_.empty() && DisabledChildren_.empty();
}

ESchedulingMode TCompositeSchedulerElement::GetMode() const
{
    return Mode_;
}

void TCompositeSchedulerElement::SetMode(ESchedulingMode mode)
{
    Mode_ = mode;
}

NProfiling::TTagId TCompositeSchedulerElement::GetProfilingTag() const
{
    return ProfilingTag_;
}

template <class TValue, class TGetter, class TSetter>
TValue TCompositeSchedulerElement::ComputeByFitting(
    const TGetter& getter,
    const TSetter& setter,
    TValue maxSum)
{
    auto checkSum = [&] (double fitFactor) -> bool {
        TValue sum = {};
        for (const auto& child : EnabledChildren_) {
            sum += getter(fitFactor, child);
        }

        if constexpr (std::is_same_v<TValue, TResourceVector>) {
            return Dominates(maxSum, sum);
        } else {
            return maxSum >= sum;
        }
    };

    // Run binary search to compute fit factor.
    double fitFactor = FloatingPointInverseLowerBound(0, 1, checkSum);

    TValue resultSum = {};

    // Compute actual values from fit factor.
    for (const auto& child : EnabledChildren_) {
        TValue value = getter(fitFactor, child);
        resultSum += value;
        setter(child, value);
    }

    return resultSum;
}

void TCompositeSchedulerElement::UpdateMinShare(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    switch (Mode_) {
        case ESchedulingMode::Fifo:
            UpdateMinShareFifo(context);
            break;

        case ESchedulingMode::FairShare:
            UpdateMinShareNormal(context);
            break;

        default:
            YT_ABORT();
    }

    // Recursively update children.
    for (const auto& child : EnabledChildren_) {
        child->UpdateMinShare(context);
    }
}


void TCompositeSchedulerElement::UpdateMinShareFifo(TUpdateFairShareContext* context)
{
    SortedEnabledChildren_ = EnabledChildren_;
    std::sort(
        begin(SortedEnabledChildren_),
        end(SortedEnabledChildren_),
        [&] (const auto& lhs, const auto& rhs) {
            return HasHigherPriorityInFifoMode(lhs.Get(), rhs.Get());
        });

    int index = 0;
    for (const auto& child : SortedEnabledChildren_) {
        auto& childAttributes = child->Attributes();

        childAttributes.RecursiveMinShare = TResourceVector::Zero();
        childAttributes.AdjustedMinShare = TResourceVector::Zero();

        childAttributes.FifoIndex = index;
        ++index;
    }
}

void TCompositeSchedulerElement::UpdateMinShareNormal(TUpdateFairShareContext* context)
{
    TResourceVector minShareSumForPools = {};
    TResourceVector minShareSumForOperations = {};
    for (const auto& child : EnabledChildren_) {
        auto& childAttributes = child->Attributes();
        double minShareRatioByResources = GetMaxResourceRatio(child->GetMinShareResources(), TotalResourceLimits_);

        // NB: Semantics of this MinShare differs from the original one.
        childAttributes.RecursiveMinShare = TResourceVector::FromDouble(minShareRatioByResources);

        // RecursiveMinShare must not be greater than LimitsShare_
        childAttributes.RecursiveMinShare = TResourceVector::Min(
            childAttributes.RecursiveMinShare,
            child->LimitsShare_);

        if (child->IsOperation()) {
            minShareSumForOperations += childAttributes.RecursiveMinShare;
        } else {
            minShareSumForPools += childAttributes.RecursiveMinShare;
        }
    }

    // If min share sum is larger than one, adjust all children min shares to sum up to one.
    if (!Dominates(Attributes_.RecursiveMinShare, minShareSumForPools)) {
        if (!Dominates(Attributes_.RecursiveMinShare + TResourceVector::SmallEpsilon(), minShareSumForPools)) {
            context->Errors.emplace_back(
                "Impossible to satisfy resources guarantees of pool %Qv, "
                "total min share of children pools is too large: %v > %v",
                GetId(),
                minShareSumForPools,
                Attributes_.RecursiveMinShare);
        }

        for (const auto& child : EnabledChildren_) {
            if (child->IsOperation()) {
                child->Attributes().RecursiveMinShare = {};
            }
        }

        // Use binary search instead of division to avoid problems with precision.
        ComputeByFitting(
            /* getter */ [&] (double fitFactor, const TSchedulerElementPtr& child) -> TResourceVector {
                return child->Attributes().RecursiveMinShare * fitFactor;
            },
            /* setter */ [&] (const TSchedulerElementPtr& child, TResourceVector value) {
                child->Attributes().RecursiveMinShare = value;
            },
            /* maxSum */ Attributes().RecursiveMinShare);
    }
    if (!Dominates(Attributes_.RecursiveMinShare, minShareSumForPools + minShareSumForOperations)) {
        // Min shares of operations are fitted silently.
        ComputeByFitting(
            /* getter */ [&] (double fitFactor, const TSchedulerElementPtr& child) -> TResourceVector {
                if (child->IsOperation()) {
                    return child->Attributes().RecursiveMinShare * fitFactor;
                } else {
                    return child->Attributes().RecursiveMinShare;
                }
            },
            /* setter */ [&] (const TSchedulerElementPtr& child, TResourceVector value) {
                child->Attributes().RecursiveMinShare = value;
            },
            /* maxSum */ Attributes().RecursiveMinShare);
    }

    double minWeight = GetMinChildWeight(EnabledChildren_);

    // Compute guaranteed shares.
    ComputeByFitting(
        [&] (double fitFactor, const TSchedulerElementPtr& child) -> TResourceVector {
            const auto& childAttributes = child->Attributes();
            auto result = TResourceVector::FromDouble(fitFactor * child->GetWeight() / minWeight);
            // Never give less than promised by min share.
            result = TResourceVector::Max(result, childAttributes.RecursiveMinShare);
            result = TResourceVector::Min(result, child->LimitsShare());
            return result;
        },
        [&] (const TSchedulerElementPtr& child, TResourceVector value) {
            auto& attributes = child->Attributes();
            attributes.GuaranteedResourcesShare = value;
        },
        Attributes_.GuaranteedResourcesShare);
}

void TCompositeSchedulerElement::PrepareUpdateFairShare(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    for (const auto& child : EnabledChildren_) {
        child->PrepareUpdateFairShare(context);
    }

    TSchedulerElement::PrepareUpdateFairShare(context);
}

void TCompositeSchedulerElement::PrepareFairShareByFitFactor(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    switch (Mode_) {
        case ESchedulingMode::Fifo:
            PrepareFairShareByFitFactorFifo(context);
            break;

        case ESchedulingMode::FairShare:
            PrepareFairShareByFitFactorNormal(context);
            break;

        default:
            YT_ABORT();
    }
}

// Fit factor for a FIFO pool is defined as the number of satisfied children plus the suggestion
// of the first child that is not satisfied, if any.
// A child is said to be satisfied when it is suggested the whole cluster (|suggestion == 1.0|).
// Note that this doesn't necessarily mean that the child's demand is satisfied.
// For an empty FIFO pool fit factor is not well defined.
//
// The unambiguity of the definition of the fit factor follows the fact that the suggestion of
// an unsatisfied child is, by definition, less than 1.
//
// The completeness of the definition of the fit factor (i.e. that it represents any
// sensible allocation for a FIFO pool) follows from the fact that for any child:
// |child->FairShareBySuggestion_(0.0) == TResourceVector::Zero()|, which, in turn, follows from the fact
// that the children of a FIFO pool do not have resource guarantees.
void TCompositeSchedulerElement::PrepareFairShareByFitFactorFifo(TUpdateFairShareContext* context)
{
    TWallTimer timer;
    auto finally = Finally([&] {
        context->PrepareFairShareByFitFactorFifoTotalTime += timer.GetElapsedCpuTime();
    });

    if (SortedEnabledChildren_.empty()) {
        FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Constant(0, 1, TResourceVector::Zero());
        return;
    }

    double rightFunctionBound = SortedEnabledChildren_.size();
    FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Constant(0, rightFunctionBound, TResourceVector::Zero());

    double currentRightBound = 0;
    for (const auto& child : SortedEnabledChildren_) {
        const auto& childFSBS = *child->FairShareBySuggestion_;

        // NB(antonkikh): The resulting function must be continuous on borders between children (see the function comment).
        YT_VERIFY(childFSBS.IsTrimmedLeft() && childFSBS.IsTrimmedRight());
        // Children of FIFO pools don't have guaranteed resources.
        YT_VERIFY(childFSBS.LeftFunctionValue() == TResourceVector::Zero());

        // TODO(antonkikh): This can be implemented much more efficiently by concatenating functions instead of adding.
        *FairShareByFitFactor_ += childFSBS
            .Shift(/* deltaArgument */ currentRightBound)
            .Extend(/* newLeftBound */ 0.0, /* newRightBound */ rightFunctionBound);
        currentRightBound += 1;
    }

    YT_VERIFY(currentRightBound == rightFunctionBound);
}

void TCompositeSchedulerElement::PrepareFairShareByFitFactorNormal(TUpdateFairShareContext* context)
{
    TWallTimer timer;
    auto finally = Finally([&] {
        context->PrepareFairShareByFitFactorNormalTotalTime += timer.GetElapsedCpuTime();
    });

    if (EnabledChildren_.empty()) {
        FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Constant(0, 1, TResourceVector::Zero());
    } else {
        std::vector<TVectorPiecewiseLinearFunction> childrenFunctions;

        double minWeight = GetMinChildWeight(EnabledChildren_);
        for (const auto& child : EnabledChildren_) {
            const auto& childFSBS = *child->FairShareBySuggestion_;

            auto childFunction = childFSBS
                .ScaleArgument(child->GetWeight() / minWeight)
                .ExtendRight(/* newRightBound */ 1.0);

            childrenFunctions.emplace_back(std::move(childFunction));
        }

        FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Sum(childrenFunctions);
    }

    // TODO(ignat): Fix randomized checks.
    // TODO(ignat): This function is not continuous
    // FairShareByFitFactor_->DoRandomizedCheckContinuous(
    //     [&] (double fitFactor) {
    //         return std::accumulate(
    //             begin(EnabledChildren_),
    //             end(EnabledChildren_),
    //             TResourceVector::Zero(),
    //             [&] (TResourceVector sum, const auto& child) {
    //                 return sum + child->FairShareBySuggestion()->ValueAt(std::min(1.0, fitFactor * (child->GetWeight() / minWeight)));
    //             });
    //     },
    //     /* logger */ Logger,
    //     /* sampleCount */ 20,
    //     /* logLevel */ NLogging::ELogLevel::Fatal);
}

TResourceVector TCompositeSchedulerElement::DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    switch (Mode_) {
        case ESchedulingMode::Fifo:
            return DoUpdateFairShareFifo(suggestion, context);

        case ESchedulingMode::FairShare:
            return DoUpdateFairShareNormal(suggestion, context);

        default:
            YT_ABORT();
    }
}

TResourceVector TCompositeSchedulerElement::DoUpdateFairShareFifo(double suggestion, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    double fitFactor = MaxFitFactorBySuggestion_->ValueAt(suggestion);

    TResourceVector usedFairShare;
    if (!SortedEnabledChildren_.empty()) {
        // See |TCompositeSchedulerElement::PrepareFairShareByFitFactorFifo| for the definition of fit factor for FIFO pools.
        int satisfiedChildrenCount = static_cast<int>(fitFactor);
        double notSatisfiedChildSuggestion = fitFactor - satisfiedChildrenCount;

        usedFairShare = TResourceVector::Zero();

        YT_VERIFY(satisfiedChildrenCount <= SortedEnabledChildren_.size());
        for (int i = 0; i < satisfiedChildrenCount; i++) {
            usedFairShare += SortedEnabledChildren_[i]->DoUpdateFairShare(1.0, context);
        }

        if (notSatisfiedChildSuggestion != 0) {
            YT_VERIFY(satisfiedChildrenCount < SortedEnabledChildren_.size());
            usedFairShare += SortedEnabledChildren_[satisfiedChildrenCount]
                ->DoUpdateFairShare(notSatisfiedChildSuggestion, context);
        }
    } else {
        // Fit factor is not well defined for an empty FIFO pool.
        usedFairShare = TResourceVector::Zero();
    }

    YT_LOG_WARNING_UNLESS(
        TResourceVector::Near(usedFairShare, FairShareBySuggestion()->ValueAt(suggestion), 1e-4 * MaxComponent(usedFairShare)),
        "Fair share significantly differs from predicted in FIFO pool ("
        "Suggestion: %.10v, "
        "UsedFairShare: %.10v, "
        "FSPredicted: %.10v, "
        "FSBFFPredicted: %.10v, "
        "FitFactor: %.10v, "
        "ChildrenCount: %v, "
        "OperationCount: %v, "
        "RunningOperationCount: %v)",
        suggestion,
        usedFairShare,
        FairShareBySuggestion()->ValueAt(suggestion),
        FairShareByFitFactor()->ValueAt(fitFactor),
        fitFactor,
        EnabledChildren_.size(),
        OperationCount(),
        RunningOperationCount());
    YT_LOG_WARNING_UNLESS(
        Dominates(GetVectorSuggestion(suggestion) + TResourceVector::Epsilon(), usedFairShare),
        "Used significantly more fair share than was suggested (Suggestion: %.6v, UsedFairShare: %.6v)",
        GetVectorSuggestion(suggestion),
        usedFairShare);

    SetFairShare(usedFairShare);
    return usedFairShare;
}

TResourceVector TCompositeSchedulerElement::DoUpdateFairShareNormal(double suggestion, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    double fitFactor = MaxFitFactorBySuggestion()->ValueAt(suggestion);
    double minWeight = GetMinChildWeight(EnabledChildren_);

    TResourceVector usedFairShare = {};
    for (const auto& child : EnabledChildren_) {
        double childSuggestion = std::min(1.0, fitFactor * (child->GetWeight() / minWeight));
        usedFairShare += child->DoUpdateFairShare(childSuggestion, context);
    }

    YT_LOG_WARNING_UNLESS(
        TResourceVector::Near(usedFairShare, FairShareBySuggestion()->ValueAt(suggestion), 1e-4 * MaxComponent(usedFairShare)),
        "Fair share significantly differs from predicted in normal pool ("
        "Suggestion: %.10v, "
        "UsedFairShare: %.10v, "
        "FSPredicted: %.10v, "
        "FSBFFPredicted: %.10v, "
        "FSChildrenSumPredicted: %.10v, "
        "FitFactor: %.10v, "
        "MinWeight: %.10v, "
        "ChildrenCount: %v, "
        "OperationCount: %v, "
        "RunningOperationCount: %v)",
        suggestion,
        usedFairShare,
        FairShareBySuggestion()->ValueAt(suggestion),
        FairShareByFitFactor()->ValueAt(fitFactor),
        std::accumulate(
            begin(EnabledChildren_),
            end(EnabledChildren_),
            TResourceVector::Zero(),
            [&] (TResourceVector sum, const auto& child) {
                return sum + child->FairShareBySuggestion()->ValueAt(std::min(1.0, fitFactor * (child->GetWeight() / minWeight)));
            }),
        fitFactor,
        minWeight,
        EnabledChildren_.size(),
        OperationCount(),
        RunningOperationCount());
    YT_LOG_WARNING_UNLESS(
        Dominates(GetVectorSuggestion(suggestion) + TResourceVector::Epsilon(), usedFairShare),
        "Used significantly more fair share than was suggested (Suggestion: %.6v, UsedFairShare: %.6v)",
        GetVectorSuggestion(suggestion),
        usedFairShare);

    SetFairShare(usedFairShare);
    return usedFairShare;
}

double TCompositeSchedulerElement::GetMinChildWeight(const TChildList& children)
{
    double minWeight = std::numeric_limits<double>::max();
    for (const auto& child : children) {
        if (child->GetWeight() > RatioComputationPrecision) {
            minWeight = std::min(minWeight, child->GetWeight());
        }
    }
    return minWeight;
}

TSchedulerElement* TCompositeSchedulerElement::GetBestActiveChild(const TDynamicAttributesList& dynamicAttributesList) const
{
    switch (Mode_) {
        case ESchedulingMode::Fifo:
            return GetBestActiveChildFifo(dynamicAttributesList);
        case ESchedulingMode::FairShare:
            return GetBestActiveChildFairShare(dynamicAttributesList);
        default:
            YT_ABORT();
    }
}

TSchedulerElement* TCompositeSchedulerElement::GetBestActiveChildFifo(const TDynamicAttributesList& dynamicAttributesList) const
{
    TSchedulerElement* bestChild = nullptr;
    for (const auto& child : SchedulableChildren_) {
        if (child->IsActive(dynamicAttributesList)) {
            if (bestChild && HasHigherPriorityInFifoMode(bestChild, child.Get())) {
                continue;
            }

            bestChild = child.Get();
        }
    }
    return bestChild;
}

TSchedulerElement* TCompositeSchedulerElement::GetBestActiveChildFairShare(const TDynamicAttributesList& dynamicAttributesList) const
{
    TSchedulerElement* bestChild = nullptr;
    double bestChildSatisfactionRatio = InfiniteSatisfactionRatio;
    for (const auto& child : SchedulableChildren_) {
        if (child->IsActive(dynamicAttributesList)) {
            double childSatisfactionRatio = dynamicAttributesList[child->GetTreeIndex()].SatisfactionRatio;
            if (!bestChild || childSatisfactionRatio < bestChildSatisfactionRatio) {
                bestChild = child.Get();
                bestChildSatisfactionRatio = childSatisfactionRatio;
            }
        }
    }
    return bestChild;
}

void TCompositeSchedulerElement::AddChild(
    TChildMap* map,
    TChildList* list,
    const TSchedulerElementPtr& child)
{
    list->push_back(child);
    YT_VERIFY(map->emplace(child, list->size() - 1).second);
}

void TCompositeSchedulerElement::RemoveChild(
    TChildMap* map,
    TChildList* list,
    const TSchedulerElementPtr& child)
{
    auto it = map->find(child);
    YT_VERIFY(it != map->end());
    if (child == list->back()) {
        list->pop_back();
    } else {
        int index = it->second;
        std::swap((*list)[index], list->back());
        list->pop_back();
        (*map)[(*list)[index]] = index;
    }
    map->erase(it);
}

bool TCompositeSchedulerElement::ContainsChild(
    const TChildMap& map,
    const TSchedulerElementPtr& child)
{
    return map.find(child) != map.end();
}

bool TCompositeSchedulerElement::HasHigherPriorityInFifoMode(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const
{
    for (auto parameter : FifoSortParameters_) {
        switch (parameter) {
            case EFifoSortParameter::Weight:
                if (lhs->GetWeight() != rhs->GetWeight()) {
                    return lhs->GetWeight() > rhs->GetWeight();
                }
                break;
            case EFifoSortParameter::StartTime: {
                const auto& lhsStartTime = lhs->GetStartTime();
                const auto& rhsStartTime = rhs->GetStartTime();
                if (lhsStartTime != rhsStartTime) {
                    return lhsStartTime < rhsStartTime;
                }
                break;
            }
            case EFifoSortParameter::PendingJobCount: {
                int lhsPendingJobCount = lhs->GetPendingJobCount();
                int rhsPendingJobCount = rhs->GetPendingJobCount();
                if (lhsPendingJobCount != rhsPendingJobCount) {
                    return lhsPendingJobCount < rhsPendingJobCount;
                }
                break;
            }
            default:
                YT_ABORT();
        }
    }
    return false;
}

int TCompositeSchedulerElement::GetAvailableRunningOperationCount() const
{
    return std::max(GetMaxRunningOperationCount() - RunningOperationCount_, 0);
}

////////////////////////////////////////////////////////////////////////////////

TPoolFixedState::TPoolFixedState(TString id)
    : Id_(std::move(id))
{ }

////////////////////////////////////////////////////////////////////////////////

TPool::TPool(
    ISchedulerStrategyHost* host,
    IFairShareTreeHost* treeHost,
    const TString& id,
    TPoolConfigPtr config,
    bool defaultConfigured,
    TFairShareStrategyTreeConfigPtr treeConfig,
    NProfiling::TTagId profilingTag,
    const TString& treeId,
    const NLogging::TLogger& logger)
    : TCompositeSchedulerElement(
        host,
        treeHost,
        std::move(treeConfig),
        profilingTag,
        treeId,
        NLogging::TLogger(logger)
            .AddTag("PoolId: %v", id)
            .AddTag("SchedulingMode: %v", config->Mode))
    , TPoolFixedState(id)
{
    DoSetConfig(std::move(config));
    DefaultConfigured_ = defaultConfigured;
}

TPool::TPool(const TPool& other, TCompositeSchedulerElement* clonedParent)
    : TCompositeSchedulerElement(other, clonedParent)
    , TPoolFixedState(other)
    , Config_(other.Config_)
    , SchedulingTagFilter_(other.SchedulingTagFilter_)
{ }

bool TPool::IsDefaultConfigured() const
{
    return DefaultConfigured_;
}

bool TPool::IsEphemeralInDefaultParentPool() const
{
    return EphemeralInDefaultParentPool_;
}

void TPool::SetUserName(const std::optional<TString>& userName)
{
    UserName_ = userName;
}

const std::optional<TString>& TPool::GetUserName() const
{
    return UserName_;
}

TPoolConfigPtr TPool::GetConfig() const
{
    return Config_;
}

void TPool::SetConfig(TPoolConfigPtr config)
{
    YT_VERIFY(Mutable_);

    DoSetConfig(std::move(config));
    DefaultConfigured_ = false;
}

void TPool::SetDefaultConfig()
{
    YT_VERIFY(Mutable_);

    DoSetConfig(New<TPoolConfig>());
    DefaultConfigured_ = true;
}

void TPool::SetEphemeralInDefaultParentPool()
{
    YT_VERIFY(Mutable_);

    EphemeralInDefaultParentPool_ = true;
}

bool TPool::IsAggressiveStarvationPreemptionAllowed() const
{
    return Config_->AllowAggressiveStarvationPreemption.value_or(true);
}

bool TPool::IsExplicit() const
{
    // NB: This is no coincidence.
    return !DefaultConfigured_;
}

bool TPool::IsAggressiveStarvationEnabled() const
{
    return Config_->EnableAggressiveStarvation;
}

TString TPool::GetId() const
{
    return Id_;
}

std::optional<double> TPool::GetSpecifiedWeight() const
{
    return Config_->Weight;
}

TJobResources TPool::GetMinShareResources() const
{
    return ToJobResources(Config_->MinShareResources, {});
}

TResourceVector TPool::GetMaxShare() const
{
    return TResourceVector::FromDouble(Config_->MaxShareRatio.value_or(1.0));
}

ESchedulableStatus TPool::GetStatus() const
{
    return TSchedulerElement::GetStatus(Attributes_.AdjustedFairShareStarvationTolerance);
}

double TPool::GetFairShareStarvationTolerance() const
{
    return Config_->FairShareStarvationTolerance.value_or(Parent_->Attributes().AdjustedFairShareStarvationTolerance);
}

TDuration TPool::GetMinSharePreemptionTimeout() const
{
    return Config_->MinSharePreemptionTimeout.value_or(Parent_->Attributes().AdjustedMinSharePreemptionTimeout);
}

TDuration TPool::GetFairSharePreemptionTimeout() const
{
    return Config_->FairSharePreemptionTimeout.value_or(Parent_->Attributes().AdjustedFairSharePreemptionTimeout);
}

double TPool::GetFairShareStarvationToleranceLimit() const
{
    return Config_->FairShareStarvationToleranceLimit.value_or(TreeConfig_->FairShareStarvationToleranceLimit);
}

TDuration TPool::GetMinSharePreemptionTimeoutLimit() const
{
    return Config_->MinSharePreemptionTimeoutLimit.value_or(TreeConfig_->MinSharePreemptionTimeoutLimit);
}

TDuration TPool::GetFairSharePreemptionTimeoutLimit() const
{
    return Config_->FairSharePreemptionTimeoutLimit.value_or(TreeConfig_->FairSharePreemptionTimeoutLimit);
}

void TPool::SetStarving(bool starving)
{
    YT_VERIFY(Mutable_);

    if (starving && !GetStarving()) {
        TSchedulerElement::SetStarving(true);
        YT_LOG_INFO("Pool is now starving (Status: %v)", GetStatus());
    } else if (!starving && GetStarving()) {
        TSchedulerElement::SetStarving(false);
        YT_LOG_INFO("Pool is no longer starving");
    }
}

void TPool::CheckForStarvation(TInstant now)
{
    YT_VERIFY(Mutable_);

    TSchedulerElement::CheckForStarvationImpl(
        Attributes_.AdjustedMinSharePreemptionTimeout,
        Attributes_.AdjustedFairSharePreemptionTimeout,
        now);
}

const TSchedulingTagFilter& TPool::GetSchedulingTagFilter() const
{
    return SchedulingTagFilter_;
}

int TPool::GetMaxRunningOperationCount() const
{
    return Config_->MaxRunningOperationCount.value_or(TreeConfig_->MaxRunningOperationCountPerPool);
}

int TPool::GetMaxOperationCount() const
{
    return Config_->MaxOperationCount.value_or(TreeConfig_->MaxOperationCountPerPool);
}

std::vector<EFifoSortParameter> TPool::GetFifoSortParameters() const
{
    return FifoSortParameters_;
}

bool TPool::AreImmediateOperationsForbidden() const
{
    return Config_->ForbidImmediateOperations;
}

THashSet<TString> TPool::GetAllowedProfilingTags() const
{
    return Config_->AllowedProfilingTags;
}

bool TPool::IsInferringChildrenWeightsFromHistoricUsageEnabled() const
{
    return Config_->InferChildrenWeightsFromHistoricUsage;
}

THistoricUsageAggregationParameters TPool::GetHistoricUsageAggregationParameters() const
{
    return THistoricUsageAggregationParameters(Config_->HistoricUsageConfig);
}

TSchedulerElementPtr TPool::Clone(TCompositeSchedulerElement* clonedParent)
{
    return New<TPool>(*this, clonedParent);
}

void TPool::AttachParent(TCompositeSchedulerElement* parent)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(!Parent_);
    YT_VERIFY(RunningOperationCount_ == 0);
    YT_VERIFY(OperationCount_ == 0);

    parent->AddChild(this);
    Parent_ = parent;
    TreeHost_->GetResourceTree()->AttachParent(ResourceTreeElement_, parent->ResourceTreeElement_);

    YT_LOG_DEBUG("Pool %Qv is attached to pool %Qv",
        Id_,
        parent->GetId());
}

void TPool::ChangeParent(TCompositeSchedulerElement* newParent)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(Parent_);
    YT_VERIFY(newParent);
    YT_VERIFY(Parent_ != newParent);

    Parent_->IncreaseOperationCount(-OperationCount());
    Parent_->IncreaseRunningOperationCount(-RunningOperationCount());
    bool enabled = Parent_->IsEnabledChild(this);
    Parent_->RemoveChild(this);

    auto oldParentId = Parent_->GetId();
    Parent_ = newParent;
    TreeHost_->GetResourceTree()->ChangeParent(ResourceTreeElement_, newParent->ResourceTreeElement_);

    Parent_->AddChild(this, enabled);
    Parent_->IncreaseOperationCount(OperationCount());
    Parent_->IncreaseRunningOperationCount(RunningOperationCount());

    YT_LOG_INFO("Parent pool is changed (NewParent: %v, OldParent: %v)",
        Parent_->GetId(),
        oldParentId);
}

void TPool::DetachParent()
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(Parent_);
    YT_VERIFY(RunningOperationCount() == 0);
    YT_VERIFY(OperationCount() == 0);

    const auto& oldParentId = Parent_->GetId();
    Parent_->RemoveChild(this);
    TreeHost_->GetResourceTree()->DetachParent(ResourceTreeElement_);

    YT_LOG_DEBUG("Pool %Qv is detached from pool %Qv",
        Id_,
        oldParentId);
}

void TPool::DoSetConfig(TPoolConfigPtr newConfig)
{
    YT_VERIFY(Mutable_);

    Config_ = std::move(newConfig);
    FifoSortParameters_ = Config_->FifoSortParameters;
    Mode_ = Config_->Mode;
    SchedulingTagFilter_ = TSchedulingTagFilter(Config_->SchedulingTagFilter);
}

TJobResources TPool::GetSpecifiedResourceLimits() const
{
    return ToJobResources(Config_->ResourceLimits, TJobResources::Infinite());
}

void TPool::BuildElementMapping(TRawOperationElementMap* enabledOperationMap, TRawOperationElementMap* disabledOperationMap, TRawPoolMap* poolMap)
{
    poolMap->emplace(GetId(), this);
    TCompositeSchedulerElement::BuildElementMapping(enabledOperationMap, disabledOperationMap, poolMap);
}

////////////////////////////////////////////////////////////////////////////////

TOperationElementFixedState::TOperationElementFixedState(
    IOperationStrategyHost* operation,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig)
    : OperationId_(operation->GetId())
    , UnschedulableReason_(operation->CheckUnschedulable())
    , SlotIndex_(std::nullopt)
    , UserName_(operation->GetAuthenticatedUser())
    , Operation_(operation)
    , ControllerConfig_(std::move(controllerConfig))
{ }

////////////////////////////////////////////////////////////////////////////////

TOperationElementSharedState::TOperationElementSharedState(
    int updatePreemptableJobsListLoggingPeriod,
    const NLogging::TLogger& logger)
    : UpdatePreemptableJobsListLoggingPeriod_(updatePreemptableJobsListLoggingPeriod)
    , Logger(logger)
{ }

TJobResources TOperationElementSharedState::Disable()
{
    TWriterGuard guard(JobPropertiesMapLock_);

    Enabled_ = false;

    TJobResources resourceUsage;
    for (const auto& [jodId, properties] : JobPropertiesMap_) {
        resourceUsage += properties.ResourceUsage;
    }

    NonpreemptableResourceUsage_ = {};
    AggressivelyPreemptableResourceUsage_ = {};
    RunningJobCount_ = 0;
    PreemptableJobs_.clear();
    AggressivelyPreemptableJobs_.clear();
    NonpreemptableJobs_.clear();
    JobPropertiesMap_.clear();

    return resourceUsage;
}

void TOperationElementSharedState::Enable()
{
    TWriterGuard guard(JobPropertiesMapLock_);

    YT_VERIFY(!Enabled_);
    Enabled_ = true;
}

bool TOperationElementSharedState::Enabled()
{
    TReaderGuard guard(JobPropertiesMapLock_);
    return Enabled_;
}

void TOperationElementSharedState::RecordHeartbeat(
    const TPackingHeartbeatSnapshot& heartbeatSnapshot,
    const TFairShareStrategyPackingConfigPtr& packingConfig)
{
    HeartbeatStatistics_.RecordHeartbeat(heartbeatSnapshot, packingConfig);
}

bool TOperationElementSharedState::CheckPacking(
    const TOperationElement* operationElement,
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

TJobResources TOperationElementSharedState::IncreaseJobResourceUsage(
    TJobId jobId,
    const TJobResources& resourcesDelta)
{
    TWriterGuard guard(JobPropertiesMapLock_);

    if (!Enabled_) {
        return {};
    }

    IncreaseJobResourceUsage(GetJobProperties(jobId), resourcesDelta);
    return resourcesDelta;
}

void TOperationElementSharedState::UpdatePreemptableJobsList(
    const TResourceVector& fairShare,
    const TJobResources& totalResourceLimits,
    double preemptionSatisfactionThreshold,
    double aggressivePreemptionSatisfactionThreshold,
    int* moveCount,
    TOperationElement* operationElement)
{
    TWriterGuard guard(JobPropertiesMapLock_);

    auto getUsageShare = [&] (const TJobResources& resourceUsage) -> TResourceVector {
        return TResourceVector::FromJobResources(resourceUsage, totalResourceLimits, 0, 1);
    };

    auto balanceLists = [&] (
        TJobIdList* left,
        TJobIdList* right,
        TJobResources resourceUsage,
        TResourceVector fairShareBound,
        std::function<void(TJobProperties*)> onMovedLeftToRight,
        std::function<void(TJobProperties*)> onMovedRightToLeft)
    {
        // Move from left to right and decrease |resourceUsage| until the next move causes
        // |operationElement->IsStrictlyDominatesNonBlocked(fairShareBound, getUsageShare(nextUsage))| to become true.
        while (!left->empty()) {
            auto jobId = left->back();
            auto* jobProperties = GetJobProperties(jobId);

            auto nextUsage = resourceUsage - jobProperties->ResourceUsage;
            if (operationElement->IsStrictlyDominatesNonBlocked(fairShareBound, getUsageShare(nextUsage))) {
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
        while (!right->empty() &&
            operationElement->IsStrictlyDominatesNonBlocked(fairShareBound, getUsageShare(resourceUsage)))
        {
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

    auto setPreemptable = [] (TJobProperties* properties) {
        properties->Preemptable = true;
        properties->AggressivelyPreemptable = true;
    };

    auto setAggressivelyPreemptable = [] (TJobProperties* properties) {
        properties->Preemptable = false;
        properties->AggressivelyPreemptable = true;
    };

    auto setNonPreemptable = [] (TJobProperties* properties) {
        properties->Preemptable = false;
        properties->AggressivelyPreemptable = false;
    };

    bool enableLogging =
        (UpdatePreemptableJobsListCount_.fetch_add(1) % UpdatePreemptableJobsListLoggingPeriod_) == 0 ||
        operationElement->DetailedLogsEnabled();

    YT_LOG_DEBUG_IF(enableLogging,
        "Update preemptable lists inputs (FairShare: %.6v, TotalResourceLimits: %v, "
        "PreemptionSatisfactionThreshold: %v, AggressivePreemptionSatisfactionThreshold: %v)",
        fairShare,
        FormatResources(totalResourceLimits),
        preemptionSatisfactionThreshold,
        aggressivePreemptionSatisfactionThreshold);

    // NB: We need 2 iterations since thresholds may change significantly such that we need
    // to move job from preemptable list to non-preemptable list through aggressively preemptable list.
    for (int iteration = 0; iteration < 2; ++iteration) {
        YT_LOG_DEBUG_IF(enableLogging,
            "Preemptable lists usage bounds before update (NonpreemptableResourceUsage: %v, AggressivelyPreemptableResourceUsage: %v, Iteration: %v)",
            FormatResources(NonpreemptableResourceUsage_),
            FormatResources(AggressivelyPreemptableResourceUsage_),
            iteration);

        auto startNonPreemptableAndAggressivelyPreemptableResourceUsage_ = NonpreemptableResourceUsage_ + AggressivelyPreemptableResourceUsage_;

        NonpreemptableResourceUsage_ = balanceLists(
            &NonpreemptableJobs_,
            &AggressivelyPreemptableJobs_,
            NonpreemptableResourceUsage_,
            fairShare * aggressivePreemptionSatisfactionThreshold,
            setAggressivelyPreemptable,
            setNonPreemptable);

        auto nonpreemptableAndAggressivelyPreemptableResourceUsage_ = balanceLists(
            &AggressivelyPreemptableJobs_,
            &PreemptableJobs_,
            startNonPreemptableAndAggressivelyPreemptableResourceUsage_,
            Preemptable_ ? fairShare * preemptionSatisfactionThreshold : TResourceVector::Infinity(),
            setPreemptable,
            setAggressivelyPreemptable);

        AggressivelyPreemptableResourceUsage_ = nonpreemptableAndAggressivelyPreemptableResourceUsage_ - NonpreemptableResourceUsage_;
    }

    YT_LOG_DEBUG_IF(enableLogging,
        "Preemptable lists usage bounds after update (NonpreemptableResourceUsage: %v, AggressivelyPreemptableResourceUsage: %v)",
        FormatResources(NonpreemptableResourceUsage_),
        FormatResources(AggressivelyPreemptableResourceUsage_));
}

void TOperationElementSharedState::SetPreemptable(bool value)
{
    Preemptable_.store(value);
}

bool TOperationElementSharedState::GetPreemptable() const
{
    return Preemptable_;
}

bool TOperationElementSharedState::IsJobKnown(TJobId jobId) const
{
    TReaderGuard guard(JobPropertiesMapLock_);

    return JobPropertiesMap_.find(jobId) != JobPropertiesMap_.end();
}

bool TOperationElementSharedState::IsJobPreemptable(TJobId jobId, bool aggressivePreemptionEnabled) const
{
    TReaderGuard guard(JobPropertiesMapLock_);

    if (!Enabled_) {
        return false;
    }

    const auto* properties = GetJobProperties(jobId);
    return aggressivePreemptionEnabled ? properties->AggressivelyPreemptable : properties->Preemptable;
}

int TOperationElementSharedState::GetRunningJobCount() const
{
    return RunningJobCount_;
}

int TOperationElementSharedState::GetPreemptableJobCount() const
{
    TReaderGuard guard(JobPropertiesMapLock_);

    return PreemptableJobs_.size();
}

int TOperationElementSharedState::GetAggressivelyPreemptableJobCount() const
{
    TReaderGuard guard(JobPropertiesMapLock_);

    return AggressivelyPreemptableJobs_.size();
}

std::optional<TJobResources> TOperationElementSharedState::AddJob(TJobId jobId, const TJobResources& resourceUsage, bool force)
{
    TWriterGuard guard(JobPropertiesMapLock_);

    if (!Enabled_ && !force) {
        return std::nullopt;
    }

    LastScheduleJobSuccessTime_ = TInstant::Now();

    PreemptableJobs_.push_back(jobId);

    auto it = JobPropertiesMap_.emplace(
        jobId,
        TJobProperties(
            /* preemptable */ true,
            /* aggressivelyPreemptable */ true,
            --PreemptableJobs_.end(),
            {}));
    YT_VERIFY(it.second);

    ++RunningJobCount_;

    IncreaseJobResourceUsage(&it.first->second, resourceUsage);
    return resourceUsage;
}

void TOperationElementSharedState::UpdatePreemptionStatusStatistics(EOperationPreemptionStatus status)
{
    auto guard = Guard(PreemptionStatusStatisticsLock_);

    PreemptionStatusStatistics_[status] += 1;
}

TPreemptionStatusStatisticsVector TOperationElementSharedState::GetPreemptionStatusStatistics() const
{
    auto guard = Guard(PreemptionStatusStatisticsLock_);

    return PreemptionStatusStatistics_;
}

void TOperationElementSharedState::OnOperationDeactivated(const TFairShareContext& context, EDeactivationReason reason)
{
    auto& shard = StateShards_[context.SchedulingContext->GetNodeShardId()];
    ++shard.DeactivationReasons[reason];
    ++shard.DeactivationReasonsFromLastNonStarvingTime[reason];
}

TEnumIndexedVector<EDeactivationReason, int> TOperationElementSharedState::GetDeactivationReasons() const
{
    TEnumIndexedVector<EDeactivationReason, int> result;
    for (const auto& shard : StateShards_) {
        for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
            result[reason] += shard.DeactivationReasons[reason].load();
        }
    }
    return result;
}

TEnumIndexedVector<EDeactivationReason, int> TOperationElementSharedState::GetDeactivationReasonsFromLastNonStarvingTime() const
{
    TEnumIndexedVector<EDeactivationReason, int> result;
    for (const auto& shard : StateShards_) {
        for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
            result[reason] += shard.DeactivationReasonsFromLastNonStarvingTime[reason].load();
        }
    }
    return result;
}

void TOperationElementSharedState::ResetDeactivationReasonsFromLastNonStarvingTime()
{
    for (auto& shard : StateShards_) {
        for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
            shard.DeactivationReasonsFromLastNonStarvingTime[reason].store(0);
        }
    }
}

TInstant TOperationElementSharedState::GetLastScheduleJobSuccessTime() const
{
    TReaderGuard guard(JobPropertiesMapLock_);

    return LastScheduleJobSuccessTime_;
}

void TOperationElement::OnOperationDeactivated(const TFairShareContext& context, EDeactivationReason reason)
{
    OperationElementSharedState_->OnOperationDeactivated(context, reason);
}

TEnumIndexedVector<EDeactivationReason, int> TOperationElement::GetDeactivationReasons() const
{
    return OperationElementSharedState_->GetDeactivationReasons();
}

TEnumIndexedVector<EDeactivationReason, int> TOperationElement::GetDeactivationReasonsFromLastNonStarvingTime() const
{
    return OperationElementSharedState_->GetDeactivationReasonsFromLastNonStarvingTime();
}

std::optional<NProfiling::TTagId> TOperationElement::GetCustomProfilingTag()
{
    if (!GetParent()) {
        return std::nullopt;
    }

    THashSet<TString> allowedProfilingTags;
    const auto* parent = GetParent();
    while (parent) {
        for (const auto& tag : parent->GetAllowedProfilingTags()) {
            allowedProfilingTags.insert(tag);
        }
        parent = parent->GetParent();
    }

    auto tagName = Spec_->CustomProfilingTag;
    if (tagName && (
            allowedProfilingTags.find(*tagName) == allowedProfilingTags.end() ||
            (TreeConfig_->CustomProfilingTagFilter && NRe2::TRe2::FullMatch(NRe2::StringPiece(*tagName), *TreeConfig_->CustomProfilingTagFilter))
        ))
    {
        tagName = std::nullopt;
    }

    if (tagName) {
        return NVectorScheduler::GetCustomProfilingTag(*tagName);
    } else {
        return NVectorScheduler::GetCustomProfilingTag(MissingCustomProfilingTag);
    }
}

bool TOperationElement::IsOperation() const
{
    return true;
}

void TOperationElement::Disable()
{
    YT_LOG_DEBUG("Operation element disabled in strategy");

    OperationElementSharedState_->Disable();
    TreeHost_->GetResourceTree()->ReleaseResources(ResourceTreeElement_);
}

void TOperationElement::Enable()
{
    YT_LOG_DEBUG("Operation element enabled in strategy");

    return OperationElementSharedState_->Enable();
}

std::optional<TJobResources> TOperationElementSharedState::RemoveJob(TJobId jobId)
{
    TWriterGuard guard(JobPropertiesMapLock_);

    if (!Enabled_) {
        return std::nullopt;
    }

    auto it = JobPropertiesMap_.find(jobId);
    YT_VERIFY(it != JobPropertiesMap_.end());

    auto* properties = &it->second;
    if (properties->Preemptable) {
        PreemptableJobs_.erase(properties->JobIdListIterator);
    } else if (properties->AggressivelyPreemptable) {
        AggressivelyPreemptableJobs_.erase(properties->JobIdListIterator);
    } else {
        NonpreemptableJobs_.erase(properties->JobIdListIterator);
    }

    --RunningJobCount_;

    auto resourceUsage = properties->ResourceUsage;
    IncreaseJobResourceUsage(properties, -resourceUsage);

    JobPropertiesMap_.erase(it);

    return resourceUsage;
}

std::optional<EDeactivationReason> TOperationElement::TryStartScheduleJob(
    const TFairShareContext& context,
    TJobResources* precommittedResourcesOutput,
    TJobResources* availableResourcesOutput)
{
    auto minNeededResources = Controller_->GetAggregatedMinNeededJobResources();

    auto nodeFreeResources = context.SchedulingContext->GetNodeFreeResourcesWithDiscount();
    if (!Dominates(nodeFreeResources, minNeededResources)) {
        return EDeactivationReason::MinNeededResourcesUnsatisfied;
    }

    // Do preliminary checks to avoid the overhead of updating and reverting precommit usage.
    auto availableResources = GetHierarchicalAvailableResources(context);
    auto availableDemand = GetLocalAvailableResourceDemand(context);
    if (!Dominates(availableResources, minNeededResources) || !Dominates(availableDemand, minNeededResources)) {
        return EDeactivationReason::ResourceLimitsExceeded;
    }

    if (!CheckDemand(minNeededResources, context)) {
        return EDeactivationReason::ResourceLimitsExceeded;
    }

    TJobResources availableResourceLimits;
    if (!TryIncreaseHierarchicalResourceUsagePrecommit(
            minNeededResources,
            &availableResourceLimits)) {
        return EDeactivationReason::ResourceLimitsExceeded;
    }

    Controller_->IncreaseConcurrentScheduleJobCalls(context.SchedulingContext->GetNodeShardId());
    Controller_->IncreaseScheduleJobCallsSinceLastUpdate(context.SchedulingContext->GetNodeShardId());

    *precommittedResourcesOutput = minNeededResources;
    *availableResourcesOutput = Min(availableResourceLimits, nodeFreeResources);
    return std::nullopt;
}

void TOperationElement::FinishScheduleJob(
    const ISchedulingContextPtr& schedulingContext,
    bool enableBackoff,
    NProfiling::TCpuInstant now)
{
    Controller_->DecreaseConcurrentScheduleJobCalls(schedulingContext->GetNodeShardId());

    if (enableBackoff) {
        Controller_->SetLastScheduleJobFailTime(now);
    }
}

void TOperationElementSharedState::IncreaseJobResourceUsage(
    TJobProperties* properties,
    const TJobResources& resourcesDelta)
{
    properties->ResourceUsage += resourcesDelta;
    if (!properties->Preemptable) {
        if (properties->AggressivelyPreemptable) {
            AggressivelyPreemptableResourceUsage_ += resourcesDelta;
        } else {
            NonpreemptableResourceUsage_ += resourcesDelta;
        }
    }
}

TOperationElementSharedState::TJobProperties* TOperationElementSharedState::GetJobProperties(TJobId jobId)
{
    auto it = JobPropertiesMap_.find(jobId);
    YT_ASSERT(it != JobPropertiesMap_.end());
    return &it->second;
}

const TOperationElementSharedState::TJobProperties* TOperationElementSharedState::GetJobProperties(TJobId jobId) const
{
    auto it = JobPropertiesMap_.find(jobId);
    YT_ASSERT(it != JobPropertiesMap_.end());
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

TOperationElement::TOperationElement(
    TFairShareStrategyTreeConfigPtr treeConfig,
    TStrategyOperationSpecPtr spec,
    TOperationFairShareTreeRuntimeParametersPtr runtimeParameters,
    TFairShareStrategyOperationControllerPtr controller,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    ISchedulerStrategyHost* host,
    IFairShareTreeHost* treeHost,
    IOperationStrategyHost* operation,
    const TString& treeId,
    const NLogging::TLogger& logger)
    : TSchedulerElement(
        host,
        treeHost,
        std::move(treeConfig),
        treeId,
        NLogging::TLogger(logger).AddTag("OperationId: %v", operation->GetId()))
    , TOperationElementFixedState(operation, std::move(controllerConfig))
    , RuntimeParameters_(std::move(runtimeParameters))
    , Spec_(spec)
    , OperationElementSharedState_(New<TOperationElementSharedState>(spec->UpdatePreemptableJobsListLoggingPeriod, Logger))
    , Controller_(std::move(controller))
    , SchedulingTagFilter_(spec->SchedulingTagFilter)
{
    PersistentAttributes_.LastNonStarvingTime = TInstant::Now();
}

TOperationElement::TOperationElement(
    const TOperationElement& other,
    TCompositeSchedulerElement* clonedParent)
    : TSchedulerElement(other, clonedParent)
    , TOperationElementFixedState(other)
    , RuntimeParameters_(other.RuntimeParameters_)
    , Spec_(other.Spec_)
    , OperationElementSharedState_(other.OperationElementSharedState_)
    , Controller_(other.Controller_)
    , RunningInThisPoolTree_(other.RunningInThisPoolTree_)
    , SchedulingTagFilter_(other.SchedulingTagFilter_)
{ }

double TOperationElement::GetFairShareStarvationTolerance() const
{
    return Spec_->FairShareStarvationTolerance.value_or(Parent_->Attributes().AdjustedFairShareStarvationTolerance);
}

TDuration TOperationElement::GetMinSharePreemptionTimeout() const
{
    return Spec_->MinSharePreemptionTimeout.value_or(Parent_->Attributes().AdjustedMinSharePreemptionTimeout);
}

TDuration TOperationElement::GetFairSharePreemptionTimeout() const
{
    return Spec_->FairSharePreemptionTimeout.value_or(Parent_->Attributes().AdjustedFairSharePreemptionTimeout);
}

void TOperationElement::DisableNonAliveElements()
{ }

void TOperationElement::PreUpdateBottomUp(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    UnschedulableReason_ = ComputeUnschedulableReason();
    SlotIndex_ = Operation_->FindSlotIndex(GetTreeId());
    ResourceUsageAtUpdate_ = GetInstantResourceUsage();
    ResourceDemand_ = Max(ComputeResourceDemand(), ResourceUsageAtUpdate_);
    ResourceTreeElement_->SetResourceLimits(GetSpecifiedResourceLimits());
    StartTime_ = Operation_->GetStartTime();

    TSchedulerElement::PreUpdateBottomUp(context);
}

void TOperationElement::UpdateBottomUp(TDynamicAttributesList* dynamicAttributesList, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    PendingJobCount_ = ComputePendingJobCount();
    MaxPossibleResourceUsage_ = Min(ResourceLimits_, ResourceDemand_);

    // It should be called after update of ResourceDemand_ and MaxPossibleResourceUsage_ since
    // these fields are used to calculate dominant resource.
    TSchedulerElement::UpdateBottomUp(dynamicAttributesList, context);

    auto allocationLimits = GetAdjustedResourceLimits(
        ResourceDemand_,
        TotalResourceLimits_,
        GetHost()->GetExecNodeMemoryDistribution(SchedulingTagFilter_ & TreeConfig_->NodesFilter));
    BestAllocationShare_ = TResourceVector::Max(
        Attributes_.UsageShare,
        TResourceVector::FromJobResources(allocationLimits, TotalResourceLimits_, 0, 1));

    RemainingDemandShare_ = Attributes_.DemandShare - Attributes_.UsageShare;
    YT_VERIFY(Dominates(RemainingDemandShare_, TResourceVector::Zero()));

    if (!IsSchedulable()) {
        (*dynamicAttributesList)[GetTreeIndex()].Active = false;
        ++context->UnschedulableReasons[*UnschedulableReason_];
    }
}

void TOperationElement::UpdatePreemption(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);
    TSchedulerElement::UpdatePreemption(context);

    // If fair share ratio equals demand ratio then we want to explicitly disable preemption.
    // It is necessary since some job's resource usage may increase before the next fair share update,
    //  and in this case we don't want any jobs to become preemptable
    bool isFairShareRatioEqualToDemandRatio =
        TResourceVector::Near(Attributes_.FairShare, Attributes_.DemandShare, RatioComparisonPrecision) &&
        !Dominates(TResourceVector::Epsilon(), Attributes_.DemandShare);

    bool newPreemptableValue = !isFairShareRatioEqualToDemandRatio;
    bool oldPreemptableValue = OperationElementSharedState_->GetPreemptable();
    if (oldPreemptableValue != newPreemptableValue) {
        YT_LOG_DEBUG("Preemptable status changed %v -> %v", oldPreemptableValue, newPreemptableValue);
        OperationElementSharedState_->SetPreemptable(newPreemptableValue);
    }

    UpdatePreemptableJobsList();
}

void TOperationElement::UpdateMinShare(TUpdateFairShareContext* context)
{ }

void TOperationElement::PrepareFairShareByFitFactor(TUpdateFairShareContext* context)
{
    TWallTimer timer;
    auto finally = Finally([&] {
        context->PrepareFairShareByFitFactorOperationsTotalTime += timer.GetElapsedCpuTime();
    });

    TVectorPiecewiseLinearFunction::TBuilder builder;

    // First we try to satisfy the current usage by giving equal fair share for each resource.
    // More precisely, for fit factor 0 <= f <= 1, fair share for resource r will be equal to min(usage[r], f * maxUsage).
    double maxUsage = MaxComponent(Attributes_.UsageShare);
    if (maxUsage == 0.0) {
        builder.PushSegment({0.0, TResourceVector::Zero()}, {1.0, TResourceVector::Zero()});
    } else {
        std::vector<double> sortedUsage;
        for (int r = 0; r < ResourceCount; ++r) {
            sortedUsage.push_back(Attributes_.UsageShare[r]);
        }
        std::sort(sortedUsage.begin(), sortedUsage.end());

        builder.AddPoint({0.0, TResourceVector::Zero()});
        double previousUsageFitFactor = 0.0;
        for (auto usage : sortedUsage) {
            double currentUsageFitFactor = usage / maxUsage;
            if (currentUsageFitFactor > previousUsageFitFactor) {
                builder.AddPoint({
                    currentUsageFitFactor,
                    TResourceVector::Min(TResourceVector::FromDouble(usage), Attributes_.UsageShare)});
                previousUsageFitFactor = currentUsageFitFactor;
            }
        }
        YT_VERIFY(previousUsageFitFactor == 1.0);
    }

    // After that we just give fair share proportionally to the remaining demand.
    double maxRemainingDemandFitFactor = 1.0;
    for (int r = 0; r < ResourceCount; ++r) {
        if (RemainingDemandShare_[r] < RatioComputationPrecision) {
            continue;
        }

        if (BestAllocationShare_[r] - Attributes_.UsageShare[r] < RemainingDemandShare_[r]) {
            maxRemainingDemandFitFactor = std::min(
                maxRemainingDemandFitFactor,
                (BestAllocationShare_[r] - Attributes_.UsageShare[r]) / RemainingDemandShare_[r]);
        }
    }

    TResourceVector rightBoundValue = Attributes_.UsageShare + RemainingDemandShare_ * maxRemainingDemandFitFactor;
    if (maxRemainingDemandFitFactor > 0.0) {
        builder.PushSegment({1.0, Attributes_.UsageShare}, {1.0 + maxRemainingDemandFitFactor, rightBoundValue});
    }
    if (maxRemainingDemandFitFactor < 1.0) {
        builder.PushSegment({1.0 + maxRemainingDemandFitFactor, rightBoundValue}, {2.0, rightBoundValue});
    }

    FairShareByFitFactor_ = builder.Finish();
}

TResourceVector TOperationElement::DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context)
{
    TResourceVector usedFairShare = FairShareBySuggestion()->ValueAt(suggestion);
    SetFairShare(usedFairShare);

    const auto fsbsSegment = FairShareBySuggestion()->SegmentAt(suggestion);
    const auto fitFactor = MaxFitFactorBySuggestion()->ValueAt(suggestion);
    const auto fsbffSegment = FairShareByFitFactor()->SegmentAt(fitFactor);

    OPERATION_LOG_DETAILED(this,
        "Updated Operation fair share. ("
        "Suggestion: %.6v, "
        "UsedFairShare: %.6v, "
        "FSBSSegmentArguments: {%.6v, %.6v}, "
        "FSBSSegmentValues: {%.6v, %.6v}, "
        "FitFactor: %.6v, "
        "FSBFFSegmentArguments: {%.6v, %.6v}, "
        "FSBFFSegmentValues: {%.6v, %.6v})",
        suggestion,
        usedFairShare,
        fsbsSegment.LeftBound(), fsbsSegment.RightBound(),
        fsbsSegment.LeftValue(), fsbsSegment.RightValue(),
        fitFactor,
        fsbffSegment.LeftBound(), fsbffSegment.RightBound(),
        fsbffSegment.LeftValue(), fsbffSegment.RightValue());
    return usedFairShare;
}

TJobResources TOperationElement::ComputePossibleResourceUsage(TJobResources limit) const
{
    auto usage = ResourceUsageAtUpdate();
    if (!Dominates(limit, usage)) {
        return usage * GetMinResourceRatio(limit, usage);
    } else {
        auto remainingDemand = ResourceDemand() - usage;
        if (remainingDemand == TJobResources()) {
            return usage;
        }

        auto remainingLimit = Max({}, limit - usage);
        // TODO(asaitgalin): Move this to MaxPossibleResourceUsage computation.
        return Min(ResourceDemand(), usage + remainingDemand * GetMinResourceRatio(remainingLimit, remainingDemand));
    }
}

bool TOperationElement::HasJobsSatisfyingResourceLimits(const TFairShareContext& context) const
{
    for (const auto& jobResources : Controller_->GetDetailedMinNeededJobResources()) {
        if (context.SchedulingContext->CanStartJob(jobResources)) {
            return true;
        }
    }
    return false;
}

void TOperationElement::UpdateDynamicAttributes(TDynamicAttributesList* dynamicAttributesList)
{
    auto& attributes = (*dynamicAttributesList)[GetTreeIndex()];
    attributes.Active = true;
    attributes.BestLeafDescendant = this;

    TSchedulerElement::UpdateDynamicAttributes(dynamicAttributesList);
}

void TOperationElement::UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config)
{
    YT_VERIFY(Mutable_);
    ControllerConfig_ = config;
}

void TOperationElement::PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled)
{
    auto& attributes = context->DynamicAttributesFor(this);

    attributes.Active = true;

    auto onOperationDeactivated = [&] (EDeactivationReason reason) {
        ++context->StageState->DeactivationReasons[reason];
        OnOperationDeactivated(*context, reason);
        attributes.Active = false;
    };

    if (!IsAlive()) {
        onOperationDeactivated(EDeactivationReason::IsNotAlive);
        return;
    }

    if (auto blockedReason = CheckBlocked(context->SchedulingContext)) {
        onOperationDeactivated(*blockedReason);
        return;
    }

    if (Spec_->PreemptionMode == EPreemptionMode::Graceful && GetStatus() == ESchedulableStatus::Normal) {
        onOperationDeactivated(EDeactivationReason::FairShareExceeded);
        return;
    }

    if (TreeConfig_->EnableSchedulingTags &&
        SchedulingTagFilterIndex_ != EmptySchedulingTagFilterIndex &&
        !context->CanSchedule[SchedulingTagFilterIndex_])
    {
        onOperationDeactivated(EDeactivationReason::UnmatchedSchedulingTag);
        return;
    }

    if (starvingOnly && !PersistentAttributes_.Starving) {
        onOperationDeactivated(EDeactivationReason::IsNotStarving);
        return;
    }

    if (Controller_->IsSaturatedInTentativeTree(
        context->SchedulingContext->GetNow(),
        TreeId_,
        TreeConfig_->TentativeTreeSaturationDeactivationPeriod))
    {
        onOperationDeactivated(EDeactivationReason::SaturatedInTentativeTree);
        return;
    }

    ++context->StageState->ActiveTreeSize;
    ++context->StageState->ActiveOperationCount;

    TSchedulerElement::PrescheduleJob(context, starvingOnly, aggressiveStarvationEnabled);
}

bool TOperationElement::HasAggressivelyStarvingElements(TFairShareContext* /*context*/, bool /*aggressiveStarvationEnabled*/) const
{
    // TODO(ignat): Support aggressive starvation by starving operation.
    return false;
}

TString TOperationElement::GetLoggingString(const TDynamicAttributes& dynamicAttributes) const
{
    return Format(
        "Scheduling info for tree %Qv = {%v, "
        "PreemptableRunningJobs: %v, AggressivelyPreemptableRunningJobs: %v, PreemptionStatusStatistics: %v, DeactivationReasons: %v}",
        GetTreeId(),
        GetLoggingAttributesString(dynamicAttributes),
        GetPreemptableJobCount(),
        GetAggressivelyPreemptableJobCount(),
        GetPreemptionStatusStatistics(),
        GetDeactivationReasons());
}

void TOperationElement::UpdateAncestorsDynamicAttributes(TFairShareContext* context, bool activateAncestors)
{
    auto* parent = GetMutableParent();
    while (parent) {
        if (activateAncestors) {
            context->DynamicAttributesFor(parent).Active = true;
        }
        parent->UpdateDynamicAttributes(&context->DynamicAttributesList);
        if (!parent->IsActive(context->DynamicAttributesList)) {
            ++context->StageState->DeactivationReasons[EDeactivationReason::NoBestLeafDescendant];
        }
        parent = parent->GetMutableParent();
    }
}

void TOperationElement::RecordHeartbeat(const TPackingHeartbeatSnapshot& heartbeatSnapshot)
{
    OperationElementSharedState_->RecordHeartbeat(heartbeatSnapshot, GetPackingConfig());
}

bool TOperationElement::CheckPacking(const TPackingHeartbeatSnapshot& heartbeatSnapshot) const
{
    auto detailedMinNeededResources = Controller_->GetDetailedMinNeededJobResources();
    // NB: We expect detailedMinNeededResources to be of size 1 most of the time.
    TJobResourcesWithQuota packingJobResourcesWithQuota;
    if (detailedMinNeededResources.empty()) {
        // Refuse packing if no information about resource requirements is provided.
        return false;
    } else if (detailedMinNeededResources.size() == 1) {
        packingJobResourcesWithQuota = detailedMinNeededResources[0];
    } else {
        auto idx = RandomNumber<ui32>(static_cast<ui32>(detailedMinNeededResources.size()));
        packingJobResourcesWithQuota = detailedMinNeededResources[idx];
    }

    return OperationElementSharedState_->CheckPacking(
        /* operationElement */ this,
        heartbeatSnapshot,
        packingJobResourcesWithQuota,
        TotalResourceLimits_,
        GetPackingConfig());
}

TFairShareScheduleJobResult TOperationElement::ScheduleJob(TFairShareContext* context, bool ignorePacking)
{
    YT_VERIFY(IsActive(context->DynamicAttributesList));

    OPERATION_LOG_DETAILED(this,
        "Trying to schedule job (SatisfactionRatio: %v, NodeId: %v, NodeResourceUsage: %v)",
        context->DynamicAttributesFor(this).SatisfactionRatio,
        context->SchedulingContext->GetNodeDescriptor().Id,
        FormatResourceUsage(context->SchedulingContext->ResourceUsage(), context->SchedulingContext->ResourceLimits()));

    auto disableOperationElement = [&] (EDeactivationReason reason) {
        OPERATION_LOG_DETAILED(this,
            "Failed to schedule job, operation deactivated "
            "(DeactivationReason: %v, NodeResourceUsage: %v)",
            FormatEnum(reason),
            FormatResourceUsage(context->SchedulingContext->ResourceUsage(), context->SchedulingContext->ResourceLimits()));
        ++context->StageState->DeactivationReasons[reason];
        OnOperationDeactivated(*context, reason);
        context->DynamicAttributesFor(this).Active = false;
        UpdateAncestorsDynamicAttributes(context);
    };

    auto recordHeartbeatWithTimer = [&] (const auto& heartbeatSnapshot) {
        NProfiling::TWallTimer timer;
        RecordHeartbeat(heartbeatSnapshot);
        context->StageState->PackingRecordHeartbeatDuration += timer.GetElapsedTime();
    };

    if (auto blockedReason = CheckBlocked(context->SchedulingContext)) {
        disableOperationElement(*blockedReason);
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    if (!HasJobsSatisfyingResourceLimits(*context)) {
        OPERATION_LOG_DETAILED(this,
            "No pending jobs can satisfy available resources on node "
            "(FreeResources: %v, DiscountResources: %v)",
            FormatResources(context->SchedulingContext->GetNodeFreeResourcesWithoutDiscount()),
            FormatResources(context->SchedulingContext->ResourceUsageDiscount()));
        disableOperationElement(EDeactivationReason::MinNeededResourcesUnsatisfied);
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    TJobResources precommittedResources;
    TJobResources availableResources;

    auto deactivationReason = TryStartScheduleJob(*context, &precommittedResources, &availableResources);
    if (deactivationReason) {
        disableOperationElement(*deactivationReason);
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    auto now = context->SchedulingContext->GetNow();
    std::optional<TPackingHeartbeatSnapshot> heartbeatSnapshot;
    if (GetPackingConfig()->Enable && !ignorePacking) {
        heartbeatSnapshot = CreateHeartbeatSnapshot(context->SchedulingContext);

        bool acceptPacking;
        {
            NProfiling::TWallTimer timer;
            acceptPacking = CheckPacking(*heartbeatSnapshot);
            context->StageState->PackingCheckDuration += timer.GetElapsedTime();
        }

        if (!acceptPacking) {
            recordHeartbeatWithTimer(*heartbeatSnapshot);
            TreeHost_->GetResourceTree()->IncreaseHierarchicalResourceUsagePrecommit(ResourceTreeElement_, -precommittedResources);
            disableOperationElement(EDeactivationReason::BadPacking);
            context->BadPackingOperations.emplace_back(this);
            FinishScheduleJob(context->SchedulingContext, /* enableBackoff */ false, now);
            return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
        }
    }

    TControllerScheduleJobResultPtr scheduleJobResult;
    {
        NProfiling::TWallTimer timer;
        scheduleJobResult = DoScheduleJob(context, availableResources, &precommittedResources);
        auto scheduleJobDuration = timer.GetElapsedTime();
        context->StageState->TotalScheduleJobDuration += scheduleJobDuration;
        context->StageState->ExecScheduleJobDuration += scheduleJobResult->Duration;
    }

    if (!scheduleJobResult->StartDescriptor) {
        for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
            context->StageState->FailedScheduleJob[reason] += scheduleJobResult->Failed[reason];
        }

        ++context->StageState->ScheduleJobFailureCount;
        disableOperationElement(EDeactivationReason::ScheduleJobFailed);

        bool enableBackoff = scheduleJobResult->IsBackoffNeeded();
        YT_LOG_DEBUG_IF(enableBackoff, "Failed to schedule job, backing off (Reasons: %v)",
            scheduleJobResult->Failed);

        TreeHost_->GetResourceTree()->IncreaseHierarchicalResourceUsagePrecommit(ResourceTreeElement_, -precommittedResources);
        FinishScheduleJob(context->SchedulingContext, /* enableBackoff */ enableBackoff, now);
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    const auto& startDescriptor = *scheduleJobResult->StartDescriptor;
    if (!OnJobStarted(startDescriptor.Id, startDescriptor.ResourceLimits.ToJobResources(), precommittedResources)) {
        Controller_->AbortJob(startDescriptor.Id, EAbortReason::SchedulingOperationDisabled);
        disableOperationElement(EDeactivationReason::OperationDisabled);
        TreeHost_->GetResourceTree()->IncreaseHierarchicalResourceUsagePrecommit(ResourceTreeElement_, -precommittedResources);
        FinishScheduleJob(context->SchedulingContext, /* enableBackoff */ false, now);
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    context->SchedulingContext->StartJob(
        GetTreeId(),
        OperationId_,
        scheduleJobResult->IncarnationId,
        startDescriptor,
        Spec_->PreemptionMode);

    UpdateDynamicAttributes(&context->DynamicAttributesList);
    UpdateAncestorsDynamicAttributes(context);

    if (heartbeatSnapshot) {
        recordHeartbeatWithTimer(*heartbeatSnapshot);
    }

    FinishScheduleJob(context->SchedulingContext, /* enableBackoff */ false, now);

    OPERATION_LOG_DETAILED(this,
        "Scheduled a job (SatisfactionRatio: %v, NodeId: %v, JobId: %v, JobResourceLimits: %v)",
        context->DynamicAttributesFor(this).SatisfactionRatio,
        context->SchedulingContext->GetNodeDescriptor().Id,
        startDescriptor.Id,
        Host_->FormatResources(startDescriptor.ResourceLimits));
    return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ true);
}

TString TOperationElement::GetId() const
{
    return ToString(OperationId_);
}

bool TOperationElement::IsAggressiveStarvationPreemptionAllowed() const
{
    return Spec_->AllowAggressiveStarvationPreemption.value_or(true);
}

std::optional<double> TOperationElement::GetSpecifiedWeight() const
{
    return RuntimeParameters_->Weight;
}

TJobResources TOperationElement::GetMinShareResources() const
{
    return ToJobResources(Spec_->MinShareResources, {});
}

TResourceVector TOperationElement::GetMaxShare() const
{
    return TResourceVector::FromDouble(Spec_->MaxShareRatio.value_or(1.0));
}

const TSchedulingTagFilter& TOperationElement::GetSchedulingTagFilter() const
{
    return SchedulingTagFilter_;
}

ESchedulableStatus TOperationElement::GetStatus() const
{
    if (UnschedulableReason_) {
        return ESchedulableStatus::Normal;
    }

    return TSchedulerElement::GetStatus(Attributes_.AdjustedFairShareStarvationTolerance);
}

void TOperationElement::SetStarving(bool starving)
{
    YT_VERIFY(Mutable_);

    if (!starving) {
        PersistentAttributes_.LastNonStarvingTime = TInstant::Now();
    }

    if (starving && !GetStarving()) {
        OperationElementSharedState_->ResetDeactivationReasonsFromLastNonStarvingTime();
        TSchedulerElement::SetStarving(true);
        YT_LOG_INFO("Operation is now starving (Status: %v)", GetStatus());
    } else if (!starving && GetStarving()) {
        TSchedulerElement::SetStarving(false);
        YT_LOG_INFO("Operation is no longer starving");
    }
}

void TOperationElement::CheckForStarvation(TInstant now)
{
    YT_VERIFY(Mutable_);

    auto minSharePreemptionTimeout = Attributes_.AdjustedMinSharePreemptionTimeout;
    auto fairSharePreemptionTimeout = Attributes_.AdjustedFairSharePreemptionTimeout;

    double jobCountRatio = GetPendingJobCount() / TreeConfig_->JobCountPreemptionTimeoutCoefficient;

    if (jobCountRatio < 1.0) {
        minSharePreemptionTimeout *= jobCountRatio;
        fairSharePreemptionTimeout *= jobCountRatio;
    }

    TSchedulerElement::CheckForStarvationImpl(
        minSharePreemptionTimeout,
        fairSharePreemptionTimeout,
        now);
}

bool TOperationElement::IsPreemptionAllowed(const TFairShareContext& context, const TFairShareStrategyTreeConfigPtr& config) const
{
    if (Spec_->PreemptionMode == EPreemptionMode::Graceful) {
        return false;
    }
    int jobCount = GetRunningJobCount();
    int maxUnpreemptableJobCount = config->MaxUnpreemptableRunningJobCount;
    if (Spec_->MaxUnpreemptableRunningJobCount) {
        maxUnpreemptableJobCount = std::min(maxUnpreemptableJobCount, *Spec_->MaxUnpreemptableRunningJobCount);
    }
    if (jobCount <= maxUnpreemptableJobCount) {
        OperationElementSharedState_->UpdatePreemptionStatusStatistics(EOperationPreemptionStatus::ForbiddenSinceLowJobCount);
        return false;
    }

    const TSchedulerElement* element = this;

    if (element->GetStarving()) {
        OperationElementSharedState_->UpdatePreemptionStatusStatistics(EOperationPreemptionStatus::ForbiddenSinceStarving);
        return false;
    }

    bool aggressivePreemptionEnabled = context.SchedulingStatistics.HasAggressivelyStarvingElements &&
        element->IsAggressiveStarvationPreemptionAllowed() &&
        IsAggressiveStarvationPreemptionAllowed();

    auto threshold = aggressivePreemptionEnabled
        ? config->AggressivePreemptionSatisfactionThreshold
        : config->PreemptionSatisfactionThreshold;

    // NB: we want to use <s>local</s> satisfaction here.
    if (element->ComputeLocalSatisfactionRatio() < threshold + RatioComparisonPrecision) {
        OperationElementSharedState_->UpdatePreemptionStatusStatistics(EOperationPreemptionStatus::ForbiddenSinceUnsatisfied);
        return false;
    }

    OperationElementSharedState_->UpdatePreemptionStatusStatistics(EOperationPreemptionStatus::Allowed);
    return true;
}

void TOperationElement::ApplyJobMetricsDelta(const TJobMetrics& delta)
{
    TreeHost_->GetResourceTree()->ApplyHierarchicalJobMetricsDelta(ResourceTreeElement_, delta);
}

void TOperationElement::IncreaseJobResourceUsage(TJobId jobId, const TJobResources& resourcesDelta)
{
    auto delta = OperationElementSharedState_->IncreaseJobResourceUsage(jobId, resourcesDelta);
    IncreaseHierarchicalResourceUsage(delta);

    UpdatePreemptableJobsList();
}

bool TOperationElement::IsJobKnown(TJobId jobId) const
{
    return OperationElementSharedState_->IsJobKnown(jobId);
}

bool TOperationElement::IsJobPreemptable(TJobId jobId, bool aggressivePreemptionEnabled) const
{
    return OperationElementSharedState_->IsJobPreemptable(jobId, aggressivePreemptionEnabled);
}

int TOperationElement::GetRunningJobCount() const
{
    return OperationElementSharedState_->GetRunningJobCount();
}

int TOperationElement::GetPreemptableJobCount() const
{
    return OperationElementSharedState_->GetPreemptableJobCount();
}

int TOperationElement::GetAggressivelyPreemptableJobCount() const
{
    return OperationElementSharedState_->GetAggressivelyPreemptableJobCount();
}

TPreemptionStatusStatisticsVector TOperationElement::GetPreemptionStatusStatistics() const
{
    return OperationElementSharedState_->GetPreemptionStatusStatistics();
}

TInstant TOperationElement::GetLastNonStarvingTime() const
{
    return PersistentAttributes_.LastNonStarvingTime;
}

TInstant TOperationElement::GetLastScheduleJobSuccessTime() const
{
    return OperationElementSharedState_->GetLastScheduleJobSuccessTime();
}

std::optional<int> TOperationElement::GetMaybeSlotIndex() const
{
    return SlotIndex_;
}

TString TOperationElement::GetUserName() const
{
    return UserName_;
}

bool TOperationElement::OnJobStarted(
    TJobId jobId,
    const TJobResources& resourceUsage,
    const TJobResources& precommittedResources,
    bool force)
{
    OPERATION_LOG_DETAILED(this, "Adding job to strategy (JobId: %v)", jobId);

    auto resourceUsageDelta = OperationElementSharedState_->AddJob(jobId, resourceUsage, force);
    if (resourceUsageDelta) {
        TreeHost_->GetResourceTree()->CommitHierarchicalResourceUsage(ResourceTreeElement_, *resourceUsageDelta, precommittedResources);
        UpdatePreemptableJobsList();
        return true;
    } else {
        return false;
    }
}

void TOperationElement::OnJobFinished(TJobId jobId)
{
    OPERATION_LOG_DETAILED(this, "Removing job from strategy (JobId: %v)", jobId);

    auto delta = OperationElementSharedState_->RemoveJob(jobId);
    if (delta) {
        IncreaseHierarchicalResourceUsage(-(*delta));
        UpdatePreemptableJobsList();
    }
}

void TOperationElement::BuildElementMapping(TRawOperationElementMap* enabledOperationMap, TRawOperationElementMap* disabledOperationMap, TRawPoolMap* poolMap)
{
    if (OperationElementSharedState_->Enabled()) {
        enabledOperationMap->emplace(OperationId_, this);
    } else {
        disabledOperationMap->emplace(OperationId_, this);
    }
}

TSchedulerElementPtr TOperationElement::Clone(TCompositeSchedulerElement* clonedParent)
{
    return New<TOperationElement>(*this, clonedParent);
}

bool TOperationElement::IsSchedulable() const
{
    return !UnschedulableReason_;
}

std::optional<EUnschedulableReason> TOperationElement::ComputeUnschedulableReason() const
{
    auto result = Operation_->CheckUnschedulable();
    if (!result && IsMaxScheduleJobCallsViolated()) {
        result = EUnschedulableReason::MaxScheduleJobCallsViolated;
    }
    return result;
}

bool TOperationElement::IsMaxScheduleJobCallsViolated() const
{
    bool result = false;
    Controller_->CheckMaxScheduleJobCallsOverdraft(
        Spec_->MaxConcurrentControllerScheduleJobCalls.value_or(
            ControllerConfig_->MaxConcurrentControllerScheduleJobCalls),
        &result);
    return result;
}

bool TOperationElement::IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(
    const ISchedulingContextPtr& schedulingContext) const
{
    return Controller_->IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(
        schedulingContext,
        ControllerConfig_->MaxConcurrentControllerScheduleJobCallsPerNodeShard);
}

bool TOperationElement::HasRecentScheduleJobFailure(NProfiling::TCpuInstant now) const
{
    return Controller_->HasRecentScheduleJobFailure(now, ControllerConfig_->ScheduleJobFailBackoffTime);
}

std::optional<EDeactivationReason> TOperationElement::CheckBlocked(
    const ISchedulingContextPtr& schedulingContext) const
{
    if (IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(schedulingContext)) {
        return EDeactivationReason::MaxConcurrentScheduleJobCallsPerNodeShardViolated;
    }

    if (HasRecentScheduleJobFailure(schedulingContext->GetNow())) {
        return EDeactivationReason::RecentScheduleJobFailed;
    }

    return std::nullopt;
}

TJobResources TOperationElement::GetHierarchicalAvailableResources(const TFairShareContext& context) const
{
    // Bound available resources with node free resources.
    auto availableResources = context.SchedulingContext->GetNodeFreeResourcesWithDiscount();

    // Bound available resources with pool free resources.
    const TSchedulerElement* parent = this;
    while (parent) {
        availableResources = Min(availableResources, parent->GetLocalAvailableResourceLimits(context));
        parent = parent->GetParent();
    }

    return availableResources;
}

TControllerScheduleJobResultPtr TOperationElement::DoScheduleJob(
    TFairShareContext* context,
    const TJobResources& availableResources,
    TJobResources* precommittedResources)
{
    ++context->SchedulingStatistics.ControllerScheduleJobCount;

    auto scheduleJobResult = Controller_->ScheduleJob(
        context->SchedulingContext,
        availableResources,
        ControllerConfig_->ScheduleJobTimeLimit,
        GetTreeId());

    // Discard the job in case of resource overcommit.
    if (scheduleJobResult->StartDescriptor) {
        const auto& startDescriptor = *scheduleJobResult->StartDescriptor;
        // Note: resourceDelta might be negative.
        const auto resourceDelta = startDescriptor.ResourceLimits.ToJobResources() - *precommittedResources;
        bool successfullyPrecommitted = TryIncreaseHierarchicalResourceUsagePrecommit(resourceDelta);
        if (successfullyPrecommitted) {
            *precommittedResources += resourceDelta;
        } else {
            auto jobId = scheduleJobResult->StartDescriptor->Id;
            const auto availableDelta = GetHierarchicalAvailableResources(*context);
            YT_LOG_DEBUG("Aborting job with resource overcommit (JobId: %v, Limits: %v, JobResources: %v)",
                jobId,
                FormatResources(*precommittedResources + availableDelta),
                FormatResources(startDescriptor.ResourceLimits.ToJobResources()));

            Controller_->AbortJob(jobId, EAbortReason::SchedulingResourceOvercommit);

            // Reset result.
            scheduleJobResult = New<TControllerScheduleJobResult>();
            scheduleJobResult->RecordFail(EScheduleJobFailReason::ResourceOvercommit);
        }
    } else if (scheduleJobResult->Failed[EScheduleJobFailReason::Timeout] > 0) {
        YT_LOG_WARNING("Job scheduling timed out");

        SetOperationAlert(
            OperationId_,
            EOperationAlertType::ScheduleJobTimedOut,
            TError("Job scheduling timed out: either scheduler is under heavy load or operation is too heavy"),
            ControllerConfig_->ScheduleJobTimeoutAlertResetTime);
    } else if (scheduleJobResult->Failed[EScheduleJobFailReason::TentativeTreeDeclined] > 0) {
        Controller_->OnTentativeTreeScheduleJobFailed(context->SchedulingContext->GetNow(), TreeId_);
    }

    return scheduleJobResult;
}

TJobResources TOperationElement::ComputeResourceDemand() const
{
    auto maybeUnschedulableReason = Operation_->CheckUnschedulable();
    if (maybeUnschedulableReason == EUnschedulableReason::IsNotRunning || maybeUnschedulableReason == EUnschedulableReason::Suspended) {
        return {};
    }
    return GetInstantResourceUsage() + Controller_->GetNeededResources();
}

TJobResources TOperationElement::GetSpecifiedResourceLimits() const
{
    return ToJobResources(RuntimeParameters_->ResourceLimits, TJobResources::Infinite());
}

int TOperationElement::ComputePendingJobCount() const
{
    return Controller_->GetPendingJobCount();
}

void TOperationElement::UpdatePreemptableJobsList()
{
    TWallTimer timer;
    int moveCount = 0;

    OperationElementSharedState_->UpdatePreemptableJobsList(
        GetFairShare(),
        TotalResourceLimits_,
        TreeConfig_->PreemptionSatisfactionThreshold,
        TreeConfig_->AggressivePreemptionSatisfactionThreshold,
        &moveCount,
        this);

    auto elapsed = timer.GetElapsedTime();

    Profiler.Update(GetTreeHost()->GetProfilingCounter("/preemptable_list_update_time"), DurationToValue(elapsed));
    Profiler.Update(GetTreeHost()->GetProfilingCounter("/preemptable_list_update_move_count"), moveCount);

    if (elapsed > TreeConfig_->UpdatePreemptableListDurationLoggingThreshold) {
        YT_LOG_DEBUG("Preemptable list update is too long (Duration: %v, MoveCount: %v)",
            elapsed.MilliSeconds(),
            moveCount);
    }
}

bool TOperationElement::TryIncreaseHierarchicalResourceUsagePrecommit(
    const TJobResources& delta,
    TJobResources* availableResourceLimitsOutput)
{
    return TreeHost_->GetResourceTree()->TryIncreaseHierarchicalResourceUsagePrecommit(
        ResourceTreeElement_,
        delta,
        availableResourceLimitsOutput);
}

void TOperationElement::AttachParent(TCompositeSchedulerElement* newParent, bool enabled)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(!Parent_);

    Parent_ = newParent;
    TreeHost_->GetResourceTree()->AttachParent(ResourceTreeElement_, newParent->ResourceTreeElement_);

    newParent->IncreaseOperationCount(1);
    newParent->AddChild(this, enabled);

    YT_LOG_DEBUG("Operation attached to pool (Pool: %v)", newParent->GetId());
}

void TOperationElement::ChangeParent(TCompositeSchedulerElement* parent)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(Parent_);

    auto oldParentId = Parent_->GetId();
    if (RunningInThisPoolTree_) {
        Parent_->IncreaseRunningOperationCount(-1);
    }
    Parent_->IncreaseOperationCount(-1);
    bool enabled = Parent_->IsEnabledChild(this);
    Parent_->RemoveChild(this);

    Parent_ = parent;
    TreeHost_->GetResourceTree()->ChangeParent(ResourceTreeElement_, parent->ResourceTreeElement_);

    RunningInThisPoolTree_ = false;  // for consistency
    Parent_->IncreaseOperationCount(1);
    Parent_->AddChild(this, enabled);

    YT_LOG_DEBUG("Operation changed pool (OldPool: %v, NewPool: %v)",
        oldParentId,
        parent->GetId());
}

void TOperationElement::DetachParent()
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(Parent_);

    auto parentId = Parent_->GetId();
    if (RunningInThisPoolTree_) {
        Parent_->IncreaseRunningOperationCount(-1);
    }
    Parent_->IncreaseOperationCount(-1);
    Parent_->RemoveChild(this);

    Parent_ = nullptr;
    TreeHost_->GetResourceTree()->DetachParent(ResourceTreeElement_);

    YT_LOG_DEBUG("Operation detached from pool (Pool: %v)", parentId);
}

void TOperationElement::MarkOperationRunningInPool()
{
    Parent_->IncreaseRunningOperationCount(1);
    RunningInThisPoolTree_ = true;
    WaitingForPool_.reset();

    YT_LOG_INFO("Operation is running in pool (Pool: %v)", Parent_->GetId());
}

bool TOperationElement::IsOperationRunningInPool()
{
    return RunningInThisPoolTree_;
}

TFairShareStrategyPackingConfigPtr TOperationElement::GetPackingConfig() const
{
    return TreeConfig_->Packing;
}

void TOperationElement::MarkWaitingFor(TCompositeSchedulerElement* violatedPool)
{
    violatedPool->WaitingOperationIds().push_back(OperationId_);
    WaitingForPool_ = violatedPool->GetId();

    YT_LOG_DEBUG("Operation is pending since max running operation count is violated (OperationId: %v, Pool: %v, Limit: %v)",
        OperationId_,
        violatedPool->GetId(),
        violatedPool->GetMaxRunningOperationCount());
}

////////////////////////////////////////////////////////////////////////////////

TRootElement::TRootElement(
    ISchedulerStrategyHost* host,
    IFairShareTreeHost* treeHost,
    TFairShareStrategyTreeConfigPtr treeConfig,
    NProfiling::TTagId profilingTag,
    const TString& treeId,
    const NLogging::TLogger& logger)
    : TCompositeSchedulerElement(
        host,
        treeHost,
        treeConfig,
        profilingTag,
        treeId,
        NLogging::TLogger(logger)
            .AddTag("PoolId: %v", RootPoolName)
            .AddTag("SchedulingMode: %v", ESchedulingMode::FairShare))
{
    SetFairShare(TResourceVector::Ones());
    Attributes_.GuaranteedResourcesShare = TResourceVector::Ones();
    Attributes_.RecursiveMinShare = TResourceVector::Ones();
    Attributes_.AdjustedMinShare = TResourceVector::Ones();

    Mode_ = ESchedulingMode::FairShare;
    Attributes_.AdjustedFairShareStarvationTolerance = GetFairShareStarvationTolerance();
    Attributes_.AdjustedMinSharePreemptionTimeout = GetMinSharePreemptionTimeout();
    Attributes_.AdjustedFairSharePreemptionTimeout = GetFairSharePreemptionTimeout();
    AdjustedFairShareStarvationToleranceLimit_ = GetFairShareStarvationToleranceLimit();
    AdjustedMinSharePreemptionTimeoutLimit_ = GetMinSharePreemptionTimeoutLimit();
    AdjustedFairSharePreemptionTimeoutLimit_ = GetFairSharePreemptionTimeoutLimit();
}

TRootElement::TRootElement(const TRootElement& other)
    : TCompositeSchedulerElement(other, nullptr)
    , TRootElementFixedState(other)
{ }

void TRootElement::UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    TCompositeSchedulerElement::UpdateTreeConfig(config);

    Attributes_.AdjustedFairShareStarvationTolerance = GetFairShareStarvationTolerance();
    Attributes_.AdjustedMinSharePreemptionTimeout = GetMinSharePreemptionTimeout();
    Attributes_.AdjustedFairSharePreemptionTimeout = GetFairSharePreemptionTimeout();
}

void TRootElement::PreUpdate(TDynamicAttributesList* dynamicAttributesList, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    TForbidContextSwitchGuard contextSwitchGuard;

    DisableNonAliveElements();
    TreeSize_ = TCompositeSchedulerElement::EnumerateElements(0, context);
    dynamicAttributesList->assign(TreeSize_, TDynamicAttributes());
    context->TotalResourceLimits = GetHost()->GetResourceLimits(TreeConfig_->NodesFilter);

    PreUpdateBottomUp(context);
}

void TRootElement::Update(TDynamicAttributesList* dynamicAttributesList, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    VERIFY_INVOKER_AFFINITY(Host_->GetFairShareUpdateInvoker());
    TForbidContextSwitchGuard contextSwitchGuard;

    UpdateBottomUp(dynamicAttributesList, context);
    UpdateMinShare(context);
    UpdateFairShare(context);
    // These function must be called after UpdateFairShare.
    UpdatePreemption(context);
    UpdateDynamicAttributes(dynamicAttributesList, context);
}

void TRootElement::UpdateFairShare(TUpdateFairShareContext* context)
{
    YT_LOG_DEBUG("Updating fair share");

    TWallTimer timer;
    PrepareUpdateFairShare(context);
    DoUpdateFairShare(/* suggestion */ 1.0, context);
    auto totalDuration = timer.GetElapsedCpuTime();

    YT_LOG_DEBUG(
        "Finished updating fair share. "
        "TotalTime: %v, "
        "PrepareFairShareByFitFactor/TotalTime: %v, "
        "PrepareFairShareByFitFactor/Operations/TotalTime: %v, "
        "PrepareFairShareByFitFactor/Fifo/TotalTime: %v, "
        "PrepareFairShareByFitFactor/Normal/TotalTime: %v, "
        "PrepareMaxFitFactorBySuggestion/TotalTime: %v, "
        "PrepareMaxFitFactorBySuggestion/PointwiseMin/TotalTime: %v, "
        "Compose/TotalTime: %v., "
        "CompressFunction/TotalTime: %v.",
        CpuDurationToDuration(totalDuration).MicroSeconds(),
        CpuDurationToDuration(context->PrepareFairShareByFitFactorTotalTime).MicroSeconds(),
        CpuDurationToDuration(context->PrepareFairShareByFitFactorOperationsTotalTime).MicroSeconds(),
        CpuDurationToDuration(context->PrepareFairShareByFitFactorFifoTotalTime).MicroSeconds(),
        CpuDurationToDuration(context->PrepareFairShareByFitFactorNormalTotalTime).MicroSeconds(),
        CpuDurationToDuration(context->PrepareMaxFitFactorBySuggestionTotalTime).MicroSeconds(),
        CpuDurationToDuration(context->PointwiseMinTotalTime).MicroSeconds(),
        CpuDurationToDuration(context->ComposeTotalTime).MicroSeconds(),
        CpuDurationToDuration(context->CompressFunctionTotalTime).MicroSeconds());
}

bool TRootElement::IsRoot() const
{
    return true;
}

const TSchedulingTagFilter& TRootElement::GetSchedulingTagFilter() const
{
    return EmptySchedulingTagFilter;
}

TString TRootElement::GetId() const
{
    return RootPoolName;
}

std::optional<double> TRootElement::GetSpecifiedWeight() const
{
    return std::nullopt;
}

TJobResources TRootElement::GetMinShareResources() const
{
    return TotalResourceLimits_;
}

TResourceVector TRootElement::GetMaxShare() const
{
    return TResourceVector::Ones();
}

double TRootElement::GetFairShareStarvationTolerance() const
{
    return TreeConfig_->FairShareStarvationTolerance;
}

TDuration TRootElement::GetMinSharePreemptionTimeout() const
{
    return TreeConfig_->MinSharePreemptionTimeout;
}

TDuration TRootElement::GetFairSharePreemptionTimeout() const
{
    return TreeConfig_->FairSharePreemptionTimeout;
}

bool TRootElement::IsAggressiveStarvationEnabled() const
{
    return TreeConfig_->EnableAggressiveStarvation;
}

void TRootElement::CheckForStarvation(TInstant now)
{
    YT_ABORT();
}

int TRootElement::GetMaxRunningOperationCount() const
{
    return TreeConfig_->MaxRunningOperationCount;
}

int TRootElement::GetMaxOperationCount() const
{
    return TreeConfig_->MaxOperationCount;
}

std::vector<EFifoSortParameter> TRootElement::GetFifoSortParameters() const
{
    YT_ABORT();
}

bool TRootElement::AreImmediateOperationsForbidden() const
{
    return TreeConfig_->ForbidImmediateOperationsInRoot;
}

THashSet<TString> TRootElement::GetAllowedProfilingTags() const
{
    return {};
}

bool TRootElement::IsInferringChildrenWeightsFromHistoricUsageEnabled() const
{
    return false;
}

TJobResources TRootElement::GetSpecifiedResourceLimits() const
{
    return TJobResources::Infinite();
}

THistoricUsageAggregationParameters TRootElement::GetHistoricUsageAggregationParameters() const
{
    return THistoricUsageAggregationParameters(EHistoricUsageAggregationMode::None);
}

TSchedulerElementPtr TRootElement::Clone(TCompositeSchedulerElement* /*clonedParent*/)
{
    YT_ABORT();
}

TRootElementPtr TRootElement::Clone()
{
    return New<TRootElement>(*this);
}

bool TRootElement::IsDefaultConfigured() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NVectorScheduler
