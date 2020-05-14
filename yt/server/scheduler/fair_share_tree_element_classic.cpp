#include "fair_share_tree_element_classic.h"

#include "fair_share_tree.h"
#include "helpers.h"
#include "resource_tree.h"
#include "resource_tree_element.h"
#include "scheduling_context.h"

#include "operation_log.h"

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>

#include <yt/core/misc/finally.h>

#include <yt/core/profiling/timing.h>

#include <util/generic/ymath.h>

#include <yt/core/misc/historic_usage_aggregator.h>

namespace NYT::NScheduler::NClassicScheduler {

using namespace NConcurrency;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = SchedulerProfiler;

////////////////////////////////////////////////////////////////////////////////

static const double RatioComputationPrecision = std::numeric_limits<double>::epsilon();
static const double RatioComparisonPrecision = std::sqrt(RatioComputationPrecision);
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
    : LoggingName(loggingName)
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

    UpdateAttributes();
    (*dynamicAttributesList)[GetTreeIndex()].Active = true;
    UpdateDynamicAttributes(dynamicAttributesList);
}

void TSchedulerElement::UpdateTopDown(TDynamicAttributesList* , TUpdateFairShareContext* )
{
    YT_VERIFY(Mutable_);
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

    // Choose dominant resource types, compute max share ratios, compute demand ratios.
    const auto& demand = ResourceDemand();
    auto usage = ResourceUsageAtUpdate();

    auto maxPossibleResourceUsage = Min(TotalResourceLimits_, MaxPossibleResourceUsage_);

    if (usage == TJobResources()) {
        Attributes_.DominantResource = GetDominantResource(demand, TotalResourceLimits_);
    } else {
        Attributes_.DominantResource = GetDominantResource(usage, TotalResourceLimits_);
    }

    Attributes_.DominantLimit = GetResource(TotalResourceLimits_, Attributes_.DominantResource);

    auto dominantDemand = GetResource(demand, Attributes_.DominantResource);
    Attributes_.DemandRatio =
        Attributes_.DominantLimit == 0 ? 1.0 : dominantDemand / Attributes_.DominantLimit;

    auto possibleUsage = ComputePossibleResourceUsage(maxPossibleResourceUsage);
    double possibleUsageRatio = GetDominantResourceUsage(possibleUsage, TotalResourceLimits_);

    Attributes_.MaxPossibleUsageRatio = std::min(
        possibleUsageRatio,
        GetMaxShareRatio());
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
        "Status: %v, DominantResource: %v, Demand: %.6lf, "
        "Usage: %.6lf, FairShare: %.6lf, Satisfaction: %.4lg, AdjustedMinShare: %.6lf, "
        "GuaranteedResourcesRatio: %.6lf, MaxPossibleUsage: %.6lf,  BestAllocation: %.6lf, "
        "Starving: %v, Weight: %v",
        GetStatus(),
        Attributes_.DominantResource,
        Attributes_.DemandRatio,
        GetResourceUsageRatio(),
        Attributes_.FairShareRatio,
        dynamicAttributes.SatisfactionRatio,
        Attributes_.AdjustedMinShareRatio,
        Attributes_.GuaranteedResourcesRatio,
        Attributes_.MaxPossibleUsageRatio,
        PersistentAttributes_.BestAllocationRatio,
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
    if (Attributes().MinShareRatio < RatioComputationPrecision) {
        return 1.0;
    }

    double parentMinShareRatio = 1.0;
    if (GetParent()) {
        parentMinShareRatio = GetParent()->Attributes().MinShareRatio;
    }

    if (parentMinShareRatio < RatioComputationPrecision) {
        return 1.0;
    }

    return Attributes().MinShareRatio * (*TreeConfig_->InferWeightFromMinShareRatioMultiplier) /
        parentMinShareRatio;
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

double TSchedulerElement::GetResourceUsageRatio() const
{
    return ComputeResourceUsageRatio(ResourceTreeElement_->GetResourceUsage());
}

double TSchedulerElement::GetResourceUsageRatioWithPrecommit() const
{
    return ComputeResourceUsageRatio(ResourceTreeElement_->GetResourceUsageWithPrecommit());
}

double TSchedulerElement::ComputeResourceUsageRatio(const TJobResources& resourceUsage) const
{
    if (Attributes_.DominantLimit == 0) {
        return 0.0;
    }
    return GetResource(resourceUsage, Attributes_.DominantResource) / Attributes_.DominantLimit;
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
    double minShareRatio = Attributes_.AdjustedMinShareRatio;
    double fairShareRatio = Attributes_.FairShareRatio;
    double usageRatio = GetResourceUsageRatioWithPrecommit();

    // Check for corner cases.
    if (fairShareRatio < RatioComputationPrecision) {
        return std::numeric_limits<double>::max();
    }

    if (Attributes_.FifoIndex >= 0) {
        // Satisfaction is defined only for top operations in FIFO pool.
        if (fairShareRatio > RatioComparisonPrecision) {
            return usageRatio / fairShareRatio;
        } else {
            return std::numeric_limits<double>::max();
        }
    }

    if (minShareRatio > RatioComputationPrecision && usageRatio < minShareRatio) {
        // Needy element, negative satisfaction.
        return usageRatio / minShareRatio - 1.0;
    } else {
        // Regular element, positive satisfaction.
        return usageRatio / fairShareRatio;
    }
}

ESchedulableStatus TSchedulerElement::GetStatus(double defaultTolerance) const
{
    double usageRatio = GetResourceUsageRatio();
    double demandRatio = Attributes_.DemandRatio;

    double tolerance =
        demandRatio < Attributes_.FairShareRatio + RatioComparisonPrecision
        ? 1.0
        : defaultTolerance;

    if (usageRatio > Attributes_.FairShareRatio * tolerance - RatioComparisonPrecision) {
        return ESchedulableStatus::Normal;
    }

    return usageRatio < Attributes_.AdjustedMinShareRatio
           ? ESchedulableStatus::BelowMinShare
           : ESchedulableStatus::BelowFairShare;
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
        case ESchedulableStatus::BelowMinShare:
            updateStarving(minSharePreemptionTimeout);
            break;

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
        return TotalResourceLimits_ * GetMaxShareRatio();
    }

    auto connectionTime = InstantToCpuInstant(Host_->GetConnectionTime());
    auto delay = DurationToCpuDuration(TreeConfig_->TotalResourceLimitsConsiderDelay);
    if (GetCpuInstant() < connectionTime + delay) {
        // Return infinity during the cluster startup.
        return TJobResources::Infinite();
    } else {
        return GetHost()->GetResourceLimits(TreeConfig_->NodesFilter & GetSchedulingTagFilter()) * GetMaxShareRatio();
    }
}

TJobResources TSchedulerElement::GetTotalResourceLimits() const
{
    return TotalResourceLimits_;
}

double TSchedulerElement::GetBestAllocationRatio() const
{
    return PersistentAttributes_.BestAllocationRatio;
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

    PersistentAttributes_.BestAllocationRatio = 0.0;
    PendingJobCount_ = 0;

    SchedulableChildren_.clear();
    TJobResources maxPossibleChildrenResourceUsage;
    for (const auto& child : EnabledChildren_) {
        child->UpdateBottomUp(dynamicAttributesList, context);

        if (IsInferringChildrenWeightsFromHistoricUsageEnabled()) {
            // NB(eshcherbin): This is a lazy parameters update so it has to be done every time.
            child->PersistentAttributes_.HistoricUsageAggregator.UpdateParameters(
                GetHistoricUsageAggregationParameters());

            auto usage = child->GetResourceUsageRatio();
            child->PersistentAttributes_.HistoricUsageAggregator.UpdateAt(context->Now, usage);
        }

        PersistentAttributes_.BestAllocationRatio = std::max(
            PersistentAttributes_.BestAllocationRatio,
            child->PersistentAttributes().BestAllocationRatio);
        PendingJobCount_ += child->GetPendingJobCount();

        maxPossibleChildrenResourceUsage += child->MaxPossibleResourceUsage();

        if (child->IsSchedulable()) {
            SchedulableChildren_.push_back(child);
        }
    }

    MaxPossibleResourceUsage_ = Min(maxPossibleChildrenResourceUsage, ResourceLimits_);
    TSchedulerElement::UpdateBottomUp(dynamicAttributesList, context);
}

void TCompositeSchedulerElement::UpdateTopDown(TDynamicAttributesList* dynamicAttributesList, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    switch (Mode_) {
        case ESchedulingMode::Fifo:
            // Easy case -- the first child get everything, others get none.
            UpdateFifo(dynamicAttributesList, context);
            break;

        case ESchedulingMode::FairShare:
            // Hard case -- compute fair shares using fit factor.
            UpdateFairShare(dynamicAttributesList, context);
            break;

        default:
            YT_ABORT();
    }

    UpdatePreemptionSettingsLimits();

    for (const auto& child : EnabledChildren_) {
        // It is necessary to update satisfaction ratio in global attributes during update.
        auto& dynamicAttributes = (*dynamicAttributesList)[child->GetTreeIndex()];
        dynamicAttributes.Active = true;
        child->UpdateDynamicAttributes(dynamicAttributesList);

        // Propagate updates to children.
        UpdateChildPreemptionSettings(child);
        child->UpdateTopDown(dynamicAttributesList, context);
    }
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

void TCompositeSchedulerElement::UpdatePreemptionSettingsLimits()
{
    YT_VERIFY(Mutable_);

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
}

void TCompositeSchedulerElement::UpdateChildPreemptionSettings(const TSchedulerElementPtr& child)
{
    YT_VERIFY(Mutable_);

    auto& childAttributes = child->Attributes();

    childAttributes.AdjustedFairShareStarvationTolerance = std::min(
        child->GetFairShareStarvationTolerance(),
        AdjustedFairShareStarvationToleranceLimit_);

    childAttributes.AdjustedMinSharePreemptionTimeout = std::max(
        child->GetMinSharePreemptionTimeout(),
        AdjustedMinSharePreemptionTimeoutLimit_);

    childAttributes.AdjustedFairSharePreemptionTimeout = std::max(
        child->GetFairSharePreemptionTimeout(),
        AdjustedFairSharePreemptionTimeoutLimit_);
}

void TCompositeSchedulerElement::UpdateDynamicAttributes(TDynamicAttributesList* dynamicAttributesList)
{
    YT_VERIFY(IsActive(*dynamicAttributesList));
    auto& attributes = (*dynamicAttributesList)[GetTreeIndex()];

    if (!IsAlive()) {
        attributes.Active = false;
        return;
    }

    // Compute local satisfaction ratio.
    attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio();
    // Adjust satisfaction ratio using children.
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

        attributes.SatisfactionRatio = std::min(
            attributes.SatisfactionRatio,
            bestChildAttributes.SatisfactionRatio);

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

// Given a non-descending continuous |f|, |f(0) = 0|, and a scalar |a|,
// computes |x \in [0,1]| s.t. |f(x) = a|.
// If |f(1) <= a| then still returns 1.
template <class F>
static double BinarySearch(const F& f, double a)
{
    if (f(1) <= a) {
        return 1.0;
    }

    double lo = 0.0;
    double hi = 1.0;
    while (hi - lo > RatioComputationPrecision) {
        double x = (lo + hi) / 2.0;
        if (f(x) < a) {
            lo = x;
        } else {
            hi = x;
        }
    }
    return (lo + hi) / 2.0;
}

template <class TGetter, class TSetter>
void TCompositeSchedulerElement::ComputeByFitting(
    const TGetter& getter,
    const TSetter& setter,
    double sum)
{
    auto getSum = [&] (double fitFactor) -> double {
        double sum = 0.0;
        for (const auto& child : EnabledChildren_) {
            sum += getter(fitFactor, child);
        }
        return sum;
    };

    // Run binary search to compute fit factor.
    double fitFactor = BinarySearch(getSum, sum);

    double resultSum = getSum(fitFactor);
    double uncertaintyRatio = 1.0;
    if (resultSum > RatioComputationPrecision && std::abs(sum - resultSum) > RatioComputationPrecision) {
        uncertaintyRatio = sum / resultSum;
    }

    // Compute actual min shares from fit factor.
    for (const auto& child : EnabledChildren_) {
        double value = getter(fitFactor, child);
        setter(child, value, uncertaintyRatio);
    }
}

void TCompositeSchedulerElement::UpdateFifo(TDynamicAttributesList* , TUpdateFairShareContext* )
{
    YT_VERIFY(Mutable_);

    auto children = EnabledChildren_;
    std::sort(
        children.begin(),
        children.end(),
        [&] (const auto& lhs, const auto& rhs) {
            return HasHigherPriorityInFifoMode(lhs.Get(), rhs.Get());
        });

    auto poolResources = TotalResourceLimits_ * Attributes_.FairShareRatio;
    auto usedFairResources = TJobResources();
    auto remainingFairResources = poolResources;

    int index = 0;
    for (const auto& child : children) {
        auto& childAttributes = child->Attributes();
        const auto& childPersistentAttributes = child->PersistentAttributes();

        childAttributes.MinShareRatio = 0.0;
        childAttributes.AdjustedMinShareRatio = 0.0;

        childAttributes.FifoIndex = index;
        ++index;

        auto offeredResources = child->ResourceDemand() * std::min(1.0, GetMinResourceRatio(remainingFairResources, child->ResourceDemand()));
        double offeredFairShareRatio = GetDominantResourceUsage(offeredResources, TotalResourceLimits_);

        double childFairShareRatio = offeredFairShareRatio;
        childFairShareRatio = std::min(childFairShareRatio, childAttributes.MaxPossibleUsageRatio);
        childFairShareRatio = std::min(childFairShareRatio, childPersistentAttributes.BestAllocationRatio);
        child->SetFairShareRatio(childFairShareRatio);

        auto acceptedResources = offeredFairShareRatio > 0
            ? offeredResources * (childFairShareRatio / offeredFairShareRatio)
            : TJobResources();

        remainingFairResources -= acceptedResources;
        usedFairResources += acceptedResources;
        if (GetDominantResourceUsage(usedFairResources, TotalResourceLimits_) > Attributes_.FairShareRatio - RatioComparisonPrecision) {
            remainingFairResources = TJobResources();
        }
    }
}

void TCompositeSchedulerElement::UpdateFairShare(TDynamicAttributesList* dynamicAttributesList, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    if (IsRoot()) {
        SetFairShareRatio(1.0);
    }

    // Compute min shares sum and min weight.
    double minShareRatioSumForPools = 0.0;
    double minShareRatioSumForOperations = 0.0;
    double minWeight = std::numeric_limits<double>::max();
    for (const auto& child : EnabledChildren_) {
        auto& childAttributes = child->Attributes();

        childAttributes.MinShareRatio = GetMaxResourceRatio(child->GetMinShareResources(), TotalResourceLimits_);
        if (child->IsOperation()) {
            minShareRatioSumForOperations += childAttributes.MinShareRatio;
        } else {
            minShareRatioSumForPools += childAttributes.MinShareRatio;
        }

        if (child->GetWeight() > RatioComputationPrecision) {
            minWeight = std::min(minWeight, child->GetWeight());
        }
    }

    // If min share sum is larger than one, adjust all children min shares to sum up to one.
    if (minShareRatioSumForPools > Attributes_.MinShareRatio + RatioComparisonPrecision) {
        context->Errors.emplace_back(
            "Impossible to satisfy resources guarantees of pool %Qv, "
            "total min share ratio of children pools is too large: %v > %v",
            GetId(),
            minShareRatioSumForPools,
            Attributes_.MinShareRatio);

        double fitFactor = Attributes_.MinShareRatio / minShareRatioSumForPools;
        for (const auto& child : EnabledChildren_) {
            auto& childAttributes = child->Attributes();
            if (child->IsOperation()) {
                childAttributes.MinShareRatio = 0.0;
            } else {
                childAttributes.MinShareRatio *= fitFactor;
            }
        }
    } else if (minShareRatioSumForPools + minShareRatioSumForOperations > Attributes_.MinShareRatio + RatioComparisonPrecision) {
        // Min share ratios of operations are fitted silently.
        double fitFactor = (Attributes_.MinShareRatio - minShareRatioSumForPools + RatioComparisonPrecision) / minShareRatioSumForOperations;
        for (const auto& child : EnabledChildren_) {
            auto& childAttributes = child->Attributes();
            if (child->IsOperation()) {
                childAttributes.MinShareRatio *= fitFactor;
            }
        }
    }

    // Compute fair shares.
    ComputeByFitting(
        [&] (double fitFactor, const TSchedulerElementPtr& child) -> double {
            const auto& childAttributes = child->Attributes();
            const auto& childPersistentAttributes = child->PersistentAttributes();
            double result = fitFactor * child->GetWeight() / minWeight;
            // Never give less than promised by min share.
            result = std::max(result, childAttributes.MinShareRatio);
            // Never give more than can be used.
            result = std::min(result, childAttributes.MaxPossibleUsageRatio);
            // Never give more than we can allocate.
            result = std::min(result, childPersistentAttributes.BestAllocationRatio);
            return result;
        },
        [&] (const TSchedulerElementPtr& child, double value, double uncertaintyRatio) {
            if (IsRoot() && uncertaintyRatio > 1.0) {
                uncertaintyRatio = 1.0;
            }
            child->SetFairShareRatio(value * uncertaintyRatio);
            if (uncertaintyRatio < 0.99 && !IsRoot()) {
                YT_LOG_DEBUG("Detected situation with parent/child fair share ratio disagreement "
                    "(Child: %v, Parent: %v, UncertaintyRatio: %v)",
                    child->GetId(),
                    child->GetParent()->GetId(),
                    uncertaintyRatio);
            }
        },
        Attributes_.FairShareRatio);

    if (IsRoot()) {
        double fairShareRatio = 0.0;
        for (const auto& child : EnabledChildren_) {
            fairShareRatio += child->GetFairShareRatio();
        }
        if (fairShareRatio < 1.0 - RatioComparisonPrecision) {
            SetFairShareRatio(fairShareRatio);
        }
    }

    // Compute guaranteed shares.
    ComputeByFitting(
        [&] (double fitFactor, const TSchedulerElementPtr& child) -> double {
            const auto& childAttributes = child->Attributes();
            double result = fitFactor * child->GetWeight() / minWeight;
            // Never give less than promised by min share.
            result = std::max(result, childAttributes.MinShareRatio);
            return result;
        },
        [&] (const TSchedulerElementPtr& child, double value, double uncertaintyRatio) {
            auto& attributes = child->Attributes();
            attributes.GuaranteedResourcesRatio = value * uncertaintyRatio;
        },
        Attributes_.GuaranteedResourcesRatio);

    // Compute adjusted min share ratios.
    for (const auto& child : EnabledChildren_) {
        auto& childAttributes = child->Attributes();
        const auto& childPersistentAttributes = child->PersistentAttributes();
        double result = childAttributes.MinShareRatio;
        // Never give more than can be used.
        result = std::min(result, childAttributes.MaxPossibleUsageRatio);
        // Never give more than we can allocate.
        result = std::min(result, childPersistentAttributes.BestAllocationRatio);
        childAttributes.AdjustedMinShareRatio = result;
    }
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
    double bestChildSatisfactionRatio = std::numeric_limits<double>::max();
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
        NLogging::TLogger(logger).AddTag("PoolId: %v", id))
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

double TPool::GetMaxShareRatio() const
{
    return Config_->MaxShareRatio.value_or(1.0);
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
    double fairShareRatio,
    const TJobResources& totalResourceLimits,
    double preemptionSatisfactionThreshold,
    double aggressivePreemptionSatisfactionThreshold,
    int* moveCount)
{
    TWriterGuard guard(JobPropertiesMapLock_);

    auto getUsageRatio = [&] (const TJobResources& resourceUsage) {
        return GetDominantResourceUsage(resourceUsage, totalResourceLimits);
    };

    auto balanceLists = [&] (
        TJobIdList* left,
        TJobIdList* right,
        TJobResources resourceUsage,
        double fairShareRatioBound,
        std::function<void(TJobProperties*)> onMovedLeftToRight,
        std::function<void(TJobProperties*)> onMovedRightToLeft)
    {
        while (!left->empty()) {
            auto jobId = left->back();
            auto* jobProperties = GetJobProperties(jobId);

            if (getUsageRatio(resourceUsage - jobProperties->ResourceUsage) < fairShareRatioBound) {
                break;
            }

            left->pop_back();
            right->push_front(jobId);
            jobProperties->JobIdListIterator = right->begin();
            onMovedLeftToRight(jobProperties);

            resourceUsage -= jobProperties->ResourceUsage;
            ++(*moveCount);
        }

        while (!right->empty()) {
            if (getUsageRatio(resourceUsage) >= fairShareRatioBound) {
                break;
            }

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

    bool enableLogging = (UpdatePreemptableJobsListCount_.fetch_add(1) % UpdatePreemptableJobsListLoggingPeriod_) == 0;

    YT_LOG_DEBUG_IF(enableLogging,
        "Update preemptable job lists inputs (FairShareRatio: %v, TotalResourceLimits: %v, "
        "PreemptionSatisfactionThreshold: %v, AggressivePreemptionSatisfactionThreshold: %v)",
        fairShareRatio,
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
            fairShareRatio * aggressivePreemptionSatisfactionThreshold,
            setAggressivelyPreemptable,
            setNonPreemptable);

        auto nonpreemptableAndAggressivelyPreemptableResourceUsage_ = balanceLists(
            &AggressivelyPreemptableJobs_,
            &PreemptableJobs_,
            startNonPreemptableAndAggressivelyPreemptableResourceUsage_,
            Preemptable_ ? fairShareRatio * preemptionSatisfactionThreshold : 1.0,
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
        return NClassicScheduler::GetCustomProfilingTag(*tagName);
    } else {
        return NClassicScheduler::GetCustomProfilingTag(MissingCustomProfilingTag);
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

    if (PersistentAttributes_.LastBestAllocationRatioUpdateTime + TreeConfig_->BestAllocationRatioUpdatePeriod > context->Now) {
        auto allocationLimits = GetAdjustedResourceLimits(
            ResourceDemand_,
            TotalResourceLimits_,
            GetHost()->GetExecNodeMemoryDistribution(SchedulingTagFilter_ & TreeConfig_->NodesFilter));
        auto dominantLimit = GetResource(TotalResourceLimits_, Attributes_.DominantResource);
        auto dominantAllocationLimit = GetResource(allocationLimits, Attributes_.DominantResource);
        PersistentAttributes_.BestAllocationRatio =
            dominantLimit == 0 ? 1.0 : dominantAllocationLimit / dominantLimit;
        PersistentAttributes_.LastBestAllocationRatioUpdateTime = context->Now;
    }

    if (!IsSchedulable()) {
        (*dynamicAttributesList)[GetTreeIndex()].Active = false;
        ++context->UnschedulableReasons[*UnschedulableReason_];
    }
}

void TOperationElement::UpdateTopDown(TDynamicAttributesList* dynamicAttributesList, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    TSchedulerElement::UpdateTopDown(dynamicAttributesList, context);
    // If fair share ratio equals demand ratio then we want to explicitly disable preemption.
    // It is necessary since some job's resource usage may increase before the next fair share update,
    //  and in this case we don't want any jobs to become preemptable
    bool isFairShareRatioEqualToDemandRatio =
        std::abs(Attributes_.DemandRatio - GetFairShareRatio()) < RatioComparisonPrecision &&
        Attributes_.DemandRatio > RatioComparisonPrecision;
    bool newPreemptableValue = !isFairShareRatioEqualToDemandRatio;
    bool oldPreemptableValue = OperationElementSharedState_->GetPreemptable();
    if (oldPreemptableValue != newPreemptableValue) {
        YT_LOG_DEBUG("Preemptable status changed %v -> %v", oldPreemptableValue, newPreemptableValue);
        OperationElementSharedState_->SetPreemptable(newPreemptableValue);
    }

    UpdatePreemptableJobsList();
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

double TOperationElement::GetMaxShareRatio() const
{
    return Spec_->MaxShareRatio.value_or(1.0);
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

    while (element && !element->IsRoot()) {
        if (element->GetStarving()) {
            OperationElementSharedState_->UpdatePreemptionStatusStatistics(EOperationPreemptionStatus::ForbiddenSinceStarvingParent);
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
            OperationElementSharedState_->UpdatePreemptionStatusStatistics(EOperationPreemptionStatus::ForbiddenSinceUnsatisfiedParentOrSelf);
            return false;
        }

        element = element->GetParent();
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
        GetFairShareRatio(),
        TotalResourceLimits_,
        TreeConfig_->PreemptionSatisfactionThreshold,
        TreeConfig_->AggressivePreemptionSatisfactionThreshold,
        &moveCount);

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
        logger)
{
    SetFairShareRatio(1.0);
    Attributes_.GuaranteedResourcesRatio = 1.0;
    Attributes_.AdjustedMinShareRatio = 1.0;
    Attributes_.MinShareRatio = 1.0;
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
    UpdateTopDown(dynamicAttributesList, context);
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

double TRootElement::GetMaxShareRatio() const
{
    return 1.0;
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

} // namespace NYT::NScheduler::NClassicScheduler
