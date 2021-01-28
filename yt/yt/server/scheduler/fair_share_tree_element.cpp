#include "fair_share_tree_element.h"

#include "fair_share_tree.h"
#include "helpers.h"
#include "piecewise_linear_function_helpers.h"
#include "resource_tree_element.h"
#include "scheduling_context.h"

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>

#include <yt/core/misc/heap.h>
#include <yt/core/misc/finally.h>
#include <yt/core/misc/historic_usage_aggregator.h>

#include <yt/core/profiling/timing.h>

#include <util/generic/ymath.h>

#include <math.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;

using NProfiling::CpuDurationToDuration;

////////////////////////////////////////////////////////////////////////////////

static const TString InvalidCustomProfilingTag("invalid");

////////////////////////////////////////////////////////////////////////////////

namespace {

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

TScheduleJobsProfilingCounters::TScheduleJobsProfilingCounters(
    const NProfiling::TRegistry& profiler)
    : PrescheduleJobTime(profiler.Timer("/preschedule_job_time"))
    , TotalControllerScheduleJobTime(profiler.Timer("/controller_schedule_job_time/total"))
    , ExecControllerScheduleJobTime(profiler.Timer("/controller_schedule_job_time/exec"))
    , StrategyScheduleJobTime(profiler.Timer("/strategy_schedule_job_time"))
    , PackingRecordHeartbeatTime(profiler.Timer("/packing_record_heartbeat_time"))
    , PackingCheckTime(profiler.Timer("/packing_check_time"))
    , AnalyzeJobsTime(profiler.Timer("/analyze_jobs"))
    , ScheduleJobAttemptCount(profiler.Counter("/schedule_job_attempt_count"))
    , ScheduleJobFailureCount(profiler.Counter("/schedule_job_failure_count"))
{
    for (auto reason : TEnumTraits<NControllerAgent::EScheduleJobFailReason>::GetDomainValues()) {
        ControllerScheduleJobFail[reason] = profiler
            .WithTag("reason", FormatEnum(reason))
            .Counter("/controller_schedule_job_fail");
    }
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TDetailedFairShare& detailedFairShare)
{
    return ToStringViaBuilder(detailedFairShare);
}

void FormatValue(TStringBuilderBase* builder, const TDetailedFairShare& detailedFairShare, TStringBuf /* format */)
{
    builder->AppendFormat(
        "{StrongGuarantee: %.6g, IntegralGuarantee: %.6g, WeightProportional: %.6g}",
        detailedFairShare.StrongGuarantee,
        detailedFairShare.IntegralGuarantee,
        detailedFairShare.WeightProportional);
}

void Serialize(const TDetailedFairShare& detailedFairShare, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("strong_guarantee").Value(detailedFairShare.StrongGuarantee)
            .Item("integral_guarantee").Value(detailedFairShare.IntegralGuarantee)
            .Item("weight_proportional").Value(detailedFairShare.WeightProportional)
            .Item("total").Value(detailedFairShare.Total)
        .EndMap();
}

void SerializeDominant(const TDetailedFairShare& detailedFairShare, TFluentAny fluent)
{
    fluent
        .BeginMap()
            .Item("strong_guarantee").Value(MaxComponent(detailedFairShare.StrongGuarantee))
            .Item("integral_guarantee").Value(MaxComponent(detailedFairShare.IntegralGuarantee))
            .Item("weight_proportional").Value(MaxComponent(detailedFairShare.WeightProportional))
            .Item("total").Value(MaxComponent(detailedFairShare.Total))
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TFairShareSchedulingStage::TFairShareSchedulingStage(TString loggingName, TScheduleJobsProfilingCounters profilingCounters)
    : LoggingName(std::move(loggingName))
    , ProfilingCounters(std::move(profilingCounters))
{ }

////////////////////////////////////////////////////////////////////////////////

TFairShareContext::TFairShareContext(
    ISchedulingContextPtr schedulingContext,
    int treeSize,
    std::vector<TSchedulingTagFilter> registeredSchedulingTagFilters,
    bool enableSchedulingInfoLogging,
    const NLogging::TLogger& logger)
    : SchedulingContext_(std::move(schedulingContext))
    , TreeSize_(treeSize)
    , RegisteredSchedulingTagFilters_(std::move(registeredSchedulingTagFilters))
    , EnableSchedulingInfoLogging_(enableSchedulingInfoLogging)
    , Logger(logger)
{ }

void TFairShareContext::PrepareForScheduling(const TRootElementPtr& rootElement)
{
    // TODO(ignat): add check that this method called before rootElement->PrescheduleJob (or refactor this code).
    if (!Initialized_) {
        Initialized_ = true;

        DynamicAttributesList_.resize(TreeSize_);
        CanSchedule_.reserve(RegisteredSchedulingTagFilters_.size());
        for (const auto& filter : RegisteredSchedulingTagFilters_) {
            CanSchedule_.push_back(SchedulingContext_->CanSchedule(filter));
        }

        rootElement->CalculateCurrentResourceUsage(this);
    } else {
        for (auto& attributes : DynamicAttributesList_) {
            attributes.Active = false;
        }
    }
}

TDynamicAttributes& TFairShareContext::DynamicAttributesFor(const TSchedulerElement* element)
{
    YT_VERIFY(Initialized_);

    int index = element->GetTreeIndex();
    YT_VERIFY(index != UnassignedTreeIndex && index < DynamicAttributesList_.size());
    return DynamicAttributesList_[index];
}

const TDynamicAttributes& TFairShareContext::DynamicAttributesFor(const TSchedulerElement* element) const
{
    YT_VERIFY(Initialized_);

    int index = element->GetTreeIndex();
    YT_VERIFY(index != UnassignedTreeIndex && index < DynamicAttributesList_.size());
    return DynamicAttributesList_[index];
}

TFairShareContext::TStageState::TStageState(TFairShareSchedulingStage* schedulingStage)
    : SchedulingStage(schedulingStage)
{ }

void TFairShareContext::StartStage(TFairShareSchedulingStage* schedulingStage)
{
    YT_VERIFY(!StageState_);
    StageState_.emplace(TStageState(schedulingStage));
}

void TFairShareContext::ProfileStageTimingsAndLogStatistics()
{
    YT_VERIFY(StageState_);

    ProfileStageTimings();

    if (StageState_->ScheduleJobAttemptCount > 0 && EnableSchedulingInfoLogging_) {
        LogStageStatistics();
    }
}

void TFairShareContext::FinishStage()
{
    YT_VERIFY(StageState_);
    StageState_ = std::nullopt;
}

void TFairShareContext::ProfileStageTimings()
{
    if (!Initialized_) {
        return;
    }

    YT_VERIFY(StageState_);

    auto* profilingCounters = &StageState_->SchedulingStage->ProfilingCounters;

    profilingCounters->PrescheduleJobTime.Record(StageState_->PrescheduleDuration);

    auto strategyScheduleJobDuration = StageState_->TotalDuration
        - StageState_->PrescheduleDuration
        - StageState_->TotalScheduleJobDuration;
    profilingCounters->StrategyScheduleJobTime.Record(strategyScheduleJobDuration);

    profilingCounters->TotalControllerScheduleJobTime.Record(StageState_->TotalScheduleJobDuration);
    profilingCounters->ExecControllerScheduleJobTime.Record(StageState_->ExecScheduleJobDuration);
    profilingCounters->PackingRecordHeartbeatTime.Record(StageState_->PackingRecordHeartbeatDuration);
    profilingCounters->PackingCheckTime.Record(StageState_->PackingCheckDuration);
    profilingCounters->AnalyzeJobsTime.Record(StageState_->AnalyzeJobsDuration);

    profilingCounters->ScheduleJobAttemptCount.Increment(StageState_->ScheduleJobAttemptCount);
    profilingCounters->ScheduleJobFailureCount.Increment(StageState_->ScheduleJobFailureCount);

    for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
        profilingCounters->ControllerScheduleJobFail[reason].Increment(StageState_->FailedScheduleJob[reason]);
    }
}

void TFairShareContext::LogStageStatistics()
{
    if (!Initialized_) {
        return;
    }

    YT_VERIFY(StageState_);

    YT_LOG_DEBUG("%v scheduling statistics (ActiveTreeSize: %v, ActiveOperationCount: %v, DeactivationReasons: %v, CanStartMoreJobs: %v, Address: %v, SchedulingSegment: %v)",
        StageState_->SchedulingStage->LoggingName,
        StageState_->ActiveTreeSize,
        StageState_->ActiveOperationCount,
        StageState_->DeactivationReasons,
        SchedulingContext_->CanStartMoreJobs(),
        SchedulingContext_->GetNodeDescriptor().Address,
        SchedulingContext_->GetSchedulingSegment());
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

void TSchedulerElement::MarkImmutable()
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
    HasSpecifiedResourceLimits_ = GetSpecifiedResourceLimits() != TJobResources::Infinite();

    auto specifiedResourceLimits = GetSpecifiedResourceLimits();
    if (PersistentAttributes_.AppliedResourceLimits != specifiedResourceLimits) {
        std::vector<TResourceTreeElementPtr> descendantOperationElements;
        if (!IsOperation() && PersistentAttributes_.AppliedResourceLimits == TJobResources::Infinite() && specifiedResourceLimits != TJobResources::Infinite()) {
            // NB: this code executed in control thread, therefore tree structure is actual and agreed with tree structure of resource tree.
            CollectResourceTreeOperationElements(&descendantOperationElements);
        }
        ResourceTreeElement_->SetResourceLimits(specifiedResourceLimits, descendantOperationElements);
        PersistentAttributes_.AppliedResourceLimits = specifiedResourceLimits;
    }
}

void TSchedulerElement::UpdateCumulativeAttributes(TUpdateFairShareContext* /* context */)
{
    YT_VERIFY(Mutable_);

    UpdateAttributes();
}

void TSchedulerElement::UpdatePreemptionAttributes()
{
    YT_VERIFY(Mutable_);

    if (Parent_) {
        Attributes_.AdjustedFairShareStarvationTolerance = std::min(
            GetFairShareStarvationTolerance(),
            Parent_->AdjustedFairShareStarvationToleranceLimit());

        Attributes_.AdjustedFairSharePreemptionTimeout = std::max(
            GetFairSharePreemptionTimeout(),
            Parent_->AdjustedFairSharePreemptionTimeoutLimit());
    }
}

void TSchedulerElement::UpdateSchedulableAttributesFromDynamicAttributes(TDynamicAttributesList* dynamicAttributesList)
{
    YT_VERIFY(Mutable_);

    auto& attributes = (*dynamicAttributesList)[GetTreeIndex()];

    UpdateDynamicAttributes(dynamicAttributesList);

    Attributes_.SatisfactionRatio = attributes.SatisfactionRatio;
    Attributes_.LocalSatisfactionRatio = ComputeLocalSatisfactionRatio(ResourceUsageAtUpdate_);
    Attributes_.Alive = attributes.Active;
}

void TSchedulerElement::UpdateAttributes()
{
    YT_VERIFY(Mutable_);

    Attributes_.LimitsShare = ComputeLimitsShare();
    YT_VERIFY(Dominates(TResourceVector::Ones(), Attributes_.LimitsShare));
    YT_VERIFY(Dominates(Attributes_.LimitsShare, TResourceVector::Zero()));

    Attributes_.StrongGuaranteeShare = TResourceVector::FromJobResources(EffectiveStrongGuaranteeResources_, TotalResourceLimits_);

    // NB: We need to ensure that |FairShareByFitFactor_(0.0)| is less than or equal to |LimitsShare| so that there exists a feasible fit factor and |MaxFitFactorBySuggestion_| is well defined.
    // To achieve this we limit |StrongGuarantee| with |LimitsShare| here, and later adjust the sum of children's |StrongGuarantee| to fit into the parent's |StrongGuarantee|.
    // This way children can't ask more than parent's |LimitsShare| when given a zero suggestion.
    Attributes_.StrongGuaranteeShare = TResourceVector::Min(Attributes_.StrongGuaranteeShare, Attributes_.LimitsShare);

    if (ResourceUsageAtUpdate_ == TJobResources()) {
        Attributes_.DominantResource = GetDominantResource(ResourceDemand_, TotalResourceLimits_);
    } else {
        Attributes_.DominantResource = GetDominantResource(ResourceUsageAtUpdate_, TotalResourceLimits_);
    }

    Attributes_.UsageShare = TResourceVector::FromJobResources(ResourceUsageAtUpdate_, TotalResourceLimits_);
    Attributes_.DemandShare = TResourceVector::FromJobResources(ResourceDemand_, TotalResourceLimits_);
    YT_VERIFY(Dominates(Attributes_.DemandShare, Attributes_.UsageShare));
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

TOperationElement* TSchedulerElement::AsOperation()
{
    return nullptr;
}

TPool* TSchedulerElement::AsPool()
{
    return nullptr;
}

TString TSchedulerElement::GetLoggingAttributesString() const
{
    return Format(
        "Status: %v, "
        "DominantResource: %v, "
        "DemandShare: %.6g, "
        "UsageShare: %.6g, "
        "LimitsShare: %.6g, "
        "StrongGuaranteeShare: %.6g, "
        "FairShare: %.6g, "
        "Satisfaction: %.4lg, "
        "LocalSatisfaction: %.4lg, "
        "PromisedFairShare: %.6g, "
        "Starving: %v, "
        "Weight: %v, "
        "Volume: %v",
        GetStatus(),
        Attributes_.DominantResource,
        Attributes_.DemandShare,
        Attributes_.UsageShare,
        Attributes_.LimitsShare,
        Attributes_.StrongGuaranteeShare,
        Attributes_.FairShare,
        Attributes_.SatisfactionRatio,
        Attributes_.LocalSatisfactionRatio,
        Attributes_.PromisedFairShare,
        GetStarving(),
        GetWeight(),
        GetAccumulatedResourceRatioVolume());
}

TString TSchedulerElement::GetLoggingString() const
{
    return Format("Scheduling info for tree %Qv = {%v}", GetTreeId(), GetLoggingAttributesString());
}

bool TSchedulerElement::IsActive(const TDynamicAttributesList& dynamicAttributesList) const
{
    return dynamicAttributesList[GetTreeIndex()].Active;
}


TJobResources TSchedulerElement::GetCurrentResourceUsage(const TDynamicAttributesList& dynamicAttributesList) const
{
    return dynamicAttributesList[GetTreeIndex()].ResourceUsage;
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

    if (!TreeConfig_->InferWeightFromStrongGuaranteeShareMultiplier) {
        return 1.0;
    }
    double strongGuaranteeDominantShare = MaxComponent(Attributes().StrongGuaranteeShare);

    if (strongGuaranteeDominantShare < RatioComputationPrecision) {
        return 1.0;
    }

    double parentStrongGuaranteeDominantShare = 1.0;
    if (GetParent()) {
        parentStrongGuaranteeDominantShare = MaxComponent(GetParent()->Attributes().StrongGuaranteeShare);
    }

    if (parentStrongGuaranteeDominantShare < RatioComputationPrecision) {
        return 1.0;
    }

    return strongGuaranteeDominantShare *
        (*TreeConfig_->InferWeightFromStrongGuaranteeShareMultiplier) /
        parentStrongGuaranteeDominantShare;
}

TResourceLimitsConfigPtr TSchedulerElement::GetStrongGuaranteeResourcesConfig() const
{
    return nullptr;
}

TJobResources TSchedulerElement::GetSpecifiedStrongGuaranteeResources() const
{
    auto guaranteeConfig = GetStrongGuaranteeResourcesConfig();
    YT_VERIFY(guaranteeConfig);
    return ToJobResources(guaranteeConfig, {});
}

void TSchedulerElement::DetermineEffectiveStrongGuaranteeResources()
{
    YT_VERIFY(Mutable_);
}

TCompositeSchedulerElement* TSchedulerElement::GetMutableParent()
{
    return Parent_;
}

const TCompositeSchedulerElement* TSchedulerElement::GetParent() const
{
    return Parent_;
}

EIntegralGuaranteeType TSchedulerElement::GetIntegralGuaranteeType() const
{
    return EIntegralGuaranteeType::None;
}

void TSchedulerElement::IncreaseHierarchicalIntegralShare(const TResourceVector& delta)
{
    auto* current = this;
    while (current) {
        current->Attributes().ProposedIntegralShare += delta;
        current = current->Parent_;
    }
}

TInstant TSchedulerElement::GetStartTime() const
{
    return StartTime_;
}

int TSchedulerElement::GetPendingJobCount() const
{
    return PendingJobCount_;
}

ESchedulableStatus TSchedulerElement::GetStatus(bool /* atUpdate */) const
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

TJobMetrics TSchedulerElement::GetJobMetrics() const
{
    return ResourceTreeElement_->GetJobMetrics();
}

bool TSchedulerElement::AreResourceLimitsViolated() const
{
    return ResourceTreeElement_->AreResourceLimitsViolated();
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

double TSchedulerElement::GetMaxShareRatio() const
{
    return MaxComponent(GetMaxShare());
}

TResourceVector TSchedulerElement::GetResourceUsageShare() const
{
    return TResourceVector::FromJobResources(ResourceUsageAtUpdate_, TotalResourceLimits_);
}

double TSchedulerElement::GetResourceDominantUsageShareAtUpdate() const
{
    return MaxComponent(Attributes_.UsageShare);
}

TString TSchedulerElement::GetTreeId() const
{
    return TreeId_;
}

bool TSchedulerElement::CheckDemand(const TJobResources& delta, const TFairShareContext& context)
{
    return ResourceTreeElement_->CheckDemand(delta, ResourceDemand(), context.DynamicAttributesFor(this).ResourceUsageDiscount);
}

TJobResources TSchedulerElement::GetLocalAvailableResourceLimits(const TFairShareContext& context) const
{
    if (HasSpecifiedResourceLimits_) {
        return ComputeAvailableResources(
            ResourceLimits_,
            ResourceTreeElement_->GetResourceUsageWithPrecommit(),
            context.DynamicAttributesFor(this).ResourceUsageDiscount);
    }
    return TJobResources::Infinite();
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
    TString id,
    EResourceTreeElementKind elementKind,
    const NLogging::TLogger& logger)
    : TSchedulerElementFixedState(host, treeHost, std::move(treeConfig), std::move(treeId))
    , ResourceTreeElement_(New<TResourceTreeElement>(
        TreeHost_->GetResourceTree(),
        id,
        elementKind))
    , Logger(logger)
{
    if (id == RootPoolName) {
        ResourceTreeElement_->MarkInitialized();
    }
}

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

double TSchedulerElement::ComputeLocalSatisfactionRatio(const TJobResources& resourceUsage) const
{
    const auto& fairShare = Attributes_.FairShare.Total;

    // Check for corner cases.
    if (Dominates(TResourceVector::SmallEpsilon(), fairShare)) {
        return InfiniteSatisfactionRatio;
    }

    auto usageShare = TResourceVector::FromJobResources(resourceUsage, TotalResourceLimits_);

    // Check if the element is over-satisfied.
    if (TResourceVector::Any(usageShare, fairShare, [] (double usage, double fair) { return usage > fair; })) {
        double satisfactionRatio = std::min(
            MaxComponent(
                Div(usageShare, fairShare, /* zeroDivByZero */ 0.0, /* oneDivByZero */ InfiniteSatisfactionRatio)),
            InfiniteSatisfactionRatio);
        YT_VERIFY(satisfactionRatio >= 1.0);
        return satisfactionRatio;
    }

    double satisfactionRatio = 0.0;
    if (AreAllResourcesBlocked()) {
        // NB(antonkikh): Using |MaxComponent| would lead to satisfaction ratio being non-monotonous.
        satisfactionRatio = MinComponent(Div(usageShare, fairShare, /* zeroDivByZero */ 1.0, /* oneDivByZero */ 1.0));
    } else {
        satisfactionRatio = 0.0;
        for (auto resourceType : TEnumTraits<EJobResourceType>::GetDomainValues()) {
            if (!IsResourceBlocked(resourceType) && fairShare[resourceType] != 0.0) {
                satisfactionRatio = std::max(satisfactionRatio, usageShare[resourceType] / fairShare[resourceType]);
            }
        }
    }

    YT_VERIFY(satisfactionRatio <= 1.0);
    return satisfactionRatio;
}

bool TSchedulerElement::IsResourceBlocked(EJobResourceType resource) const
{
    return Attributes_.DemandShare[resource] == Attributes_.FairShare.Total[resource];
}

bool TSchedulerElement::AreAllResourcesBlocked() const
{
    return Attributes_.DemandShare == Attributes_.FairShare.Total;
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

ESchedulableStatus TSchedulerElement::GetStatusImpl(double tolerance, bool atUpdate) const
{
    auto usageShare = atUpdate
        ? Attributes_.UsageShare
        : GetResourceUsageShare();

    if (Dominates(Attributes_.FairShare.Total + TResourceVector::Epsilon(), Attributes_.DemandShare)) {
        tolerance = 1.0;
    }

    if (IsStrictlyDominatesNonBlocked(Attributes_.FairShare.Total * tolerance, usageShare)) {
        return ESchedulableStatus::BelowFairShare;
    }

    return ESchedulableStatus::Normal;
}

void TSchedulerElement::CheckForStarvationImpl(TDuration fairSharePreemptionTimeout, TInstant now)
{
    YT_VERIFY(Mutable_);

    auto status = GetStatus();
    switch (status) {
        case ESchedulableStatus::BelowFairShare:
            if (!PersistentAttributes_.BelowFairShareSince) {
                PersistentAttributes_.BelowFairShareSince = now;
            } else if (now > *PersistentAttributes_.BelowFairShareSince + fairSharePreemptionTimeout) {
                SetStarving(true);
            }
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

TResourceVector TSchedulerElement::ComputeLimitsShare() const
{
    return TResourceVector::FromJobResources(Min(ResourceLimits_, TotalResourceLimits_), TotalResourceLimits_);
}

TResourceVector TSchedulerElement::GetVectorSuggestion(double suggestion) const
{
    auto vectorSuggestion = TResourceVector::FromDouble(suggestion) + Attributes().StrongGuaranteeShare;
    vectorSuggestion = TResourceVector::Min(vectorSuggestion, Attributes().LimitsShare);
    return vectorSuggestion;
}

void TSchedulerElement::PrepareFairShareFunctions(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    if (AreFairShareFunctionsPrepared_) {
        return;
    }

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
    YT_VERIFY(MaxFitFactorBySuggestion_->LeftFunctionBound() == 0.0);
    YT_VERIFY(MaxFitFactorBySuggestion_->RightFunctionBound() == 1.0);
    NDetail::VerifyNondecreasing(*MaxFitFactorBySuggestion_, Logger);
    YT_VERIFY(MaxFitFactorBySuggestion_->IsTrimmed());

    {
        TWallTimer timer;
        FairShareBySuggestion_ = FairShareByFitFactor_->Compose(*MaxFitFactorBySuggestion_);
        context->ComposeTotalTime += timer.GetElapsedCpuTime();
    }
    YT_VERIFY(FairShareBySuggestion_.has_value());
    YT_VERIFY(FairShareBySuggestion_->LeftFunctionBound() == 0.0);
    YT_VERIFY(FairShareBySuggestion_->RightFunctionBound() == 1.0);
    NDetail::VerifyNondecreasing(*FairShareBySuggestion_, Logger);
    YT_VERIFY(FairShareBySuggestion_->IsTrimmed());

    {
        TWallTimer timer;
        *FairShareBySuggestion_ = NDetail::CompressFunction(*FairShareBySuggestion_, NDetail::CompressFunctionEpsilon);
        context->CompressFunctionTotalTime += timer.GetElapsedCpuTime();
    }
    NDetail::VerifyNondecreasing(*FairShareBySuggestion_, Logger);

    AreFairShareFunctionsPrepared_ = true;
}

void TSchedulerElement::ResetFairShareFunctions()
{
    AreFairShareFunctionsPrepared_ = false;
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

        double limit = Attributes().LimitsShare[r];
        // NB(eshcherbin): We definitely cannot use a precise inequality here. See YT-13864.
        YT_VERIFY(fsbffComponent.LeftFunctionValue() < limit + RatioComputationPrecision);
        limit = std::min(std::max(limit, fsbffComponent.LeftFunctionValue()), fsbffComponent.RightFunctionValue());

        double guarantee = Attributes().GetGuaranteeShare()[r];
        guarantee = std::min(std::max(guarantee, fsbffComponent.LeftFunctionValue()), limit);

        auto mffForComponent = std::move(fsbffComponent)
            .Transpose()
            .Narrow(guarantee, limit)
            .TrimLeft()
            .Shift(/* deltaArgument */ -guarantee)
            .ExtendRight(/* newRightBound */ 1.0)
            .Trim();
        mffForComponents.push_back(std::move(mffForComponent));
    }

    {
        TWallTimer timer;
        MaxFitFactorBySuggestion_ = PointwiseMin(mffForComponents);
        context->PointwiseMinTotalTime += timer.GetElapsedCpuTime();
    }
}

std::optional<TMeteringKey> TSchedulerElement::GetMeteringKey() const
{
    return std::nullopt;
}

void TSchedulerElement::BuildResourceMetering(const std::optional<TMeteringKey>& /*key*/, TMeteringMap* /*statistics*/) const
{ }

double TSchedulerElement::GetAccumulatedResourceRatioVolume() const
{
    return GetMinResourceRatio(PersistentAttributes_.AccumulatedResourceVolume, TotalResourceLimits_);
}

TJobResources TSchedulerElement::GetAccumulatedResourceVolume() const
{
    return PersistentAttributes_.AccumulatedResourceVolume;
}

void TSchedulerElement::InitAccumulatedResourceVolume(TJobResources resourceVolume)
{
    YT_VERIFY(PersistentAttributes_.AccumulatedResourceVolume == TJobResources());
    PersistentAttributes_.AccumulatedResourceVolume = resourceVolume;
}

double TSchedulerElement::GetIntegralShareRatioByVolume() const
{
    return GetAccumulatedResourceRatioVolume() / TreeConfig_->IntegralGuarantees->SmoothPeriod.SecondsFloat();
}

void TSchedulerElement::Profile(ISensorWriter* writer, bool profilingCompatibilityEnabled) const
{
    const auto& detailedFairShare = Attributes().FairShare;

    if (profilingCompatibilityEnabled) {
        writer->AddGauge("/fair_share_ratio_x100000", static_cast<i64>(MaxComponent(Attributes().FairShare.Total) * 1e5));
        writer->AddGauge("/usage_ratio_x100000", static_cast<i64>(GetResourceDominantUsageShareAtUpdate() * 1e5));
        writer->AddGauge("/demand_ratio_x100000", static_cast<i64>(MaxComponent(Attributes().DemandShare) * 1e5));
        writer->AddGauge("/unlimited_demand_fair_share_ratio_x100000", static_cast<i64>(MaxComponent(Attributes().PromisedFairShare) * 1e5));
        writer->AddGauge("/accumulated_resource_ratio_volume_x100000", static_cast<i64>(GetAccumulatedResourceRatioVolume() * 1e5));
        writer->AddGauge("/min_share_guarantee_ratio_x100000", static_cast<i64>(MaxComponent(detailedFairShare.StrongGuarantee) * 1e5));
        writer->AddGauge("/integral_guarantee_ratio_x100000", static_cast<i64>(MaxComponent(detailedFairShare.IntegralGuarantee) * 1e5));
        writer->AddGauge("/weight_proportional_ratio_x100000", static_cast<i64>(MaxComponent(detailedFairShare.WeightProportional) * 1e5));
    } else {
        writer->AddGauge("/dominant_fair_share", MaxComponent(Attributes().FairShare.Total));
        writer->AddGauge("/dominant_usage_share", GetResourceDominantUsageShareAtUpdate());
        writer->AddGauge("/dominant_demand_share", MaxComponent(Attributes().DemandShare));
        writer->AddGauge("/promised_dominant_fair_share", MaxComponent(Attributes().PromisedFairShare));
        writer->AddGauge("/accumulated_volume_dominant_share", GetAccumulatedResourceRatioVolume());
        writer->AddGauge("/dominant_fair_share/strong_guarantee", MaxComponent(detailedFairShare.StrongGuarantee));
        writer->AddGauge("/dominant_fair_share/integral_guarantee", MaxComponent(detailedFairShare.IntegralGuarantee));
        writer->AddGauge("/dominant_fair_share/weight_proportional", MaxComponent(detailedFairShare.WeightProportional));
        writer->AddGauge("/dominant_fair_share/total", MaxComponent(detailedFairShare.Total));
    }

    ProfileResources(writer, ResourceUsageAtUpdate(), "/resource_usage");
    ProfileResources(writer, ResourceLimits(), "/resource_limits");
    ProfileResources(writer, ResourceDemand(), "/resource_demand");

    GetJobMetrics().Profile(writer);

    bool enableVectorProfiling;
    if (IsOperation()) {
        enableVectorProfiling = TreeConfig_->EnableOperationsVectorProfiling;
    } else {
        enableVectorProfiling = TreeConfig_->EnablePoolsVectorProfiling;
    }

    if (enableVectorProfiling) {
        const auto& profiledResources = IsOperation()
            ? TreeConfig_->ProfiledOperationResources
            : TreeConfig_->ProfiledPoolResources;

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
            Attributes_.UsageShare,
            "/usage_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            Attributes_.DemandShare,
            "/demand_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            Attributes_.LimitsShare,
            "/limits_share");

        if (profilingCompatibilityEnabled) {
            ProfileResourceVector(
                writer,
                profiledResources,
                Attributes_.StrongGuaranteeShare,
                "/min_share");
        } else {
            ProfileResourceVector(
                writer,
                profiledResources,
                Attributes_.StrongGuaranteeShare,
                "/strong_guarantee_share");
        }

        ProfileResourceVector(
            writer,
            profiledResources,
            Attributes_.ProposedIntegralShare,
            "/proposed_integral_share");

        ProfileResourceVector(
            writer,
            profiledResources,
            Attributes_.PromisedFairShare,
            "/promised_fair_share");

        if (!IsOperation()) {
            ProfileResources(
                writer,
                GetAccumulatedResourceVolume(),
                "/accumulated_resource_volume");
        }
    }
}

bool TSchedulerElement::AreDetailedLogsEnabled() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TCompositeSchedulerElement::TCompositeSchedulerElement(
    ISchedulerStrategyHost* host,
    IFairShareTreeHost* treeHost,
    TFairShareStrategyTreeConfigPtr treeConfig,
    NProfiling::TTagId profilingTag,
    const TString& treeId,
    const TString& id,
    EResourceTreeElementKind elementKind,
    const NLogging::TLogger& logger)
    : TSchedulerElement(host, treeHost, std::move(treeConfig), treeId, id, elementKind, logger)
    , Profiler_(TreeHost_->GetProfiler()
        .WithRequiredTag("pool", id, -1))
    , BufferedProducer_(New<TBufferedProducer>())
{
    Profiler_.AddProducer("/pools", BufferedProducer_);
}

TCompositeSchedulerElement::TCompositeSchedulerElement(
    const TCompositeSchedulerElement& other,
    TCompositeSchedulerElement* clonedParent)
    : TSchedulerElement(other, clonedParent)
    , TCompositeSchedulerElementFixedState(other)
    , Profiler_(other.Profiler_)
    , BufferedProducer_(other.BufferedProducer_)
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

void TCompositeSchedulerElement::MarkImmutable()
{
    TSchedulerElement::MarkImmutable();
    for (const auto& child : EnabledChildren_) {
        child->MarkImmutable();
    }
    for (const auto& child : DisabledChildren_) {
        child->MarkImmutable();
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

void TCompositeSchedulerElement::UpdateCumulativeAttributes(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    PendingJobCount_ = 0;

    Attributes_.BurstRatio = GetSpecifiedBurstRatio();
    Attributes_.TotalBurstRatio = Attributes_.BurstRatio;
    Attributes_.ResourceFlowRatio = GetSpecifiedResourceFlowRatio();
    Attributes_.TotalResourceFlowRatio = Attributes_.ResourceFlowRatio;

    SchedulableChildren_.clear();
    for (const auto& child : EnabledChildren_) {
        child->UpdateCumulativeAttributes(context);

        if (IsInferringChildrenWeightsFromHistoricUsageEnabled()) {
            // NB(eshcherbin): This is a lazy parameters update so it has to be done every time.
            child->PersistentAttributes_.HistoricUsageAggregator.UpdateParameters(
                GetHistoricUsageAggregationParameters());

            // TODO(eshcherbin): Should we use vectors instead of ratios?
            // Yes, but nobody uses this feature yet, so it's not really important.
            auto usageRatio = MaxComponent(child->GetResourceUsageShare());
            child->PersistentAttributes_.HistoricUsageAggregator.UpdateAt(context->Now, usageRatio);
        }

        Attributes_.TotalResourceFlowRatio += child->Attributes().TotalResourceFlowRatio;
        Attributes_.TotalBurstRatio += child->Attributes().TotalBurstRatio;

        if (child->IsSchedulable()) {
            SchedulableChildren_.push_back(child);
        }

        PendingJobCount_ += child->GetPendingJobCount();
    }

    TSchedulerElement::UpdateCumulativeAttributes(context);

    if (Mode_ == ESchedulingMode::Fifo) {
        PrepareFifoPool();
    }
}

void TCompositeSchedulerElement::PublishFairShareAndUpdatePreemption()
{
    // This version is global and used to balance preemption lists.
    ResourceTreeElement_->SetFairShare(Attributes_.FairShare.Total);

    UpdatePreemptionAttributes();

    for (const auto& child : EnabledChildren_) {
        child->PublishFairShareAndUpdatePreemption();
    }
}

void TCompositeSchedulerElement::UpdatePreemptionAttributes()
{
    YT_VERIFY(Mutable_);
    TSchedulerElement::UpdatePreemptionAttributes();

    if (Parent_) {
        AdjustedFairShareStarvationToleranceLimit_ = std::min(
            GetFairShareStarvationToleranceLimit(),
            Parent_->AdjustedFairShareStarvationToleranceLimit());

        AdjustedFairSharePreemptionTimeoutLimit_ = std::max(
            GetFairSharePreemptionTimeoutLimit(),
            Parent_->AdjustedFairSharePreemptionTimeoutLimit());
    }
}

void TCompositeSchedulerElement::UpdateSchedulableAttributesFromDynamicAttributes(TDynamicAttributesList* dynamicAttributesList)
{
    for (const auto& child : EnabledChildren_) {
        child->UpdateSchedulableAttributesFromDynamicAttributes(dynamicAttributesList);
    }

    TSchedulerElement::UpdateSchedulableAttributesFromDynamicAttributes(dynamicAttributesList);
}

double TCompositeSchedulerElement::GetFairShareStarvationToleranceLimit() const
{
    return 1.0;
}

TDuration TCompositeSchedulerElement::GetFairSharePreemptionTimeoutLimit() const
{
    return TDuration::Zero();
}

void TCompositeSchedulerElement::UpdateDynamicAttributes(TDynamicAttributesList* dynamicAttributesList)
{
    auto& attributes = (*dynamicAttributesList)[GetTreeIndex()];

    if (!IsAlive()) {
        attributes.Active = false;
        return;
    }

    // Satisfaction ratio of a composite element is the minimum of its children's satisfaction ratios.
    // NB(eshcherbin): We initialize with local satisfaction ratio in case all children have no pending jobs
    // and thus are not in the |SchedulableChildren_| list.
    if (Mutable_) {
        attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio(ResourceUsageAtUpdate_);
    } else if (TreeConfig_->UseRecentResourceUsageForLocalSatisfaction) {
        attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio(GetInstantResourceUsage());
    } else {
        attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio(attributes.ResourceUsage);
    }

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

        attributes.SatisfactionRatio = std::min(bestChildAttributes.SatisfactionRatio, attributes.SatisfactionRatio);
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

void TCompositeSchedulerElement::CalculateCurrentResourceUsage(TFairShareContext* context)
{
    auto& attributes = context->DynamicAttributesFor(this);

    attributes.ResourceUsage = TJobResources();
    for (const auto& child : EnabledChildren_) {
        child->CalculateCurrentResourceUsage(context);
        attributes.ResourceUsage += child->GetCurrentResourceUsage(context->DynamicAttributesList());
    }
}

void TCompositeSchedulerElement::PrescheduleJob(
    TFairShareContext* context,
    EPrescheduleJobOperationCriterion operationCriterion,
    bool aggressiveStarvationEnabled)
{
    auto& attributes = context->DynamicAttributesFor(this);

    if (!IsAlive()) {
        ++context->StageState()->DeactivationReasons[EDeactivationReason::IsNotAlive];
        YT_VERIFY(!attributes.Active);
        return;
    }

    if (TreeConfig_->EnableSchedulingTags &&
        SchedulingTagFilterIndex_ != EmptySchedulingTagFilterIndex &&
        !context->CanSchedule()[SchedulingTagFilterIndex_])
    {
        ++context->StageState()->DeactivationReasons[EDeactivationReason::UnmatchedSchedulingTag];
        YT_VERIFY(!attributes.Active);
        return;
    }

    auto starving = PersistentAttributes_.Starving;
    aggressiveStarvationEnabled = aggressiveStarvationEnabled || IsAggressiveStarvationEnabled();
    if (starving && aggressiveStarvationEnabled) {
        context->SetHasAggressivelyStarvingElements(true);
    }

    auto operationCriterionForChildren = operationCriterion;
    {
        // If pool is starving, any child will do.
        bool satisfiedByPoolAggressiveStarvation = starving &&
            operationCriterion == EPrescheduleJobOperationCriterion::AggressivelyStarvingOnly &&
            aggressiveStarvationEnabled;
        bool satisfiedByPoolStarvation = starving && operationCriterion == EPrescheduleJobOperationCriterion::StarvingOnly;
        bool satisfiedByPool = satisfiedByPoolAggressiveStarvation || satisfiedByPoolStarvation;

        if (satisfiedByPool) {
            operationCriterionForChildren = EPrescheduleJobOperationCriterion::All;
        }
    }

    for (const auto& child : SchedulableChildren_) {
        child->PrescheduleJob(context, operationCriterionForChildren, aggressiveStarvationEnabled);
    }

    UpdateDynamicAttributes(&context->DynamicAttributesList());

    InitializeChildHeap(context);

    if (attributes.Active) {
        ++context->StageState()->ActiveTreeSize;
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

NProfiling::TRegistry TCompositeSchedulerElement::GetProfiler() const
{
    return Profiler_;
}

TFairShareScheduleJobResult TCompositeSchedulerElement::ScheduleJob(TFairShareContext* context, bool ignorePacking)
{
    auto& attributes = context->DynamicAttributesFor(this);
    if (!attributes.Active) {
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    auto bestLeafDescendant = attributes.BestLeafDescendant;
    if (!bestLeafDescendant->IsAlive()) {
        UpdateDynamicAttributes(&context->DynamicAttributesList());
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
        child->PersistentAttributes_.ResetOnElementEnabled();
    }

    auto& map = enabled ? EnabledChildToIndex_ : DisabledChildToIndex_;
    auto& list = enabled ? EnabledChildren_ : DisabledChildren_;
    AddChild(&map, &list, child);
}

void TCompositeSchedulerElement::EnableChild(const TSchedulerElementPtr& child)
{
    YT_VERIFY(Mutable_);

    child->PersistentAttributes_.ResetOnElementEnabled();

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

void TCompositeSchedulerElement::ProfileFull(bool profilingCompatibilityEnabled)
{
    TSensorBuffer buffer;
    Profile(&buffer, profilingCompatibilityEnabled);
    buffer.AddGauge("/max_operation_count", GetMaxOperationCount());
    buffer.AddGauge("/max_running_operation_count", GetMaxRunningOperationCount());
    buffer.AddGauge("/running_operation_count", RunningOperationCount());
    buffer.AddGauge("/total_operation_count", OperationCount());
    if (profilingCompatibilityEnabled) {
        ProfileResources(&buffer, GetSpecifiedStrongGuaranteeResources(), "/min_share_resources");
        ProfileResources(&buffer, EffectiveStrongGuaranteeResources(), "/effective_min_share_resources");
    } else {
        ProfileResources(&buffer, GetSpecifiedStrongGuaranteeResources(), "/strong_guarantee_resources");
        ProfileResources(&buffer, EffectiveStrongGuaranteeResources(), "/effective_strong_guarantee_resources");
    }
    BufferedProducer_->Update(std::move(buffer));
}

template <class TValue, class TGetter, class TSetter>
TValue TCompositeSchedulerElement::ComputeByFitting(
    const TGetter& getter,
    const TSetter& setter,
    TValue maxSum,
    bool strictMode)
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

    double fitFactor;
    if (!strictMode && !checkSum(0.0)) {
        // Even left bound doesn't satisfy predicate.
        fitFactor = 0.0;
    } else {
        // Run binary search to compute fit factor.
        fitFactor = FloatingPointInverseLowerBound(0.0, 1.0, checkSum);
    }

    TValue resultSum = {};

    // Compute actual values from fit factor.
    for (const auto& child : EnabledChildren_) {
        TValue value = getter(fitFactor, child);
        resultSum += value;
        setter(child, value);
    }

    return resultSum;
}

void TCompositeSchedulerElement::CollectOperationSchedulingSegmentContexts(
    THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const
{
    for (const auto& child : EnabledChildren_) {
        child->CollectOperationSchedulingSegmentContexts(operationContexts);
    }
}

void TCompositeSchedulerElement::ApplyOperationSchedulingSegmentChanges(
    const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts)
{
    for (const auto& child : EnabledChildren_) {
        child->ApplyOperationSchedulingSegmentChanges(operationContexts);
    }
}

void TCompositeSchedulerElement::CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const
{
    for (const auto& child : EnabledChildren_) {
        child->CollectResourceTreeOperationElements(elements);
    }
}

void TCompositeSchedulerElement::InitIntegralPoolLists(TUpdateFairShareContext* context)
{
    for (const auto& child : EnabledChildren_) {
        if (TPool* childPool = child->AsPool()) {
            switch (childPool->GetIntegralGuaranteeType()) {
                case EIntegralGuaranteeType::Burst:
                    context->BurstPools.push_back(childPool);
                    break;
                case EIntegralGuaranteeType::Relaxed:
                    context->RelaxedPools.push_back(childPool);
                    break;
                case EIntegralGuaranteeType::None:
                    childPool->InitIntegralPoolLists(context);
                    break;
            }
        }
    }
}

void TCompositeSchedulerElement::AdjustStrongGuarantees()
{
    TResourceVector totalPoolChildrenStrongGuaranteeShare;
    TResourceVector totalChildrenStrongGuaranteeShare;
    for (const auto& child : EnabledChildren_) {
        totalChildrenStrongGuaranteeShare += child->Attributes().StrongGuaranteeShare;

        if (!child->IsOperation()) {
            totalPoolChildrenStrongGuaranteeShare += child->Attributes().StrongGuaranteeShare;
        }
    }

    if (!Dominates(Attributes_.StrongGuaranteeShare, totalPoolChildrenStrongGuaranteeShare)) {
        // Drop strong guarantee shares of operations, adjust strong guarantee shares of pools.
        for (const auto& child : EnabledChildren_) {
            if (child->IsOperation()) {
                child->Attributes().StrongGuaranteeShare = TResourceVector::Zero();
            }
        }

        // Use binary search instead of division to avoid problems with precision.
        ComputeByFitting(
            /* getter */ [&] (double fitFactor, const TSchedulerElementPtr& child) -> TResourceVector {
                return child->Attributes().StrongGuaranteeShare * fitFactor;
            },
            /* setter */ [&] (const TSchedulerElementPtr& child, const TResourceVector& value) {
                YT_LOG_DEBUG("Adjusting strong guarantee shares (ChildId: %v, OldStrongGuaranteeShare: %v, NewStrongGuaranteeShare: %v)",
                    child->GetId(),
                    child->Attributes().StrongGuaranteeShare,
                    value);
                child->Attributes().StrongGuaranteeShare = value;
            },
            /* maxSum */ Attributes().StrongGuaranteeShare);
    } else if (!Dominates(Attributes_.StrongGuaranteeShare, totalChildrenStrongGuaranteeShare)) {
        // Adjust strong guarantee shares of operations, preserve strong guarantee shares of pools.
        ComputeByFitting(
            /* getter */ [&] (double fitFactor, const TSchedulerElementPtr& child) -> TResourceVector {
                if (child->IsOperation()) {
                    return child->Attributes().StrongGuaranteeShare * fitFactor;
                } else {
                    return child->Attributes().StrongGuaranteeShare;
                }
            },
            /* setter */ [&] (const TSchedulerElementPtr& child, const TResourceVector& value) {
                YT_LOG_DEBUG("Adjusting string guarantee shares (ChildId: %v, OldStrongGuaranteeShare: %v, NewStrongGuaranteeShare: %v)",
                    child->GetId(),
                    child->Attributes().StrongGuaranteeShare,
                    value);
                child->Attributes().StrongGuaranteeShare = value;
            },
            /* maxSum */ Attributes().StrongGuaranteeShare);
    }

    if (IsRoot()) {
        Attributes_.PromisedFairShare = TResourceVector::FromJobResources(TotalResourceLimits_, TotalResourceLimits_);
    }

    double weightSum = 0.0;
    auto undistributedPromisedFairShare = Attributes_.PromisedFairShare;
    for (const auto& child : EnabledChildren_) {
        weightSum += child->GetWeight();
        // NB: Sum of total strong guarantee share and total resource flow can be greater than total resource limits. This results in a scheduler alert.
        // However, no additional adjustment is done so we need to handle this case here as well.
        child->Attributes().PromisedFairShare = TResourceVector::Min(
            child->Attributes().StrongGuaranteeShare + TResourceVector::FromDouble(child->Attributes().TotalResourceFlowRatio),
            undistributedPromisedFairShare);
        undistributedPromisedFairShare -= child->Attributes().PromisedFairShare;
    }

    for (auto resourceType : TEnumTraits<EJobResourceType>::GetDomainValues()) {
        for (const auto& child : EnabledChildren_) {
            child->Attributes().PromisedFairShare[resourceType] += undistributedPromisedFairShare[resourceType] * child->GetWeight() / weightSum;
        }
    }

    for (const auto& child : EnabledChildren_) {
        if (auto* childPool = child->AsPool()) {
            childPool->AdjustStrongGuarantees();
        }
    }
}

void TCompositeSchedulerElement::PrepareFifoPool()
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
        child->Attributes().FifoIndex = index;
        ++index;
    }
}

void TCompositeSchedulerElement::PrepareFairShareFunctions(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    for (const auto& child : EnabledChildren_) {
        child->PrepareFairShareFunctions(context);
    }

    TSchedulerElement::PrepareFairShareFunctions(context);
}

std::vector<TSchedulerElementPtr> TCompositeSchedulerElement::GetEnabledChildren()
{
    return EnabledChildren_;
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
// Note that we assume all children have no guaranteed resources, so for any child:
// |child->FairShareBySuggestion_(0.0) == TResourceVector::Zero()|, and 0.0 is not a discontinuity
// point of |child->FairShareBySuggestion_|.
void TCompositeSchedulerElement::PrepareFairShareByFitFactorFifo(TUpdateFairShareContext* context)
{
    TWallTimer timer;
    auto finally = Finally([&] {
        context->PrepareFairShareByFitFactorFifoTotalTime += timer.GetElapsedCpuTime();
    });

    if (SortedEnabledChildren_.empty()) {
        FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Constant(0.0, 1.0, TResourceVector::Zero());
        return;
    }

    double rightFunctionBound = SortedEnabledChildren_.size();
    FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Constant(0.0, rightFunctionBound, TResourceVector::Zero());

    double currentRightBound = 0.0;
    for (const auto& child : SortedEnabledChildren_) {
        const auto& childFSBS = *child->FairShareBySuggestion_;

        // NB(eshcherbin): Children of FIFO pools don't have guaranteed resources. See the function comment.
        YT_VERIFY(childFSBS.IsTrimmedLeft() && childFSBS.IsTrimmedRight());
        YT_VERIFY(childFSBS.LeftFunctionValue() == TResourceVector::Zero());

        // TODO(antonkikh): This can be implemented much more efficiently by concatenating functions instead of adding.
        *FairShareByFitFactor_ += childFSBS
            .Shift(/* deltaArgument */ currentRightBound)
            .Extend(/* newLeftBound */ 0.0, /* newRightBound */ rightFunctionBound);
        currentRightBound += 1.0;
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
        FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Constant(0.0, 1.0, TResourceVector::Zero());
        return;
    }

    std::vector<TVectorPiecewiseLinearFunction> childrenFunctions;
    double minWeight = GetMinChildWeight(EnabledChildren_);
    for (const auto& child : EnabledChildren_) {
        const auto& childFSBS = *child->FairShareBySuggestion_;

        auto childFunction = childFSBS
            .ScaleArgument(child->GetWeight() / minWeight)
            .ExtendRight(/* newRightBound */ 1.0);

        childrenFunctions.push_back(std::move(childFunction));
    }

    FairShareByFitFactor_ = TVectorPiecewiseLinearFunction::Sum(childrenFunctions);
}

// Returns a vector of suggestions for children from |SortedEnabledChildren_| based on the given fit factor.
auto TCompositeSchedulerElement::GetEnabledChildSuggestionsFifo(double fitFactor) -> TChildSuggestions
{
    YT_VERIFY(fitFactor <= SortedEnabledChildren_.size());

    int satisfiedChildCount = static_cast<int>(fitFactor);
    double unsatisfiedChildSuggestion = fitFactor - satisfiedChildCount;

    TChildSuggestions childSuggestions(SortedEnabledChildren_.size(), 0.0);
    for (int i = 0; i < satisfiedChildCount; i++) {
        childSuggestions[i] = 1.0;
    }

    if (unsatisfiedChildSuggestion != 0.0) {
        childSuggestions[satisfiedChildCount] = unsatisfiedChildSuggestion;
    }

    return childSuggestions;
}

// Returns a vector of suggestions for children from |EnabledChildren_| based on the given fit factor.
auto TCompositeSchedulerElement::GetEnabledChildSuggestionsNormal(double fitFactor) -> TChildSuggestions
{
    const double minWeight = GetMinChildWeight(EnabledChildren_);

    TChildSuggestions childSuggestions;
    for (const auto& child : EnabledChildren_) {
        childSuggestions.push_back(std::min(1.0, fitFactor * (child->GetWeight() / minWeight)));
    }

    return childSuggestions;
}

// Computes the actual total fair share allocated for all child operations. The total fair share is
// guaranteed to be not greater than |FairShareBySuggestion_(suggestion)|.
// This property is important as it allows us to pass suggestions to children and be sure that children
// won't claim too much fair share.
// Note that according to our mathematical model the total fair share should be exactly equal to
// |FairShareBySuggestion_(suggestion)|, however, in reality this is not always satisfied due to
// floating point precision errors and weights. Thus, we may end up allocating slightly less than predicted.
TResourceVector TCompositeSchedulerElement::DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    if (EnabledChildren_.empty()) {
        Attributes_.SetFairShare(TResourceVector::Zero());
        return TResourceVector::Zero();
    }

    auto suggestedFairShare = FairShareBySuggestion()->ValueAt(suggestion);

    // Find the right fit factor to use when computing suggestions for children.

    // NB(eshcherbin): Vector of suggestions returned by |getEnabledChildSuggestions| must be consistent
    // with |enabledChildren|, i.e. i-th suggestion is meant to be given to i-th enabled child.
    // This implicit correspondence between children and suggestions is done for optimization purposes.
    auto& enabledChildren = (Mode_ == ESchedulingMode::Fifo)
        ? SortedEnabledChildren_
        : EnabledChildren_;
    auto getEnabledChildSuggestions = (Mode_ == ESchedulingMode::Fifo)
        ? std::bind(&TCompositeSchedulerElement::GetEnabledChildSuggestionsFifo, this, std::placeholders::_1)
        : std::bind(&TCompositeSchedulerElement::GetEnabledChildSuggestionsNormal, this, std::placeholders::_1);

    auto getChildrenSuggestedFairShare = [&] (double fitFactor) {
        auto childSuggestions = getEnabledChildSuggestions(fitFactor);
        YT_VERIFY(childSuggestions.size() == enabledChildren.size());

        TResourceVector childrenSuggestedFairShare;
        for (int i = 0; i < enabledChildren.size(); ++i) {
            const auto& child = enabledChildren[i];
            auto childSuggestion = childSuggestions[i];

            childrenSuggestedFairShare += child->FairShareBySuggestion()->ValueAt(childSuggestion);
        }

        return childrenSuggestedFairShare;
    };
    auto checkFitFactor = [&] (double fitFactor) {
        // Check that we can safely use the given fit factor to compute suggestions for children.
        return Dominates(suggestedFairShare + TResourceVector::SmallEpsilon(), getChildrenSuggestedFairShare(fitFactor));
    };

    // Usually MFFBS(suggestion) is the right fit factor to use for child suggestions.
    auto fitFactor = MaxFitFactorBySuggestion()->ValueAt(suggestion);
    if (!checkFitFactor(fitFactor)) {
        YT_ASSERT(checkFitFactor(0.0));

        // However, sometimes we need to tweak MFFBS(suggestion) in order not to suggest too much to children.
        // NB(eshcherbin): Possible to optimize this by using galloping, as the target fit factor
        // should be very, very close to our first estimate.
        fitFactor = FloatingPointInverseLowerBound(
            /* lo */ 0.0,
            /* hi */ fitFactor,
            /* predicate */ checkFitFactor);
    }

    // Propagate suggestions to children and collect the total used fair share.

    auto childSuggestions = getEnabledChildSuggestions(fitFactor);
    YT_VERIFY(childSuggestions.size() == enabledChildren.size());

    TResourceVector usedFairShare;
    for (int i = 0; i < enabledChildren.size(); ++i) {
        const auto& child = enabledChildren[i];
        auto childSuggestion = childSuggestions[i];

        usedFairShare += child->DoUpdateFairShare(childSuggestion, context);
    }

    // Validate and set used fair share.

    bool usedShareNearSuggestedShare =
        TResourceVector::Near(usedFairShare, suggestedFairShare, 1e-4 * MaxComponent(usedFairShare));
    bool suggestedShareNearlyDominatesUsedShare = Dominates(suggestedFairShare + TResourceVector::SmallEpsilon(), usedFairShare);
    YT_LOG_WARNING_UNLESS(
        usedShareNearSuggestedShare && suggestedShareNearlyDominatesUsedShare,
        "Fair share significantly differs from predicted in pool ("
        "Mode: %v, "
        "Suggestion: %.20v, "
        "VectorSuggestion: %.20v, "
        "SuggestedFairShare: %.20v, "
        "UsedFairShare: %.20v, "
        "Difference: %.20v, "
        "FitFactor: %.20v, "
        "FSBFFPredicted: %.20v, "
        "ChildrenSuggestedFairShare: %.20v, "
        "ChildrenCount: %v, "
        "OperationCount: %v, "
        "RunningOperationCount: %v)",
        GetMode(),
        suggestion,
        GetVectorSuggestion(suggestion),
        suggestedFairShare,
        usedFairShare,
        suggestedFairShare - usedFairShare,
        fitFactor,
        FairShareByFitFactor()->ValueAt(fitFactor),
        getChildrenSuggestedFairShare(fitFactor),
        enabledChildren.size(),
        OperationCount(),
        RunningOperationCount());

    YT_VERIFY(suggestedShareNearlyDominatesUsedShare);

    Attributes_.SetFairShare(usedFairShare);
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

void TCompositeSchedulerElement::DetermineEffectiveStrongGuaranteeResources()
{
    YT_VERIFY(Mutable_);

    TJobResources totalExplicitChildrenGuaranteeResources;
    TJobResources totalEffectiveChildrenGuaranteeResources;
    auto mainResource = TreeConfig_->MainResource;
    for (const auto& child : EnabledChildren_) {
        auto& childEffectiveGuaranteeResources = child->EffectiveStrongGuaranteeResources();
        childEffectiveGuaranteeResources = child->GetSpecifiedStrongGuaranteeResources();
        totalExplicitChildrenGuaranteeResources += childEffectiveGuaranteeResources;

        double mainResourceRatio = GetResource(EffectiveStrongGuaranteeResources_, mainResource) > 0
            ? GetResource(childEffectiveGuaranteeResources, mainResource) / GetResource(EffectiveStrongGuaranteeResources_, mainResource)
            : 0.0;
        auto childGuaranteeConfig = child->GetStrongGuaranteeResourcesConfig();
        #define XX(name, Name) \
            if (!childGuaranteeConfig->Name && EJobResourceType::Name != mainResource) { \
                auto parentGuarantee = EffectiveStrongGuaranteeResources_.Get##Name(); \
                childEffectiveGuaranteeResources.Set##Name(parentGuarantee * mainResourceRatio); \
            }
        ITERATE_JOB_RESOURCES(XX)
        #undef XX

        totalEffectiveChildrenGuaranteeResources += childEffectiveGuaranteeResources;
    }

    // NB: It is possible to overcommit guarantees at the first level of the tree, so we don't want to do
    // additional checks and rescaling. Instead, we handle this later when we adjust |StrongGuaranteeShare|.
    if (!IsRoot()) {
        // NB: This should never happen because we validate the guarantees at master.
        YT_LOG_WARNING_IF(!Dominates(EffectiveStrongGuaranteeResources_, totalExplicitChildrenGuaranteeResources),
            "Total children's explicit strong guarantees exceeds the effective strong guarantee at pool"
            "(EffectiveStrongGuarantees: %v, TotalExplicitChildrenGuarantees: %v)",
            EffectiveStrongGuaranteeResources_,
            totalExplicitChildrenGuaranteeResources);

        auto residualGuaranteeResources = Max(EffectiveStrongGuaranteeResources_ - totalExplicitChildrenGuaranteeResources, TJobResources{});
        auto totalImplicitChildrenGuaranteeResources = totalEffectiveChildrenGuaranteeResources - totalExplicitChildrenGuaranteeResources;
        auto adjustImplicitGuaranteesForResource = [&] (EJobResourceType resourceType, auto TResourceLimitsConfig::* resourceDataMember) {
            if (resourceType == mainResource) {
                return;
            }

            auto residualGuarantee = GetResource(residualGuaranteeResources, resourceType);
            auto totalImplicitChildrenGuarantee = GetResource(totalImplicitChildrenGuaranteeResources, resourceType);
            if (residualGuarantee >= totalImplicitChildrenGuarantee) {
                return;
            }

            double scalingFactor = residualGuarantee / totalImplicitChildrenGuarantee;
            for (const auto& child : EnabledChildren_) {
                if ((child->GetStrongGuaranteeResourcesConfig().Get()->*resourceDataMember).has_value()) {
                    continue;
                }

                auto childGuarantee = GetResource(child->EffectiveStrongGuaranteeResources(), resourceType);
                SetResource(child->EffectiveStrongGuaranteeResources(), resourceType, childGuarantee * scalingFactor);
            }
        };

        #define XX(name, Name) \
            adjustImplicitGuaranteesForResource(EJobResourceType::Name, &TResourceLimitsConfig::Name);
        ITERATE_JOB_RESOURCES(XX)
        #undef XX
    }

    for (const auto& child : EnabledChildren_) {
        child->DetermineEffectiveStrongGuaranteeResources();
    }
}

TSchedulerElement* TCompositeSchedulerElement::GetBestActiveChild(const TDynamicAttributesList& dynamicAttributesList) const
{
    const auto& childHeap = dynamicAttributesList[GetTreeIndex()].ChildHeap;
    if (childHeap) {
        auto* element = childHeap->GetTop();
        return dynamicAttributesList[element->GetTreeIndex()].Active
            ? element
            : nullptr;
    }

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

void TCompositeSchedulerElement::BuildResourceMetering(const std::optional<TMeteringKey>& parentKey, TMeteringMap *statistics) const
{
    auto key = GetMeteringKey();
    YT_VERIFY(key || parentKey);

    if (key) {
        YT_VERIFY(statistics->insert({*key, TMeteringStatistics(GetSpecifiedStrongGuaranteeResources(), ResourceUsageAtUpdate())}).second);
    }

    for (const auto& child : EnabledChildren_) {
        child->BuildResourceMetering(key ? key : parentKey, statistics);
    }

    if (key && parentKey) {
        statistics->at(*parentKey) -= statistics->at(*key);
    }
}

TJobResources TCompositeSchedulerElement::GetIntegralPoolCapacity() const
{
    return TotalResourceLimits_ * Attributes_.ResourceFlowRatio * TreeConfig_->IntegralGuarantees->PoolCapacitySaturationPeriod.SecondsFloat();
}

void TCompositeSchedulerElement::InitializeChildHeap(TFairShareContext* context)
{
    if (SchedulableChildren_.size() < TreeConfig_->MinChildHeapSize) {
        return;
    }

    TChildHeap::TComparator elementComparator;

    switch (Mode_) {
        case ESchedulingMode::Fifo:
            elementComparator = [this, context] (TSchedulerElement* lhs, TSchedulerElement* rhs) {
                auto& lhsAttributes = context->DynamicAttributesFor(lhs);
                auto& rhsAttributes = context->DynamicAttributesFor(rhs);
                if (lhsAttributes.Active != rhsAttributes.Active) {
                    return rhsAttributes.Active < lhsAttributes.Active;
                }
                return HasHigherPriorityInFifoMode(lhs, rhs);
            };
            break;
        case ESchedulingMode::FairShare:
            elementComparator = [context] (TSchedulerElement* lhs, TSchedulerElement* rhs) {
                auto& lhsAttributes = context->DynamicAttributesFor(lhs);
                auto& rhsAttributes = context->DynamicAttributesFor(rhs);
                if (lhsAttributes.Active != rhsAttributes.Active) {
                    return rhsAttributes.Active < lhsAttributes.Active;
                }
                return lhsAttributes.SatisfactionRatio < rhsAttributes.SatisfactionRatio;
            };
            break;
        default:
            YT_ABORT();
    }

    auto& attributes = context->DynamicAttributesFor(this);
    attributes.ChildHeap = std::make_unique<TChildHeap>(
        SchedulableChildren_,
        &context->DynamicAttributesList(),
        elementComparator);
}

void TCompositeSchedulerElement::UpdateChild(TFairShareContext* context, TSchedulerElement* child)
{
    auto& attributes = context->DynamicAttributesFor(this);
    if (attributes.ChildHeap) {
        attributes.ChildHeap->Update(child);
    }
}

TResourceVector TCompositeSchedulerElement::GetHierarchicalAvailableLimitsShare() const
{
    auto* current = this;
    auto resultLimitsShare = TResourceVector::Ones();
    while (!current->IsRoot()) {
        const auto& limitsShare = current->Attributes().LimitsShare;
        const auto& effectiveGuaranteeShare = TResourceVector::Min(
            current->Attributes().GetGuaranteeShare(),
            current->Attributes().DemandShare);

        resultLimitsShare = TResourceVector::Min(resultLimitsShare, limitsShare - effectiveGuaranteeShare);
        YT_VERIFY(Dominates(resultLimitsShare, TResourceVector::Zero()));

        current = current->GetParent();
    }

    return resultLimitsShare;
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
        id,
        EResourceTreeElementKind::Pool,
        logger.WithTag("PoolId: %v, SchedulingMode: %v",
            id,
            config->Mode))
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

TPool* TPool::AsPool()
{
    return this;
}

TString TPool::GetId() const
{
    return Id_;
}

std::optional<double> TPool::GetSpecifiedWeight() const
{
    return Config_->Weight;
}

TResourceLimitsConfigPtr TPool::GetStrongGuaranteeResourcesConfig() const
{
    return Config_->StrongGuaranteeResources;
}

TResourceVector TPool::GetMaxShare() const
{
    return TResourceVector::FromDouble(Config_->MaxShareRatio.value_or(1.0));
}

EIntegralGuaranteeType TPool::GetIntegralGuaranteeType() const
{
    return Config_->IntegralGuarantees->GuaranteeType;
}

ESchedulableStatus TPool::GetStatus(bool atUpdate) const
{
    return TSchedulerElement::GetStatusImpl(Attributes_.AdjustedFairShareStarvationTolerance, atUpdate);
}

double TPool::GetFairShareStarvationTolerance() const
{
    return Config_->FairShareStarvationTolerance.value_or(Parent_->Attributes().AdjustedFairShareStarvationTolerance);
}

TDuration TPool::GetFairSharePreemptionTimeout() const
{
    return Config_->FairSharePreemptionTimeout.value_or(Parent_->Attributes().AdjustedFairSharePreemptionTimeout);
}

double TPool::GetFairShareStarvationToleranceLimit() const
{
    return Config_->FairShareStarvationToleranceLimit.value_or(TreeConfig_->FairShareStarvationToleranceLimit);
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

    TSchedulerElement::CheckForStarvationImpl(Attributes_.AdjustedFairSharePreemptionTimeout, now);
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
    TreeHost_->GetResourceTree()->ScheduleDetachParent(ResourceTreeElement_);

    YT_LOG_DEBUG("Pool is detached (Pool: %v, ParentPool: %v)",
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

std::optional<TMeteringKey> TPool::GetMeteringKey() const
{
    if (Config_->Abc) {
        return TMeteringKey{
            .AbcId  = Config_->Abc->Id,
            .TreeId = GetTreeId(),
            .PoolId = GetId(),
        };
    }

    return std::nullopt;
}

double TPool::GetSpecifiedBurstRatio() const
{
    return GetMaxResourceRatio(ToJobResources(Config_->IntegralGuarantees->BurstGuaranteeResources, {}), TotalResourceLimits_);
}

double TPool::GetSpecifiedResourceFlowRatio() const
{
    return GetMaxResourceRatio(ToJobResources(Config_->IntegralGuarantees->ResourceFlow, {}), TotalResourceLimits_);
}

void TPool::UpdateAccumulatedResourceVolume(TDuration periodSinceLastUpdate)
{
    if (TotalResourceLimits_ == TJobResources()) {
        YT_LOG_DEBUG("Skip update of accumulated resource volume");
        return;
    }
    auto oldVolume = PersistentAttributes_.AccumulatedResourceVolume;
    auto upperLimit = Max(oldVolume, GetIntegralPoolCapacity());

    PersistentAttributes_.AccumulatedResourceVolume += TotalResourceLimits_ * Attributes_.ResourceFlowRatio * periodSinceLastUpdate.SecondsFloat();
    PersistentAttributes_.AccumulatedResourceVolume -= TotalResourceLimits_ * PersistentAttributes_.LastIntegralShareRatio * periodSinceLastUpdate.SecondsFloat();
    PersistentAttributes_.AccumulatedResourceVolume = Max(PersistentAttributes_.AccumulatedResourceVolume, TJobResources());
    PersistentAttributes_.AccumulatedResourceVolume = Min(PersistentAttributes_.AccumulatedResourceVolume, upperLimit);

    YT_LOG_DEBUG(
        "Accumulated resource volume updated "
        "(ResourceFlowRatio: %v, PeriodSinceLastUpdateInSeconds: %v, TotalResourceLimits: %v, "
        "LastIntegralShareRatio: %v, PoolCapacity: %v, OldVolume: %v, UpdatedVolume: %v)",
        Attributes_.ResourceFlowRatio,
        periodSinceLastUpdate.SecondsFloat(),
        TotalResourceLimits_,
        PersistentAttributes_.LastIntegralShareRatio,
        GetIntegralPoolCapacity(),
        oldVolume,
        PersistentAttributes_.AccumulatedResourceVolume);
}

void TPool::ApplyLimitsForRelaxedPool()
{
    YT_VERIFY(GetIntegralGuaranteeType() == EIntegralGuaranteeType::Relaxed);

    auto relaxedPoolLimit = TResourceVector::Min(
        TResourceVector::FromDouble(GetIntegralShareRatioByVolume()),
        GetIntegralShareLimitForRelaxedPool());
    relaxedPoolLimit += Attributes_.StrongGuaranteeShare;
    Attributes_.LimitsShare = TResourceVector::Min(Attributes_.LimitsShare, relaxedPoolLimit);
}

TResourceVector TPool::GetIntegralShareLimitForRelaxedPool() const
{
    YT_VERIFY(GetIntegralGuaranteeType() == EIntegralGuaranteeType::Relaxed);
    return TResourceVector::FromDouble(Attributes_.ResourceFlowRatio) * TreeConfig_->IntegralGuarantees->RelaxedShareMultiplierLimit;
}

bool TPool::AreDetailedLogsEnabled() const
{
    return Config_->EnableDetailedLogs;
}

////////////////////////////////////////////////////////////////////////////////

TOperationElementFixedState::TOperationElementFixedState(
    IOperationStrategyHost* operation,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig)
    : OperationId_(operation->GetId())
    , UnschedulableReason_(operation->CheckUnschedulable())
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
    auto guard = WriterGuard(JobPropertiesMapLock_);

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
    auto guard = WriterGuard(JobPropertiesMapLock_);

    YT_VERIFY(!Enabled_);
    Enabled_ = true;
}

bool TOperationElementSharedState::Enabled()
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);
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

TJobResources TOperationElementSharedState::SetJobResourceUsage(
    TJobId jobId,
    const TJobResources& resources)
{
    auto guard = WriterGuard(JobPropertiesMapLock_);

    if (!Enabled_) {
        return {};
    }

    return SetJobResourceUsage(GetJobProperties(jobId), resources);
}

void TOperationElementSharedState::UpdatePreemptableJobsList(
    const TResourceVector& fairShare,
    const TJobResources& totalResourceLimits,
    double preemptionSatisfactionThreshold,
    double aggressivePreemptionSatisfactionThreshold,
    int* moveCount,
    TOperationElement* operationElement)
{
    auto guard = WriterGuard(JobPropertiesMapLock_);

    auto getUsageShare = [&] (const TJobResources& resourceUsage) -> TResourceVector {
        return TResourceVector::FromJobResources(resourceUsage, totalResourceLimits);
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
        operationElement->AreDetailedLogsEnabled();

    YT_LOG_DEBUG_IF(enableLogging,
        "Update preemptable lists inputs (FairShare: %.6g, TotalResourceLimits: %v, "
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
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    return JobPropertiesMap_.find(jobId) != JobPropertiesMap_.end();
}

bool TOperationElementSharedState::IsJobPreemptable(TJobId jobId, bool aggressivePreemptionEnabled) const
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);

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
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    return PreemptableJobs_.size();
}

int TOperationElementSharedState::GetAggressivelyPreemptableJobCount() const
{
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    return AggressivelyPreemptableJobs_.size();
}

bool TOperationElementSharedState::AddJob(TJobId jobId, const TJobResources& resourceUsage, bool force)
{
    auto guard = WriterGuard(JobPropertiesMapLock_);

    if (!Enabled_ && !force) {
        return false;
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

    SetJobResourceUsage(&it.first->second, resourceUsage);

    return true;
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

void TOperationElementSharedState::OnMinNeededResourcesUnsatisfied(
    const TFairShareContext& context,
    const TJobResources& availableResources,
    const TJobResources& minNeededResources)
{
    auto& shard = StateShards_[context.SchedulingContext()->GetNodeShardId()];
    #define XX(name, Name) \
        if (availableResources.Get##Name() < minNeededResources.Get##Name()) { \
            ++shard.MinNeededResourcesUnsatisfiedCount[EJobResourceType::Name]; \
        }
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

TEnumIndexedVector<EJobResourceType, int> TOperationElementSharedState::GetMinNeededResourcesUnsatisfiedCount() const
{
    TEnumIndexedVector<EJobResourceType, int> result;
    for (const auto& shard : StateShards_) {
        for (auto resource : TEnumTraits<EJobResourceType>::GetDomainValues()) {
            result[resource] += shard.MinNeededResourcesUnsatisfiedCount[resource].load();
        }
    }
    return result;
}

void TOperationElementSharedState::OnOperationDeactivated(const TFairShareContext& context, EDeactivationReason reason)
{
    auto& shard = StateShards_[context.SchedulingContext()->GetNodeShardId()];
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
    auto guard = ReaderGuard(JobPropertiesMapLock_);

    return LastScheduleJobSuccessTime_;
}

void TOperationElement::OnMinNeededResourcesUnsatisfied(
    const TFairShareContext& context,
    const TJobResources& availableResources,
    const TJobResources& minNeededResources)
{
    OperationElementSharedState_->OnMinNeededResourcesUnsatisfied(context, availableResources, minNeededResources);
}

TEnumIndexedVector<EJobResourceType, int> TOperationElement::GetMinNeededResourcesUnsatisfiedCount() const
{
    return OperationElementSharedState_->GetMinNeededResourcesUnsatisfiedCount();
}

void TOperationElement::OnOperationDeactivated(TFairShareContext* context, EDeactivationReason reason)
{
    ++context->StageState()->DeactivationReasons[reason];
    OperationElementSharedState_->OnOperationDeactivated(*context, reason);
}

TEnumIndexedVector<EDeactivationReason, int> TOperationElement::GetDeactivationReasons() const
{
    return OperationElementSharedState_->GetDeactivationReasons();
}

TEnumIndexedVector<EDeactivationReason, int> TOperationElement::GetDeactivationReasonsFromLastNonStarvingTime() const
{
    return OperationElementSharedState_->GetDeactivationReasonsFromLastNonStarvingTime();
}

std::optional<TString> TOperationElement::GetCustomProfilingTag() const
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

bool TOperationElement::IsOperation() const
{
    return true;
}

void TOperationElement::Disable(bool markAsNonAlive)
{
    YT_LOG_DEBUG("Operation element disabled in strategy");

    OperationElementSharedState_->Disable();
    TreeHost_->GetResourceTree()->ReleaseResources(ResourceTreeElement_, markAsNonAlive);
}

void TOperationElement::Enable()
{
    YT_LOG_DEBUG("Operation element enabled in strategy");

    return OperationElementSharedState_->Enable();
}

std::optional<TJobResources> TOperationElementSharedState::RemoveJob(TJobId jobId)
{
    auto guard = WriterGuard(JobPropertiesMapLock_);

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
    SetJobResourceUsage(properties, TJobResources());

    JobPropertiesMap_.erase(it);

    return resourceUsage;
}

std::optional<EDeactivationReason> TOperationElement::TryStartScheduleJob(
    const TFairShareContext& context,
    TJobResources* precommittedResourcesOutput,
    TJobResources* availableResourcesOutput)
{
    const auto& minNeededResources = AggregatedMinNeededJobResources_;

    auto nodeFreeResources = context.SchedulingContext()->GetNodeFreeResourcesWithDiscount();
    if (!Dominates(nodeFreeResources, minNeededResources)) {
        OnMinNeededResourcesUnsatisfied(context, nodeFreeResources, minNeededResources);
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
    auto increaseResult = TryIncreaseHierarchicalResourceUsagePrecommit(
        minNeededResources,
        &availableResourceLimits);

    if (increaseResult == EResourceTreeIncreaseResult::ResourceLimitExceeded) {
        return EDeactivationReason::ResourceLimitsExceeded;
    }
    if (increaseResult == EResourceTreeIncreaseResult::ElementIsNotAlive) {
        return EDeactivationReason::IsNotAlive;
    }

    Controller_->IncreaseConcurrentScheduleJobCalls(context.SchedulingContext()->GetNodeShardId());
    Controller_->IncreaseScheduleJobCallsSinceLastUpdate(context.SchedulingContext()->GetNodeShardId());

    *precommittedResourcesOutput = minNeededResources;
    *availableResourcesOutput = Min(availableResourceLimits, nodeFreeResources);
    return std::nullopt;
}

void TOperationElement::FinishScheduleJob(const ISchedulingContextPtr& schedulingContext)
{
    Controller_->DecreaseConcurrentScheduleJobCalls(schedulingContext->GetNodeShardId());
}

TJobResources TOperationElementSharedState::SetJobResourceUsage(
    TJobProperties* properties,
    const TJobResources& resources)
{
    auto delta = resources - properties->ResourceUsage;
    properties->ResourceUsage = resources;
    if (!properties->Preemptable) {
        if (properties->AggressivelyPreemptable) {
            AggressivelyPreemptableResourceUsage_ += delta;
        } else {
            NonpreemptableResourceUsage_ += delta;
        }
    }
    return delta;
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
        ToString(operation->GetId()),
        EResourceTreeElementKind::Operation,
        logger.WithTag("OperationId: %v", operation->GetId()))
    , TOperationElementFixedState(operation, std::move(controllerConfig))
    , RuntimeParameters_(std::move(runtimeParameters))
    , Spec_(std::move(spec))
    , OperationElementSharedState_(New<TOperationElementSharedState>(Spec_->UpdatePreemptableJobsListLoggingPeriod, Logger))
    , Controller_(std::move(controller))
    , SchedulingTagFilter_(Spec_->SchedulingTagFilter)
    , BufferedProducer_(New<TBufferedProducer>())
{ }

TOperationElement::TOperationElement(
    const TOperationElement& other,
    TCompositeSchedulerElement* clonedParent)
    : TSchedulerElement(other, clonedParent)
    , TOperationElementFixedState(other)
    , RuntimeParameters_(other.RuntimeParameters_)
    , Spec_(other.Spec_)
    , SchedulingSegment_(other.SchedulingSegment_)
    , SpecifiedSchedulingSegmentDataCenters_(other.SpecifiedSchedulingSegmentDataCenters_)
    , OperationElementSharedState_(other.OperationElementSharedState_)
    , Controller_(other.Controller_)
    , RunningInThisPoolTree_(other.RunningInThisPoolTree_)
    , SchedulingTagFilter_(other.SchedulingTagFilter_)
    , BufferedProducer_(other.BufferedProducer_)
{ }

double TOperationElement::GetFairShareStarvationTolerance() const
{
    return Spec_->FairShareStarvationTolerance.value_or(Parent_->Attributes().AdjustedFairShareStarvationTolerance);
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

    PendingJobCount_ = Controller_->GetPendingJobCount();
    DetailedMinNeededJobResources_ = Controller_->GetDetailedMinNeededJobResources();
    AggregatedMinNeededJobResources_ = Controller_->GetAggregatedMinNeededJobResources();
    TotalNeededResources_ = Controller_->GetNeededResources();

    UnschedulableReason_ = ComputeUnschedulableReason();
    ResourceUsageAtUpdate_ = GetInstantResourceUsage();
    // Must be calculated after ResourceUsageAtUpdate_
    ResourceDemand_ = ComputeResourceDemand();
    StartTime_ = Operation_->GetStartTime();

    TSchedulerElement::PreUpdateBottomUp(context);
}

void TOperationElement::UpdateCumulativeAttributes(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    if (PersistentAttributes_.LastBestAllocationRatioUpdateTime + TreeConfig_->BestAllocationRatioUpdatePeriod > context->Now) {
        auto allocationLimits = GetAdjustedResourceLimits(
            ResourceDemand_,
            TotalResourceLimits_,
            GetHost()->GetExecNodeMemoryDistribution(SchedulingTagFilter_ & TreeConfig_->NodesFilter));
        PersistentAttributes_.BestAllocationShare = TResourceVector::FromJobResources(allocationLimits, TotalResourceLimits_);
        PersistentAttributes_.LastBestAllocationRatioUpdateTime = context->Now;
    }

    // This should be called after |BestAllocationShare| update since it is used to compute the limits.
    TSchedulerElement::UpdateCumulativeAttributes(context);

    if (!IsSchedulable()) {
        ++context->UnschedulableReasons[*UnschedulableReason_];
    }
}

void TOperationElement::PublishFairShareAndUpdatePreemption()
{
    // This version is global and used to balance preemption lists.
    ResourceTreeElement_->SetFairShare(Attributes_.FairShare.Total);

    UpdatePreemptionAttributes();
}

void TOperationElement::UpdatePreemptionAttributes()
{
    YT_VERIFY(Mutable_);
    TSchedulerElement::UpdatePreemptionAttributes();

    // If fair share ratio equals demand ratio then we want to explicitly disable preemption.
    // It is necessary since some job's resource usage may increase before the next fair share update,
    //  and in this case we don't want any jobs to become preemptable
    bool isDominantFairShareEqualToDominantDemandShare =
        TResourceVector::Near(Attributes_.FairShare.Total, Attributes_.DemandShare, RatioComparisonPrecision) &&
        !Dominates(TResourceVector::Epsilon(), Attributes_.DemandShare);

    bool newPreemptableValue = !isDominantFairShareEqualToDominantDemandShare;
    bool oldPreemptableValue = OperationElementSharedState_->GetPreemptable();
    if (oldPreemptableValue != newPreemptableValue) {
        YT_LOG_DEBUG("Preemptable status changed %v -> %v", oldPreemptableValue, newPreemptableValue);
        OperationElementSharedState_->SetPreemptable(newPreemptableValue);
    }

    UpdatePreemptableJobsList();
}

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
        SmallVector<double, ResourceCount> sortedUsage(Attributes_.UsageShare.begin(), Attributes_.UsageShare.end());
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
    builder.PushSegment({{1.0, Attributes_.UsageShare}, {2.0, Attributes_.DemandShare}});

    FairShareByFitFactor_ = builder.Finish();
}

TResourceVector TOperationElement::DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context)
{
    TResourceVector usedFairShare = FairShareBySuggestion()->ValueAt(suggestion);
    Attributes_.SetFairShare(usedFairShare);

    const auto fsbsSegment = FairShareBySuggestion()->SegmentAt(suggestion);
    const auto fitFactor = MaxFitFactorBySuggestion()->ValueAt(suggestion);
    const auto fsbffSegment = FairShareByFitFactor()->SegmentAt(fitFactor);

    YT_ELEMENT_LOG_DETAILED(this,
        "Updated operation fair share ("
        "Suggestion: %.10g, "
        "UsedFairShare: %.10g, "
        "FSBSSegmentArguments: {%.10g, %.10g}, "
        "FSBSSegmentValues: {%.10g, %.10g}, "
        "FitFactor: %.10g, "
        "FSBFFSegmentArguments: {%.10g, %.10g}, "
        "FSBFFSegmentValues: {%.10g, %.10g})",
        suggestion,
        usedFairShare,
        fsbsSegment.LeftBound(), fsbsSegment.RightBound(),
        fsbsSegment.LeftValue(), fsbsSegment.RightValue(),
        fitFactor,
        fsbffSegment.LeftBound(), fsbffSegment.RightBound(),
        fsbffSegment.LeftValue(), fsbffSegment.RightValue());
    return usedFairShare;
}

bool TOperationElement::HasJobsSatisfyingResourceLimits(const TFairShareContext& context) const
{
    for (const auto& jobResources : DetailedMinNeededJobResources_) {
        if (context.SchedulingContext()->CanStartJob(jobResources)) {
            return true;
        }
    }
    return false;
}

void TOperationElement::UpdateDynamicAttributes(TDynamicAttributesList* dynamicAttributesList)
{
    auto& attributes = (*dynamicAttributesList)[GetTreeIndex()];
    attributes.BestLeafDescendant = this;
    attributes.Active = IsAlive();
    if (Mutable_) {
        attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio(ResourceUsageAtUpdate_);
    } else if (TreeConfig_->UseRecentResourceUsageForLocalSatisfaction) {
        attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio(GetInstantResourceUsage());
    } else {
        attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio(attributes.ResourceUsage);
    }
}

void TOperationElement::UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    YT_VERIFY(Mutable_);

    if (TreeConfig_->SchedulingSegments->Mode != config->SchedulingSegments->Mode) {
        InitOrUpdateSchedulingSegment(config->SchedulingSegments->Mode);
    }

    TSchedulerElement::UpdateTreeConfig(config);
}

void TOperationElement::UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config)
{
    YT_VERIFY(Mutable_);
    ControllerConfig_ = config;
}

void TOperationElement::CalculateCurrentResourceUsage(TFairShareContext* context)
{
    auto& attributes = context->DynamicAttributesFor(this);

    attributes.ResourceUsage = IsAlive()
        ? GetInstantResourceUsage()
        : TJobResources();
}

void TOperationElement::PrescheduleJob(
    TFairShareContext* context,
    EPrescheduleJobOperationCriterion operationCriterion,
    bool aggressiveStarvationEnabled)
{
    auto& attributes = context->DynamicAttributesFor(this);

    // Reset operation element activeness (it can be active after scheduling without preepmtion).
    attributes.Active = false;

    auto onOperationDeactivated = [&] (EDeactivationReason reason) {
        OnOperationDeactivated(context, reason);
    };

    if (!IsAlive()) {
        onOperationDeactivated(EDeactivationReason::IsNotAlive);
        return;
    }

    if (auto blockedReason = CheckBlocked(context->SchedulingContext())) {
        onOperationDeactivated(*blockedReason);
        return;
    }

    if (Spec_->PreemptionMode == EPreemptionMode::Graceful &&
        GetStatus(/* atUpdate */ false) == ESchedulableStatus::Normal)
    {
        onOperationDeactivated(EDeactivationReason::FairShareExceeded);
        return;
    }

    if (TreeConfig_->EnableSchedulingTags &&
        SchedulingTagFilterIndex_ != EmptySchedulingTagFilterIndex &&
        !context->CanSchedule()[SchedulingTagFilterIndex_])
    {
        onOperationDeactivated(EDeactivationReason::UnmatchedSchedulingTag);
        return;
    }

    if (!IsSchedulingSegmentCompatibleWithNode(
        context->SchedulingContext()->GetSchedulingSegment(),
        context->SchedulingContext()->GetNodeDescriptor().DataCenter))
    {
        onOperationDeactivated(EDeactivationReason::IncompatibleSchedulingSegment);
        return;
    }

    if (operationCriterion == EPrescheduleJobOperationCriterion::AggressivelyStarvingOnly &&
        !(PersistentAttributes_.Starving && aggressiveStarvationEnabled))
    {
        onOperationDeactivated(EDeactivationReason::IsNotAggressivelyStarving);
        return;
    }

    if (operationCriterion == EPrescheduleJobOperationCriterion::StarvingOnly &&
        !PersistentAttributes_.Starving)
    {
        onOperationDeactivated(EDeactivationReason::IsNotStarving);
        return;
    }

    if (Controller_->IsSaturatedInTentativeTree(
        context->SchedulingContext()->GetNow(),
        TreeId_,
        TreeConfig_->TentativeTreeSaturationDeactivationPeriod))
    {
        onOperationDeactivated(EDeactivationReason::SaturatedInTentativeTree);
        return;
    }

    UpdateDynamicAttributes(&context->DynamicAttributesList());

    if (attributes.Active) {
        ++context->StageState()->ActiveTreeSize;
        ++context->StageState()->ActiveOperationCount;
    }
}

bool TOperationElement::HasAggressivelyStarvingElements(TFairShareContext* /*context*/, bool /*aggressiveStarvationEnabled*/) const
{
    // TODO(ignat): Support aggressive starvation by starving operation.
    return false;
}

TString TOperationElement::GetLoggingString() const
{
    return Format(
        "Scheduling info for tree %Qv = {%v, "
        "PendingJobs: %v, AggregatedMinNeededResources: %v, SchedulingSegment: %v, SchedulingSegmentDataCenter: %v, "
        "PreemptableRunningJobs: %v, AggressivelyPreemptableRunningJobs: %v, PreemptionStatusStatistics: %v, "
        "DeactivationReasons: %v, MinNeededResourcesUnsatisfiedCount: %v}",
        GetTreeId(),
        GetLoggingAttributesString(),
        PendingJobCount_,
        AggregatedMinNeededJobResources_,
        SchedulingSegment_,
        PersistentAttributes_.SchedulingSegmentDataCenter,
        GetPreemptableJobCount(),
        GetAggressivelyPreemptableJobCount(),
        GetPreemptionStatusStatistics(),
        GetDeactivationReasons(),
        GetMinNeededResourcesUnsatisfiedCount());
}

void TOperationElement::UpdateAncestorsDynamicAttributes(
    TFairShareContext* context,
    const TJobResources& resourceUsageDelta,
    bool checkAncestorsActiveness)
{
    auto* parent = GetMutableParent();
    while (parent) {
        bool activeBefore = context->DynamicAttributesFor(parent).Active;
        if (checkAncestorsActiveness) {
            YT_VERIFY(activeBefore);
        }

        context->DynamicAttributesFor(parent).ResourceUsage += resourceUsageDelta;

        parent->UpdateDynamicAttributes(&context->DynamicAttributesList());

        bool activeAfter = context->DynamicAttributesFor(parent).Active;
        if (activeBefore && !activeAfter) {
            ++context->StageState()->DeactivationReasons[EDeactivationReason::NoBestLeafDescendant];
        }

        if (parent->GetMutableParent()) {
            parent->GetMutableParent()->UpdateChild(context, parent);
        }

        parent = parent->GetMutableParent();
    }
}

void TOperationElement::DeactivateOperation(TFairShareContext* context, EDeactivationReason reason)
{
    auto& attributes = context->DynamicAttributesList()[GetTreeIndex()];
    YT_VERIFY(attributes.Active);
    attributes.Active = false;
    GetMutableParent()->UpdateChild(context, this);
    UpdateAncestorsDynamicAttributes(context, /* deltaResourceUsage */ TJobResources());
    OnOperationDeactivated(context, reason);
}

void TOperationElement::ActivateOperation(TFairShareContext* context)
{
    auto& attributes = context->DynamicAttributesList()[GetTreeIndex()];
    YT_VERIFY(!attributes.Active);
    attributes.Active = true;
    GetMutableParent()->UpdateChild(context, this);
    UpdateAncestorsDynamicAttributes(context, /* deltaResourceUsage */ TJobResources(), /* checkAncestorsActiveness */ false);
}

void TOperationElement::RecordHeartbeat(const TPackingHeartbeatSnapshot& heartbeatSnapshot)
{
    OperationElementSharedState_->RecordHeartbeat(heartbeatSnapshot, GetPackingConfig());
}

bool TOperationElement::CheckPacking(const TPackingHeartbeatSnapshot& heartbeatSnapshot) const
{
    // NB: We expect DetailedMinNeededResources_ to be of size 1 most of the time.
    TJobResourcesWithQuota packingJobResourcesWithQuota;
    if (DetailedMinNeededJobResources_.empty()) {
        // Refuse packing if no information about resource requirements is provided.
        return false;
    } else if (DetailedMinNeededJobResources_.size() == 1) {
        packingJobResourcesWithQuota = DetailedMinNeededJobResources_[0];
    } else {
        auto idx = RandomNumber<ui32>(static_cast<ui32>(DetailedMinNeededJobResources_.size()));
        packingJobResourcesWithQuota = DetailedMinNeededJobResources_[idx];
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
    YT_VERIFY(IsActive(context->DynamicAttributesList()));

    YT_ELEMENT_LOG_DETAILED(this,
        "Trying to schedule job (SatisfactionRatio: %v, NodeId: %v, NodeResourceUsage: %v)",
        context->DynamicAttributesFor(this).SatisfactionRatio,
        context->SchedulingContext()->GetNodeDescriptor().Id,
        FormatResourceUsage(context->SchedulingContext()->ResourceUsage(), context->SchedulingContext()->ResourceLimits()));

    auto deactivateOperationElement = [&] (EDeactivationReason reason) {
        YT_ELEMENT_LOG_DETAILED(this,
            "Failed to schedule job, operation deactivated "
            "(DeactivationReason: %v, NodeResourceUsage: %v)",
            FormatEnum(reason),
            FormatResourceUsage(context->SchedulingContext()->ResourceUsage(), context->SchedulingContext()->ResourceLimits()));
        DeactivateOperation(context, reason);
    };

    auto recordHeartbeatWithTimer = [&] (const auto& heartbeatSnapshot) {
        NProfiling::TWallTimer timer;
        RecordHeartbeat(heartbeatSnapshot);
        context->StageState()->PackingRecordHeartbeatDuration += timer.GetElapsedTime();
    };

    if (auto blockedReason = CheckBlocked(context->SchedulingContext())) {
        deactivateOperationElement(*blockedReason);
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    if (!HasJobsSatisfyingResourceLimits(*context)) {
        YT_ELEMENT_LOG_DETAILED(this,
            "No pending jobs can satisfy available resources on node ("
            "FreeResources: %v, DiscountResources: %v, "
            "MinNeededResources: %v, DetailedMinNeededResources: %v, "
            "Address: %v)",
            FormatResources(context->SchedulingContext()->GetNodeFreeResourcesWithoutDiscount()),
            FormatResources(context->SchedulingContext()->ResourceUsageDiscount()),
            FormatResources(AggregatedMinNeededJobResources_),
            MakeFormattableView(
                Controller_->GetDetailedMinNeededJobResources(),
                [&] (TStringBuilderBase* builder, const TJobResourcesWithQuota& resources) {
                    builder->AppendFormat("%v",
                        Host_->FormatResources(resources));
                }),
            context->SchedulingContext()->GetNodeDescriptor().Address);

        OnMinNeededResourcesUnsatisfied(
            *context,
            context->SchedulingContext()->GetNodeFreeResourcesWithDiscount(),
            AggregatedMinNeededJobResources_);
        deactivateOperationElement(EDeactivationReason::MinNeededResourcesUnsatisfied);
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    TJobResources precommittedResources;
    TJobResources availableResources;

    auto deactivationReason = TryStartScheduleJob(*context, &precommittedResources, &availableResources);
    if (deactivationReason) {
        deactivateOperationElement(*deactivationReason);
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    std::optional<TPackingHeartbeatSnapshot> heartbeatSnapshot;
    if (GetPackingConfig()->Enable && !ignorePacking) {
        heartbeatSnapshot = CreateHeartbeatSnapshot(context->SchedulingContext());

        bool acceptPacking;
        {
            NProfiling::TWallTimer timer;
            acceptPacking = CheckPacking(*heartbeatSnapshot);
            context->StageState()->PackingCheckDuration += timer.GetElapsedTime();
        }

        if (!acceptPacking) {
            recordHeartbeatWithTimer(*heartbeatSnapshot);
            TreeHost_->GetResourceTree()->IncreaseHierarchicalResourceUsagePrecommit(ResourceTreeElement_, -precommittedResources);
            deactivateOperationElement(EDeactivationReason::BadPacking);
            context->BadPackingOperations().emplace_back(this);
            FinishScheduleJob(context->SchedulingContext());
            return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
        }
    }

    TControllerScheduleJobResultPtr scheduleJobResult;
    {
        NProfiling::TWallTimer timer;
        scheduleJobResult = DoScheduleJob(context, availableResources, &precommittedResources);
        auto scheduleJobDuration = timer.GetElapsedTime();
        context->StageState()->TotalScheduleJobDuration += scheduleJobDuration;
        context->StageState()->ExecScheduleJobDuration += scheduleJobResult->Duration;
    }

    if (!scheduleJobResult->StartDescriptor) {
        for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
            context->StageState()->FailedScheduleJob[reason] += scheduleJobResult->Failed[reason];
        }

        ++context->StageState()->ScheduleJobFailureCount;
        deactivateOperationElement(EDeactivationReason::ScheduleJobFailed);

        Controller_->OnScheduleJobFailed(
            context->SchedulingContext()->GetNow(),
            TreeId_,
            scheduleJobResult);

        TreeHost_->GetResourceTree()->IncreaseHierarchicalResourceUsagePrecommit(ResourceTreeElement_, -precommittedResources);

        FinishScheduleJob(context->SchedulingContext());

        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    const auto& startDescriptor = *scheduleJobResult->StartDescriptor;
    if (!OnJobStarted(startDescriptor.Id, startDescriptor.ResourceLimits.ToJobResources(), precommittedResources)) {
        Controller_->AbortJob(startDescriptor.Id, EAbortReason::SchedulingOperationDisabled);
        deactivateOperationElement(EDeactivationReason::OperationDisabled);
        TreeHost_->GetResourceTree()->IncreaseHierarchicalResourceUsagePrecommit(ResourceTreeElement_, -precommittedResources);
        FinishScheduleJob(context->SchedulingContext());
        return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ false);
    }

    context->SchedulingContext()->StartJob(
        GetTreeId(),
        OperationId_,
        scheduleJobResult->IncarnationId,
        startDescriptor,
        Spec_->PreemptionMode);

    auto resourceUsageBeforeUpdate = GetCurrentResourceUsage(context->DynamicAttributesList());
    CalculateCurrentResourceUsage(context);
    UpdateDynamicAttributes(&context->DynamicAttributesList());
    auto resourceUsageAfterUpdate = GetCurrentResourceUsage(context->DynamicAttributesList());

    auto resourceUsageDelta = resourceUsageAfterUpdate - resourceUsageBeforeUpdate;

    GetMutableParent()->UpdateChild(context, this);
    UpdateAncestorsDynamicAttributes(context, resourceUsageDelta);

    if (heartbeatSnapshot) {
        recordHeartbeatWithTimer(*heartbeatSnapshot);
    }

    FinishScheduleJob(context->SchedulingContext());

    YT_ELEMENT_LOG_DETAILED(this,
        "Scheduled a job (SatisfactionRatio: %v, NodeId: %v, JobId: %v, JobResourceLimits: %v)",
        context->DynamicAttributesFor(this).SatisfactionRatio,
        context->SchedulingContext()->GetNodeDescriptor().Id,
        startDescriptor.Id,
        Host_->FormatResources(startDescriptor.ResourceLimits));
    return TFairShareScheduleJobResult(/* finished */ true, /* scheduled */ true);
}

TString TOperationElement::GetId() const
{
    return ToString(OperationId_);
}

TOperationElement* TOperationElement::AsOperation()
{
    return this;
}

bool TOperationElement::IsAggressiveStarvationPreemptionAllowed() const
{
    return Spec_->AllowAggressiveStarvationPreemption.value_or(true);
}

std::optional<double> TOperationElement::GetSpecifiedWeight() const
{
    return RuntimeParameters_->Weight;
}

TResourceLimitsConfigPtr TOperationElement::GetStrongGuaranteeResourcesConfig() const
{
    return Spec_->StrongGuaranteeResources;
}

TResourceVector TOperationElement::GetMaxShare() const
{
    return TResourceVector::FromDouble(Spec_->MaxShareRatio.value_or(1.0));
}

const TSchedulingTagFilter& TOperationElement::GetSchedulingTagFilter() const
{
    return SchedulingTagFilter_;
}

ESchedulableStatus TOperationElement::GetStatus(bool atUpdate) const
{
    if (UnschedulableReason_) {
        return ESchedulableStatus::Normal;
    }

    return TSchedulerElement::GetStatusImpl(Attributes_.AdjustedFairShareStarvationTolerance, atUpdate);
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

    auto fairSharePreemptionTimeout = Attributes_.AdjustedFairSharePreemptionTimeout;

    double jobCountRatio = GetPendingJobCount() / TreeConfig_->JobCountPreemptionTimeoutCoefficient;
    if (jobCountRatio < 1.0) {
        fairSharePreemptionTimeout *= jobCountRatio;
    }

    TSchedulerElement::CheckForStarvationImpl(fairSharePreemptionTimeout, now);
}

bool TOperationElement::IsPreemptionAllowed(
    bool isAggressivePreemption,
    const TDynamicAttributesList& dynamicAttributesList,
    const TFairShareStrategyTreeConfigPtr& config) const
{
    if (Spec_->PreemptionMode == EPreemptionMode::Graceful) {
        return false;
    }

    int maxUnpreemptableJobCount = config->MaxUnpreemptableRunningJobCount;
    if (Spec_->MaxUnpreemptableRunningJobCount) {
        maxUnpreemptableJobCount = std::min(maxUnpreemptableJobCount, *Spec_->MaxUnpreemptableRunningJobCount);
    }

    int jobCount = GetRunningJobCount();
    if (jobCount <= maxUnpreemptableJobCount) {
        OperationElementSharedState_->UpdatePreemptionStatusStatistics(EOperationPreemptionStatus::ForbiddenSinceLowJobCount);
        return false;
    }

    // TODO(eshcherbin): Rethink this check, perhaps we don't need to perform it at every ancestor (see: YT-13670)
    const TSchedulerElement* element = this;
    while (element && !element->IsRoot()) {
        if (config->PreemptionCheckStarvation && element->GetStarving()) {
            OperationElementSharedState_->UpdatePreemptionStatusStatistics(EOperationPreemptionStatus::ForbiddenSinceStarvingParentOrSelf);
            return false;
        }

        bool aggressivePreemptionEnabled = isAggressivePreemption &&
            element->IsAggressiveStarvationPreemptionAllowed() &&
            IsAggressiveStarvationPreemptionAllowed();
        auto threshold = aggressivePreemptionEnabled
            ? config->AggressivePreemptionSatisfactionThreshold
            : config->PreemptionSatisfactionThreshold;

        // NB: We want to use *local* satisfaction ratio here.
        double localSatisfactionRatio;
        if (TreeConfig_->UseRecentResourceUsageForLocalSatisfaction) {
            localSatisfactionRatio = element->ComputeLocalSatisfactionRatio(element->GetInstantResourceUsage());
        } else {
            const auto& elementAttributes = dynamicAttributesList[element->GetTreeIndex()];
            localSatisfactionRatio = element->ComputeLocalSatisfactionRatio(elementAttributes.ResourceUsage);
        }
        if (config->PreemptionCheckSatisfaction && localSatisfactionRatio < threshold + RatioComparisonPrecision) {
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

void TOperationElement::SetJobResourceUsage(TJobId jobId, const TJobResources& resources)
{
    auto delta = OperationElementSharedState_->SetJobResourceUsage(jobId, resources);
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

void TOperationElement::ProfileFull(bool profilingCompatibilityEnabled)
{
    TSensorBuffer buffer;
    Profile(&buffer, profilingCompatibilityEnabled);
    BufferedProducer_->Update(std::move(buffer));
}

TString TOperationElement::GetUserName() const
{
    return UserName_;
}

TResourceVector TOperationElement::ComputeLimitsShare() const
{
    return TResourceVector::Min(TSchedulerElement::ComputeLimitsShare(), PersistentAttributes_.BestAllocationShare);
}

bool TOperationElement::OnJobStarted(
    TJobId jobId,
    const TJobResources& resourceUsage,
    const TJobResources& precommittedResources,
    bool force)
{
    YT_ELEMENT_LOG_DETAILED(this, "Adding job to strategy (JobId: %v)", jobId);

    if (OperationElementSharedState_->AddJob(jobId, resourceUsage, force)) {
        TreeHost_->GetResourceTree()->CommitHierarchicalResourceUsage(ResourceTreeElement_, resourceUsage, precommittedResources);
        UpdatePreemptableJobsList();
        return true;
    } else {
        return false;
    }
}

void TOperationElement::OnJobFinished(TJobId jobId)
{
    YT_ELEMENT_LOG_DETAILED(this, "Removing job from strategy (JobId: %v)", jobId);

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
    return Controller_->HasRecentScheduleJobFailure(now);
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
    auto availableResources = context.SchedulingContext()->GetNodeFreeResourcesWithDiscount();

    // Bound available resources with pool free resources.
    const TSchedulerElement* parent = this;
    while (parent) {
        availableResources = Min(availableResources, parent->GetLocalAvailableResourceLimits(context));
        parent = parent->GetParent();
    }

    return availableResources;
}

TJobResources TOperationElement::GetLocalAvailableResourceDemand(const TFairShareContext& context) const
{
    return ComputeAvailableResources(
        ResourceDemand(),
        ResourceTreeElement_->GetResourceUsageWithPrecommit(),
        context.DynamicAttributesFor(this).ResourceUsageDiscount);
}


TControllerScheduleJobResultPtr TOperationElement::DoScheduleJob(
    TFairShareContext* context,
    const TJobResources& availableResources,
    TJobResources* precommittedResources)
{
    ++context->SchedulingStatistics().ControllerScheduleJobCount;

    auto scheduleJobResult = Controller_->ScheduleJob(
        context->SchedulingContext(),
        availableResources,
        ControllerConfig_->ScheduleJobTimeLimit,
        GetTreeId(),
        TreeConfig_);

    // Discard the job in case of resource overcommit.
    if (scheduleJobResult->StartDescriptor) {
        const auto& startDescriptor = *scheduleJobResult->StartDescriptor;
        // Note: resourceDelta might be negative.
        const auto resourceDelta = startDescriptor.ResourceLimits.ToJobResources() - *precommittedResources;
        auto increaseResult = TryIncreaseHierarchicalResourceUsagePrecommit(resourceDelta);
        switch (increaseResult) {
            case EResourceTreeIncreaseResult::Success: {
                *precommittedResources += resourceDelta;
                break;
            }
            case EResourceTreeIncreaseResult::ResourceLimitExceeded: {
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
                break;
            }
            case EResourceTreeIncreaseResult::ElementIsNotAlive: {
                auto jobId = scheduleJobResult->StartDescriptor->Id;
                YT_LOG_DEBUG("Aborting job as operation is not alive in tree anymore (JobId: %v)", jobId);

                Controller_->AbortJob(jobId, EAbortReason::SchedulingOperationIsNotAlive);

                scheduleJobResult = New<TControllerScheduleJobResult>();
                scheduleJobResult->RecordFail(EScheduleJobFailReason::OperationIsNotAlive);
                break;
            }
            default:
                YT_ABORT();
        }
    } else if (scheduleJobResult->Failed[EScheduleJobFailReason::Timeout] > 0) {
        YT_LOG_WARNING("Job scheduling timed out");

        SetOperationAlert(
            OperationId_,
            EOperationAlertType::ScheduleJobTimedOut,
            TError("Job scheduling timed out: either scheduler is under heavy load or operation is too heavy"),
            ControllerConfig_->ScheduleJobTimeoutAlertResetTime);
    }

    return scheduleJobResult;
}

TJobResources TOperationElement::ComputeResourceDemand() const
{
    auto maybeUnschedulableReason = Operation_->CheckUnschedulable();
    if (maybeUnschedulableReason == EUnschedulableReason::IsNotRunning || maybeUnschedulableReason == EUnschedulableReason::Suspended) {
        return ResourceUsageAtUpdate_;
    }
    return ResourceUsageAtUpdate_ + TotalNeededResources_;
}

TJobResources TOperationElement::GetSpecifiedResourceLimits() const
{
    return ToJobResources(RuntimeParameters_->ResourceLimits, TJobResources::Infinite());
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

    if (elapsed > TreeConfig_->UpdatePreemptableListDurationLoggingThreshold) {
        YT_LOG_DEBUG("Preemptable list update is too long (Duration: %v, MoveCount: %v)",
            elapsed.MilliSeconds(),
            moveCount);
    }
}

EResourceTreeIncreaseResult TOperationElement::TryIncreaseHierarchicalResourceUsagePrecommit(
    const TJobResources& delta,
    TJobResources* availableResourceLimitsOutput)
{
    return TreeHost_->GetResourceTree()->TryIncreaseHierarchicalResourceUsagePrecommit(
        ResourceTreeElement_,
        delta,
        availableResourceLimitsOutput);
}

void TOperationElement::AttachParent(TCompositeSchedulerElement* newParent, int slotIndex)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(!Parent_);

    Parent_ = newParent;
    SlotIndex_ = slotIndex;
    TreeHost_->GetResourceTree()->AttachParent(ResourceTreeElement_, newParent->ResourceTreeElement_);

    newParent->IncreaseOperationCount(1);
    newParent->AddChild(this, /* enabled */ false);

    UpdateProfilers();

    YT_LOG_DEBUG("Operation attached to pool (Pool: %v)", newParent->GetId());
}

void TOperationElement::ChangeParent(TCompositeSchedulerElement* parent, int slotIndex)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(Parent_);

    SlotIndex_ = slotIndex;

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

    UpdateProfilers();

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
    TreeHost_->GetResourceTree()->ScheduleDetachParent(ResourceTreeElement_);

    YT_LOG_DEBUG("Operation detached from pool (Pool: %v)", parentId);
}

void TOperationElement::MarkOperationRunningInPool()
{
    Parent_->IncreaseRunningOperationCount(1);
    RunningInThisPoolTree_ = true;
    PendingByPool_.reset();

    YT_LOG_INFO("Operation is running in pool (Pool: %v)", Parent_->GetId());
}

bool TOperationElement::IsOperationRunningInPool() const
{
    return RunningInThisPoolTree_;
}
    
void TOperationElement::UpdateProfilers()
{
    YT_VERIFY(GetParent());
    YT_VERIFY(SlotIndex_ != UndefinedSlotIndex);

    auto treeProfiler = TreeHost_->GetProfiler();

    BufferedProducer_ = New<TBufferedProducer>();
    
    treeProfiler
        .WithRequiredTag("pool", GetParent()->GetId(), -1)
        .WithRequiredTag("slot_index", ToString(SlotIndex_), -1)
        .AddProducer("/operations_by_slot", BufferedProducer_);

    auto parent = GetParent();
    while (parent != nullptr) {
        bool enableProfiling = false;
        if (!parent->IsRoot()) {
            const auto* pool = static_cast<const TPool*>(parent);
            enableProfiling = pool->GetConfig()->EnableByUserProfiling.value_or(TreeConfig_->EnableByUserProfiling);
        } else {
            enableProfiling = TreeConfig_->EnableByUserProfiling;
        }

        if (enableProfiling) {
            auto userProfiler = treeProfiler
                .WithTag("pool", parent->GetId(), -1)
                .WithRequiredTag("user_name", GetUserName(), -1);

            if (auto customTag = GetCustomProfilingTag()) {
                userProfiler = userProfiler.WithTag("custom", *customTag, -1);
            }

            userProfiler.AddProducer("/operations_by_user", BufferedProducer_);
        }

        parent = parent->GetParent();
    }
}

TFairShareStrategyPackingConfigPtr TOperationElement::GetPackingConfig() const
{
    return TreeConfig_->Packing;
}

void TOperationElement::MarkPendingBy(TCompositeSchedulerElement* violatedPool)
{
    violatedPool->PendingOperationIds().push_back(OperationId_);
    PendingByPool_ = violatedPool->GetId();

    YT_LOG_DEBUG("Operation is pending since max running operation count is violated (OperationId: %v, Pool: %v, Limit: %v)",
        OperationId_,
        violatedPool->GetId(),
        violatedPool->GetMaxRunningOperationCount());
}

void TOperationElement::InitOrUpdateSchedulingSegment(ESegmentedSchedulingMode mode)
{
    auto maybeInitialMinNeededResources = Operation_->GetInitialAggregatedMinNeededResources();
    auto segment = Spec_->SchedulingSegment.value_or(
        TStrategySchedulingSegmentManager::GetSegmentForOperation(mode,
            maybeInitialMinNeededResources.value_or(TJobResources{})));

    if (SchedulingSegment_ != segment) {
        YT_LOG_DEBUG("Setting new scheduling segment for operation (Segment: %v, Mode: %v, InitialMinNeededResources: %v, SpecifiedSegment: %v)",
            segment,
            mode,
            maybeInitialMinNeededResources,
            Spec_->SchedulingSegment);

        SchedulingSegment_ = segment;
        SpecifiedSchedulingSegmentDataCenters_ = Spec_->SchedulingSegmentDataCenters;
        if (!IsDataCenterAwareSchedulingSegment(segment)) {
            PersistentAttributes_.SchedulingSegmentDataCenter.reset();
        }
    }
}

bool TOperationElement::IsLimitingAncestorCheckEnabled() const
{
    return Spec_->EnableLimitingAncestorCheck;
}

bool TOperationElement::AreDetailedLogsEnabled() const
{
    return RuntimeParameters_->EnableDetailedLogs;
}

bool TOperationElement::IsSchedulingSegmentCompatibleWithNode(ESchedulingSegment nodeSegment, const TDataCenter& nodeDataCenter) const
{
    if (TreeConfig_->SchedulingSegments->Mode == ESegmentedSchedulingMode::Disabled) {
        return true;
    }

    if (!SchedulingSegment_) {
        return false;
    }

    if (IsDataCenterAwareSchedulingSegment(*SchedulingSegment_)) {
        if (!PersistentAttributes_.SchedulingSegmentDataCenter) {
            // We have not decided on the operation's data center yet.
            return false;
        }

        return SchedulingSegment_ == nodeSegment && PersistentAttributes_.SchedulingSegmentDataCenter == nodeDataCenter;
    }

    YT_VERIFY(!PersistentAttributes_.SchedulingSegmentDataCenter);

    return *SchedulingSegment_ == nodeSegment;
}

void TOperationElement::CollectOperationSchedulingSegmentContexts(
    THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const
{
    YT_VERIFY(operationContexts->emplace(
        OperationId_,
        TOperationSchedulingSegmentContext{
            .ResourceDemand = ResourceDemand_,
            .ResourceUsage = ResourceUsageAtUpdate_,
            .DemandShare = Attributes_.DemandShare,
            .FairShare = Attributes_.FairShare.Total,
            .SpecifiedDataCenters = SpecifiedSchedulingSegmentDataCenters_,
            .Segment = SchedulingSegment_,
            .DataCenter = PersistentAttributes_.SchedulingSegmentDataCenter,
            .FailingToScheduleAtDataCenterSince = PersistentAttributes_.FailingToScheduleAtDataCenterSince,
        }).second);
}

void TOperationElement::ApplyOperationSchedulingSegmentChanges(
    const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts)
{
    const auto& context = GetOrCrash(operationContexts, OperationId_);
    PersistentAttributes_.SchedulingSegmentDataCenter = context.DataCenter;
    PersistentAttributes_.FailingToScheduleAtDataCenterSince = context.FailingToScheduleAtDataCenterSince;
}

void TOperationElement::CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const
{
    elements->push_back(ResourceTreeElement_);
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
        RootPoolName,
        EResourceTreeElementKind::Root,
        logger.WithTag("PoolId: %v, SchedulingMode: %v",
            RootPoolName,
            ESchedulingMode::FairShare))
{

    Mode_ = ESchedulingMode::FairShare;
    Attributes_.AdjustedFairShareStarvationTolerance = GetFairShareStarvationTolerance();
    Attributes_.AdjustedFairSharePreemptionTimeout = GetFairSharePreemptionTimeout();
    AdjustedFairShareStarvationToleranceLimit_ = GetFairShareStarvationToleranceLimit();
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
    Attributes_.AdjustedFairSharePreemptionTimeout = GetFairSharePreemptionTimeout();
}

void TRootElement::PreUpdate(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    TForbidContextSwitchGuard contextSwitchGuard;

    DisableNonAliveElements();
    TreeSize_ = TCompositeSchedulerElement::EnumerateElements(0, context);
    context->TotalResourceLimits = GetHost()->GetResourceLimits(TreeConfig_->NodesFilter);

    PreUpdateBottomUp(context);
}

/// Steps of fair share update:
///
/// 1. Initialize burst and relaxed pool lists. This is a single pass through the tree.
///
/// 2. Update attributes needed for calculation of fair share (LimitsShare, DemandShare, UsageShare, StrongGuaranteeShare and others);
///
/// 3. Consume and refill accumulated resource volume of integral pools.
///   The amount of resources consumed by a pool is based on its integral guarantee share since the last fair share update.
///   Refilling is based on the resource flow ratio which was calculated in the previous step.
///
/// 4. Validate that the sum of burst and strong guarantee shares meet the total resources and that the strong guarantee share of every pool meets the limits share of that pool.
///   Shrink the guarantees in case of limits violations.
///
/// 5. Calculate integral shares for burst pools.
///   We temporarily increase the pool's resource guarantees by burst guarantees, and calculate how many resources the pool would consume within these extended guarantees.
///   Then we subtract the pool's strong guarantee share from the consumed resources to estimate the integral shares.
///   Descendants of burst pools have their fair share functions built on this step.
///
/// 6. Estimate the amount of available resources after satisfying strong and burst guarantees of all pools.
///
/// 7. Distribute available resources among the relaxed pools using binary search.
///   We build fair share functions for descendants of relaxed pools in this step.
///
/// 8. Build fair share functions and compute final fair shares of all pools.
///   The weight proportional component emerges here.
///
/// 9. Publish the computed fair share to the shared resource tree and update the operations' preemptable job lists.
///
/// 10. Update dynamic attributes based on the calculated fair share (for orchid).
///
/// 11. Manage scheduling segments.
///    We build the tree's scheduling segment state and assign eligible operations in DC-aware segments to data centers.
///

void TRootElement::Update(TUpdateFairShareContext* context)
{
    YT_VERIFY(Mutable_);

    VERIFY_INVOKER_AFFINITY(Host_->GetFairShareUpdateInvoker());
    TForbidContextSwitchGuard contextSwitchGuard;

    DetermineEffectiveStrongGuaranteeResources();
    InitIntegralPoolLists(context);
    UpdateCumulativeAttributes(context);
    ConsumeAndRefillIntegralPools(context);
    ValidateAndAdjustSpecifiedGuarantees(context);

    UpdateBurstPoolIntegralShares(context);
    auto availableShare = EstimateAvailableShare();
    UpdateRelaxedPoolIntegralShares(context, availableShare);

    UpdateFairShare(context);

    PublishFairShareAndUpdatePreemption();

    // We calculate SatisfactionRatio by computing dynamic attributes using the same algorithm as during the scheduling phase.
    TDynamicAttributesList dynamicAttributesList{static_cast<size_t>(TreeSize_)};
    UpdateSchedulableAttributesFromDynamicAttributes(&dynamicAttributesList);

    ManageSchedulingSegments(context);
}

void TRootElement::UpdateFairShare(TUpdateFairShareContext* context)
{
    YT_LOG_DEBUG("Updating fair share");

    TWallTimer timer;
    PrepareFairShareFunctions(context);
    DoUpdateFairShare(/* suggestion */ 1.0, context);
    UpdateRootFairShare();
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

void TRootElement::UpdateRootFairShare()
{
    // Make fair share at root equal to sum of children.
    TResourceVector totalUsedStrongGuaranteeShare;
    TResourceVector totalFairShare;
    for (const auto& child : EnabledChildren_) {
        totalUsedStrongGuaranteeShare += child->Attributes().FairShare.StrongGuarantee;
        totalFairShare += child->Attributes().FairShare.Total;
    }

    // NB(eshcherbin): In order to compute the detailed fair share components correctly,
    // we need to set |Attributes_.StrongGuaranteeShare| to the actual used strong guarantee share before calling |SetFairShare|.
    // However, afterwards it seems more natural to restore the previous value, which shows
    // the total configured strong guarantee shares in the tree.
    {
        auto staticStrongGuaranteeShare = Attributes_.StrongGuaranteeShare;
        Attributes_.StrongGuaranteeShare = totalUsedStrongGuaranteeShare;
        Attributes_.SetFairShare(totalFairShare);
        Attributes_.StrongGuaranteeShare = staticStrongGuaranteeShare;
    }
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

TJobResources TRootElement::GetSpecifiedStrongGuaranteeResources() const
{
    return TotalResourceLimits_;
}

void TRootElement::DetermineEffectiveStrongGuaranteeResources()
{
    YT_VERIFY(Mutable_);

    EffectiveStrongGuaranteeResources_ = TotalResourceLimits_;

    TCompositeSchedulerElement::DetermineEffectiveStrongGuaranteeResources();
}

TResourceVector TRootElement::GetMaxShare() const
{
    return TResourceVector::Ones();
}

double TRootElement::GetFairShareStarvationTolerance() const
{
    return TreeConfig_->FairShareStarvationTolerance;
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

std::optional<TMeteringKey> TRootElement::GetMeteringKey() const
{
    return TMeteringKey{
        .AbcId = Host_->GetDefaultAbcId(),
        .TreeId = GetTreeId(),
        .PoolId = GetId(),
    };
}

void TRootElement::BuildResourceDistributionInfo(TFluentMap fluent) const
{
    double distributedStrongGuaranteeDominantShare = 0.0;
    for (const auto& child : EnabledChildren_) {
        // TODO(renadeen): Fix when strong guarantee share becomes disproportional.
        distributedStrongGuaranteeDominantShare += GetMaxResourceRatio(child->GetSpecifiedStrongGuaranteeResources(), TotalResourceLimits_);
    }
    double maxDistributedIntegralRatio = std::max(Attributes_.TotalBurstRatio, Attributes_.TotalResourceFlowRatio);
    double undistributedResourceFlowRatio = std::max(Attributes_.TotalBurstRatio - Attributes_.TotalResourceFlowRatio, 0.0);
    double undistributedBurstGuaranteeRatio = std::max(Attributes_.TotalResourceFlowRatio - Attributes_.TotalBurstRatio, 0.0);

    fluent
        .Item("distributed_strong_guarantee_resources").Value(TotalResourceLimits_ * distributedStrongGuaranteeDominantShare)
        .Item("distributed_resource_flow").Value(TotalResourceLimits_ * Attributes_.TotalResourceFlowRatio)
        .Item("distributed_burst_guarantee_resources").Value(TotalResourceLimits_ * Attributes_.TotalBurstRatio)
        .Item("undistributed_resources").Value(TotalResourceLimits_ * (1.0 - distributedStrongGuaranteeDominantShare - maxDistributedIntegralRatio))
        .Item("undistributed_resource_flow").Value(TotalResourceLimits_ * undistributedResourceFlowRatio)
        .Item("undistributed_burst_guarantee_resources").Value(TotalResourceLimits_ * undistributedBurstGuaranteeRatio);
}

void TRootElement::UpdateCumulativeAttributes(TUpdateFairShareContext* context)
{
    TCompositeSchedulerElement::UpdateCumulativeAttributes(context);
    Attributes_.StrongGuaranteeShare = TResourceVector::Zero();
    for (const auto& child : EnabledChildren_) {
        Attributes_.StrongGuaranteeShare += child->Attributes().StrongGuaranteeShare;
    }
}

void TRootElement::ValidateAndAdjustSpecifiedGuarantees(TUpdateFairShareContext* context)
{
    auto totalResourceFlow = TotalResourceLimits_ * Attributes_.TotalResourceFlowRatio;
    auto strongGuaranteeResources = TotalResourceLimits_ * Attributes_.StrongGuaranteeShare;
    if (!Dominates(TotalResourceLimits_, strongGuaranteeResources + totalResourceFlow)) {
        context->Errors.push_back(TError("Strong guarantees and resource flows exceed total cluster resources")
            << TErrorAttribute("total_strong_guarantee_resources", strongGuaranteeResources)
            << TErrorAttribute("total_resource_flow", totalResourceFlow)
            << TErrorAttribute("total_cluster_resources", TotalResourceLimits_));
    }
    auto totalBurstResources = TotalResourceLimits_ * Attributes_.TotalBurstRatio;
    if (!Dominates(TotalResourceLimits_, strongGuaranteeResources + totalBurstResources)) {
        context->Errors.push_back(TError("Strong guarantees and burst guarantees exceed total cluster resources")
            << TErrorAttribute("total_strong_guarantee_resources", strongGuaranteeResources)
            << TErrorAttribute("total_burst_resources", totalBurstResources)
            << TErrorAttribute("total_cluster_resources", TotalResourceLimits_));

        auto checkSum = [&] (double fitFactor) -> bool {
            auto sum = Attributes_.StrongGuaranteeShare * fitFactor;
            for (const auto& pool : context->BurstPools) {
                sum += TResourceVector::FromDouble(pool->Attributes().BurstRatio) * fitFactor;
            }
            return Dominates(TResourceVector::Ones(), sum);
        };

        double fitFactor = FloatingPointInverseLowerBound(0.0, 1.0, checkSum);

        Attributes_.StrongGuaranteeShare = Attributes_.StrongGuaranteeShare * fitFactor;
        for (const auto& pool : context->BurstPools) {
            pool->Attributes().BurstRatio *= fitFactor;
        }
    }
    AdjustStrongGuarantees();
}

void TRootElement::UpdateBurstPoolIntegralShares(TUpdateFairShareContext* context)
{
    for (auto& burstPool : context->BurstPools) {
        auto integralRatio = std::min(burstPool->Attributes().BurstRatio, burstPool->GetIntegralShareRatioByVolume());
        auto proposedIntegralShare = TResourceVector::Min(
            TResourceVector::FromDouble(integralRatio),
            burstPool->GetHierarchicalAvailableLimitsShare());
        YT_VERIFY(Dominates(proposedIntegralShare, TResourceVector::Zero()));

        burstPool->Attributes().ProposedIntegralShare = proposedIntegralShare;
        burstPool->PrepareFairShareFunctions(context);
        burstPool->Attributes().ProposedIntegralShare = TResourceVector::Zero();

        auto fairShareWithinGuarantees = burstPool->FairShareBySuggestion()->ValueAt(0.0);
        auto integralShare = TResourceVector::Max(fairShareWithinGuarantees - burstPool->Attributes().StrongGuaranteeShare, TResourceVector::Zero());
        burstPool->IncreaseHierarchicalIntegralShare(integralShare);
        burstPool->PersistentAttributes().LastIntegralShareRatio = MaxComponent(integralShare);
        burstPool->ResetFairShareFunctions();

        YT_LOG_DEBUG(
            "Provided integral share for burst pool "
            "(Pool: %v, ShareRatioByVolume: %v, ProposedIntegralShare: %v, FSWithingGuarantees: %v, IntegralShare: %v)",
            burstPool->GetId(),
            burstPool->GetIntegralShareRatioByVolume(),
            proposedIntegralShare,
            fairShareWithinGuarantees,
            integralShare);
    }
}

void TRootElement::ConsumeAndRefillIntegralPools(TUpdateFairShareContext* context)
{
    if (context->PreviousUpdateTime) {
        auto periodSinceLastUpdate = context->Now - *context->PreviousUpdateTime;
        for (const auto& pool : context->BurstPools) {
            pool->UpdateAccumulatedResourceVolume(periodSinceLastUpdate);
        }
        for (const auto& pool : context->RelaxedPools) {
            pool->UpdateAccumulatedResourceVolume(periodSinceLastUpdate);
        }
    }
}

void TRootElement::UpdateRelaxedPoolIntegralShares(TUpdateFairShareContext* context, const TResourceVector& availableShare)
{
    if (context->RelaxedPools.empty()) {
        return;
    }

    auto& relaxedPools = context->RelaxedPools;
    std::vector<double> weights;
    std::vector<TResourceVector> originalLimits;
    for (auto& relaxedPool : relaxedPools) {
        weights.push_back(relaxedPool->GetIntegralShareRatioByVolume());
        originalLimits.push_back(relaxedPool->Attributes().LimitsShare);
        relaxedPool->ApplyLimitsForRelaxedPool();
        relaxedPool->PrepareFairShareFunctions(context);
    }
    double minWeight = *std::min_element(weights.begin(), weights.end());
    for (auto& weight : weights) {
        weight = weight / minWeight;
    }

    auto checkFitFactor = [&] (double fitFactor) {
        TResourceVector fairShareResult;
        for (int index = 0; index < relaxedPools.size(); ++index) {
            auto suggestion = std::min(1.0, fitFactor * weights[index]);
            auto fairShare = relaxedPools[index]->FairShareBySuggestion()->ValueAt(suggestion);
            fairShareResult += TResourceVector::Max(fairShare - relaxedPools[index]->Attributes().StrongGuaranteeShare, TResourceVector::Zero());
        }

        return Dominates(availableShare, fairShareResult);
    };

    auto fitFactor = FloatingPointInverseLowerBound(
        /* lo */ 0.0,
        /* hi */ 1.0,
        /* predicate */ checkFitFactor);

    for (int index = 0; index < relaxedPools.size(); ++index) {
        auto weight = weights[index];
        const auto& relaxedPool = relaxedPools[index];
        auto suggestion = std::min(1.0, fitFactor * weight);
        auto fairShareWithinGuarantees = relaxedPool->FairShareBySuggestion()->ValueAt(suggestion);
        auto integralShare = TResourceVector::Max(fairShareWithinGuarantees - relaxedPool->Attributes().StrongGuaranteeShare, TResourceVector::Zero());

        relaxedPool->Attributes().LimitsShare = originalLimits[index];
        auto limitedIntegralShare = TResourceVector::Min(
            integralShare,
            relaxedPool->GetHierarchicalAvailableLimitsShare());
        YT_VERIFY(Dominates(limitedIntegralShare, TResourceVector::Zero()));
        relaxedPool->IncreaseHierarchicalIntegralShare(limitedIntegralShare);
        relaxedPool->ResetFairShareFunctions();
        relaxedPool->PersistentAttributes().LastIntegralShareRatio = MaxComponent(limitedIntegralShare);

        YT_LOG_DEBUG("Provided integral share for relaxed pool "
            "(Pool: %v, ShareRatioByVolume: %v, Suggestion: %v, FSWithingGuarantees: %v, IntegralShare: %v, LimitedIntegralShare: %v)",
            relaxedPool->GetId(),
            relaxedPool->GetIntegralShareRatioByVolume(),
            suggestion,
            fairShareWithinGuarantees,
            integralShare,
            limitedIntegralShare);
    }
}

void TRootElement::ManageSchedulingSegments(TUpdateFairShareContext* context)
{
    TManageTreeSchedulingSegmentsContext manageSegmentsContext{
        .TreeConfig = TreeConfig_,
        .TotalResourceLimits = TotalResourceLimits_,
        .ResourceLimitsPerDataCenter = std::move(context->ResourceLimitsPerDataCenter),
    };

    if (TreeConfig_->SchedulingSegments->Mode != ESegmentedSchedulingMode::Disabled) {
        CollectOperationSchedulingSegmentContexts(&manageSegmentsContext.Operations);
    }

    TStrategySchedulingSegmentManager::ManageSegmentsInTree(&manageSegmentsContext, TreeId_);

    context->SchedulingSegmentsState = std::move(manageSegmentsContext.SchedulingSegmentsState);
    if (TreeConfig_->SchedulingSegments->Mode != ESegmentedSchedulingMode::Disabled) {
        ApplyOperationSchedulingSegmentChanges(manageSegmentsContext.Operations);
    }
}

TResourceVector TRootElement::EstimateAvailableShare()
{
    auto freeClusterShare = TResourceVector::Ones();
    for (const auto& pool : EnabledChildren_) {
        auto usedShare = TResourceVector::Min(pool->Attributes().GetGuaranteeShare(), pool->Attributes().DemandShare);
        freeClusterShare -= usedShare;
    }
    return freeClusterShare;
}

double TRootElement::GetSpecifiedBurstRatio() const
{
    return 0.0;
}

double TRootElement::GetSpecifiedResourceFlowRatio() const
{
    return 0.0;
}

////////////////////////////////////////////////////////////////////////////////

TChildHeap::TChildHeap(
    const std::vector<TSchedulerElementPtr>& children,
    TDynamicAttributesList* dynamicAttributesList,
    TComparator comparator)
    : DynamicAttributesList_(*dynamicAttributesList)
    , Comparator_(comparator)
{
    ChildHeap_.reserve(children.size());
    for (const auto& child : children) {
        ChildHeap_.push_back(child.Get());
    }
    MakeHeap(ChildHeap_.begin(), ChildHeap_.end(), Comparator_);

    for (size_t index = 0; index < ChildHeap_.size(); ++index) {
        DynamicAttributesList_[ChildHeap_[index]->GetTreeIndex()].HeapIndex = index;
    }
}

TSchedulerElement* TChildHeap::GetTop()
{
    YT_VERIFY(!ChildHeap_.empty());
    return ChildHeap_.front();
}

void TChildHeap::Update(TSchedulerElement* child)
{
    int heapIndex = DynamicAttributesList_[child->GetTreeIndex()].HeapIndex;
    YT_VERIFY(heapIndex != InvalidHeapIndex);
    AdjustHeapItem(
        ChildHeap_.begin(),
        ChildHeap_.end(),
        ChildHeap_.begin() + heapIndex,
        Comparator_,
        std::bind(&TChildHeap::OnAssign, this, std::placeholders::_1));
}

const std::vector<TSchedulerElement*>& TChildHeap::GetHeap() const
{
    return ChildHeap_;
}

void TChildHeap::OnAssign(size_t offset)
{
    DynamicAttributesList_[ChildHeap_[offset]->GetTreeIndex()].HeapIndex = offset;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
