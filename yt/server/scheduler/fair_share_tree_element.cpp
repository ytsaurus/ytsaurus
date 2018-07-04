#include "fair_share_tree_element.h"

#include "scheduling_context.h"

#include <yt/server/controller_agent/operation_controller.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>

#include <yt/core/misc/finally.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;

////////////////////////////////////////////////////////////////////////////////

static const double RatioComputationPrecision = std::numeric_limits<double>::epsilon();
static const double RatioComparisonPrecision = sqrt(RatioComputationPrecision);

////////////////////////////////////////////////////////////////////////////////

TAggregateGauge& GetCounter(const TString& treeId, const TString& name)
{
    using TCounterKey = std::pair<TString, TString>;
    using TCounterValue = std::unique_ptr<TAggregateGauge>;

    static THashMap<TCounterKey, TCounterValue> counters;

    TCounterKey key = std::make_pair(treeId, name);
    auto it = counters.find(key);
    if (it == counters.end()) {
        auto tag = TProfileManager::Get()->RegisterTag("tree", treeId);
        auto ptr = std::make_unique<TAggregateGauge>(name, TTagIdList{tag});
        it = counters.emplace(key, std::move(ptr)).first;
    }

    return *it->second;
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
    return defaultValue;
}

////////////////////////////////////////////////////////////////////////////////

TFairShareContext::TFairShareContext(const ISchedulingContextPtr& schedulingContext)
    : SchedulingContext(schedulingContext)
{ }

void TFairShareContext::Initialize(int treeSize, const std::vector<TSchedulingTagFilter>& registeredSchedulingTagFilters)
{
    YCHECK(!Initialized);

    Initialized = true;

    DynamicAttributesList.resize(treeSize);
    CanSchedule.reserve(registeredSchedulingTagFilters.size());
    for (const auto& filter : registeredSchedulingTagFilters) {
        CanSchedule.push_back(SchedulingContext->CanSchedule(filter));
    }
}

TDynamicAttributes& TFairShareContext::DynamicAttributes(const TSchedulerElement* element)
{
    int index = element->GetTreeIndex();
    YCHECK(index != UnassignedTreeIndex && index < DynamicAttributesList.size());
    return DynamicAttributesList[index];
}

const TDynamicAttributes& TFairShareContext::DynamicAttributes(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YCHECK(index != UnassignedTreeIndex && index < DynamicAttributesList.size());
    return DynamicAttributesList[index];
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerElementFixedState::TSchedulerElementFixedState(
    ISchedulerStrategyHost* host,
    const TFairShareStrategyTreeConfigPtr& treeConfig,
    const TString& treeId)
    : ResourceDemand_(ZeroJobResources())
    , ResourceLimits_(InfiniteJobResources())
    , MaxPossibleResourceUsage_(ZeroJobResources())
    , Host_(host)
    , TreeConfig_(treeConfig)
    , TotalResourceLimits_(host->GetResourceLimits(treeConfig->NodesFilter))
    , TreeId_(treeId)
{ }

////////////////////////////////////////////////////////////////////////////////

TJobResources TSchedulerElementSharedState::GetResourceUsage()
{
    TReaderGuard guard(ResourceUsageLock_);

    return ResourceUsage_;
}

TJobResources TSchedulerElementSharedState::GetResourceUsagePrecommit()
{
    TReaderGuard guard(ResourceUsageLock_);

    return ResourceUsagePrecommit_;
}

TJobMetrics TSchedulerElementSharedState::GetJobMetrics()
{
    TReaderGuard guard(JobMetricsLock_);

    return JobMetrics_;
}

void TSchedulerElementSharedState::IncreaseResourceUsage(const TJobResources& delta)
{
    TWriterGuard guard(ResourceUsageLock_);

    ResourceUsage_ += delta;
}

void TSchedulerElementSharedState::IncreaseResourceUsagePrecommit(const TJobResources& delta)
{
    TWriterGuard guard(ResourceUsageLock_);

    ResourceUsagePrecommit_ += delta;
}


void TSchedulerElementSharedState::ApplyJobMetricsDelta(const TJobMetrics& delta)
{
    TWriterGuard guard(JobMetricsLock_);

    JobMetrics_ += delta;
}

double TSchedulerElementSharedState::GetResourceUsageRatio(
    EResourceType dominantResource,
    double dominantResourceLimit)
{
    TReaderGuard guard(ResourceUsageLock_);

    if (dominantResourceLimit == 0) {
        return 0.0;
    }
    return GetResource(ResourceUsage_, dominantResource) / dominantResourceLimit;
}

////////////////////////////////////////////////////////////////////////////////

int TSchedulerElement::EnumerateNodes(int startIndex)
{
    YCHECK(!Cloned_);

    TreeIndex_ = startIndex++;
    return startIndex;
}

void TSchedulerElement::UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    YCHECK(!Cloned_);

    TreeConfig_ = config;
}

void TSchedulerElement::Update(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    UpdateBottomUp(dynamicAttributesList);
    UpdateTopDown(dynamicAttributesList);
}

void TSchedulerElement::UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    TotalResourceLimits_ = GetHost()->GetResourceLimits(TreeConfig_->NodesFilter);
    UpdateAttributes();
    dynamicAttributesList[GetTreeIndex()].Active = true;
    UpdateDynamicAttributes(dynamicAttributesList);
}

void TSchedulerElement::UpdateTopDown(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);
}

void TSchedulerElement::UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList)
{
    auto& attributes = dynamicAttributesList[GetTreeIndex()];
    YCHECK(attributes.Active);
    attributes.SatisfactionRatio = ComputeLocalSatisfactionRatio();
    attributes.Active = IsAlive();
}

void TSchedulerElement::PrescheduleJob(TFairShareContext* context, bool /*starvingOnly*/, bool /*aggressiveStarvationEnabled*/)
{
    UpdateDynamicAttributes(context->DynamicAttributesList);
}

void TSchedulerElement::UpdateAttributes()
{
    YCHECK(!Cloned_);

    // Choose dominant resource types, compute max share ratios, compute demand ratios.
    const auto& demand = ResourceDemand();
    auto usage = GetResourceUsage();

    auto maxPossibleResourceUsage = Min(TotalResourceLimits_, MaxPossibleResourceUsage_);

    if (usage == ZeroJobResources()) {
        Attributes_.DominantResource = GetDominantResource(demand, TotalResourceLimits_);
    } else {
        Attributes_.DominantResource = GetDominantResource(usage, TotalResourceLimits_);
    }

    Attributes_.DominantLimit = GetResource(TotalResourceLimits_, Attributes_.DominantResource);

    auto dominantDemand = GetResource(demand, Attributes_.DominantResource);
    Attributes_.DemandRatio =
        Attributes_.DominantLimit == 0 ? 1.0 : dominantDemand / Attributes_.DominantLimit;

    double possibleUsageRatio = Attributes_.DemandRatio;
    if (TreeConfig_->EnableNewPossibleResourceUsageComputation) {
        auto possibleUsage = ComputePossibleResourceUsage(maxPossibleResourceUsage);
        possibleUsageRatio = GetDominantResourceUsage(possibleUsage, TotalResourceLimits_);
    } else {
        if (GetResourceUsageRatio() <= TreeConfig_->ThresholdToEnableMaxPossibleUsageRegularization * Attributes_.DemandRatio)
        {
            auto possibleUsage = usage + ComputePossibleResourceUsage(maxPossibleResourceUsage - usage);
            possibleUsageRatio = GetDominantResourceUsage(possibleUsage, TotalResourceLimits_);
        }
    }

    Attributes_.MaxPossibleUsageRatio = std::min(
        possibleUsageRatio,
        GetMaxShareRatio());
}

const TSchedulingTagFilter& TSchedulerElement::GetSchedulingTagFilter() const
{
    return EmptySchedulingTagFilter;
}

bool TSchedulerElement::IsActive(const TDynamicAttributesList& dynamicAttributesList) const
{
    return dynamicAttributesList[GetTreeIndex()].Active;
}

TCompositeSchedulerElement* TSchedulerElement::GetParent() const
{
    return Parent_;
}

void TSchedulerElement::SetParent(TCompositeSchedulerElement* parent)
{
    YCHECK(!Cloned_);

    Parent_ = parent;
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
    return Starving_;
}

void TSchedulerElement::SetStarving(bool starving)
{
    YCHECK(!Cloned_);

    Starving_ = starving;
}

TJobResources TSchedulerElement::GetResourceUsage() const
{
    auto resourceUsage = SharedState_->GetResourceUsage();
    if (resourceUsage.GetUserSlots() > 0 && resourceUsage.GetMemory() == 0) {
        LOG_WARNING("Found usage of schedulable element %Qv with non-zero "
            "user slots and zero memory (TreeId: %v)",
            GetId(),
            GetTreeId());
    }
    return resourceUsage;
}

TJobResources TSchedulerElement::GetResourceUsagePrecommit() const
{
    return SharedState_->GetResourceUsagePrecommit();
}

TJobMetrics TSchedulerElement::GetJobMetrics() const
{
    return SharedState_->GetJobMetrics();
}

double TSchedulerElement::GetResourceUsageRatio() const
{
    return SharedState_->GetResourceUsageRatio(
        Attributes_.DominantResource,
        Attributes_.DominantLimit);
}

TString TSchedulerElement::GetTreeId() const
{
    return TreeId_;
}

void TSchedulerElement::IncreaseLocalResourceUsage(const TJobResources& delta)
{
    SharedState_->IncreaseResourceUsage(delta);
}

void TSchedulerElement::IncreaseLocalResourceUsagePrecommit(const TJobResources& delta)
{
    SharedState_->IncreaseResourceUsagePrecommit(delta);
}


void TSchedulerElement::ApplyJobMetricsDeltaLocal(const TJobMetrics& delta)
{
    SharedState_->ApplyJobMetricsDelta(delta);
}

TSchedulerElement::TSchedulerElement(
    ISchedulerStrategyHost* host,
    const TFairShareStrategyTreeConfigPtr& treeConfig,
    const TString& treeId)
    : TSchedulerElementFixedState(host, treeConfig, treeId)
    , SharedState_(New<TSchedulerElementSharedState>())
{ }

TSchedulerElement::TSchedulerElement(
    const TSchedulerElement& other,
    TCompositeSchedulerElement* clonedParent)
    : TSchedulerElementFixedState(other)
    , SharedState_(other.SharedState_)
{
    Parent_ = clonedParent;
    Cloned_ = true;
}

ISchedulerStrategyHost* TSchedulerElement::GetHost() const
{
    YCHECK(!Cloned_);

    return Host_;
}

double TSchedulerElement::ComputeLocalSatisfactionRatio() const
{
    double minShareRatio = Attributes_.AdjustedMinShareRatio;
    double fairShareRatio = Attributes_.FairShareRatio;
    double usageRatio = GetResourceUsageRatio();

    // Check for corner cases.
    if (fairShareRatio < RatioComputationPrecision) {
        return std::numeric_limits<double>::max();
    }

    // Starvation is disabled for operations in FIFO pool.
    if (Attributes_.FifoIndex >= 0) {
        return std::numeric_limits<double>::max();
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
    YCHECK(!Cloned_);

    auto status = GetStatus();
    switch (status) {
        case ESchedulableStatus::BelowMinShare:
            if (!BelowFairShareSince_) {
                BelowFairShareSince_ = now;
            } else if (BelowFairShareSince_.Get() < now - minSharePreemptionTimeout) {
                SetStarving(true);
            }
            break;

        case ESchedulableStatus::BelowFairShare:
            if (!BelowFairShareSince_) {
                BelowFairShareSince_ = now;
            } else if (BelowFairShareSince_.Get() < now - fairSharePreemptionTimeout) {
                SetStarving(true);
            }
            break;

        case ESchedulableStatus::Normal:
            BelowFairShareSince_ = Null;
            SetStarving(false);
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TSchedulerElement::SetOperationAlert(
    const TOperationId& operationId,
    EOperationAlertType alertType,
    const TError& alert,
    TNullable<TDuration> timeout)
{
    Host_->SetOperationAlert(operationId, alertType, alert, timeout);
}

////////////////////////////////////////////////////////////////////////////////

TCompositeSchedulerElement::TCompositeSchedulerElement(
    ISchedulerStrategyHost* host,
    TFairShareStrategyTreeConfigPtr treeConfig,
    NProfiling::TTagId profilingTag,
    const TString& treeId)
    : TSchedulerElement(host, treeConfig, treeId)
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
            YCHECK(clonedMap->emplace(childClone, clonedList->size() - 1).second);
        }
    };
    cloneChildren(other.EnabledChildren_, &EnabledChildToIndex_, &EnabledChildren_);
    cloneChildren(other.DisabledChildren_, &DisabledChildToIndex_, &DisabledChildren_);
}

int TCompositeSchedulerElement::EnumerateNodes(int startIndex)
{
    YCHECK(!Cloned_);

    startIndex = TSchedulerElement::EnumerateNodes(startIndex);
    for (const auto& child : EnabledChildren_) {
        startIndex = child->EnumerateNodes(startIndex);
    }
    return startIndex;
}

void TCompositeSchedulerElement::UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    YCHECK(!Cloned_);

    TSchedulerElement::UpdateTreeConfig(config);

    auto updateChildrenConfig = [&config] (TChildList& list) {
        for (const auto& child : list) {
            child->UpdateTreeConfig(config);
        }
    };

    updateChildrenConfig(EnabledChildren_);
    updateChildrenConfig(DisabledChildren_);
}

void TCompositeSchedulerElement::UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    Attributes_.BestAllocationRatio = 0.0;
    PendingJobCount_ = 0;
    ResourceDemand_ = ZeroJobResources();
    auto maxPossibleChildrenResourceUsage_ = ZeroJobResources();
    for (const auto& child : EnabledChildren_) {
        child->UpdateBottomUp(dynamicAttributesList);

        Attributes_.BestAllocationRatio = std::max(
            Attributes_.BestAllocationRatio,
            child->Attributes().BestAllocationRatio);

        PendingJobCount_ += child->GetPendingJobCount();
        ResourceDemand_ += child->ResourceDemand();
        maxPossibleChildrenResourceUsage_ += child->MaxPossibleResourceUsage();
    }
    MaxPossibleResourceUsage_ = Min(maxPossibleChildrenResourceUsage_, ResourceLimits_);
    TSchedulerElement::UpdateBottomUp(dynamicAttributesList);
}

void TCompositeSchedulerElement::UpdateTopDown(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    switch (Mode_) {
        case ESchedulingMode::Fifo:
            // Easy case -- the first child get everything, others get none.
            UpdateFifo(dynamicAttributesList);
            break;

        case ESchedulingMode::FairShare:
            // Hard case -- compute fair shares using fit factor.
            UpdateFairShare(dynamicAttributesList);
            break;

        default:
            Y_UNREACHABLE();
    }

    UpdatePreemptionSettingsLimits();

    // Propagate updates to children.
    for (const auto& child : EnabledChildren_) {
        UpdateChildPreemptionSettings(child);
        child->UpdateTopDown(dynamicAttributesList);
    }
}

TJobResources TCompositeSchedulerElement::ComputePossibleResourceUsage(TJobResources limit) const
{
    if (!TreeConfig_->EnableNewPossibleResourceUsageComputation) {
        limit = Min(limit, MaxPossibleResourceUsage() - GetResourceUsage());
    }

    auto additionalUsage = ZeroJobResources();

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
    YCHECK(!Cloned_);

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
    YCHECK(!Cloned_);

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

void TCompositeSchedulerElement::UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(IsActive(dynamicAttributesList));
    auto& attributes = dynamicAttributesList[GetTreeIndex()];

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

    while (auto bestChild = GetBestActiveChild(dynamicAttributesList)) {
        const auto& bestChildAttributes = dynamicAttributesList[bestChild->GetTreeIndex()];
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

void TCompositeSchedulerElement::BuildOperationToElementMapping(TOperationElementByIdMap* operationElementByIdMap)
{
    for (const auto& child : EnabledChildren_) {
        child->BuildOperationToElementMapping(operationElementByIdMap);
    }
}

void TCompositeSchedulerElement::IncreaseOperationCount(int delta)
{
    OperationCount_ += delta;

    auto parent = GetParent();
    while (parent) {
        parent->OperationCount() += delta;
        parent = parent->GetParent();
    }
}

void TCompositeSchedulerElement::IncreaseRunningOperationCount(int delta)
{
    RunningOperationCount_ += delta;

    auto parent = GetParent();
    while (parent) {
        parent->RunningOperationCount() += delta;
        parent = parent->GetParent();
    }
}

void TCompositeSchedulerElement::PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled)
{
    auto& attributes = context->DynamicAttributes(this);

    attributes.Active = true;

    if (!IsAlive()) {
        ++context->DeactivationReasons[EDeactivationReason::IsNotAlive];
        attributes.Active = false;
        return;
    }

    if (TreeConfig_->EnableSchedulingTags &&
        SchedulingTagFilterIndex_ != EmptySchedulingTagFilterIndex &&
        !context->CanSchedule[SchedulingTagFilterIndex_])
    {
        ++context->DeactivationReasons[EDeactivationReason::UnmatchedSchedulingTag];
        attributes.Active = false;
        return;
    }

    aggressiveStarvationEnabled = aggressiveStarvationEnabled || IsAggressiveStarvationEnabled();
    if (Starving_ && aggressiveStarvationEnabled) {
        context->SchedulingStatistics.HasAggressivelyStarvingNodes = true;
    }

    // If pool is starving, any child will do.
    bool starvingOnlyChildren = Starving_ ? false : starvingOnly;
    for (const auto& child : EnabledChildren_) {
        child->PrescheduleJob(context, starvingOnlyChildren, aggressiveStarvationEnabled);
    }

    TSchedulerElement::PrescheduleJob(context, starvingOnly, aggressiveStarvationEnabled);

    if (attributes.Active) {
        ++context->ActiveTreeSize;
    }
}

bool TCompositeSchedulerElement::HasAggressivelyStarvingNodes(TFairShareContext* context, bool aggressiveStarvationEnabled) const
{
    // TODO(ignat): eliminate copy/paste
    aggressiveStarvationEnabled = aggressiveStarvationEnabled || IsAggressiveStarvationEnabled();
    if (Starving_ && aggressiveStarvationEnabled) {
        return true;
    }

    for (const auto& child : EnabledChildren_) {
        if (child->HasAggressivelyStarvingNodes(context, aggressiveStarvationEnabled)) {
            return true;
        }
    }

    return false;
}

bool TCompositeSchedulerElement::ScheduleJob(TFairShareContext* context)
{
    auto& attributes = context->DynamicAttributes(this);
    if (!attributes.Active) {
        return false;
    }

    auto bestLeafDescendant = attributes.BestLeafDescendant;
    if (!bestLeafDescendant->IsAlive()) {
        UpdateDynamicAttributes(context->DynamicAttributesList);
        if (!attributes.Active) {
            return false;
        }
        bestLeafDescendant = attributes.BestLeafDescendant;
    }

    // NB: Ignore the child's result.
    bestLeafDescendant->ScheduleJob(context);
    return true;
}

void TCompositeSchedulerElement::IncreaseResourceUsage(const TJobResources& delta)
{
    auto* currentElement = this;
    while (currentElement) {
        currentElement->IncreaseLocalResourceUsage(delta);
        currentElement = currentElement->GetParent();
    }
}

void TCompositeSchedulerElement::IncreaseResourceUsagePrecommit(const TJobResources& delta)
{
    auto* currentElement = this;
    while (currentElement) {
        currentElement->IncreaseLocalResourceUsagePrecommit(delta);
        currentElement = currentElement->GetParent();
    }
}

void TCompositeSchedulerElement::ApplyJobMetricsDelta(const TJobMetrics& delta)
{
    auto* currentElement = this;
    while (currentElement) {
        currentElement->ApplyJobMetricsDeltaLocal(delta);
        currentElement = currentElement->GetParent();
    }
}

bool TCompositeSchedulerElement::IsRoot() const
{
    return false;
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

void TCompositeSchedulerElement::AddChild(const TSchedulerElementPtr& child, bool enabled)
{
    YCHECK(!Cloned_);

    auto& map = enabled ? EnabledChildToIndex_ : DisabledChildToIndex_;
    auto& list = enabled ? EnabledChildren_ : DisabledChildren_;
    AddChild(&map, &list, child);
}

void TCompositeSchedulerElement::EnableChild(const TSchedulerElementPtr& child)
{
    YCHECK(!Cloned_);

    RemoveChild(&DisabledChildToIndex_, &DisabledChildren_, child);
    AddChild(&EnabledChildToIndex_, &EnabledChildren_, child);
}

void TCompositeSchedulerElement::DisableChild(const TSchedulerElementPtr& child)
{
    YCHECK(!Cloned_);

    if (EnabledChildToIndex_.find(child) == EnabledChildToIndex_.end()) {
        return;
    }

    RemoveChild(&EnabledChildToIndex_, &EnabledChildren_, child);
    AddChild(&DisabledChildToIndex_, &DisabledChildren_, child);
}

void TCompositeSchedulerElement::RemoveChild(const TSchedulerElementPtr& child)
{
    YCHECK(!Cloned_);

    bool enabled = ContainsChild(EnabledChildToIndex_, child);
    auto& map = enabled ? EnabledChildToIndex_ : DisabledChildToIndex_;
    auto& list = enabled ? EnabledChildren_ : DisabledChildren_;
    RemoveChild(&map, &list, child);
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
    double uncertantyRatio = 1.0;
    if (resultSum > RatioComputationPrecision && std::abs(sum - resultSum) > RatioComputationPrecision) {
        uncertantyRatio = sum / resultSum;
    }

    // Compute actual min shares from fit factor.
    for (const auto& child : EnabledChildren_) {
        double value = getter(fitFactor, child);
        setter(child, value, uncertantyRatio);
    }
}

void TCompositeSchedulerElement::UpdateFifo(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    auto children = EnabledChildren_;
    std::sort(children.begin(), children.end(), BIND(&TCompositeSchedulerElement::HasHigherPriorityInFifoMode, MakeStrong(this)));

    double remainingFairShareRatio = Attributes_.FairShareRatio;

    int index = 0;
    for (const auto& child : children) {
        auto& childAttributes = child->Attributes();

        childAttributes.AdjustedMinShareRatio = 0.0;

        childAttributes.FifoIndex = index;
        ++index;

        double childFairShareRatio = remainingFairShareRatio;
        childFairShareRatio = std::min(childFairShareRatio, childAttributes.MaxPossibleUsageRatio);
        childFairShareRatio = std::min(childFairShareRatio, childAttributes.BestAllocationRatio);
        child->SetFairShareRatio(childFairShareRatio);
        remainingFairShareRatio -= childFairShareRatio;
    }
}

void TCompositeSchedulerElement::UpdateFairShare(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    UpdateFairShareAlerts_.clear();

    // Compute min shares sum and min weight.
    double minShareRatioSum = 0.0;
    double minWeight = 1.0;
    for (const auto& child : EnabledChildren_) {
        auto& childAttributes = child->Attributes();
        auto minShareRatio = child->GetMinShareRatio();
        minShareRatioSum += minShareRatio;
        childAttributes.RecursiveMinShareRatio = Attributes_.RecursiveMinShareRatio * minShareRatio;

        if (minShareRatio > 0 && Attributes_.RecursiveMinShareRatio == 0) {
            UpdateFairShareAlerts_.emplace_back(
                "Min share ratio setting for %Qv has no effect "
                "because min share ratio of parent pool %Qv is zero",
                child->GetId(),
                GetId());
        }

        if (child->GetWeight() > RatioComputationPrecision) {
            minWeight = std::min(minWeight, child->GetWeight());
        }
    }

    // If min share sum is larger than one, adjust all children min shares to sum up to one.
    if (minShareRatioSum > 1.0 + RatioComparisonPrecision) {
        UpdateFairShareAlerts_.emplace_back(
            "Total min share ratio of children of %Qv is too large: %v > 1",
            GetId(),
            minShareRatioSum);

        double fitFactor = 1.0 / minShareRatioSum;
        for (const auto& child : EnabledChildren_) {
            auto& childAttributes = child->Attributes();
            childAttributes.RecursiveMinShareRatio *= fitFactor;
        }
    }

    minShareRatioSum = 0.0;
    for (const auto& child : EnabledChildren_) {
        auto& childAttributes = child->Attributes();
        childAttributes.AdjustedMinShareRatio = std::max(
            childAttributes.RecursiveMinShareRatio,
            GetMaxResourceRatio(child->GetMinShareResources(), TotalResourceLimits_));
        minShareRatioSum += childAttributes.AdjustedMinShareRatio;
    }

    if (minShareRatioSum > Attributes_.GuaranteedResourcesRatio + RatioComparisonPrecision) {
        UpdateFairShareAlerts_.emplace_back(
            "Impossible to satisfy resources guarantees for children of %Qv, "
            "given out resources share is greater than guaranteed resources share: %v > %v",
            GetId(),
            minShareRatioSum,
            Attributes_.GuaranteedResourcesRatio);

        double fitFactor = Attributes_.GuaranteedResourcesRatio / minShareRatioSum;
        for (const auto& child : EnabledChildren_) {
            auto& childAttributes = child->Attributes();
            childAttributes.AdjustedMinShareRatio *= fitFactor;
        }
    }

    // Compute fair shares.
    ComputeByFitting(
        [&] (double fitFactor, const TSchedulerElementPtr& child) -> double {
            const auto& childAttributes = child->Attributes();
            double result = fitFactor * child->GetWeight() / minWeight;
            // Never give less than promised by min share.
            result = std::max(result, childAttributes.AdjustedMinShareRatio);
            // Never give more than can be used.
            result = std::min(result, childAttributes.MaxPossibleUsageRatio);
            // Never give more than we can allocate.
            result = std::min(result, childAttributes.BestAllocationRatio);
            return result;
        },
        [&] (const TSchedulerElementPtr& child, double value, double uncertantyRatio) {
            if (IsRoot() && uncertantyRatio > 1.0) {
                uncertantyRatio = 1.0;
            }
            child->SetFairShareRatio(value * uncertantyRatio);
        },
        Attributes_.FairShareRatio);


    // Compute guaranteed shares.
    ComputeByFitting(
        [&] (double fitFactor, const TSchedulerElementPtr& child) -> double {
            const auto& childAttributes = child->Attributes();
            double result = fitFactor * child->GetWeight() / minWeight;
            // Never give less than promised by min share.
            result = std::max(result, childAttributes.AdjustedMinShareRatio);
            return result;
        },
        [&] (const TSchedulerElementPtr& child, double value, double uncertantyRatio) {
            auto& attributes = child->Attributes();
            attributes.GuaranteedResourcesRatio = value * uncertantyRatio;
        },
        Attributes_.GuaranteedResourcesRatio);

    // Trim adjusted min share ratio with demand ratio.
    for (const auto& child : EnabledChildren_) {
        auto& childAttributes = child->Attributes();
        double result = childAttributes.AdjustedMinShareRatio;
        // Never give more than can be used.
        result = std::min(result, childAttributes.MaxPossibleUsageRatio);
        // Never give more than we can allocate.
        result = std::min(result, childAttributes.BestAllocationRatio);
        childAttributes.AdjustedMinShareRatio = result;
    }
}

TSchedulerElementPtr TCompositeSchedulerElement::GetBestActiveChild(const TDynamicAttributesList& dynamicAttributesList) const
{
    switch (Mode_) {
        case ESchedulingMode::Fifo:
            return GetBestActiveChildFifo(dynamicAttributesList);
        case ESchedulingMode::FairShare:
            return GetBestActiveChildFairShare(dynamicAttributesList);
        default:
            Y_UNREACHABLE();
    }
}

TSchedulerElementPtr TCompositeSchedulerElement::GetBestActiveChildFifo(const TDynamicAttributesList& dynamicAttributesList) const
{
    TSchedulerElement* bestChild = nullptr;
    for (const auto& child : EnabledChildren_) {
        if (child->IsActive(dynamicAttributesList)) {
            if (bestChild && HasHigherPriorityInFifoMode(bestChild, child)) {
                continue;
            }

            bestChild = child.Get();
        }
    }
    return bestChild;
}

TSchedulerElementPtr TCompositeSchedulerElement::GetBestActiveChildFairShare(const TDynamicAttributesList& dynamicAttributesList) const
{
    TSchedulerElement* bestChild = nullptr;
    double bestChildSatisfactionRatio = std::numeric_limits<double>::max();
    for (const auto& child : EnabledChildren_) {
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
    YCHECK(map->emplace(child, list->size() - 1).second);
}

void TCompositeSchedulerElement::RemoveChild(
    TChildMap* map,
    TChildList* list,
    const TSchedulerElementPtr& child)
{
    auto it = map->find(child);
    YCHECK(it != map->end());
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

bool TCompositeSchedulerElement::HasHigherPriorityInFifoMode(const TSchedulerElementPtr& lhs, const TSchedulerElementPtr& rhs) const
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
                Y_UNREACHABLE();
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TPoolFixedState::TPoolFixedState(const TString& id)
    : Id_(id)
{ }

////////////////////////////////////////////////////////////////////////////////

TPool::TPool(
    ISchedulerStrategyHost* host,
    const TString& id,
    TPoolConfigPtr config,
    bool defaultConfigured,
    TFairShareStrategyTreeConfigPtr treeConfig,
    NProfiling::TTagId profilingTag,
    const TString& treeId)
    : TCompositeSchedulerElement(host, treeConfig, profilingTag, treeId)
    , TPoolFixedState(id)
{
    DoSetConfig(config);
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

void TPool::SetUserName(const TNullable<TString>& userName)
{
    UserName_ = userName;
}

const TNullable<TString>& TPool::GetUserName() const
{
    return UserName_;
}

TPoolConfigPtr TPool::GetConfig()
{
    return Config_;
}

void TPool::SetConfig(TPoolConfigPtr config)
{
    YCHECK(!Cloned_);

    DoSetConfig(config);
    DefaultConfigured_ = false;
}

void TPool::SetDefaultConfig()
{
    YCHECK(!Cloned_);

    DoSetConfig(New<TPoolConfig>());
    DefaultConfigured_ = true;
}

bool TPool::IsAggressiveStarvationPreemptionAllowed() const
{
    return Config_->AllowAggressiveStarvationPreemption.Get(true);
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

double TPool::GetWeight() const
{
    return Config_->Weight.Get(1.0);
}

double TPool::GetMinShareRatio() const
{
    return Config_->MinShareRatio.Get(0.0);
}

TJobResources TPool::GetMinShareResources() const
{
    return ToJobResources(Config_->MinShareResources, ZeroJobResources());
}

double TPool::GetMaxShareRatio() const
{
    return Config_->MaxShareRatio.Get(1.0);
}

ESchedulableStatus TPool::GetStatus() const
{
    return TSchedulerElement::GetStatus(Attributes_.AdjustedFairShareStarvationTolerance);
}

double TPool::GetFairShareStarvationTolerance() const
{
    return Config_->FairShareStarvationTolerance.Get(Parent_->Attributes().AdjustedFairShareStarvationTolerance);
}

TDuration TPool::GetMinSharePreemptionTimeout() const
{
    return Config_->MinSharePreemptionTimeout.Get(Parent_->Attributes().AdjustedMinSharePreemptionTimeout);
}

TDuration TPool::GetFairSharePreemptionTimeout() const
{
    return Config_->FairSharePreemptionTimeout.Get(Parent_->Attributes().AdjustedFairSharePreemptionTimeout);
}

double TPool::GetFairShareStarvationToleranceLimit() const
{
    return Config_->FairShareStarvationToleranceLimit.Get(TreeConfig_->FairShareStarvationToleranceLimit);
}

TDuration TPool::GetMinSharePreemptionTimeoutLimit() const
{
    return Config_->MinSharePreemptionTimeoutLimit.Get(TreeConfig_->MinSharePreemptionTimeoutLimit);
}

TDuration TPool::GetFairSharePreemptionTimeoutLimit() const
{
    return Config_->FairSharePreemptionTimeoutLimit.Get(TreeConfig_->FairSharePreemptionTimeoutLimit);
}

void TPool::SetStarving(bool starving)
{
    YCHECK(!Cloned_);

    if (starving && !GetStarving()) {
        TSchedulerElement::SetStarving(true);
        LOG_INFO("Pool is now starving (TreeId: %v, PoolId: %v, Status: %v)",
            GetTreeId(),
            GetId(),
            GetStatus());
    } else if (!starving && GetStarving()) {
        TSchedulerElement::SetStarving(false);
        LOG_INFO("Pool is no longer starving (TreeId: %v, PoolId: %v)",
            GetTreeId(),
            GetId());
    }
}

void TPool::CheckForStarvation(TInstant now)
{
    YCHECK(!Cloned_);

    TSchedulerElement::CheckForStarvationImpl(
        Attributes_.AdjustedMinSharePreemptionTimeout,
        Attributes_.AdjustedFairSharePreemptionTimeout,
        now);
}

const TSchedulingTagFilter& TPool::GetSchedulingTagFilter() const
{
    return SchedulingTagFilter_;
}

void TPool::UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    ResourceLimits_ = ComputeResourceLimits();
    TCompositeSchedulerElement::UpdateBottomUp(dynamicAttributesList);
}

int TPool::GetMaxRunningOperationCount() const
{
    return Config_->MaxRunningOperationCount.Get(TreeConfig_->MaxRunningOperationCountPerPool);
}

int TPool::GetMaxOperationCount() const
{
    return Config_->MaxOperationCount.Get(TreeConfig_->MaxOperationCountPerPool);
}

std::vector<EFifoSortParameter> TPool::GetFifoSortParameters() const
{
    return FifoSortParameters_;
}

bool TPool::AreImmediateOperationsForbidden() const
{
    return Config_->ForbidImmediateOperations;
}

TSchedulerElementPtr TPool::Clone(TCompositeSchedulerElement* clonedParent)
{
    return New<TPool>(*this, clonedParent);
}

void TPool::DoSetConfig(TPoolConfigPtr newConfig)
{
    YCHECK(!Cloned_);

    Config_ = newConfig;
    FifoSortParameters_ = Config_->FifoSortParameters;
    Mode_ = Config_->Mode;
    SchedulingTagFilter_ = TSchedulingTagFilter(Config_->SchedulingTagFilter);
}

TJobResources TPool::ComputeResourceLimits() const
{
    auto maxShareLimits = Host_->GetConnectionTime() + TreeConfig_->TotalResourceLimitsConsiderDelay < TInstant::Now()
        ? GetHost()->GetResourceLimits(GetSchedulingTagFilter()) * GetMaxShareRatio()
        : InfiniteJobResources();
    auto perTypeLimits = ToJobResources(Config_->ResourceLimits, InfiniteJobResources());
    return Min(maxShareLimits, perTypeLimits);
}

////////////////////////////////////////////////////////////////////////////////

TOperationElementFixedState::TOperationElementFixedState(
    IOperationStrategyHost* operation,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig)
    : OperationId_(operation->GetId())
    , Schedulable_(operation->IsSchedulable())
    , Operation_(operation)
    , ControllerConfig_(std::move(controllerConfig))
{ }

////////////////////////////////////////////////////////////////////////////////

TJobResources TOperationElementSharedState::Disable()
{
    TWriterGuard guard(JobPropertiesMapLock_);

    Enabled_ = false;

    auto resourceUsage = ZeroJobResources();
    for (const auto& pair : JobPropertiesMap_) {
        resourceUsage += pair.second.ResourceUsage;
    }

    PreemptableJobs_.clear();
    AggressivelyPreemptableJobs_.clear();
    NonpreemptableJobs_.clear();
    JobPropertiesMap_.clear();

    return resourceUsage;
}

void TOperationElementSharedState::Enable()
{
    TWriterGuard guard(JobPropertiesMapLock_);

    YCHECK(!Enabled_);
    Enabled_ = true;
}

TJobResources TOperationElementSharedState::IncreaseJobResourceUsage(
    const TJobId& jobId,
    const TJobResources& resourcesDelta)
{
    TWriterGuard guard(JobPropertiesMapLock_);

    if (!Enabled_) {
        return ZeroJobResources();
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

    // NB: We need 2 iterations since thresholds may change significantly such that we need
    // to move job from preemptable list to non-preemptable list through aggressively preemptable list.
    for (int iteration = 0; iteration < 2; ++iteration) {
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
            fairShareRatio * preemptionSatisfactionThreshold,
            setPreemptable,
            setAggressivelyPreemptable);

        AggressivelyPreemptableResourceUsage_ = nonpreemptableAndAggressivelyPreemptableResourceUsage_ - NonpreemptableResourceUsage_;
    }
}

bool TOperationElementSharedState::IsJobKnown(const TJobId& jobId) const
{
    TReaderGuard guard(JobPropertiesMapLock_);

    return JobPropertiesMap_.find(jobId) != JobPropertiesMap_.end();
}

bool TOperationElementSharedState::IsJobPreemptable(const TJobId& jobId, bool aggressivePreemptionEnabled) const
{
    TReaderGuard guard(JobPropertiesMapLock_);

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

TJobResources TOperationElementSharedState::AddJob(const TJobId& jobId, const TJobResources& resourceUsage, bool force)
{
    TWriterGuard guard(JobPropertiesMapLock_);

    if (!Enabled_ && !force) {
        return ZeroJobResources();
    }

    PreemptableJobs_.push_back(jobId);

    auto it = JobPropertiesMap_.insert(std::make_pair(
        jobId,
        TJobProperties(
            /* preemptable */ true,
            /* aggressivelyPreemptable */ true,
            --PreemptableJobs_.end(),
            ZeroJobResources())));
    YCHECK(it.second);

    ++RunningJobCount_;

    IncreaseJobResourceUsage(&it.first->second, resourceUsage);
    return resourceUsage;
}

void TOperationElement::Disable()
{
    LOG_DEBUG("Operation element disabled in strategy (OperationId: %v)", OperationId_);

    auto delta = SharedState_->Disable();
    IncreaseLocalResourceUsage(-delta);
}

void TOperationElement::Enable()
{
    LOG_DEBUG("Operation element enabled in strategy (OperationId: %v)", OperationId_);

    return SharedState_->Enable();
}

TJobResources TOperationElementSharedState::RemoveJob(const TJobId& jobId)
{
    TWriterGuard guard(JobPropertiesMapLock_);

    if (!Enabled_) {
        return ZeroJobResources();
    }

    auto it = JobPropertiesMap_.find(jobId);
    YCHECK(it != JobPropertiesMap_.end());

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

bool TOperationElement::TryStartScheduleJob(
    NProfiling::TCpuInstant now,
    const TJobResources& jobLimits,
    const TJobResources& minNeededResources)
{
    auto blocked = Controller_->IsBlocked(
        now,
        Spec_->MaxConcurrentControllerScheduleJobCalls.Get(ControllerConfig_->MaxConcurrentControllerScheduleJobCalls),
        ControllerConfig_->ScheduleJobFailBackoffTime);
    if (blocked) {
        return false;
    }

    if (!Dominates(jobLimits, minNeededResources)) {
        return false;
    }

    IncreaseResourceUsagePrecommit(minNeededResources);

    Controller_->IncreaseConcurrentScheduleJobCalls();

    return true;
}

void TOperationElement::FinishScheduleJob(
    bool enableBackoff,
    NProfiling::TCpuInstant now,
    const TJobResources& minNeededResources)
{
    Controller_->DecreaseConcurrentScheduleJobCalls();

    if (enableBackoff) {
        Controller_->SetLastScheduleJobFailTime(now);
    }

    IncreaseResourceUsagePrecommit(-minNeededResources);
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

TOperationElementSharedState::TJobProperties* TOperationElementSharedState::GetJobProperties(const TJobId& jobId)
{
    auto it = JobPropertiesMap_.find(jobId);
    Y_ASSERT(it != JobPropertiesMap_.end());
    return &it->second;
}

const TOperationElementSharedState::TJobProperties* TOperationElementSharedState::GetJobProperties(const TJobId& jobId) const
{
    auto it = JobPropertiesMap_.find(jobId);
    Y_ASSERT(it != JobPropertiesMap_.end());
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

TOperationElement::TOperationElement(
    TFairShareStrategyTreeConfigPtr treeConfig,
    TStrategyOperationSpecPtr spec,
    TOperationFairShareTreeRuntimeParametersPtr runtimeParams,
    TFairShareStrategyOperationControllerPtr controller,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    ISchedulerStrategyHost* host,
    IOperationStrategyHost* operation,
    const TString& treeId)
    : TSchedulerElement(host, treeConfig, treeId)
    , TOperationElementFixedState(operation, controllerConfig)
    , RuntimeParams_(runtimeParams)
    , Spec_(spec)
    , SchedulingTagFilter_(spec->SchedulingTagFilter)
    , SharedState_(New<TOperationElementSharedState>())
    , Controller_(controller)
{ }

TOperationElement::TOperationElement(
    const TOperationElement& other,
    TCompositeSchedulerElement* clonedParent)
    : TSchedulerElement(other, clonedParent)
    , TOperationElementFixedState(other)
    , RuntimeParams_(other.RuntimeParams_)
    , Spec_(other.Spec_)
    , SchedulingTagFilter_(other.SchedulingTagFilter_)
    , SharedState_(other.SharedState_)
    , Controller_(other.Controller_)
{ }

double TOperationElement::GetFairShareStarvationTolerance() const
{
    return Spec_->FairShareStarvationTolerance.Get(Parent_->Attributes().AdjustedFairShareStarvationTolerance);
}

TDuration TOperationElement::GetMinSharePreemptionTimeout() const
{
    return Spec_->MinSharePreemptionTimeout.Get(Parent_->Attributes().AdjustedMinSharePreemptionTimeout);
}

TDuration TOperationElement::GetFairSharePreemptionTimeout() const
{
    return Spec_->FairSharePreemptionTimeout.Get(Parent_->Attributes().AdjustedFairSharePreemptionTimeout);
}

void TOperationElement::UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    Schedulable_ = Operation_->IsSchedulable();
    ResourceDemand_ = ComputeResourceDemand();
    ResourceLimits_ = ComputeResourceLimits();
    MaxPossibleResourceUsage_ = ComputeMaxPossibleResourceUsage();
    PendingJobCount_ = ComputePendingJobCount();
    StartTime_ = Operation_->GetStartTime();

    // It should be called after update of ResourceDemand_ and MaxPossibleResourceUsage_ since
    // these fields are used to calculate dominant resource.
    TSchedulerElement::UpdateBottomUp(dynamicAttributesList);

    auto allocationLimits = GetAdjustedResourceLimits(
        ResourceDemand_,
        TotalResourceLimits_,
        GetHost()->GetExecNodeMemoryDistribution(SchedulingTagFilter_));

    auto dominantLimit = GetResource(TotalResourceLimits_, Attributes_.DominantResource);
    auto dominantAllocationLimit = GetResource(allocationLimits, Attributes_.DominantResource);

    Attributes_.BestAllocationRatio =
        dominantLimit == 0 ? 1.0 : dominantAllocationLimit / dominantLimit;
}

void TOperationElement::UpdateTopDown(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    TSchedulerElement::UpdateTopDown(dynamicAttributesList);

    UpdatePreemptableJobsList();
}

TJobResources TOperationElement::ComputePossibleResourceUsage(TJobResources limit) const
{
    auto usage = GetResourceUsage();
    if (TreeConfig_->EnableNewPossibleResourceUsageComputation) {
        if (!Dominates(limit, usage)) {
            return usage * GetMinResourceRatio(limit, usage);
        } else {
            auto remainingDemand = ResourceDemand() - usage;
            if (remainingDemand == ZeroJobResources()) {
                return usage;
            }

            auto remainingLimit = Max(ZeroJobResources(), limit - usage);
            // TODO(asaitgalin): Move this to MaxPossibleResourceUsage computation.
            return Min(ResourceDemand(), usage + remainingDemand * GetMinResourceRatio(remainingLimit, remainingDemand));
        }
    } else {
        // Max possible resource usage can be less than usage just after scheduler connection
        // when not all nodes come with heartbeat to the scheduler.
        limit = Max(ZeroJobResources(), Min(limit, MaxPossibleResourceUsage() - usage));
        if (usage == ZeroJobResources()) {
            return limit;
        } else {
            return usage * GetMinResourceRatio(limit, usage);
        }
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

void TOperationElement::UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList)
{
    auto& attributes = dynamicAttributesList[GetTreeIndex()];
    attributes.Active = true;
    attributes.BestLeafDescendant = this;

    TSchedulerElement::UpdateDynamicAttributes(dynamicAttributesList);
}

void TOperationElement::UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config)
{
    YCHECK(!Cloned_);
    ControllerConfig_ = config;
}

void TOperationElement::PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled)
{
    auto& attributes = context->DynamicAttributes(this);

    attributes.Active = true;

    if (!IsAlive()) {
        ++context->DeactivationReasons[EDeactivationReason::IsNotAlive];
        attributes.Active = false;
        return;
    }

    if (TreeConfig_->EnableSchedulingTags &&
        SchedulingTagFilterIndex_ != EmptySchedulingTagFilterIndex &&
        !context->CanSchedule[SchedulingTagFilterIndex_])
    {
        ++context->DeactivationReasons[EDeactivationReason::UnmatchedSchedulingTag];
        attributes.Active = false;
        return;
    }

    if (starvingOnly && !Starving_) {
        ++context->DeactivationReasons[EDeactivationReason::IsNotStarving];
        attributes.Active = false;
        return;
    }

    if (IsBlocked(context->SchedulingContext->GetNow())) {
        ++context->DeactivationReasons[EDeactivationReason::IsBlocked];
        attributes.Active = false;
        return;
    }

    ++context->ActiveTreeSize;
    ++context->ActiveOperationCount;

    TSchedulerElement::PrescheduleJob(context, starvingOnly, aggressiveStarvationEnabled);
}

bool TOperationElement::HasAggressivelyStarvingNodes(TFairShareContext* context, bool aggressiveStarvationEnabled) const
{
    // TODO(ignat): Support aggressive starvation by starving operation.
    return false;
}

bool TOperationElement::ScheduleJob(TFairShareContext* context)
{
    YCHECK(IsActive(context->DynamicAttributesList));

    auto updateAncestorsAttributes = [&] () {
        auto* parent = GetParent();
        while (parent) {
            parent->UpdateDynamicAttributes(context->DynamicAttributesList);
            if (!context->DynamicAttributesList[parent->GetTreeIndex()].Active) {
                ++context->DeactivationReasons[EDeactivationReason::NoBestLeafDescendant];
            }
            parent = parent->GetParent();
        }
    };

    auto disableOperationElement = [&] (EDeactivationReason reason) {
        ++context->DeactivationReasons[reason];
        context->DynamicAttributes(this).Active = false;
        updateAncestorsAttributes();
    };

    auto now = context->SchedulingContext->GetNow();
    if (IsBlocked(now)) {
        disableOperationElement(EDeactivationReason::IsBlocked);
        return false;
    }

    if (!HasJobsSatisfyingResourceLimits(*context)) {
        LOG_TRACE(
            "No pending jobs can satisfy available resources on node "
            "(TreeId: %v, OperationId: %v, FreeResources: %v, DiscountResources: %v)",
            GetTreeId(),
            OperationId_,
            FormatResources(context->SchedulingContext->GetFreeResources()),
            FormatResources(context->SchedulingContext->ResourceUsageDiscount()));
        disableOperationElement(EDeactivationReason::MinNeededResourcesUnsatisfied);
        return false;
    }

    auto jobLimits = GetHierarchicalResourceLimits(*context);
    auto minNeededResources = Controller_->GetAggregatedMinNeededJobResources();
    if (!TryStartScheduleJob(
        now,
        jobLimits,
        minNeededResources))
    {
        disableOperationElement(EDeactivationReason::TryStartScheduleJobFailed);
        return false;
    }

    NProfiling::TWallTimer timer;
    auto scheduleJobResult = DoScheduleJob(context, jobLimits, minNeededResources);
    auto scheduleJobDuration = timer.GetElapsedTime();
    context->TotalScheduleJobDuration += scheduleJobDuration;
    context->ExecScheduleJobDuration += scheduleJobResult->Duration;

    for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
        context->FailedScheduleJob[reason] += scheduleJobResult->Failed[reason];
    }

    if (!scheduleJobResult->StartDescriptor) {
        ++context->ScheduleJobFailureCount;
        disableOperationElement(EDeactivationReason::ScheduleJobFailed);

        bool enableBackoff = scheduleJobResult->IsBackoffNeeded();
        LOG_DEBUG_IF(enableBackoff, "Failed to schedule job, backing off (TreeId: %v, OperationId: %v, Reasons: %v)",
            GetTreeId(),
            OperationId_,
            scheduleJobResult->Failed);

        FinishScheduleJob(/*enableBackoff*/ enableBackoff, now, minNeededResources);
        return false;
    }

    const auto& startDescriptor = *scheduleJobResult->StartDescriptor;
    context->SchedulingContext->ResourceUsage() += startDescriptor.ResourceLimits;
    OnJobStarted(startDescriptor.Id, startDescriptor.ResourceLimits);
    context->SchedulingContext->StartJob(
        GetTreeId(),
        OperationId_,
        scheduleJobResult->IncarnationId,
        startDescriptor);

    UpdateDynamicAttributes(context->DynamicAttributesList);
    updateAncestorsAttributes();

    FinishScheduleJob(/*enableBackoff*/ false, now, minNeededResources);
    return true;
}

TString TOperationElement::GetId() const
{
    return ToString(OperationId_);
}

bool TOperationElement::IsAggressiveStarvationPreemptionAllowed() const
{
    return Spec_->AllowAggressiveStarvationPreemption &&
        GetParent()->IsAggressiveStarvationPreemptionAllowed();
}

double TOperationElement::GetWeight() const
{
    return RuntimeParams_->Weight.Get(1.0);
}

double TOperationElement::GetMinShareRatio() const
{
    return Spec_->MinShareRatio.Get(0.0);
}

TJobResources TOperationElement::GetMinShareResources() const
{
    return ToJobResources(Spec_->MinShareResources, ZeroJobResources());
}

double TOperationElement::GetMaxShareRatio() const
{
    return Spec_->MaxShareRatio.Get(1.0);
}

const TSchedulingTagFilter& TOperationElement::GetSchedulingTagFilter() const
{
    return SchedulingTagFilter_;
}

ESchedulableStatus TOperationElement::GetStatus() const
{
    if (!Schedulable_) {
        return ESchedulableStatus::Normal;
    }

    if (GetPendingJobCount() == 0) {
        return ESchedulableStatus::Normal;
    }

    return TSchedulerElement::GetStatus(Attributes_.AdjustedFairShareStarvationTolerance);
}

void TOperationElement::SetStarving(bool starving)
{
    YCHECK(!Cloned_);

    if (starving && !GetStarving()) {
        TSchedulerElement::SetStarving(true);
        LOG_INFO("Operation is now starving (TreeId: %v, OperationId: %v, Status: %v)",
            GetTreeId(),
            GetId(),
            GetStatus());
    } else if (!starving && GetStarving()) {
        TSchedulerElement::SetStarving(false);
        LOG_INFO("Operation is no longer starving (TreeId: %v, OperationId: %v)",
            GetTreeId(),
            GetId());
    }
}

void TOperationElement::CheckForStarvation(TInstant now)
{
    YCHECK(!Cloned_);

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

bool TOperationElement::IsPreemptionAllowed(const TFairShareContext& context) const
{
    auto* parent = GetParent();

    while (parent) {
        if (parent->GetStarving()) {
            return false;
        }

        if (context.DynamicAttributes(parent).SatisfactionRatio < (1.0 + RatioComputationPrecision) &&
            (!parent->IsAggressiveStarvationPreemptionAllowed() || !IsAggressiveStarvationPreemptionAllowed()))
        {
            return false;
        }

        parent = parent->GetParent();
    }

    return true;
}

void TOperationElement::IncreaseResourceUsage(const TJobResources& delta)
{
    IncreaseLocalResourceUsage(delta);
    GetParent()->IncreaseResourceUsage(delta);
}

void TOperationElement::IncreaseResourceUsagePrecommit(const TJobResources& delta)
{
    IncreaseLocalResourceUsagePrecommit(delta);
    GetParent()->IncreaseResourceUsagePrecommit(delta);
}

void TOperationElement::ApplyJobMetricsDelta(const TJobMetrics& delta)
{
    ApplyJobMetricsDeltaLocal(delta);
    GetParent()->ApplyJobMetricsDelta(delta);
}

void TOperationElement::IncreaseJobResourceUsage(const TJobId& jobId, const TJobResources& resourcesDelta)
{
    auto delta = SharedState_->IncreaseJobResourceUsage(jobId, resourcesDelta);
    IncreaseResourceUsage(delta);

    UpdatePreemptableJobsList();
}

bool TOperationElement::IsJobKnown(const TJobId& jobId) const
{
    return SharedState_->IsJobKnown(jobId);
}

bool TOperationElement::IsJobPreemptable(const TJobId& jobId, bool aggressivePreemptionEnabled) const
{
    return SharedState_->IsJobPreemptable(jobId, aggressivePreemptionEnabled);
}

int TOperationElement::GetRunningJobCount() const
{
    return SharedState_->GetRunningJobCount();
}

int TOperationElement::GetPreemptableJobCount() const
{
    return SharedState_->GetPreemptableJobCount();
}

int TOperationElement::GetAggressivelyPreemptableJobCount() const
{
    return SharedState_->GetAggressivelyPreemptableJobCount();
}

int TOperationElement::GetSlotIndex() const
{
    auto slotIndex = Operation_->FindSlotIndex(GetTreeId());
    YCHECK(slotIndex);
    return *slotIndex;
}

void TOperationElement::OnJobStarted(const TJobId& jobId, const TJobResources& resourceUsage, bool force)
{
    // XXX(ignat): remove before deploy on production clusters.
    LOG_DEBUG("Adding job to strategy (JobId: %v)", jobId);

    auto delta = SharedState_->AddJob(jobId, resourceUsage, force);
    IncreaseResourceUsage(delta);

    UpdatePreemptableJobsList();
}

void TOperationElement::OnJobFinished(const TJobId& jobId)
{
    // XXX(ignat): remove before deploy on production clusters.
    LOG_DEBUG("Removing job from strategy (JobId: %v)", jobId);

    auto delta = SharedState_->RemoveJob(jobId);
    IncreaseResourceUsage(-delta);

    UpdatePreemptableJobsList();
}

void TOperationElement::BuildOperationToElementMapping(TOperationElementByIdMap* operationElementByIdMap)
{
    operationElementByIdMap->emplace(OperationId_, this);
}

TSchedulerElementPtr TOperationElement::Clone(TCompositeSchedulerElement* clonedParent)
{
    return New<TOperationElement>(*this, clonedParent);
}

bool TOperationElement::IsSchedulable() const
{
    YCHECK(!Cloned_);

    return Schedulable_;
}

bool TOperationElement::IsBlocked(NProfiling::TCpuInstant now) const
{
    return
        !Schedulable_ ||
        GetPendingJobCount() == 0 ||
        Controller_->IsBlocked(
            now,
            Spec_->MaxConcurrentControllerScheduleJobCalls.Get(ControllerConfig_->MaxConcurrentControllerScheduleJobCalls),
            ControllerConfig_->ScheduleJobFailBackoffTime);
}

TJobResources TOperationElement::GetHierarchicalResourceLimits(const TFairShareContext& context) const
{
    const auto& schedulingContext = context.SchedulingContext;

    // Bound limits with node free resources.
    auto limits =
        schedulingContext->ResourceLimits()
        - schedulingContext->ResourceUsage()
        + schedulingContext->ResourceUsageDiscount();

    // Bound limits with pool free resources.
    const TSchedulerElement* parent = this;
    while (parent) {
        auto parentLimits =
            parent->ResourceLimits()
            - parent->GetResourceUsage()
            - parent->GetResourceUsagePrecommit()
            + context.DynamicAttributes(parent).ResourceUsageDiscount;

        limits = Min(limits, parentLimits);
        parent = parent->GetParent();
    }

    return limits;
}

TScheduleJobResultPtr TOperationElement::DoScheduleJob(
    TFairShareContext* context,
    const TJobResources& jobLimits,
    const TJobResources& jobResourceDiscount)
{
    ++context->SchedulingStatistics.ControllerScheduleJobCount;

    auto scheduleJobResult = Controller_->ScheduleJob(
        context->SchedulingContext,
        jobLimits,
        ControllerConfig_->ScheduleJobTimeLimit,
        GetTreeId());

    // Discard the job in case of resource overcommit.
    if (scheduleJobResult->StartDescriptor) {
        const auto& startDescriptor = *scheduleJobResult->StartDescriptor;
        auto jobLimits = GetHierarchicalResourceLimits(*context) + jobResourceDiscount;
        if (!Dominates(jobLimits, startDescriptor.ResourceLimits)) {
            const auto& jobId = scheduleJobResult->StartDescriptor->Id;
            LOG_DEBUG("Aborting job with resource overcommit: %v > %v (JobId: %v, OperationId: %v)",
                FormatResources(startDescriptor.ResourceLimits),
                FormatResources(jobLimits),
                jobId,
                OperationId_);

            Controller_->AbortJob(jobId, EAbortReason::SchedulingResourceOvercommit);

            // Reset result.
            scheduleJobResult = New<TScheduleJobResult>();
            scheduleJobResult->RecordFail(EScheduleJobFailReason::ResourceOvercommit);
        }
    } else if (scheduleJobResult->Failed[EScheduleJobFailReason::Timeout] > 0) {
        LOG_WARNING("Job scheduling timed out (OperationId: %v)",
            OperationId_);

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
    if (Operation_->IsSchedulable()) {
        return GetResourceUsage() + Controller_->GetNeededResources();
    }
    return ZeroJobResources();
}

TJobResources TOperationElement::ComputeResourceLimits() const
{
    auto maxShareLimits = Host_->GetConnectionTime() + TreeConfig_->TotalResourceLimitsConsiderDelay < TInstant::Now()
        ? GetHost()->GetResourceLimits(GetSchedulingTagFilter()) * GetMaxShareRatio()
        : InfiniteJobResources();
    auto perTypeLimits = ToJobResources(RuntimeParams_->ResourceLimits, InfiniteJobResources());
    return Min(maxShareLimits, perTypeLimits);
}

TJobResources TOperationElement::ComputeMaxPossibleResourceUsage() const
{
    return Min(ResourceLimits(), ResourceDemand());
}

int TOperationElement::ComputePendingJobCount() const
{
    return Controller_->GetPendingJobCount();
}

void TOperationElement::UpdatePreemptableJobsList()
{
    TWallTimer timer;
    int moveCount = 0;

    SharedState_->UpdatePreemptableJobsList(
        GetFairShareRatio(),
        TotalResourceLimits_,
        TreeConfig_->PreemptionSatisfactionThreshold,
        TreeConfig_->AggressivePreemptionSatisfactionThreshold,
        &moveCount);

    auto elapsed = timer.GetElapsedTime();

    Profiler.Update(GetCounter(GetTreeId(), "/preemptable_list_update_time"), DurationToValue(elapsed));
    Profiler.Update(GetCounter(GetTreeId(), "/preemptable_list_update_move_count"), moveCount);

    if (elapsed > TreeConfig_->UpdatePreemptableListDurationLoggingThreshold) {
        LOG_DEBUG("Preemptable list update is too long (Duration: %v, MoveCount: %v, OperationId: %v, TreeId: %v)",
            elapsed.MilliSeconds(),
            moveCount,
            OperationId_,
            GetTreeId());
    }
}

////////////////////////////////////////////////////////////////////////////////

TRootElement::TRootElement(
    ISchedulerStrategyHost* host,
    TFairShareStrategyTreeConfigPtr treeConfig,
    NProfiling::TTagId profilingTag,
    const TString& treeId)
    : TCompositeSchedulerElement(
        host,
        treeConfig,
        profilingTag,
        treeId)
{
    SetFairShareRatio(1.0);
    Attributes_.GuaranteedResourcesRatio = 1.0;
    Attributes_.AdjustedMinShareRatio = 1.0;
    Attributes_.RecursiveMinShareRatio = 1.0;
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

void TRootElement::Update(TDynamicAttributesList& dynamicAttributesList)
{
    YCHECK(!Cloned_);

    TreeSize_ = TCompositeSchedulerElement::EnumerateNodes(0);
    dynamicAttributesList.assign(TreeSize_, TDynamicAttributes());
    TCompositeSchedulerElement::Update(dynamicAttributesList);
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
    return TString(RootPoolName);
}

double TRootElement::GetWeight() const
{
    return 1.0;
}

double TRootElement::GetMinShareRatio() const
{
    return 1.0;
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

void TRootElement::CheckForStarvation(TInstant now)
{
    Y_UNREACHABLE();
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
    Y_UNREACHABLE();
}

bool TRootElement::AreImmediateOperationsForbidden() const
{
    return TreeConfig_->ForbidImmediateOperationsInRoot;
}

TSchedulerElementPtr TRootElement::Clone(TCompositeSchedulerElement* /*clonedParent*/)
{
    Y_UNREACHABLE();
}

TRootElementPtr TRootElement::Clone()
{
    return New<TRootElement>(*this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
