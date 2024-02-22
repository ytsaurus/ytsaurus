#include "fair_share_tree_element.h"

#include "fair_share_tree.h"
#include "helpers.h"
#include "resource_tree_element.h"
#include "scheduling_context.h"

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/digest.h>
#include <yt/yt/core/misc/string_builder.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/generic/ymath.h>

#include <math.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NYson;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;

using NProfiling::CpuDurationToDuration;
using NVectorHdrf::ToJobResources;

////////////////////////////////////////////////////////////////////////////////

void TPersistentAttributes::ResetOnElementEnabled()
{
    // NB: We don't want to reset all attributes.
    auto resetAttributes = TPersistentAttributes();
    resetAttributes.IntegralResourcesState = IntegralResourcesState;
    resetAttributes.LastNonStarvingTime = TInstant::Now();
    resetAttributes.AppliedSpecifiedResourceLimits = AppliedSpecifiedResourceLimits;
    *this = resetAttributes;
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerElementFixedState::TSchedulerElementFixedState(
    ISchedulerStrategyHost* strategyHost,
    IFairShareTreeElementHost* treeElementHost,
    TFairShareStrategyTreeConfigPtr treeConfig,
    TString treeId)
    : TreeConfig_(std::move(treeConfig))
    , StrategyHost_(strategyHost)
    , TreeElementHost_(treeElementHost)
    , TotalResourceLimits_(strategyHost->GetResourceLimits(TreeConfig_->NodesFilter))
    , TreeId_(std::move(treeId))
{ }

////////////////////////////////////////////////////////////////////////////////

void TSchedulerElement::MarkImmutable()
{
    Mutable_ = false;
}

int TSchedulerElement::EnumerateElements(int startIndex, bool isSchedulableValueFilter)
{
    YT_VERIFY(Mutable_);

    if (isSchedulableValueFilter == IsSchedulable()) {
        TreeIndex_ = startIndex++;
    }

    return startIndex;
}

void TSchedulerElement::UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    YT_VERIFY(Mutable_);

    TreeConfig_ = config;
}

void TSchedulerElement::PreUpdateBottomUp(NVectorHdrf::TFairShareUpdateContext* context)
{
    YT_VERIFY(Mutable_);

    // NB: The order of computation must not be changed.
    TotalResourceLimits_ = context->TotalResourceLimits;
    SchedulingTagFilterResourceLimits_ = ComputeSchedulingTagFilterResourceLimits();
    MaybeSpecifiedResourceLimits_ = ComputeMaybeSpecifiedResourceLimits();
    ResourceLimits_ = ComputeResourceLimits();

    if (PersistentAttributes_.AppliedSpecifiedResourceLimits != MaybeSpecifiedResourceLimits_) {
        std::vector<TResourceTreeElementPtr> descendantOperationElements;
        if (!IsOperation() && !PersistentAttributes_.AppliedSpecifiedResourceLimits && MaybeSpecifiedResourceLimits_) {
            // NB: This code executed in control thread, therefore tree structure is actual and agreed with tree structure of resource tree.
            CollectResourceTreeOperationElements(&descendantOperationElements);
        }

        YT_LOG_INFO("Updating applied specified resource limits (NewSpecifiedResourceLimits: %v, CurrentSpecifiedResourceLimits: %v)",
            MaybeSpecifiedResourceLimits_,
            PersistentAttributes_.AppliedSpecifiedResourceLimits);

        ResourceTreeElement_->SetSpecifiedResourceLimits(MaybeSpecifiedResourceLimits_, descendantOperationElements);
        PersistentAttributes_.AppliedSpecifiedResourceLimits = MaybeSpecifiedResourceLimits_;
    }
}

void TSchedulerElement::ComputeSatisfactionRatioAtUpdate()
{
    YT_VERIFY(Mutable_);

    PostUpdateAttributes_.LocalSatisfactionRatio = ComputeLocalSatisfactionRatio(ResourceUsageAtUpdate_);
    PostUpdateAttributes_.SatisfactionRatio = PostUpdateAttributes_.LocalSatisfactionRatio;
}

void TSchedulerElement::ResetSchedulableCounters()
{
    SchedulableElementCount_ = 0;
    SchedulablePoolCount_ = 0;
    SchedulableOperationCount_ = 0;
}

const TSchedulingTagFilter& TSchedulerElement::GetSchedulingTagFilter() const
{
    return EmptySchedulingTagFilter;
}

void TSchedulerElement::BuildLoggingStringAttributes(TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    delimitedBuilder->AppendFormat(
        "Status: %v, DominantResource: %v, DemandShare: %.6g, UsageShare: %.6g, LimitsShare: %.6g, "
        "StrongGuaranteeShare: %.6g, TotalFairShare: %.6g, FairShare: %.6g, Satisfaction: %.4lg, LocalSatisfaction: %.4lg, "
        "PromisedFairShare: %.6g, StarvationStatus: %v, Weight: %v, Volume: %v",
        GetStatus(),
        Attributes_.DominantResource,
        Attributes_.DemandShare,
        Attributes_.UsageShare,
        Attributes_.LimitsShare,
        Attributes_.StrongGuaranteeShare,
        Attributes_.FairShare.Total,
        Attributes_.FairShare,
        PostUpdateAttributes_.SatisfactionRatio,
        PostUpdateAttributes_.LocalSatisfactionRatio,
        Attributes_.PromisedFairShare,
        GetStarvationStatus(),
        GetWeight(),
        GetAccumulatedResourceRatioVolume());
}

TString TSchedulerElement::GetLoggingString(const TFairShareTreeSnapshotPtr& treeSnapshot) const
{
    TStringBuilder builder;
    builder.AppendFormat("Scheduling info for tree %Qv = {", GetTreeId());

    TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
    BuildLoggingStringAttributes(delimitedBuilder);
    TreeElementHost_->BuildElementLoggingStringAttributes(treeSnapshot, this, delimitedBuilder);

    builder.AppendString("}");

    return builder.Flush();
}

double TSchedulerElement::GetWeight() const
{
    auto specifiedWeight = GetSpecifiedWeight();

    if (auto parent = GetParent();
        parent && parent->IsInferringChildrenWeightsFromHistoricUsageEnabled())
    {
        // TODO(eshcherbin): Make the method of calculating weights from historic usage configurable.
        auto multiplier = Exp2(-1.0 * PersistentAttributes_.HistoricUsageAggregator.GetAverage());
        auto weight = specifiedWeight ? *specifiedWeight : 1.0;
        return weight * multiplier;
    }

    if (specifiedWeight) {
        return *specifiedWeight;
    }

    if (!TreeConfig_->InferWeightFromGuaranteesShareMultiplier) {
        return 1.0;
    }
    double selfGuaranteeDominantShare = MaxComponent(Attributes().StrongGuaranteeShare) + Attributes().TotalResourceFlowRatio;

    if (selfGuaranteeDominantShare < NVectorHdrf::RatioComputationPrecision) {
        return 1.0;
    }

    double parentGuaranteeDominantShare = 1.0;
    if (GetParent()) {
        parentGuaranteeDominantShare = MaxComponent(GetParent()->Attributes().StrongGuaranteeShare) + GetParent()->Attributes().TotalResourceFlowRatio;
    }

    if (parentGuaranteeDominantShare < NVectorHdrf::RatioComputationPrecision) {
        return 1.0;
    }

    return selfGuaranteeDominantShare *
        (*TreeConfig_->InferWeightFromGuaranteesShareMultiplier) /
        parentGuaranteeDominantShare;
}

TSchedulableAttributes& TSchedulerElement::Attributes()
{
    return Attributes_;
}

const TSchedulableAttributes& TSchedulerElement::Attributes() const
{
    return Attributes_;
}

const TJobResources& TSchedulerElement::GetResourceDemand() const
{
    return ResourceDemand_;
}

const TJobResources& TSchedulerElement::GetResourceUsageAtUpdate() const
{
    return ResourceUsageAtUpdate_;
}

const TJobResources& TSchedulerElement::GetResourceLimits() const
{
    return ResourceLimits_;
}

const NVectorHdrf::TJobResourcesConfig* TSchedulerElement::GetStrongGuaranteeResourcesConfig() const
{
    return nullptr;
}

TJobResources TSchedulerElement::GetSpecifiedStrongGuaranteeResources() const
{
    const auto* guaranteeConfig = GetStrongGuaranteeResourcesConfig();
    YT_VERIFY(guaranteeConfig);
    return NVectorHdrf::ToJobResources(*guaranteeConfig, {});
}

TSchedulerCompositeElement* TSchedulerElement::GetMutableParent()
{
    return Parent_;
}

const TSchedulerCompositeElement* TSchedulerElement::GetParent() const
{
    return Parent_;
}

NVectorHdrf::TCompositeElement* TSchedulerElement::GetParentElement() const
{
    return Parent_;
}

TInstant TSchedulerElement::GetStartTime() const
{
    return StartTime_;
}

i64 TSchedulerElement::GetPendingAllocationCount() const
{
    return PendingAllocationCount_;
}

ESchedulableStatus TSchedulerElement::GetStatus() const
{
    return ESchedulableStatus::Normal;
}

EStarvationStatus TSchedulerElement::GetStarvationStatus() const
{
    return PersistentAttributes_.StarvationStatus;
}

void TSchedulerElement::SetStarvationStatus(EStarvationStatus starvationStatus)
{
    YT_VERIFY(Mutable_);

    PersistentAttributes_.StarvationStatus = starvationStatus;
}

std::optional<TJobResources> TSchedulerElement::ComputeMaybeSpecifiedResourceLimits() const
{
    if (auto limitsConfig = GetSpecifiedResourceLimitsConfig(); limitsConfig && limitsConfig->IsNonTrivial()) {
        return ToJobResources(limitsConfig, TJobResources::Infinite());
    }

    return {};
}

bool TSchedulerElement::AreSpecifiedResourceLimitsViolated() const
{
    return ResourceTreeElement_->AreSpecifiedResourceLimitsViolated();
}

TJobResources TSchedulerElement::GetInstantResourceUsage() const
{
    auto resourceUsage = TreeConfig_->UseResourceUsageWithPrecommit
        ? ResourceTreeElement_->GetResourceUsageWithPrecommit()
        : ResourceTreeElement_->GetResourceUsage();
    if (resourceUsage.GetUserSlots() > 0 && resourceUsage.GetMemory() == 0) {
        YT_LOG_WARNING("Found usage of schedulable element with non-zero user slots and zero memory (ElementId: %v, Usage: %v)",
            GetId(),
            FormatResources(resourceUsage));
    }
    return resourceUsage;
}

double TSchedulerElement::GetMaxShareRatio() const
{
    return MaxComponent(GetMaxShare());
}

double TSchedulerElement::GetResourceDominantUsageShareAtUpdate() const
{
    return MaxComponent(Attributes_.UsageShare);
}

TString TSchedulerElement::GetTreeId() const
{
    return TreeId_;
}

bool TSchedulerElement::CheckAvailableDemand(const TJobResources& delta)
{
    return ResourceTreeElement_->CheckAvailableDemand(delta, GetResourceDemand());
}

TSchedulerElement::TSchedulerElement(
    ISchedulerStrategyHost* strategyHost,
    IFairShareTreeElementHost* treeElementHost,
    TFairShareStrategyTreeConfigPtr treeConfig,
    TString treeId,
    TString id,
    EResourceTreeElementKind elementKind,
    const NLogging::TLogger& logger)
    : TSchedulerElementFixedState(strategyHost, treeElementHost, std::move(treeConfig), std::move(treeId))
    , ResourceTreeElement_(New<TResourceTreeElement>(
        TreeElementHost_->GetResourceTree(),
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
    TSchedulerCompositeElement* clonedParent)
    : TSchedulerElementFixedState(other)
    , ResourceTreeElement_(other.ResourceTreeElement_)
    , Logger(other.Logger)
{
    Parent_ = clonedParent;
}

ISchedulerStrategyHost* TSchedulerElement::GetHost() const
{
    YT_VERIFY(Mutable_);

    return StrategyHost_;
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
                Div(usageShare, fairShare, /*zeroDivByZero*/ 0.0, /*oneDivByZero*/ InfiniteSatisfactionRatio)),
            InfiniteSatisfactionRatio);
        YT_VERIFY(satisfactionRatio >= 1.0);
        return satisfactionRatio;
    }

    double satisfactionRatio = 0.0;
    if (AreAllResourcesBlocked()) {
        // NB(antonkikh): Using |MaxComponent| would lead to satisfaction ratio being non-monotonous.
        satisfactionRatio = MinComponent(Div(usageShare, fairShare, /*zeroDivByZero*/ 1.0, /*oneDivByZero*/ 1.0));
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
    // Fair share may be slightly greater than demand share due to precision errors. See: YT-15359.
    return Attributes_.FairShare.Total[resource] >= Attributes_.DemandShare[resource];
}

bool TSchedulerElement::AreAllResourcesBlocked() const
{
    // Fair share may be slightly greater than demand share due to precision errors. See: YT-15359.
    return Dominates(Attributes_.FairShare.Total, Attributes_.DemandShare);
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
// TODO(eshcherbin): Rename to StrictlyDominatesNonBlocked.
bool TSchedulerElement::IsStrictlyDominatesNonBlocked(const TResourceVector& lhs, const TResourceVector& rhs) const
{
    if (AreAllResourcesBlocked()) {
        return TResourceVector::Any(lhs, rhs, [] (double x, double y) { return x > y; });
    }

    for (size_t i = 0; i < TResourceVector::Size; i++) {
        if (!IsResourceBlocked(TResourceVector::GetResourceTypeById(i)) && lhs[i] <= rhs[i]) {
            return false;
        }
    }

    return true;
}

ESchedulableStatus TSchedulerElement::GetStatusImpl(double tolerance) const
{
    // Fair share may be slightly greater than demand share due to precision errors. See: YT-15359.
    auto adjustedFairShareBound = TResourceVector::Min(Attributes_.FairShare.Total * tolerance, Attributes_.DemandShare);
    if (IsStrictlyDominatesNonBlocked(adjustedFairShareBound, Attributes_.UsageShare)) {
        return ESchedulableStatus::BelowFairShare;
    }

    return ESchedulableStatus::Normal;
}

void TSchedulerElement::CheckForStarvationImpl(
    TDuration fairShareStarvationTimeout,
    TDuration fairShareAggressiveStarvationTimeout,
    TInstant now)
{
    YT_VERIFY(Mutable_);

    auto status = GetStatus();
    switch (status) {
        case ESchedulableStatus::BelowFairShare:
            if (!PersistentAttributes_.BelowFairShareSince) {
                PersistentAttributes_.BelowFairShareSince = now;
            } else if (EffectiveAggressiveStarvationEnabled_ && now > *PersistentAttributes_.BelowFairShareSince + fairShareAggressiveStarvationTimeout) {
                SetStarvationStatus(EStarvationStatus::AggressivelyStarving);
            } else if (now > *PersistentAttributes_.BelowFairShareSince + fairShareStarvationTimeout) {
                SetStarvationStatus(EStarvationStatus::Starving);
            }
            break;

        case ESchedulableStatus::Normal:
            PersistentAttributes_.BelowFairShareSince = std::nullopt;
            SetStarvationStatus(EStarvationStatus::NonStarving);
            break;

        default:
            YT_ABORT();
    }
}

TJobResources TSchedulerElement::ComputeResourceLimits() const
{
    auto limits = Min(
        GetSchedulingTagFilterResourceLimits(),
        GetMaxShareResourceLimits());
    if (MaybeSpecifiedResourceLimits_) {
        limits = Min(limits, *MaybeSpecifiedResourceLimits_);
    }

    return limits;
}

bool TSchedulerElement::AreTotalResourceLimitsStable() const
{
    auto connectionTime = InstantToCpuInstant(StrategyHost_->GetConnectionTime());
    auto timeout = DurationToCpuDuration(TreeConfig_->NodeReconnectionTimeout);
    auto now = GetCpuInstant();
    return now >= connectionTime + timeout;
}

TJobResources TSchedulerElement::ComputeSchedulingTagFilterResourceLimits() const
{
    // Shortcut: if the scheduling tag filter is empty then we just use the resource limits for
    // the tree's nodes filter, which were computed earlier in PreUpdateBottomUp.
    if (GetSchedulingTagFilter() == EmptySchedulingTagFilter) {
        return TotalResourceLimits_;
    }

    if (!AreTotalResourceLimitsStable()) {
        // Return infinity during some time after scheduler reconnection.
        return TJobResources::Infinite();
    }

    return GetHost()->GetResourceLimits(TreeConfig_->NodesFilter & GetSchedulingTagFilter());
}

TJobResources TSchedulerElement::GetSchedulingTagFilterResourceLimits() const
{
    return SchedulingTagFilterResourceLimits_;
}

TJobResources TSchedulerElement::GetTotalResourceLimits() const
{
    return TotalResourceLimits_;
}

TJobResources TSchedulerElement::GetMaxShareResourceLimits() const
{
    return GetTotalResourceLimits() * GetMaxShare();
}

void TSchedulerElement::BuildResourceMetering(
    const std::optional<TMeteringKey>& /*key*/,
    const THashMap<TString, TResourceVolume>& /*poolResourceUsages*/,
    TMeteringMap* /*statistics*/) const
{ }

double TSchedulerElement::GetAccumulatedResourceRatioVolume() const
{
    return PersistentAttributes_.IntegralResourcesState.AccumulatedVolume.GetMinResourceRatio(TotalResourceLimits_);
}

TResourceVolume TSchedulerElement::GetAccumulatedResourceVolume() const
{
    return PersistentAttributes_.IntegralResourcesState.AccumulatedVolume;
}

void TSchedulerElement::InitAccumulatedResourceVolume(TResourceVolume resourceVolume)
{
    YT_VERIFY(PersistentAttributes_.IntegralResourcesState.AccumulatedVolume == TResourceVolume());
    PersistentAttributes_.IntegralResourcesState.AccumulatedVolume = resourceVolume;
}

bool TSchedulerElement::IsAlive() const
{
    return ResourceTreeElement_->GetAlive();
}

void TSchedulerElement::SetNonAlive()
{
    ResourceTreeElement_->SetNonAlive();
}

TJobResources TSchedulerElement::GetResourceUsageWithPrecommit() const
{
    return ResourceTreeElement_->GetResourceUsageWithPrecommit();
}

const NLogging::TLogger& TSchedulerElement::GetLogger() const
{
    return Logger;
}

bool TSchedulerElement::AreDetailedLogsEnabled() const
{
    return false;
}

void TSchedulerElement::UpdateRecursiveAttributes()
{
    YT_VERIFY(Mutable_);

    if (IsRoot()) {
        YT_VERIFY(GetSpecifiedFairShareStarvationTolerance());
        EffectiveFairShareStarvationTolerance_ = *GetSpecifiedFairShareStarvationTolerance();

        YT_VERIFY(GetSpecifiedFairShareStarvationTimeout());
        EffectiveFairShareStarvationTimeout_ = *GetSpecifiedFairShareStarvationTimeout();

        YT_VERIFY(IsAggressiveStarvationEnabled());
        EffectiveAggressiveStarvationEnabled_ = *IsAggressiveStarvationEnabled();

        YT_VERIFY(GetSpecifiedNonPreemptibleResourceUsageThresholdConfig());
        EffectiveNonPreemptibleResourceUsageThresholdConfig_ = GetSpecifiedNonPreemptibleResourceUsageThresholdConfig();
    } else {
        YT_VERIFY(Parent_);

        EffectiveFairShareStarvationTolerance_ = GetSpecifiedFairShareStarvationTolerance().value_or(
            Parent_->GetEffectiveFairShareStarvationTolerance());

        EffectiveFairShareStarvationTimeout_ = GetSpecifiedFairShareStarvationTimeout().value_or(
            Parent_->GetEffectiveFairShareStarvationTimeout());

        EffectiveAggressiveStarvationEnabled_ = IsAggressiveStarvationEnabled()
            .value_or(Parent_->GetEffectiveAggressiveStarvationEnabled());

        EffectiveNonPreemptibleResourceUsageThresholdConfig_ = Parent_->EffectiveNonPreemptibleResourceUsageThresholdConfig();
        if (const auto& specifiedConfig = GetSpecifiedNonPreemptibleResourceUsageThresholdConfig()) {
            EffectiveNonPreemptibleResourceUsageThresholdConfig_ = specifiedConfig;
        }
    }

    LimitedDemandShare_ = ComputeLimitedDemandShare();
}

void TSchedulerElement::UpdateStarvationStatuses(TInstant now, bool enablePoolStarvation)
{
    YT_VERIFY(Mutable_);

    if (enablePoolStarvation || IsOperation()) {
        CheckForStarvation(now);
    }

    if (!IsRoot()) {
        LowestStarvingAncestor_ = GetStarvationStatus() != EStarvationStatus::NonStarving
            ? this
            : Parent_->GetLowestStarvingAncestor();
        LowestAggressivelyStarvingAncestor_ = GetStarvationStatus() == EStarvationStatus::AggressivelyStarving
            ? this
            : Parent_->GetLowestAggressivelyStarvingAncestor();
    }
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerCompositeElement::TSchedulerCompositeElement(
    ISchedulerStrategyHost* strategyHost,
    IFairShareTreeElementHost* treeElementHost,
    TFairShareStrategyTreeConfigPtr treeConfig,
    const TString& treeId,
    const TString& id,
    EResourceTreeElementKind elementKind,
    const NLogging::TLogger& logger)
    : TSchedulerElement(strategyHost, treeElementHost, std::move(treeConfig), treeId, id, elementKind, logger)
{ }

TSchedulerCompositeElement::TSchedulerCompositeElement(
    const TSchedulerCompositeElement& other,
    TSchedulerCompositeElement* clonedParent)
    : TSchedulerElement(other, clonedParent)
    , TSchedulerCompositeElementFixedState(other)
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

void TSchedulerCompositeElement::MarkImmutable()
{
    TSchedulerElement::MarkImmutable();
    for (const auto& child : EnabledChildren_) {
        child->MarkImmutable();
    }
    for (const auto& child : DisabledChildren_) {
        child->MarkImmutable();
    }
}

int TSchedulerCompositeElement::EnumerateElements(int startIndex, bool isSchedulableValueFilter)
{
    YT_VERIFY(Mutable_);

    startIndex = TSchedulerElement::EnumerateElements(startIndex, isSchedulableValueFilter);
    for (const auto& child : EnabledChildren_) {
        startIndex = child->EnumerateElements(startIndex, isSchedulableValueFilter);
    }
    return startIndex;
}

void TSchedulerCompositeElement::DisableNonAliveElements()
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

void TSchedulerCompositeElement::UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config)
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

void TSchedulerCompositeElement::PreUpdateBottomUp(NVectorHdrf::TFairShareUpdateContext* context)
{
    YT_VERIFY(Mutable_);

    ResourceUsageAtUpdate_ = {};
    ResourceDemand_ = {};

    for (const auto& child : EnabledChildren_) {
        child->PreUpdateBottomUp(context);

        ResourceUsageAtUpdate_ += child->GetResourceUsageAtUpdate();
        ResourceDemand_ += child->GetResourceDemand();
        PendingAllocationCount_ += child->GetPendingAllocationCount();

        if (IsInferringChildrenWeightsFromHistoricUsageEnabled()) {
            // NB(eshcherbin): This is a lazy parameters update so it has to be done every time.
            child->PersistentAttributes_.HistoricUsageAggregator.SetHalflife(
                GetHistoricUsageAggregatorPeriod());

            // TODO(eshcherbin): Should we use vectors instead of ratios?
            // Yes, but nobody uses this feature yet, so it's not really important.
            // NB(eshcherbin): |child->Attributes().UsageShare| is not calculated at this stage yet, so we do it manually.
            auto usageShare = TResourceVector::FromJobResources(child->GetResourceUsageAtUpdate(), child->GetTotalResourceLimits());
            child->PersistentAttributes_.HistoricUsageAggregator.UpdateAt(context->Now, MaxComponent(usageShare));
        }
    }

    TSchedulerElement::PreUpdateBottomUp(context);
}

void TSchedulerCompositeElement::BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context)
{
    PostUpdateAttributes_.UnschedulableOperationsResourceUsage = TJobResources();
    SchedulableChildren_.clear();

    ResetSchedulableCounters();
    auto updateSchedulableCounters = [&] (const TSchedulerElementPtr& child) {
        SchedulableElementCount_ += child->SchedulableElementCount();
        SchedulablePoolCount_ += child->SchedulablePoolCount();
        SchedulableOperationCount_ += child->SchedulableOperationCount();
    };

    auto maxSchedulableElementCount = TreeConfig_->MaxSchedulableElementCountInFifoPool;

    if (Mode_ == ESchedulingMode::FairShare || !maxSchedulableElementCount.has_value()) {
        for (const auto& child : EnabledChildren_) {
            child->BuildSchedulableChildrenLists(context);
            PostUpdateAttributes_.UnschedulableOperationsResourceUsage += child->PostUpdateAttributes().UnschedulableOperationsResourceUsage;
            if (child->IsSchedulable()) {
                SchedulableChildren_.push_back(child.Get());
                updateSchedulableCounters(child);
            }
        }
    } else { // Fifo pool, MaxSchedulableElementCountInFifoPool specified.
        std::vector<TSchedulerOperationElement*> sortedChildren;
        for (const auto& child : EnabledChildren_) {
            YT_VERIFY(child->IsOperation());
            sortedChildren.push_back(dynamic_cast<TSchedulerOperationElement*>(child.Get()));
        }

        std::sort(
            sortedChildren.begin(),
            sortedChildren.end(),
            [&] (const TSchedulerOperationElement* lhs, const TSchedulerOperationElement* rhs) {
                return lhs->Attributes().FifoIndex < rhs->Attributes().FifoIndex;
            });

        for (auto* child : sortedChildren) {
            child->BuildSchedulableChildrenLists(context);
            if (child->IsSchedulable()) {
                bool shouldSkip = SchedulableElementCount_ >= *maxSchedulableElementCount &&
                    Dominates(TResourceVector::SmallEpsilon(), child->Attributes().FairShare.Total);
                if (shouldSkip) {
                    child->OnFifoSchedulableElementCountLimitReached(context);
                } else {
                    SchedulableChildren_.push_back(child);
                    updateSchedulableCounters(child);
                }
            }

            PostUpdateAttributes_.UnschedulableOperationsResourceUsage += child->PostUpdateAttributes().UnschedulableOperationsResourceUsage;
        }
    }

    if (IsRoot() || IsSchedulable()) {
        ++SchedulableElementCount_;
        ++SchedulablePoolCount_;
    }
}

void TSchedulerCompositeElement::ComputeSatisfactionRatioAtUpdate()
{
    TSchedulerElement::ComputeSatisfactionRatioAtUpdate();

    auto isBetterChild = [&] (const TSchedulerElement* lhs, const TSchedulerElement* rhs) {
        if (ShouldUseFifoSchedulingOrder()) {
            return HasHigherPriorityInFifoMode(lhs, rhs);
        }

        return lhs->PostUpdateAttributes().SatisfactionRatio < rhs->PostUpdateAttributes().SatisfactionRatio;
    };

    TSchedulerElement* bestChild = nullptr;
    for (const auto& child : EnabledChildren_) {
        child->ComputeSatisfactionRatioAtUpdate();

        if (!child->IsSchedulable()) {
            continue;
        }

        if (!bestChild || isBetterChild(child.Get(), bestChild)) {
            bestChild = child.Get();
        }
    }

    if (!bestChild) {
        return;
    }

    PostUpdateAttributes_.SatisfactionRatio = bestChild->PostUpdateAttributes().SatisfactionRatio;
    if (EffectiveUsePoolSatisfactionForScheduling_) {
        PostUpdateAttributes_.SatisfactionRatio  = std::min(
            PostUpdateAttributes_.SatisfactionRatio,
            PostUpdateAttributes_.LocalSatisfactionRatio);
    }
}

void TSchedulerCompositeElement::BuildElementMapping(TFairSharePostUpdateContext* context)
{
    for (const auto& child : EnabledChildren_) {
        child->BuildElementMapping(context);
    }
    for (const auto& child : DisabledChildren_) {
        if (child->IsOperation()) {
            child->BuildElementMapping(context);
        }
    }
}

void TSchedulerCompositeElement::IncreaseOperationCount(int delta)
{
    DoIncreaseOperationCount(delta, &TSchedulerCompositeElement::OperationCount_);
}

void TSchedulerCompositeElement::IncreaseRunningOperationCount(int delta)
{
    DoIncreaseOperationCount(delta, &TSchedulerCompositeElement::RunningOperationCount_);
}

void TSchedulerCompositeElement::IncreaseLightweightRunningOperationCount(int delta)
{
    DoIncreaseOperationCount(delta, &TSchedulerCompositeElement::LightweightRunningOperationCount_);
}

void TSchedulerCompositeElement::DoIncreaseOperationCount(int delta, int TSchedulerCompositeElement::* operationCounter)
{
    auto* current = this;
    while (current) {
        current->*operationCounter += delta;
        current = current->GetMutableParent();
    }
}

bool TSchedulerCompositeElement::IsSchedulable() const
{
    return IsRoot() || !SchedulableChildren_.empty();
}

bool TSchedulerCompositeElement::IsExplicit() const
{
    return false;
}

void TSchedulerCompositeElement::AddChild(TSchedulerElement* child, bool enabled)
{
    YT_VERIFY(Mutable_);

    if (enabled) {
        child->PersistentAttributes_.ResetOnElementEnabled();
    }

    auto& map = enabled ? EnabledChildToIndex_ : DisabledChildToIndex_;
    auto& list = enabled ? EnabledChildren_ : DisabledChildren_;
    AddChild(&map, &list, child);
}

void TSchedulerCompositeElement::EnableChild(const TSchedulerElementPtr& child)
{
    YT_VERIFY(Mutable_);

    child->PersistentAttributes_.ResetOnElementEnabled();

    RemoveChild(&DisabledChildToIndex_, &DisabledChildren_, child);
    AddChild(&EnabledChildToIndex_, &EnabledChildren_, child);
}

void TSchedulerCompositeElement::DisableChild(const TSchedulerElementPtr& child)
{
    YT_VERIFY(Mutable_);

    if (EnabledChildToIndex_.find(child) == EnabledChildToIndex_.end()) {
        return;
    }

    RemoveChild(&EnabledChildToIndex_, &EnabledChildren_, child);
    AddChild(&DisabledChildToIndex_, &DisabledChildren_, child);
}

void TSchedulerCompositeElement::RemoveChild(TSchedulerElement* child)
{
    YT_VERIFY(Mutable_);

    bool enabled = ContainsChild(EnabledChildToIndex_, child);
    auto& map = enabled ? EnabledChildToIndex_ : DisabledChildToIndex_;
    auto& list = enabled ? EnabledChildren_ : DisabledChildren_;
    RemoveChild(&map, &list, child);
}

bool TSchedulerCompositeElement::IsEnabledChild(TSchedulerElement* child)
{
    return ContainsChild(EnabledChildToIndex_, child);
}

bool TSchedulerCompositeElement::IsEmpty() const
{
    return EnabledChildren_.empty() && DisabledChildren_.empty();
}

void TSchedulerCompositeElement::CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const
{
    for (const auto& child : EnabledChildren_) {
        child->CollectResourceTreeOperationElements(elements);
    }
}

NVectorHdrf::TElement* TSchedulerCompositeElement::GetChild(int index)
{
    return EnabledChildren_[index].Get();
}

const NVectorHdrf::TElement* TSchedulerCompositeElement::GetChild(int index) const
{
    return EnabledChildren_[index].Get();
}

int TSchedulerCompositeElement::GetChildCount() const
{
    return EnabledChildren_.size();
}

std::vector<TSchedulerOperationElement*> TSchedulerCompositeElement::GetChildOperations() const
{
    std::vector<TSchedulerOperationElement*> result;
    result.reserve(std::size(EnabledChildren_) + std::size(DisabledChildren_));

    for (const auto& child : EnabledChildren_) {
        if (child->IsOperation()) {
            result.push_back(static_cast<TSchedulerOperationElement*>(child.Get()));
        }
    }
    for (const auto& child : DisabledChildren_) {
        if (child->IsOperation()) {
            result.push_back(static_cast<TSchedulerOperationElement*>(child.Get()));
        }
    }

    return result;
}

int TSchedulerCompositeElement::GetChildOperationCount() const noexcept
{
    int count = 0;

    for (const auto& child : EnabledChildren_) {
        if (child->IsOperation()) {
            ++count;
        }
    }
    for (const auto& child : DisabledChildren_) {
        if (child->IsOperation()) {
            ++count;
        }
    }

    return count;
}

int TSchedulerCompositeElement::GetChildPoolCount() const noexcept
{
    int count = 0;
    for (const auto& child : EnabledChildren_) {
        if (!child->IsOperation()) {
            ++count;
        }
    }
    return count;
}

ESchedulingMode TSchedulerCompositeElement::GetMode() const
{
    return Mode_;
}

bool TSchedulerCompositeElement::HasHigherPriorityInFifoMode(const NVectorHdrf::TElement* lhs, const NVectorHdrf::TElement* rhs) const
{
    const auto* lhsElement = dynamic_cast<const TSchedulerElement*>(lhs);
    const auto* rhsElement = dynamic_cast<const TSchedulerElement*>(rhs);

    YT_VERIFY(lhsElement);
    YT_VERIFY(rhsElement);

    return HasHigherPriorityInFifoMode(lhsElement, rhsElement);
}

const std::vector<TSchedulerElementPtr>& TSchedulerCompositeElement::EnabledChildren() const
{
    return EnabledChildren_;
}

void TSchedulerCompositeElement::AddChild(
    TChildMap* map,
    TChildList* list,
    const TSchedulerElementPtr& child)
{
    list->push_back(child);
    YT_VERIFY(map->emplace(child, list->size() - 1).second);
}

void TSchedulerCompositeElement::RemoveChild(
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

bool TSchedulerCompositeElement::ContainsChild(
    const TChildMap& map,
    const TSchedulerElementPtr& child)
{
    return map.find(child) != map.end();
}

bool TSchedulerCompositeElement::ShouldUseFifoSchedulingOrder() const
{
    return Mode_ == ESchedulingMode::Fifo &&
        EffectiveFifoPoolSchedulingOrder_ == EFifoPoolSchedulingOrder::Fifo;
}

bool TSchedulerCompositeElement::HasHigherPriorityInFifoMode(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const
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
            case EFifoSortParameter::PendingJobCount:
            case EFifoSortParameter::PendingAllocationCount: {
                int lhsPendingAllocationCount = lhs->GetPendingAllocationCount();
                int rhsPendingAllocationCount = rhs->GetPendingAllocationCount();
                if (lhsPendingAllocationCount != rhsPendingAllocationCount) {
                    return lhsPendingAllocationCount < rhsPendingAllocationCount;
                }
                break;
            }
            default:
                YT_ABORT();
        }
    }
    return false;
}

int TSchedulerCompositeElement::GetAvailableRunningOperationCount() const
{
    return std::max(GetMaxRunningOperationCount() - RunningOperationCount_, 0);
}

bool TSchedulerCompositeElement::GetEffectiveLightweightOperationsEnabled() const
{
    return AreLightweightOperationsEnabled() && Mode_ == ESchedulingMode::Fifo;
}

TResourceVolume TSchedulerCompositeElement::GetIntegralPoolCapacity() const
{
    return TResourceVolume(TotalResourceLimits_ * Attributes_.ResourceFlowRatio, TreeConfig_->IntegralGuarantees->PoolCapacitySaturationPeriod);
}

void TSchedulerCompositeElement::UpdateRecursiveAttributes()
{
    YT_VERIFY(Mutable_);

    TSchedulerElement::UpdateRecursiveAttributes();

    if (IsRoot()) {
        YT_VERIFY(GetSpecifiedFifoPoolSchedulingOrder());
        EffectiveFifoPoolSchedulingOrder_ = *GetSpecifiedFifoPoolSchedulingOrder();

        YT_VERIFY(ShouldUsePoolSatisfactionForScheduling());
        EffectiveUsePoolSatisfactionForScheduling_ = *ShouldUsePoolSatisfactionForScheduling();
    } else {
        YT_VERIFY(Parent_);

        EffectiveFifoPoolSchedulingOrder_ = GetSpecifiedFifoPoolSchedulingOrder().value_or(
            Parent_->GetEffectiveFifoPoolSchedulingOrder());

        EffectiveUsePoolSatisfactionForScheduling_ = ShouldUsePoolSatisfactionForScheduling().value_or(
            Parent_->GetEffectiveUsePoolSatisfactionForScheduling());
    }

    for (const auto& child : EnabledChildren_) {
        child->UpdateRecursiveAttributes();
    }
}

void TSchedulerCompositeElement::UpdateStarvationStatuses(TInstant now, bool enablePoolStarvation)
{
    YT_VERIFY(Mutable_);

    TSchedulerElement::UpdateStarvationStatuses(now, enablePoolStarvation);

    for (const auto& child : EnabledChildren_) {
        child->UpdateStarvationStatuses(now, enablePoolStarvation);
    }
}

TYPath TSchedulerCompositeElement::GetFullPath(bool explicitOnly, bool withTreeId) const
{
    std::vector<TString> tokens;
    const auto* current = this;
    while (!current->IsRoot()) {
        if (!explicitOnly || current->IsExplicit()) {
            tokens.push_back(current->GetId());
        }
        current = current->GetParent();
    }

    std::reverse(tokens.begin(), tokens.end());

    TYPath path;
    if (withTreeId) {
        path = "/" + NYPath::ToYPathLiteral(TreeId_);
    }
    for (const auto& token : tokens) {
        path.append('/');
        path.append(NYPath::ToYPathLiteral(token));
    }
    return path;
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerPoolElementFixedState::TSchedulerPoolElementFixedState(TString id, NObjectClient::TObjectId objectId)
    : Id_(std::move(id))
    , ObjectId_(objectId)
{ }

////////////////////////////////////////////////////////////////////////////////

TSchedulerPoolElement::TSchedulerPoolElement(
    ISchedulerStrategyHost* strategyHost,
    IFairShareTreeElementHost* treeElementHost,
    const TString& id,
    TGuid objectId,
    TPoolConfigPtr config,
    bool defaultConfigured,
    TFairShareStrategyTreeConfigPtr treeConfig,
    const TString& treeId,
    const NLogging::TLogger& logger)
    : TSchedulerCompositeElement(
        strategyHost,
        treeElementHost,
        std::move(treeConfig),
        treeId,
        id,
        EResourceTreeElementKind::Pool,
        logger.WithTag("Pool: %v, SchedulingMode: %v",
            id,
            config->Mode))
    , TSchedulerPoolElementFixedState(id, objectId)
{
    DoSetConfig(std::move(config));
    DefaultConfigured_ = defaultConfigured;
}

TSchedulerPoolElement::TSchedulerPoolElement(const TSchedulerPoolElement& other, TSchedulerCompositeElement* clonedParent)
    : TSchedulerCompositeElement(other, clonedParent)
    , TSchedulerPoolElementFixedState(other)
    , Config_(other.Config_)
{ }

bool TSchedulerPoolElement::IsDefaultConfigured() const
{
    return DefaultConfigured_;
}

bool TSchedulerPoolElement::IsEphemeralInDefaultParentPool() const
{
    return EphemeralInDefaultParentPool_;
}

void TSchedulerPoolElement::SetUserName(const std::optional<TString>& userName)
{
    UserName_ = userName;
}

const std::optional<TString>& TSchedulerPoolElement::GetUserName() const
{
    return UserName_;
}

TPoolConfigPtr TSchedulerPoolElement::GetConfig() const
{
    return Config_;
}

void TSchedulerPoolElement::SetConfig(TPoolConfigPtr config)
{
    YT_VERIFY(Mutable_);

    DoSetConfig(std::move(config));
    DefaultConfigured_ = false;
}

void TSchedulerPoolElement::SetDefaultConfig()
{
    YT_VERIFY(Mutable_);

    DoSetConfig(New<TPoolConfig>());
    DefaultConfigured_ = true;
}

void TSchedulerPoolElement::SetObjectId(NObjectClient::TObjectId objectId)
{
    YT_VERIFY(Mutable_);

    ObjectId_ = objectId;
}

void TSchedulerPoolElement::SetEphemeralInDefaultParentPool()
{
    YT_VERIFY(Mutable_);

    EphemeralInDefaultParentPool_ = true;
}

bool TSchedulerPoolElement::IsExplicit() const
{
    // NB: This is no coincidence.
    return !DefaultConfigured_;
}

std::optional<bool> TSchedulerPoolElement::IsAggressiveStarvationEnabled() const
{
    return Config_->EnableAggressiveStarvation;
}

TJobResourcesConfigPtr TSchedulerPoolElement::GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const
{
    return Config_->NonPreemptibleResourceUsageThreshold;
}

std::optional<EFifoPoolSchedulingOrder> TSchedulerPoolElement::GetSpecifiedFifoPoolSchedulingOrder() const
{
    return Config_->FifoPoolSchedulingOrder;
}

std::optional<bool> TSchedulerPoolElement::ShouldUsePoolSatisfactionForScheduling() const
{
    return Config_->UsePoolSatisfactionForScheduling;
}

TString TSchedulerPoolElement::GetId() const
{
    return Id_;
}

std::optional<double> TSchedulerPoolElement::GetSpecifiedWeight() const
{
    return Config_->Weight;
}

const NVectorHdrf::TJobResourcesConfig* TSchedulerPoolElement::GetStrongGuaranteeResourcesConfig() const
{
    return Config_->StrongGuaranteeResources.Get();
}

TResourceVector TSchedulerPoolElement::GetMaxShare() const
{
    return TResourceVector::FromDouble(Config_->MaxShareRatio.value_or(1.0));
}

EIntegralGuaranteeType TSchedulerPoolElement::GetIntegralGuaranteeType() const
{
    return Config_->IntegralGuarantees->GuaranteeType;
}

const TIntegralResourcesState& TSchedulerPoolElement::IntegralResourcesState() const
{
    return PersistentAttributes_.IntegralResourcesState;
}

TIntegralResourcesState& TSchedulerPoolElement::IntegralResourcesState()
{
    return PersistentAttributes_.IntegralResourcesState;
}

ESchedulableStatus TSchedulerPoolElement::GetStatus() const
{
    return TSchedulerElement::GetStatusImpl(EffectiveFairShareStarvationTolerance_);
}

std::optional<double> TSchedulerPoolElement::GetSpecifiedFairShareStarvationTolerance() const
{
    return Config_->FairShareStarvationTolerance;
}

std::optional<TDuration> TSchedulerPoolElement::GetSpecifiedFairShareStarvationTimeout() const
{
    return Config_->FairShareStarvationTimeout;
}

void TSchedulerPoolElement::SetStarvationStatus(EStarvationStatus starvationStatus)
{
    YT_VERIFY(Mutable_);

    if (starvationStatus != GetStarvationStatus()) {
        YT_LOG_INFO("Pool starvation status changed (Current: %v, New: %v)",
            GetStarvationStatus(),
            starvationStatus);
    }
    TSchedulerElement::SetStarvationStatus(starvationStatus);
}

void TSchedulerPoolElement::CheckForStarvation(TInstant now)
{
    YT_VERIFY(Mutable_);

    TSchedulerElement::CheckForStarvationImpl(
        EffectiveFairShareStarvationTimeout_,
        TreeConfig_->FairShareAggressiveStarvationTimeout,
        now);
}

const TSchedulingTagFilter& TSchedulerPoolElement::GetSchedulingTagFilter() const
{
    return SchedulingTagFilter_;
}

int TSchedulerPoolElement::GetMaxRunningOperationCount() const
{
    return Config_->MaxRunningOperationCount.value_or(TreeConfig_->MaxRunningOperationCountPerPool);
}

bool TSchedulerPoolElement::AreLightweightOperationsEnabled() const
{
    return Config_->EnableLightweightOperations;
}

int TSchedulerPoolElement::GetMaxOperationCount() const
{
    return Config_->MaxOperationCount.value_or(TreeConfig_->MaxOperationCountPerPool);
}

TPoolIntegralGuaranteesConfigPtr TSchedulerPoolElement::GetIntegralGuaranteesConfig() const
{
    return Config_->IntegralGuarantees;
}

std::vector<EFifoSortParameter> TSchedulerPoolElement::GetFifoSortParameters() const
{
    return FifoSortParameters_;
}

bool TSchedulerPoolElement::AreImmediateOperationsForbidden() const
{
    return Config_->ForbidImmediateOperations;
}

bool TSchedulerPoolElement::IsEphemeralHub() const
{
    return Config_->CreateEphemeralSubpools;
}

THashSet<TString> TSchedulerPoolElement::GetAllowedProfilingTags() const
{
    return Config_->AllowedProfilingTags;
}

bool TSchedulerPoolElement::IsFairShareTruncationInFifoPoolEnabled() const
{
    return Config_->EnableFairShareTruncationInFifoPool.value_or(
        TreeConfig_->EnableFairShareTruncationInFifoPool);
}

bool TSchedulerPoolElement::ShouldComputePromisedGuaranteeFairShare() const
{
    return Config_->ComputePromisedGuaranteeFairShare;
}

bool TSchedulerPoolElement::IsInferringChildrenWeightsFromHistoricUsageEnabled() const
{
    return Config_->InferChildrenWeightsFromHistoricUsage;
}

TDuration TSchedulerPoolElement::GetHistoricUsageAggregatorPeriod() const
{
    return Config_->HistoricUsageAggregationPeriod.value_or(TDuration::Zero());
}

void TSchedulerPoolElement::BuildResourceMetering(
    const std::optional<TMeteringKey>& parentKey,
    const THashMap<TString, TResourceVolume>& poolResourceUsages,
    TMeteringMap* meteringMap) const
{
    std::optional<TMeteringKey> key;
    if (Config_->Abc) {
        key = TMeteringKey{
            .AbcId  = Config_->Abc->Id,
            .TreeId = GetTreeId(),
            .PoolId = GetId(),
            .MeteringTags = Config_->MeteringTags,
        };
    }

    YT_VERIFY(key || parentKey);

    bool isIntegral = Config_->IntegralGuarantees->GuaranteeType != EIntegralGuaranteeType::None;

    TResourceVolume accumulatedResourceUsageVolume;
    {
        auto it = poolResourceUsages.find(GetId());
        if (it != poolResourceUsages.end()) {
            accumulatedResourceUsageVolume = it->second;
        }
    }

    auto meteringStatistics = TMeteringStatistics(
        GetSpecifiedStrongGuaranteeResources(),
        isIntegral ? ToJobResources(Config_->IntegralGuarantees->ResourceFlow, {}) : TJobResources(),
        isIntegral ? ToJobResources(Config_->IntegralGuarantees->BurstGuaranteeResources, {}) : TJobResources(),
        GetResourceUsageAtUpdate(),
        accumulatedResourceUsageVolume);

    if (key) {
        auto insertResult = meteringMap->insert({*key, meteringStatistics});
        YT_VERIFY(insertResult.second);
    } else {
        GetOrCrash(*meteringMap, *parentKey).AccountChild(meteringStatistics);
    }

    for (const auto& child : EnabledChildren_) {
        child->BuildResourceMetering(
            /*parentKey*/ key ? key : parentKey,
            poolResourceUsages,
            meteringMap);
    }

    if (key && parentKey) {
        GetOrCrash(*meteringMap, *parentKey).DiscountChild(meteringStatistics);
    }
}

TSchedulerElementPtr TSchedulerPoolElement::Clone(TSchedulerCompositeElement* clonedParent)
{
    return New<TSchedulerPoolElement>(*this, clonedParent);
}

ESchedulerElementType TSchedulerPoolElement::GetType() const
{
    return ESchedulerElementType::Pool;
}

void TSchedulerPoolElement::AttachParent(TSchedulerCompositeElement* parent)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(!Parent_);
    YT_VERIFY(RunningOperationCount_ == 0);
    YT_VERIFY(OperationCount_ == 0);

    parent->AddChild(this);
    Parent_ = parent;
    TreeElementHost_->GetResourceTree()->AttachParent(ResourceTreeElement_, parent->ResourceTreeElement_);

    YT_LOG_DEBUG("Pool is attached (Pool: %v, ParentPool: %v)",
        Id_,
        parent->GetId());
}

const TSchedulerCompositeElement* TSchedulerPoolElement::GetNearestAncestorWithResourceLimits(const TSchedulerCompositeElement* element) const
{
    do {
        if (element->PersistentAttributes().AppliedSpecifiedResourceLimits) {
            return element;
        }
    } while (element = element->GetParent());

    return nullptr;
}

void TSchedulerPoolElement::ChangeParent(TSchedulerCompositeElement* newParent)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(Parent_);
    YT_VERIFY(newParent);
    YT_VERIFY(Parent_ != newParent);

    auto oldParent = Parent_;
    bool enabled = Parent_->IsEnabledChild(this);

    Parent_->IncreaseOperationCount(-OperationCount());
    Parent_->IncreaseRunningOperationCount(-RunningOperationCount());
    Parent_->IncreaseLightweightRunningOperationCount(-LightweightRunningOperationCount());
    Parent_->RemoveChild(this);

    Parent_ = newParent;

    auto* sourceAncestorWithResourceLimits = GetNearestAncestorWithResourceLimits(oldParent);
    auto* destinationAncestorWithResourceLimits = GetNearestAncestorWithResourceLimits(newParent);

    bool ancestorWithResourceLimitsChanged =
        !PersistentAttributes_.AppliedSpecifiedResourceLimits &&
        sourceAncestorWithResourceLimits != destinationAncestorWithResourceLimits;
    if (ancestorWithResourceLimitsChanged) {
        std::vector<TResourceTreeElementPtr> descendantOperationElements;
        CollectResourceTreeOperationElements(&descendantOperationElements);

        TreeElementHost_->GetResourceTree()->ChangeParent(
            ResourceTreeElement_,
            newParent->ResourceTreeElement_,
            descendantOperationElements);
    } else {
        TreeElementHost_->GetResourceTree()->ChangeParent(
            ResourceTreeElement_,
            newParent->ResourceTreeElement_,
            /*descendantOperationElements*/ std::nullopt);
    }

    Parent_->AddChild(this, enabled);
    Parent_->IncreaseOperationCount(OperationCount());
    Parent_->IncreaseRunningOperationCount(RunningOperationCount());
    Parent_->IncreaseLightweightRunningOperationCount(LightweightRunningOperationCount());

    YT_LOG_INFO("Parent pool is changed ("
        "NewParent: %v, "
        "OldParent: %v, "
        "CurrentResourceLimits: %v, "
        "SourceAncestorWithResourceLimits: %v, "
        "DestinationAncestorWithResourceLimits: %v, "
        "AncestorWithResourceLimitsChanged: %v)",
        newParent->GetId(),
        oldParent->GetId(),
        PersistentAttributes_.AppliedSpecifiedResourceLimits,
        sourceAncestorWithResourceLimits
            ? std::optional(sourceAncestorWithResourceLimits->GetId())
            : std::nullopt,
        destinationAncestorWithResourceLimits
            ? std::optional(destinationAncestorWithResourceLimits->GetId())
            : std::nullopt,
        ancestorWithResourceLimitsChanged);
}

void TSchedulerPoolElement::DetachParent()
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(Parent_);
    YT_VERIFY(RunningOperationCount() == 0);
    YT_VERIFY(OperationCount() == 0);

    const auto& oldParentId = Parent_->GetId();
    Parent_->RemoveChild(this);
    TreeElementHost_->GetResourceTree()->ScheduleDetachParent(ResourceTreeElement_);

    YT_LOG_DEBUG("Pool is detached (Pool: %v, ParentPool: %v)",
        Id_,
        oldParentId);
}

void TSchedulerPoolElement::DoSetConfig(TPoolConfigPtr newConfig)
{
    YT_VERIFY(Mutable_);

    Config_ = std::move(newConfig);
    FifoSortParameters_ = Config_->FifoSortParameters;
    Mode_ = Config_->Mode;
    SchedulingTagFilter_ = TSchedulingTagFilter(Config_->SchedulingTagFilter);
}

TJobResourcesConfigPtr TSchedulerPoolElement::GetSpecifiedResourceLimitsConfig() const
{
    return Config_->ResourceLimits;
}

void TSchedulerPoolElement::BuildElementMapping(TFairSharePostUpdateContext* context)
{
    context->PoolNameToElement.emplace(GetId(), this);
    TSchedulerCompositeElement::BuildElementMapping(context);
}

double TSchedulerPoolElement::GetSpecifiedBurstRatio() const
{
    if (Config_->IntegralGuarantees->GuaranteeType == EIntegralGuaranteeType::None) {
        return 0;
    }
    return GetMaxResourceRatio(ToJobResources(Config_->IntegralGuarantees->BurstGuaranteeResources, {}), TotalResourceLimits_);
}

double TSchedulerPoolElement::GetSpecifiedResourceFlowRatio() const
{
    if (Config_->IntegralGuarantees->GuaranteeType == EIntegralGuaranteeType::None) {
        return 0;
    }
    return GetMaxResourceRatio(ToJobResources(Config_->IntegralGuarantees->ResourceFlow, {}), TotalResourceLimits_);
}

TResourceVector TSchedulerPoolElement::GetIntegralShareLimitForRelaxedPool() const
{
    YT_VERIFY(GetIntegralGuaranteeType() == EIntegralGuaranteeType::Relaxed);
    auto multiplier = Config_->IntegralGuarantees->RelaxedShareMultiplierLimit.value_or(TreeConfig_->IntegralGuarantees->RelaxedShareMultiplierLimit);
    return TResourceVector::FromDouble(Attributes_.ResourceFlowRatio) * multiplier;
}

bool TSchedulerPoolElement::CanAcceptFreeVolume() const
{
    return Config_->IntegralGuarantees->CanAcceptFreeVolume;
}

bool TSchedulerPoolElement::ShouldDistributeFreeVolumeAmongChildren() const
{
    return Config_->IntegralGuarantees->ShouldDistributeFreeVolumeAmongChildren.value_or(
        TreeConfig_->ShouldDistributeFreeVolumeAmongChildren);
}

bool TSchedulerPoolElement::AreDetailedLogsEnabled() const
{
    return Config_->EnableDetailedLogs;
}

TGuid TSchedulerPoolElement::GetObjectId() const
{
    return ObjectId_;
}

const TOffloadingSettings& TSchedulerPoolElement::GetOffloadingSettings() const
{
    return Config_->OffloadingSettings;
}

std::optional<bool> TSchedulerPoolElement::IsIdleCpuPolicyAllowed() const
{
    if (Config_->AllowIdleCpuPolicy.has_value()) {
        return *Config_->AllowIdleCpuPolicy;
    }

    return Parent_->IsIdleCpuPolicyAllowed();
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerOperationElementFixedState::TSchedulerOperationElementFixedState(
    IOperationStrategyHost* operation,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    TSchedulingTagFilter schedulingTagFilter)
    : OperationId_(operation->GetId())
    , OperationHost_(operation)
    , ControllerConfig_(std::move(controllerConfig))
    , UserName_(operation->GetAuthenticatedUser())
    , Type_(operation->GetType())
    , TrimmedAnnotations_(operation->GetTrimmedAnnotations())
    , SchedulingTagFilter_(std::move(schedulingTagFilter))
{ }

////////////////////////////////////////////////////////////////////////////////

TSchedulerOperationElement::TSchedulerOperationElement(
    TFairShareStrategyTreeConfigPtr treeConfig,
    TStrategyOperationSpecPtr spec,
    TOperationFairShareTreeRuntimeParametersPtr runtimeParameters,
    TFairShareStrategyOperationControllerPtr controller,
    TFairShareStrategyOperationControllerConfigPtr controllerConfig,
    TFairShareStrategyOperationStatePtr state,
    ISchedulerStrategyHost* strategyHost,
    IFairShareTreeElementHost* treeElementHost,
    IOperationStrategyHost* operation,
    const TString& treeId,
    const NLogging::TLogger& logger)
    : TSchedulerElement(
        strategyHost,
        treeElementHost,
        std::move(treeConfig),
        treeId,
        ToString(operation->GetId()),
        EResourceTreeElementKind::Operation,
        logger.WithTag("OperationId: %v", operation->GetId()))
    , TSchedulerOperationElementFixedState(operation, std::move(controllerConfig), TSchedulingTagFilter(spec->SchedulingTagFilter))
    , Spec_(std::move(spec))
    , RuntimeParameters_(std::move(runtimeParameters))
    , Controller_(std::move(controller))
    , FairShareStrategyOperationState_(std::move(state))
{ }

TSchedulerOperationElement::TSchedulerOperationElement(
    const TSchedulerOperationElement& other,
    TSchedulerCompositeElement* clonedParent)
    : TSchedulerElement(other, clonedParent)
    , TSchedulerOperationElementFixedState(other)
    , Spec_(other.Spec_)
    , RuntimeParameters_(other.RuntimeParameters_)
    , Controller_(other.Controller_)
{ }

std::optional<double> TSchedulerOperationElement::GetSpecifiedFairShareStarvationTolerance() const
{
    return std::nullopt;
}

std::optional<TDuration> TSchedulerOperationElement::GetSpecifiedFairShareStarvationTimeout() const
{
    return std::nullopt;
}

void TSchedulerOperationElement::DisableNonAliveElements()
{ }

void TSchedulerOperationElement::PreUpdateBottomUp(NVectorHdrf::TFairShareUpdateContext* context)
{
    YT_VERIFY(Mutable_);

    TotalNeededResources_ = Controller_->GetNeededResources().GetNeededResourcesForTree(TreeId_);
    PendingAllocationCount_ = TotalNeededResources_.GetUserSlots();
    DetailedMinNeededAllocationResources_ = Controller_->GetDetailedMinNeededAllocationResources();
    AggregatedMinNeededAllocationResources_ = Controller_->GetAggregatedMinNeededAllocationResources();
    ScheduleAllocationBackoffCheckEnabled_ = Controller_->ScheduleAllocationBackoffObserved();

    UnschedulableReason_ = ComputeUnschedulableReason();
    ResourceUsageAtUpdate_ = GetInstantResourceUsage();
    // Must be calculated after ResourceUsageAtUpdate_
    ResourceDemand_ = ComputeResourceDemand();
    Tentative_ = RuntimeParameters_->Tentative;
    StartTime_ = OperationHost_->GetStartTime();

    TSchedulerElement::PreUpdateBottomUp(context);

    // NB(eshcherbin): This is a hotfix, see YT-19127.
    if (Spec_->ApplySpecifiedResourceLimitsToDemand && MaybeSpecifiedResourceLimits_) {
        TotalNeededResources_ = Max(
            Min(ResourceDemand_, *MaybeSpecifiedResourceLimits_) - ResourceUsageAtUpdate_,
            TJobResources());
        ResourceDemand_ = ResourceUsageAtUpdate_ + TotalNeededResources_;
        PendingAllocationCount_ = TotalNeededResources_.GetUserSlots();
    }

    // NB: It was moved from regular fair share update for performing split.
    // It can be performed in fair share thread as second step of preupdate.
    if (context->Now >= PersistentAttributes_.LastBestAllocationShareUpdateTime + TreeConfig_->BestAllocationShareUpdatePeriod &&
        AreTotalResourceLimitsStable())
    {
        auto allocationLimits = GetAdjustedResourceLimits(
            ResourceDemand_,
            TotalResourceLimits_,
            GetHost()->GetExecNodeMemoryDistribution(SchedulingTagFilter_ & TreeConfig_->NodesFilter));
        PersistentAttributes_.BestAllocationShare = TResourceVector::FromJobResources(allocationLimits, TotalResourceLimits_);
        PersistentAttributes_.LastBestAllocationShareUpdateTime = context->Now;

        YT_LOG_DEBUG("Updated operation best allocation share (AdjustedResourceLimits: %v, TotalResourceLimits: %v, BestAllocationShare: %.6g)",
            FormatResources(allocationLimits),
            FormatResources(TotalResourceLimits_),
            PersistentAttributes_.BestAllocationShare);
    }

    for (const auto& allocationResourcesWithQuota : DetailedMinNeededAllocationResources_) {
        for (auto [index, _] : allocationResourcesWithQuota.DiskQuota().DiskSpacePerMedium) {
            DiskRequestMedia_.insert(index);
        }
    }
}

void TSchedulerOperationElement::BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context)
{
    ResetSchedulableCounters();
    if (IsSchedulable()) {
        ++SchedulableElementCount_;
        ++SchedulableOperationCount_;
    } else {
        ++context->UnschedulableReasons[*UnschedulableReason_];
        PostUpdateAttributes_.UnschedulableOperationsResourceUsage = GetInstantResourceUsage();
    }
}

void TSchedulerOperationElement::UpdateRecursiveAttributes()
{
    TSchedulerElement::UpdateRecursiveAttributes();

    // TODO(eshcherbin): Consider deleting this option from operation spec, as it is useless.
    if (auto unpreemptibleAllocationCount = Spec_->MaxUnpreemptibleRunningAllocationCount) {
        auto effectiveThresholdConfig = EffectiveNonPreemptibleResourceUsageThresholdConfig_->Clone();
        if (effectiveThresholdConfig->UserSlots) {
            effectiveThresholdConfig->UserSlots = std::min(
                *effectiveThresholdConfig->UserSlots,
                *unpreemptibleAllocationCount);
        } else {
            effectiveThresholdConfig->UserSlots = *unpreemptibleAllocationCount;
        }

        EffectiveNonPreemptibleResourceUsageThresholdConfig_ = std::move(effectiveThresholdConfig);
    }
}

void TSchedulerOperationElement::OnFifoSchedulableElementCountLimitReached(TFairSharePostUpdateContext* context)
{
    UnschedulableReason_ = EUnschedulableReason::FifoSchedulableElementCountLimitReached;
    ++context->UnschedulableReasons[*UnschedulableReason_];
    PostUpdateAttributes_.UnschedulableOperationsResourceUsage = GetInstantResourceUsage();
}

void TSchedulerOperationElement::UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config)
{
    YT_VERIFY(Mutable_);
    ControllerConfig_ = config;
}

void TSchedulerOperationElement::BuildLoggingStringAttributes(TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    TSchedulerElement::BuildLoggingStringAttributes(delimitedBuilder);

    delimitedBuilder->AppendFormat(
        "PendingAllocations: %v, AggregatedMinNeededResources: %v",
        PendingAllocationCount_,
        AggregatedMinNeededAllocationResources_);
}

bool TSchedulerOperationElement::AreDetailedLogsEnabled() const
{
    return RuntimeParameters_->EnableDetailedLogs;
}

TString TSchedulerOperationElement::GetId() const
{
    return ToString(OperationId_);
}

TOperationId TSchedulerOperationElement::GetOperationId() const
{
    return OperationId_;
}

void TSchedulerOperationElement::SetRuntimeParameters(TOperationFairShareTreeRuntimeParametersPtr runtimeParameters)
{
    RuntimeParameters_ = std::move(runtimeParameters);

    Controller_->SetDetailedLogsEnabled(RuntimeParameters_->EnableDetailedLogs);
}

TOperationFairShareTreeRuntimeParametersPtr TSchedulerOperationElement::GetRuntimeParameters() const
{
    return RuntimeParameters_;
}

std::optional<bool> TSchedulerOperationElement::IsAggressiveStarvationEnabled() const
{
    // TODO(eshcherbin): There is no way we really want to have this option in operation spec.
    return Spec_->EnableAggressiveStarvation;
}

TJobResourcesConfigPtr TSchedulerOperationElement::GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const
{
    return {};
}

std::optional<double> TSchedulerOperationElement::GetSpecifiedWeight() const
{
    return RuntimeParameters_->Weight;
}

const NVectorHdrf::TJobResourcesConfig* TSchedulerOperationElement::GetStrongGuaranteeResourcesConfig() const
{
    return Spec_->StrongGuaranteeResources.Get();
}

TResourceVector TSchedulerOperationElement::GetMaxShare() const
{
    return TResourceVector::FromDouble(Spec_->MaxShareRatio.value_or(1.0));
}

const TFairShareStrategyOperationStatePtr& TSchedulerOperationElement::GetFairShareStrategyOperationState() const
{
    return FairShareStrategyOperationState_;
}

const TSchedulingTagFilter& TSchedulerOperationElement::GetSchedulingTagFilter() const
{
    return SchedulingTagFilter_;
}

ESchedulableStatus TSchedulerOperationElement::GetStatus() const
{
    if (UnschedulableReason_) {
        return ESchedulableStatus::Normal;
    }

    double tolerance = EffectiveFairShareStarvationTolerance_;
    if (Dominates(Attributes_.FairShare.Total + TResourceVector::Epsilon(), Attributes_.DemandShare)) {
        tolerance = 1.0;
    }

    return TSchedulerElement::GetStatusImpl(tolerance);
}

void TSchedulerOperationElement::SetStarvationStatus(EStarvationStatus starvationStatus)
{
    YT_VERIFY(Mutable_);

    if (starvationStatus == EStarvationStatus::NonStarving) {
        PersistentAttributes_.LastNonStarvingTime = TInstant::Now();
    }

    auto currentStarvationStatus = GetStarvationStatus();
    if (starvationStatus != currentStarvationStatus) {
        YT_LOG_INFO("Operation starvation status changed (Current: %v, New: %v)",
            currentStarvationStatus,
            starvationStatus);

        TSchedulerElement::SetStarvationStatus(starvationStatus);
    }
}

void TSchedulerOperationElement::CheckForStarvation(TInstant now)
{
    YT_VERIFY(Mutable_);

    auto fairShareStarvationTimeout = EffectiveFairShareStarvationTimeout_;
    auto fairShareAggressiveStarvationTimeout = TreeConfig_->FairShareAggressiveStarvationTimeout;

    double allocationCountRatio = GetPendingAllocationCount() / TreeConfig_->AllocationCountPreemptionTimeoutCoefficient;
    if (allocationCountRatio < 1.0) {
        fairShareStarvationTimeout *= allocationCountRatio;
        fairShareAggressiveStarvationTimeout *= allocationCountRatio;
    }

    TSchedulerElement::CheckForStarvationImpl(
        fairShareStarvationTimeout,
        fairShareAggressiveStarvationTimeout,
        now);
}

TInstant TSchedulerOperationElement::GetLastNonStarvingTime() const
{
    return PersistentAttributes_.LastNonStarvingTime;
}

int TSchedulerOperationElement::GetSlotIndex() const
{
    return SlotIndex_;
}

TString TSchedulerOperationElement::GetUserName() const
{
    return UserName_;
}

EOperationType TSchedulerOperationElement::GetOperationType() const
{
    return Type_;
}

const TYsonString& TSchedulerOperationElement::GetTrimmedAnnotations() const
{
    return TrimmedAnnotations_;
}

TResourceVector TSchedulerOperationElement::GetBestAllocationShare() const
{
    return PersistentAttributes_.BestAllocationShare;
}

bool TSchedulerOperationElement::IsGang() const
{
    return Spec_->IsGang;
}

void TSchedulerOperationElement::BuildElementMapping(TFairSharePostUpdateContext* context)
{
    if (Parent_->IsEnabledChild(this)) {
        context->EnabledOperationIdToElement.emplace(OperationId_, this);
    } else {
        context->DisabledOperationIdToElement.emplace(OperationId_, this);
    }
}

TSchedulerElementPtr TSchedulerOperationElement::Clone(TSchedulerCompositeElement* clonedParent)
{
    return New<TSchedulerOperationElement>(*this, clonedParent);
}

ESchedulerElementType TSchedulerOperationElement::GetType() const
{
    return ESchedulerElementType::Operation;
}

bool TSchedulerOperationElement::IsSchedulable() const
{
    return !UnschedulableReason_;
}

std::optional<EUnschedulableReason> TSchedulerOperationElement::ComputeUnschedulableReason() const
{
    auto result = OperationHost_->CheckUnschedulable(TreeId_);
    if (!result && IsMaxScheduleAllocationCallsViolated()) {
        result = EUnschedulableReason::MaxScheduleAllocationCallsViolated;
    }
    return result;
}

TControllerEpoch TSchedulerOperationElement::GetControllerEpoch() const
{
    return Controller_->GetEpoch();
}

void TSchedulerOperationElement::OnScheduleAllocationStarted(const ISchedulingContextPtr& schedulingContext)
{
    Controller_->OnScheduleAllocationStarted(schedulingContext);
}

void TSchedulerOperationElement::OnScheduleAllocationFinished(const ISchedulingContextPtr& schedulingContext)
{
    Controller_->OnScheduleAllocationFinished(schedulingContext);
}

bool TSchedulerOperationElement::IsMaxScheduleAllocationCallsViolated() const
{
    return Controller_->CheckMaxScheduleAllocationCallsOverdraft(
        Spec_->MaxConcurrentControllerScheduleAllocationCalls.value_or(
            ControllerConfig_->MaxConcurrentControllerScheduleAllocationCalls));
}

bool TSchedulerOperationElement::IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(
    const ISchedulingContextPtr& schedulingContext) const
{
    return Controller_->IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(schedulingContext);
}

bool TSchedulerOperationElement::IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(
    const ISchedulingContextPtr& schedulingContext) const
{
    return Controller_->IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(schedulingContext);
}

bool TSchedulerOperationElement::HasRecentScheduleAllocationFailure(NProfiling::TCpuInstant now) const
{
    return Controller_->HasRecentScheduleAllocationFailure(now);
}

bool TSchedulerOperationElement::IsSaturatedInTentativeTree(
    NProfiling::TCpuInstant now,
    const TString& treeId,
    TDuration saturationDeactivationTimeout) const
{
    return Controller_->IsSaturatedInTentativeTree(now, treeId, saturationDeactivationTimeout);
}

TControllerScheduleAllocationResultPtr TSchedulerOperationElement::ScheduleAllocation(
    const ISchedulingContextPtr& context,
    const TJobResources& availableResources,
    const TDiskResources& availableDiskResources,
    TDuration timeLimit,
    const TString& treeId,
    const TFairShareStrategyTreeConfigPtr& treeConfig)
{
    return Controller_->ScheduleAllocation(
        context,
        availableResources,
        availableDiskResources,
        timeLimit,
        treeId,
        GetParent()->GetFullPath(/*explicitOnly*/ false), treeConfig);
}

void TSchedulerOperationElement::OnScheduleAllocationFailed(
    TCpuInstant now,
    const TString& treeId,
    const TControllerScheduleAllocationResultPtr& scheduleAllocationResult)
{
    Controller_->OnScheduleAllocationFailed(now, treeId, scheduleAllocationResult);
}

void TSchedulerOperationElement::AbortAllocation(
    TAllocationId allocationId,
    EAbortReason abortReason,
    TControllerEpoch allocationEpoch)
{
    Controller_->AbortAllocation(allocationId, abortReason, allocationEpoch);
}

TJobResources TSchedulerOperationElement::GetAggregatedInitialMinNeededResources() const
{
    // COMPAT(eshcherbin)
    if (auto cypressMinNeededResources = OperationHost_->GetAggregatedInitialMinNeededResources()) {
        return *cypressMinNeededResources;
    }

    return Controller_->GetAggregatedInitialMinNeededAllocationResources();
}

EResourceTreeIncreaseResult TSchedulerOperationElement::TryIncreaseHierarchicalResourceUsagePrecommit(
    const TJobResources& delta,
    TJobResources* availableResourceLimitsOutput)
{
    return TreeElementHost_->GetResourceTree()->TryIncreaseHierarchicalResourceUsagePrecommit(
        ResourceTreeElement_,
        delta,
        availableResourceLimitsOutput);
}

void TSchedulerOperationElement::IncreaseHierarchicalResourceUsage(const TJobResources& delta)
{
    TreeElementHost_->GetResourceTree()->IncreaseHierarchicalResourceUsage(ResourceTreeElement_, delta);
}

void TSchedulerOperationElement::DecreaseHierarchicalResourceUsagePrecommit(const TJobResources& precommittedResources)
{
    TreeElementHost_->GetResourceTree()->IncreaseHierarchicalResourceUsagePrecommit(ResourceTreeElement_, -precommittedResources);
}

void TSchedulerOperationElement::CommitHierarchicalResourceUsage(const TJobResources& resourceUsage, const TJobResources& precommitedResources)
{
    TreeElementHost_->GetResourceTree()->CommitHierarchicalResourceUsage(ResourceTreeElement_, resourceUsage, precommitedResources);
}

void TSchedulerOperationElement::ReleaseResources(bool markAsNonAlive)
{
    TreeElementHost_->GetResourceTree()->ReleaseResources(ResourceTreeElement_, markAsNonAlive);
}

TJobResources TSchedulerOperationElement::ComputeResourceDemand() const
{
    auto maybeUnschedulableReason = OperationHost_->CheckUnschedulable(TreeId_);
    if (maybeUnschedulableReason == EUnschedulableReason::IsNotRunning || maybeUnschedulableReason == EUnschedulableReason::Suspended) {
        return ResourceUsageAtUpdate_;
    }
    return ResourceUsageAtUpdate_ + TotalNeededResources_;
}

TJobResourcesConfigPtr TSchedulerOperationElement::GetSpecifiedResourceLimitsConfig() const
{
    return RuntimeParameters_->ResourceLimits;
}

void TSchedulerOperationElement::AttachParent(TSchedulerCompositeElement* newParent, int slotIndex)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(!Parent_);

    Parent_ = newParent;
    SlotIndex_ = slotIndex;
    TreeElementHost_->GetResourceTree()->AttachParent(ResourceTreeElement_, newParent->ResourceTreeElement_);

    newParent->IncreaseOperationCount(1);
    newParent->AddChild(this, /*enabled*/ false);

    YT_LOG_DEBUG("Operation attached to pool (Pool: %v)", newParent->GetId());
}

void TSchedulerOperationElement::ChangeParent(TSchedulerCompositeElement* parent, int slotIndex)
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(Parent_);

    SlotIndex_ = slotIndex;

    auto oldParentId = Parent_->GetId();
    if (RunningInThisPoolTree_) {
        if (IsLightweight()) {
            Parent_->IncreaseLightweightRunningOperationCount(-1);
        } else {
            Parent_->IncreaseRunningOperationCount(-1);
        }
    }
    Parent_->IncreaseOperationCount(-1);
    bool enabled = Parent_->IsEnabledChild(this);
    Parent_->RemoveChild(this);

    Parent_ = parent;
    TreeElementHost_->GetResourceTree()->ChangeParent(
        ResourceTreeElement_,
        parent->ResourceTreeElement_,
        /*descendantOperationElements*/ std::nullopt);

    RunningInThisPoolTree_ = false;  // for consistency
    Parent_->IncreaseOperationCount(1);
    Parent_->AddChild(this, enabled);

    YT_LOG_DEBUG("Operation changed pool (OldPool: %v, NewPool: %v)",
        oldParentId,
        parent->GetId());
}

void TSchedulerOperationElement::DetachParent()
{
    YT_VERIFY(Mutable_);
    YT_VERIFY(Parent_);

    auto parentId = Parent_->GetId();
    if (RunningInThisPoolTree_) {
        if (IsLightweight()) {
            Parent_->IncreaseLightweightRunningOperationCount(-1);
        } else {
            Parent_->IncreaseRunningOperationCount(-1);
        }
    }
    Parent_->IncreaseOperationCount(-1);
    Parent_->RemoveChild(this);

    Parent_ = nullptr;
    TreeElementHost_->GetResourceTree()->ScheduleDetachParent(ResourceTreeElement_);

    YT_LOG_DEBUG("Operation detached from pool (Pool: %v)", parentId);
}

void TSchedulerOperationElement::MarkOperationRunningInPool()
{
    bool lightweight = IsLightweight();
    if (lightweight) {
        Parent_->IncreaseLightweightRunningOperationCount(1);
    } else {
        Parent_->IncreaseRunningOperationCount(1);
    }
    RunningInThisPoolTree_ = true;
    PendingByPool_.reset();

    YT_LOG_INFO("Operation is running in pool (Pool: %v, Lightweight: %v)",
        Parent_->GetId(),
        lightweight);
}

bool TSchedulerOperationElement::IsOperationRunningInPool() const
{
    return RunningInThisPoolTree_;
}

// NB(eshcherbin): Lightweight operations are a special kind of operations which aren't counted in pool's running operation count,
// therefore their count is not as limited as for regular operations. From strategy's point of view, there operations should be
// quick to be fully scheduled and not consume much precious time during scheduling heartbeat.
// Operation is lightweight iff it is eligible and it's running in a pool where lightweight operations are enabled.
// Currently, only vanilla operations are considered eligible.
bool TSchedulerOperationElement::IsLightweightEligible() const
{
    // TODO(eshcherbin): Do we want to restrict this to only vanilla operations with a single job?
    return Type_ == EOperationType::Vanilla;
}

bool TSchedulerOperationElement::IsLightweight() const
{
    return IsLightweightEligible() && Parent_->AreLightweightOperationsEnabled();
}

void TSchedulerOperationElement::MarkPendingBy(TSchedulerCompositeElement* violatedPool)
{
    violatedPool->PendingOperationIds().push_back(OperationId_);
    PendingByPool_ = violatedPool->GetId();

    YT_LOG_DEBUG("Operation is pending since max running operation count is violated (OperationId: %v, Pool: %v, Limit: %v)",
        OperationId_,
        violatedPool->GetId(),
        violatedPool->GetMaxRunningOperationCount());
}

bool TSchedulerOperationElement::IsLimitingAncestorCheckEnabled() const
{
    return Spec_->EnableLimitingAncestorCheck;
}

void TSchedulerOperationElement::CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const
{
    elements->push_back(ResourceTreeElement_);
}

bool TSchedulerOperationElement::IsIdleCpuPolicyAllowed() const
{
    if (Spec_->AllowIdleCpuPolicy.has_value()) {
        return *Spec_->AllowIdleCpuPolicy;
    }

    auto parent = GetParent();
    while (parent) {
        auto isAllowed = GetParent()->IsIdleCpuPolicyAllowed();
        if (isAllowed.has_value()) {
            return *isAllowed;
        }
        parent = parent->GetParent();
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

TSchedulerRootElement::TSchedulerRootElement(
    ISchedulerStrategyHost* strategyHost,
    IFairShareTreeElementHost* treeElementHost,
    TFairShareStrategyTreeConfigPtr treeConfig,
    const TString& treeId,
    const NLogging::TLogger& logger)
    : TSchedulerCompositeElement(
        strategyHost,
        treeElementHost,
        treeConfig,
        treeId,
        RootPoolName,
        EResourceTreeElementKind::Root,
        logger.WithTag("Pool: %v, SchedulingMode: %v",
            RootPoolName,
            ESchedulingMode::FairShare))
{
    Mode_ = ESchedulingMode::FairShare;
}

TSchedulerRootElement::TSchedulerRootElement(const TSchedulerRootElement& other)
    : TSchedulerCompositeElement(other, nullptr)
    , TSchedulerRootElementFixedState(other)
{ }

void TSchedulerRootElement::PreUpdate(NVectorHdrf::TFairShareUpdateContext* context)
{
    YT_VERIFY(Mutable_);

    TForbidContextSwitchGuard contextSwitchGuard;

    DisableNonAliveElements();

    PreUpdateBottomUp(context);
}

/// Steps of fair share post update:
///
/// 1. Publish the computed fair share to the shared resource tree and update the operations' preemptible allocation lists.
///
/// 2. Update dynamic attributes based on the calculated fair share (for orchid).
void TSchedulerRootElement::PostUpdate(TFairSharePostUpdateContext* postUpdateContext)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetFairShareUpdateInvoker());

    YT_VERIFY(Mutable_);

    BuildSchedulableChildrenLists(postUpdateContext);

    // Calculate tree sizes.
    int schedulableElementCount = EnumerateElements(/*startIndex*/ 0, /*isSchedulableValueFilter*/ true);
    YT_VERIFY(schedulableElementCount == SchedulableElementCount_);
    TreeSize_ = EnumerateElements(/*startIndex*/ schedulableElementCount, /*isSchedulableValueFilter*/ false);

    BuildElementMapping(postUpdateContext);

    UpdateRecursiveAttributes();

    ComputeSatisfactionRatioAtUpdate();
    BuildPoolSatisfactionDigests(postUpdateContext);
}

const TSchedulingTagFilter& TSchedulerRootElement::GetSchedulingTagFilter() const
{
    return EmptySchedulingTagFilter;
}

TString TSchedulerRootElement::GetId() const
{
    return RootPoolName;
}

std::optional<double> TSchedulerRootElement::GetSpecifiedWeight() const
{
    return std::nullopt;
}

TJobResources TSchedulerRootElement::GetSpecifiedStrongGuaranteeResources() const
{
    return TotalResourceLimits_;
}

TResourceVector TSchedulerRootElement::GetMaxShare() const
{
    return TResourceVector::Ones();
}

std::optional<double> TSchedulerRootElement::GetSpecifiedFairShareStarvationTolerance() const
{
    return TreeConfig_->FairShareStarvationTolerance;
}

std::optional<TDuration> TSchedulerRootElement::GetSpecifiedFairShareStarvationTimeout() const
{
    return TreeConfig_->FairShareStarvationTimeout;
}

std::optional<bool> TSchedulerRootElement::IsAggressiveStarvationEnabled() const
{
    return TreeConfig_->EnableAggressiveStarvation;
}

TJobResourcesConfigPtr TSchedulerRootElement::GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const
{
    return TreeConfig_->NonPreemptibleResourceUsageThreshold;
}

std::optional<EFifoPoolSchedulingOrder> TSchedulerRootElement::GetSpecifiedFifoPoolSchedulingOrder() const
{
    return TreeConfig_->FifoPoolSchedulingOrder;
}

std::optional<bool> TSchedulerRootElement::ShouldUsePoolSatisfactionForScheduling() const
{
    return TreeConfig_->UsePoolSatisfactionForScheduling;
}

void TSchedulerRootElement::BuildPoolSatisfactionDigests(TFairSharePostUpdateContext* postUpdateContext)
{
    PostUpdateAttributes_.SatisfactionDigest = CreateHistogramDigest(TreeConfig_->PerPoolSatisfactionDigest);
    for (const auto& [_, pool] : postUpdateContext->PoolNameToElement) {
        pool->PostUpdateAttributes_.SatisfactionDigest = CreateHistogramDigest(TreeConfig_->PerPoolSatisfactionDigest);
    }

    for (const auto& [_, operation] : postUpdateContext->EnabledOperationIdToElement) {
        double operationSatisfaction = operation->PostUpdateAttributes().SatisfactionRatio;
        auto* ancestor = operation->GetMutableParent();
        while (ancestor) {
            const auto& digest = ancestor->PostUpdateAttributes().SatisfactionDigest;
            YT_ASSERT(digest);
            digest->AddSample(operationSatisfaction);

            ancestor = ancestor->GetMutableParent();
        }
    }
}

void TSchedulerRootElement::CheckForStarvation(TInstant /*now*/)
{ }

int TSchedulerRootElement::GetMaxRunningOperationCount() const
{
    return TreeConfig_->MaxRunningOperationCount;
}

int TSchedulerRootElement::GetMaxOperationCount() const
{
    return TreeConfig_->MaxOperationCount;
}

bool TSchedulerRootElement::AreLightweightOperationsEnabled() const
{
    return false;
}

TPoolIntegralGuaranteesConfigPtr TSchedulerRootElement::GetIntegralGuaranteesConfig() const
{
    return New<TPoolIntegralGuaranteesConfig>();
}

std::vector<EFifoSortParameter> TSchedulerRootElement::GetFifoSortParameters() const
{
    YT_ABORT();
}

bool TSchedulerRootElement::AreImmediateOperationsForbidden() const
{
    return TreeConfig_->ForbidImmediateOperationsInRoot;
}

bool TSchedulerRootElement::IsEphemeralHub() const
{
    return false;
}

THashSet<TString> TSchedulerRootElement::GetAllowedProfilingTags() const
{
    return {};
}

bool TSchedulerRootElement::IsFairShareTruncationInFifoPoolEnabled() const
{
    return TreeConfig_->EnableFairShareTruncationInFifoPool;
}

bool TSchedulerRootElement::ShouldComputePromisedGuaranteeFairShare() const
{
    return false;
}

bool TSchedulerRootElement::CanAcceptFreeVolume() const
{
    // This value is not used.
    return false;
}

bool TSchedulerRootElement::ShouldDistributeFreeVolumeAmongChildren() const
{
    return false;
}

bool TSchedulerRootElement::IsInferringChildrenWeightsFromHistoricUsageEnabled() const
{
    return false;
}

TJobResourcesConfigPtr TSchedulerRootElement::GetSpecifiedResourceLimitsConfig() const
{
    return {};
}

TDuration TSchedulerRootElement::GetHistoricUsageAggregatorPeriod() const
{
    return TDuration::Zero();
}

void TSchedulerRootElement::BuildResourceMetering(
    const std::optional<TMeteringKey>& /*parentKey*/,
    const THashMap<TString, TResourceVolume>& poolResourceUsages,
    TMeteringMap* meteringMap) const
{
    auto key = TMeteringKey{
        .AbcId = StrategyHost_->GetDefaultAbcId(),
        .TreeId = GetTreeId(),
        .PoolId = GetId(),
    };

    TResourceVolume accumulatedResourceUsageVolume;
    {
        auto it = poolResourceUsages.find(GetId());
        if (it != poolResourceUsages.end()) {
            accumulatedResourceUsageVolume = it->second;
        }
    }

    TJobResources TotalStrongGuaranteeResources;
    for (const auto& child : EnabledChildren_) {
        TotalStrongGuaranteeResources += child->GetSpecifiedStrongGuaranteeResources();
    }

    auto insertResult = meteringMap->insert({
        key,
        TMeteringStatistics(
            /*strongGuaranteeResources*/ TotalStrongGuaranteeResources,
            /*resourceFlow*/ {},
            /*burstGuaranteResources*/ {},
            GetResourceUsageAtUpdate(),
            accumulatedResourceUsageVolume)});
    YT_VERIFY(insertResult.second);

    for (const auto& child : EnabledChildren_) {
        child->BuildResourceMetering(/*parentKey*/ key, poolResourceUsages, meteringMap);
    }
}

TSchedulerElementPtr TSchedulerRootElement::Clone(TSchedulerCompositeElement* /*clonedParent*/)
{
    YT_ABORT();
}

TSchedulerRootElementPtr TSchedulerRootElement::Clone()
{
    return New<TSchedulerRootElement>(*this);
}

ESchedulerElementType TSchedulerRootElement::GetType() const
{
    return ESchedulerElementType::Root;
}

bool TSchedulerRootElement::IsDefaultConfigured() const
{
    return false;
}

TResourceDistributionInfo TSchedulerRootElement::GetResourceDistributionInfo() const
{
    double maxDistributedIntegralRatio = std::max(Attributes_.TotalBurstRatio, Attributes_.TotalResourceFlowRatio);
    double undistributedResourceFlowRatio = std::max(Attributes_.TotalBurstRatio - Attributes_.TotalResourceFlowRatio, 0.0);
    double undistributedBurstGuaranteeRatio = std::max(Attributes_.TotalResourceFlowRatio - Attributes_.TotalBurstRatio, 0.0);

    TResourceDistributionInfo info;
    for (const auto& child : EnabledChildren_) {
        info.DistributedStrongGuaranteeResources += child->GetSpecifiedStrongGuaranteeResources();
    }
    info.DistributedResourceFlow = TotalResourceLimits_ * Attributes_.TotalResourceFlowRatio;
    info.DistributedBurstGuaranteeResources = TotalResourceLimits_ * Attributes_.TotalBurstRatio;
    info.DistributedResources = info.DistributedStrongGuaranteeResources + TotalResourceLimits_ * maxDistributedIntegralRatio;
    info.UndistributedResources = TotalResourceLimits_ - info.DistributedResources;
    info.UndistributedResourceFlow = TotalResourceLimits_ * undistributedResourceFlowRatio;
    info.UndistributedBurstGuaranteeResources = TotalResourceLimits_ * undistributedBurstGuaranteeRatio;

    return info;
}

void TSchedulerRootElement::BuildResourceDistributionInfo(TFluentMap fluent) const
{
    auto info = GetResourceDistributionInfo();
    fluent
        .Item("distributed_strong_guarantee_resources").Value(info.DistributedStrongGuaranteeResources)
        .Item("distributed_resource_flow").Value(info.DistributedResourceFlow)
        .Item("distributed_burst_guarantee_resources").Value(info.DistributedBurstGuaranteeResources)
        .Item("distributed_resources").Value(info.DistributedResources)
        .Item("undistributed_resources").Value(info.UndistributedResources)
        .Item("undistributed_resource_flow").Value(info.UndistributedResourceFlow)
        .Item("undistributed_burst_guarantee_resources").Value(info.UndistributedBurstGuaranteeResources);
}

double TSchedulerRootElement::GetSpecifiedBurstRatio() const
{
    return 0.0;
}

double TSchedulerRootElement::GetSpecifiedResourceFlowRatio() const
{
    return 0.0;
}

TGuid TSchedulerRootElement::GetObjectId() const
{
    return {};
}

const TOffloadingSettings& TSchedulerRootElement::GetOffloadingSettings() const
{
    return EmptyOffloadingSettings;
}

std::optional<bool> TSchedulerRootElement::IsIdleCpuPolicyAllowed() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
