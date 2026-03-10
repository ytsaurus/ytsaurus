#include "pool_tree_element.h"

#include "private.h"
#include "operation.h"
#include "resource_tree_element.h"
#include "operation_controller.h"
#include "job_resources_helpers.h"

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/digest.h>
#include <yt/yt/core/misc/string_builder.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/generic/ymath.h>

#include <math.h>

namespace NYT::NScheduler::NStrategy {

using namespace NPolicy;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NYson;
using namespace NYTree;
using namespace NProfiling;
using namespace NControllerAgent;

using NProfiling::CpuDurationToDuration;
using NVectorHdrf::ToJobResources;

////////////////////////////////////////////////////////////////////////////////

static const TString InvalidCustomProfilingTag("invalid");

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

TPoolTreeElementFixedState::TPoolTreeElementFixedState(
    IStrategyHost* strategyHost,
    IPoolTreeElementHost* treeElementHost,
    TStrategyTreeConfigPtr treeConfig,
    std::string treeId)
    : TreeConfig_(std::move(treeConfig))
    , StrategyHost_(strategyHost)
    , TreeElementHost_(treeElementHost)
    , TotalResourceLimits_(strategyHost->GetResourceLimits(TreeConfig_->NodeTagFilter))
    , TreeId_(std::move(treeId))
{ }

////////////////////////////////////////////////////////////////////////////////

void TPoolTreeElement::MarkImmutable()
{
    Mutable_ = false;
}

int TPoolTreeElement::EnumerateElements(int startIndex, bool isSchedulableValueFilter)
{
    YT_VERIFY(Mutable_);

    if (isSchedulableValueFilter == IsSchedulable()) {
        TreeIndex_ = startIndex++;
    }

    return startIndex;
}

void TPoolTreeElement::UpdateTreeConfig(const TStrategyTreeConfigPtr& config)
{
    YT_VERIFY(Mutable_);

    TreeConfig_ = config;
}

void TPoolTreeElement::InitializeUpdate(TInstant /*now*/)
{
    YT_VERIFY(Mutable_);

    MaybeSpecifiedResourceLimits_ = ComputeMaybeSpecifiedResourceLimits();
    SpecifiedResourceLimitsOvercommitTolerance_ = ComputeSpecifiedResourceLimitsOvercommitTolerance();

    if (PersistentAttributes_.AppliedSpecifiedResourceLimits != MaybeSpecifiedResourceLimits_ ||
        SpecifiedResourceLimitsOvercommitTolerance_ != PersistentAttributes_.AppliedSpecifiedResourceLimitsOvercommitTolerance)
    {
        std::vector<TResourceTreeElementPtr> descendantOperationElements;
        if (!IsOperation() && !PersistentAttributes_.AppliedSpecifiedResourceLimits && MaybeSpecifiedResourceLimits_) {
            // NB: This code executed in control thread, therefore tree structure is actual and agreed with tree structure of resource tree.
            CollectResourceTreeOperationElements(&descendantOperationElements);
        }

        YT_LOG_INFO(
            "Updating applied specified resource limits "
            "(NewSpecifiedResourceLimits: %v, CurrentSpecifiedResourceLimits: %v, "
            "NewOvercommitTolerance: %v, CurrentOvercommitTolerance: %v)",
            MaybeSpecifiedResourceLimits_,
            PersistentAttributes_.AppliedSpecifiedResourceLimits,
            SpecifiedResourceLimitsOvercommitTolerance_,
            PersistentAttributes_.AppliedSpecifiedResourceLimitsOvercommitTolerance);

        ResourceTreeElement_->SetSpecifiedResourceLimits(
            MaybeSpecifiedResourceLimits_,
            SpecifiedResourceLimitsOvercommitTolerance_,
            descendantOperationElements);

        PersistentAttributes_.AppliedSpecifiedResourceLimits = MaybeSpecifiedResourceLimits_;
        PersistentAttributes_.AppliedSpecifiedResourceLimitsOvercommitTolerance = SpecifiedResourceLimitsOvercommitTolerance_;
    }
}

void TPoolTreeElement::PreUpdate(TFairSharePreUpdateContext* context)
{
    YT_VERIFY(Mutable_);

    // NB: The order of computation must not be changed.
    TotalResourceLimits_ = context->TotalResourceLimits;
    SchedulingTagFilterResourceLimits_ = ComputeSchedulingTagFilterResourceLimits(context);
    ResourceLimits_ = ComputeResourceLimits();
}

void TPoolTreeElement::ComputeSatisfactionRatioAtUpdate()
{
    YT_VERIFY(Mutable_);

    PostUpdateAttributes_.LocalSatisfactionRatio = ComputeLocalSatisfactionRatio(ResourceUsageAtUpdate_);
    PostUpdateAttributes_.SatisfactionRatio = PostUpdateAttributes_.LocalSatisfactionRatio;
}

void TPoolTreeElement::ResetSchedulableCounters()
{
    SchedulableElementCount_ = 0;
    SchedulablePoolCount_ = 0;
    SchedulableOperationCount_ = 0;
}

const TSchedulingTagFilter& TPoolTreeElement::GetSchedulingTagFilter() const
{
    return EmptySchedulingTagFilter;
}

void TPoolTreeElement::BuildLoggingStringAttributes(TDelimitedStringBuilderWrapper& delimitedBuilder) const
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

TString TPoolTreeElement::GetLoggingString(const TPoolTreeSnapshotPtr& treeSnapshot) const
{
    TStringBuilder builder;
    builder.AppendFormat("Scheduling info for tree %Qv = {", GetTreeId());

    TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
    BuildLoggingStringAttributes(delimitedBuilder);
    TreeElementHost_->BuildElementLoggingStringAttributes(treeSnapshot, this, delimitedBuilder);

    builder.AppendString("}");

    return builder.Flush();
}

double TPoolTreeElement::GetWeight() const
{
    auto specifiedWeight = GetSpecifiedWeight();

    if (auto parent = GetParent();
        parent && parent->GetMaybeHistoricUsageAggregatorPeriod().has_value())
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

    if (selfGuaranteeDominantShare < NVectorHdrf::Epsilon) {
        return 1.0;
    }

    double parentGuaranteeDominantShare = 1.0;
    if (GetParent()) {
        parentGuaranteeDominantShare = MaxComponent(GetParent()->Attributes().StrongGuaranteeShare) + GetParent()->Attributes().TotalResourceFlowRatio;
    }

    if (parentGuaranteeDominantShare < NVectorHdrf::Epsilon) {
        return 1.0;
    }

    return selfGuaranteeDominantShare *
        (*TreeConfig_->InferWeightFromGuaranteesShareMultiplier) /
        parentGuaranteeDominantShare;
}

TSchedulableAttributes& TPoolTreeElement::Attributes()
{
    return Attributes_;
}

const TSchedulableAttributes& TPoolTreeElement::Attributes() const
{
    return Attributes_;
}

const TJobResources& TPoolTreeElement::GetResourceDemand() const
{
    return ResourceDemand_;
}

const TJobResources& TPoolTreeElement::GetResourceUsageAtUpdate() const
{
    return ResourceUsageAtUpdate_;
}

const TJobResources& TPoolTreeElement::GetResourceLimits() const
{
    return ResourceLimits_;
}

const NVectorHdrf::TJobResourcesConfig* TPoolTreeElement::GetStrongGuaranteeResourcesConfig() const
{
    return nullptr;
}

TJobResources TPoolTreeElement::GetSpecifiedStrongGuaranteeResources() const
{
    const auto* guaranteeConfig = GetStrongGuaranteeResourcesConfig();
    YT_VERIFY(guaranteeConfig);
    return NVectorHdrf::ToJobResources(*guaranteeConfig, {});
}

TJobResources TPoolTreeElement::GetSpecifiedBurstGuaranteeResources() const
{
    return {};
}

TJobResources TPoolTreeElement::GetSpecifiedResourceFlow() const
{
    return {};
}

TPoolTreeCompositeElement* TPoolTreeElement::GetMutableParent()
{
    return Parent_;
}

const TPoolTreeCompositeElement* TPoolTreeElement::GetParent() const
{
    return Parent_;
}

NVectorHdrf::TCompositeElement* TPoolTreeElement::GetParentElement() const
{
    return Parent_;
}

i64 TPoolTreeElement::GetPendingAllocationCount() const
{
    return PendingAllocationCount_;
}

ESchedulableStatus TPoolTreeElement::GetStatus() const
{
    return ESchedulableStatus::Normal;
}

EStarvationStatus TPoolTreeElement::GetStarvationStatus() const
{
    return PersistentAttributes_.StarvationStatus;
}

void TPoolTreeElement::SetStarvationStatus(EStarvationStatus starvationStatus, TInstant /*now*/)
{
    YT_VERIFY(Mutable_);

    PersistentAttributes_.StarvationStatus = starvationStatus;
}

std::optional<TJobResources> TPoolTreeElement::ComputeMaybeSpecifiedResourceLimits() const
{
    if (auto limitsConfig = GetSpecifiedResourceLimitsConfig(); limitsConfig && limitsConfig->IsNonTrivial()) {
        return ToJobResources(limitsConfig, TJobResources::Infinite());
    }

    return {};
}

TJobResources TPoolTreeElement::ComputeSpecifiedResourceLimitsOvercommitTolerance() const
{
    return ToJobResources(GetSpecifiedResourceLimitsOvercommitToleranceConfig(), TJobResources());
}

bool TPoolTreeElement::AreSpecifiedResourceLimitsViolated() const
{
    return ResourceTreeElement_->AreSpecifiedResourceLimitsViolated();
}

TJobResources TPoolTreeElement::GetInstantResourceUsage(bool withPrecommit) const
{
    auto resourceUsage = withPrecommit
        ? ResourceTreeElement_->GetResourceUsageWithPrecommit()
        : ResourceTreeElement_->GetResourceUsage();
    if (resourceUsage.GetUserSlots() > 0 && resourceUsage.GetMemory() == 0) {
        YT_LOG_WARNING("Found usage of schedulable element with non-zero user slots and zero memory (ElementId: %v, Usage: %v)",
            GetId(),
            FormatResources(resourceUsage));
    }
    return resourceUsage;
}

TResourceTreeElement::TDetailedResourceUsage TPoolTreeElement::GetInstantDetailedResourceUsage() const
{
    return  ResourceTreeElement_->GetDetailedResourceUsage();
}

double TPoolTreeElement::GetResourceDominantUsageShareAtUpdate() const
{
    return MaxComponent(Attributes_.UsageShare);
}

std::string TPoolTreeElement::GetTreeId() const
{
    return TreeId_;
}

bool TPoolTreeElement::CheckAvailableDemand(const TJobResources& delta)
{
    return ResourceTreeElement_->CheckAvailableDemand(delta, GetResourceDemand());
}

TPoolTreeElement::TPoolTreeElement(
    IStrategyHost* strategyHost,
    IPoolTreeElementHost* treeElementHost,
    TStrategyTreeConfigPtr treeConfig,
    std::string treeId,
    TString id,
    EResourceTreeElementKind elementKind,
    const NLogging::TLogger& logger)
    : TPoolTreeElementFixedState(strategyHost, treeElementHost, std::move(treeConfig), std::move(treeId))
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

TPoolTreeElement::TPoolTreeElement(
    const TPoolTreeElement& other,
    TPoolTreeCompositeElement* clonedParent)
    : TPoolTreeElementFixedState(other)
    , ResourceTreeElement_(other.ResourceTreeElement_)
    , Logger(other.Logger)
{
    Parent_ = clonedParent;
}

IStrategyHost* TPoolTreeElement::GetHost() const
{
    YT_VERIFY(Mutable_);

    return StrategyHost_;
}

double TPoolTreeElement::ComputeLocalSatisfactionRatio(const TJobResources& resourceUsage) const
{
    const auto& fairShare = Attributes_.FairShare.Total;

    // Check for corner cases.
    if (Dominates(TResourceVector::Epsilon(), fairShare)) {
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

bool TPoolTreeElement::IsResourceBlocked(EJobResourceType resource) const
{
    // Fair share may be slightly greater than demand share due to precision errors. See: YT-15359.
    return Attributes_.FairShare.Total[resource] >= Attributes_.DemandShare[resource];
}

bool TPoolTreeElement::AreAllResourcesBlocked() const
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
bool TPoolTreeElement::IsStrictlyDominatesNonBlocked(const TResourceVector& lhs, const TResourceVector& rhs) const
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

ESchedulableStatus TPoolTreeElement::GetStatusImpl(double tolerance) const
{
    // Fair share may be slightly greater than demand share due to precision errors. See: YT-15359.
    auto adjustedFairShareBound = TResourceVector::Min(Attributes_.FairShare.Total * tolerance, Attributes_.DemandShare);
    if (IsStrictlyDominatesNonBlocked(adjustedFairShareBound, Attributes_.UsageShare)) {
        return ESchedulableStatus::BelowFairShare;
    }

    return ESchedulableStatus::Normal;
}

void TPoolTreeElement::CheckForStarvationImpl(
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
                SetStarvationStatus(EStarvationStatus::AggressivelyStarving, now);
            } else if (now > *PersistentAttributes_.BelowFairShareSince + fairShareStarvationTimeout) {
                SetStarvationStatus(EStarvationStatus::Starving, now);
            }
            break;

        case ESchedulableStatus::Normal:
            PersistentAttributes_.BelowFairShareSince = std::nullopt;
            SetStarvationStatus(EStarvationStatus::NonStarving, now);
            break;

        default:
            YT_ABORT();
    }
}

TJobResources TPoolTreeElement::ComputeResourceLimits() const
{
    auto limits = GetSchedulingTagFilterResourceLimits();
    if (MaybeSpecifiedResourceLimits_) {
        limits = Min(limits, *MaybeSpecifiedResourceLimits_);
    }

    return limits;
}

bool TPoolTreeElement::AreTotalResourceLimitsStable() const
{
    auto connectionTime = InstantToCpuInstant(StrategyHost_->GetConnectionTime());
    auto timeout = DurationToCpuDuration(TreeConfig_->NodeReconnectionTimeout);
    auto now = GetCpuInstant();
    return now >= connectionTime + timeout;
}

TJobResources TPoolTreeElement::ComputeSchedulingTagFilterResourceLimits(TFairSharePreUpdateContext* context) const
{
    // Shortcut: if the scheduling tag filter is empty then we just use the resource limits for
    // the tree's nodes filter, which were computed earlier in PreUpdate.
    if (GetSchedulingTagFilter() == EmptySchedulingTagFilter) {
        return TotalResourceLimits_;
    }

    if (!AreTotalResourceLimitsStable()) {
        // Return infinity during some time after scheduler reconnection.
        return TJobResources::Infinite();
    }

    auto tagFilter = TreeConfig_->NodeTagFilter & GetSchedulingTagFilter();

    auto& resourceLimitsByTagFilter = context->ResourceLimitsByTagFilter;
    auto it = resourceLimitsByTagFilter.find(tagFilter);
    if (it != resourceLimitsByTagFilter.end()) {
        return it->second;
    }

    auto result = GetHost()->GetResourceLimits(tagFilter);
    EmplaceOrCrash(resourceLimitsByTagFilter, tagFilter, result);
    return result;
}

TJobResources TPoolTreeElement::GetSchedulingTagFilterResourceLimits() const
{
    return SchedulingTagFilterResourceLimits_;
}

TJobResources TPoolTreeElement::GetTotalResourceLimits() const
{
    return TotalResourceLimits_;
}

TJobResourcesConfigPtr TPoolTreeElement::GetSpecifiedResourceLimitsOvercommitToleranceConfig() const
{
    static const TJobResourcesConfigPtr EmptyConfig = New<TJobResourcesConfig>();
    return EmptyConfig;
}

void TPoolTreeElement::BuildResourceMetering(
    const std::optional<TMeteringKey>& /*lowestMeteredAncestorKey*/,
    const THashMap<TString, TResourceVolume>& /*poolResourceUsages*/,
    TMeteringMap* /*statistics*/) const
{ }

bool TPoolTreeElement::IsDemandFullySatisfied() const
{
    return Dominates(Attributes_.FairShare.Total + TResourceVector::Epsilon(), Attributes_.DemandShare);
}

double TPoolTreeElement::GetAccumulatedResourceRatioVolume() const
{
    return PersistentAttributes_.IntegralResourcesState.AccumulatedVolume.GetMinResourceRatio(TotalResourceLimits_);
}

TResourceVolume TPoolTreeElement::GetAccumulatedResourceVolume() const
{
    return PersistentAttributes_.IntegralResourcesState.AccumulatedVolume;
}

void TPoolTreeElement::InitAccumulatedResourceVolume(TResourceVolume resourceVolume)
{
    YT_VERIFY(PersistentAttributes_.IntegralResourcesState.AccumulatedVolume == TResourceVolume());
    PersistentAttributes_.IntegralResourcesState.AccumulatedVolume = resourceVolume;
}

bool TPoolTreeElement::IsAlive() const
{
    return ResourceTreeElement_->GetAlive();
}

void TPoolTreeElement::SetNonAlive()
{
    ResourceTreeElement_->SetNonAlive();
}

TJobResources TPoolTreeElement::GetResourceUsageWithPrecommit() const
{
    return ResourceTreeElement_->GetResourceUsageWithPrecommit();
}

const NLogging::TLogger& TPoolTreeElement::GetLogger() const
{
    return Logger;
}

bool TPoolTreeElement::AreDetailedLogsEnabled() const
{
    return false;
}

// TODO(eshcherbin): Refactor or rename, because limited demand share is not a recursive attribute.
void TPoolTreeElement::UpdateRecursiveAttributes()
{
    YT_VERIFY(Mutable_);

    LimitedDemandShare_ = ComputeLimitedDemandShare();
}

void TPoolTreeElement::UpdateStarvationStatuses(TInstant now, bool enablePoolStarvation)
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

TPoolTreeCompositeElement::TPoolTreeCompositeElement(
    IStrategyHost* strategyHost,
    IPoolTreeElementHost* treeElementHost,
    TStrategyTreeConfigPtr treeConfig,
    const std::string& treeId,
    const TString& id,
    EResourceTreeElementKind elementKind,
    const NLogging::TLogger& logger)
    : TPoolTreeElement(strategyHost, treeElementHost, std::move(treeConfig), treeId, id, elementKind, logger)
{ }

TPoolTreeCompositeElement::TPoolTreeCompositeElement(
    const TPoolTreeCompositeElement& other,
    TPoolTreeCompositeElement* clonedParent)
    : TPoolTreeElement(other, clonedParent)
    , TPoolTreeCompositeElementFixedState(other)
{
    auto cloneChildren = [&] (
        const std::vector<TPoolTreeElementPtr>& list,
        THashMap<TPoolTreeElementPtr, int>* clonedMap,
        std::vector<TPoolTreeElementPtr>* clonedList)
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

void TPoolTreeCompositeElement::MarkImmutable()
{
    TPoolTreeElement::MarkImmutable();
    for (const auto& child : EnabledChildren_) {
        child->MarkImmutable();
    }
    for (const auto& child : DisabledChildren_) {
        child->MarkImmutable();
    }
}

int TPoolTreeCompositeElement::EnumerateElements(int startIndex, bool isSchedulableValueFilter)
{
    YT_VERIFY(Mutable_);

    startIndex = TPoolTreeElement::EnumerateElements(startIndex, isSchedulableValueFilter);
    for (const auto& child : EnabledChildren_) {
        startIndex = child->EnumerateElements(startIndex, isSchedulableValueFilter);
    }
    return startIndex;
}

void TPoolTreeCompositeElement::DisableNonAliveElements()
{
    std::vector<TPoolTreeElementPtr> childrenToDisable;
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

void TPoolTreeCompositeElement::UpdateTreeConfig(const TStrategyTreeConfigPtr& config)
{
    YT_VERIFY(Mutable_);

    TPoolTreeElement::UpdateTreeConfig(config);

    auto updateChildrenConfig = [&config] (TChildList& list) {
        for (const auto& child : list) {
            child->UpdateTreeConfig(config);
        }
    };

    updateChildrenConfig(EnabledChildren_);
    updateChildrenConfig(DisabledChildren_);
}

void TPoolTreeCompositeElement::InitializeUpdate(TInstant now)
{
    YT_VERIFY(Mutable_);

    ResourceUsageAtUpdate_ = {};
    ResourceDemand_ = {};

    for (const auto& child : EnabledChildren_) {
        child->InitializeUpdate(now);

        ResourceUsageAtUpdate_ += child->GetResourceUsageAtUpdate();
        ResourceDemand_ += child->GetResourceDemand();
        PendingAllocationCount_ += child->GetPendingAllocationCount();

        if (auto historicUsageAggregationPeriod = GetMaybeHistoricUsageAggregatorPeriod()) {
            // NB(eshcherbin): This is a lazy parameters update so it has to be done every time.
            child->PersistentAttributes_.HistoricUsageAggregator.SetHalflife(*historicUsageAggregationPeriod);

            // TODO(eshcherbin): Should we use vectors instead of ratios?
            // Yes, but nobody uses this feature yet, so it's not really important.
            // NB(eshcherbin): |child->Attributes().UsageShare| is not calculated at this stage yet, so we do it manually.
            auto usageShare = TResourceVector::FromJobResources(child->GetResourceUsageAtUpdate(), child->GetTotalResourceLimits());
            child->PersistentAttributes_.HistoricUsageAggregator.UpdateAt(now, MaxComponent(usageShare));
        }
    }

    TPoolTreeElement::InitializeUpdate(now);
}

void TPoolTreeCompositeElement::PreUpdate(TFairSharePreUpdateContext* context)
{
    YT_VERIFY(Mutable_);
    for (const auto& child : EnabledChildren_) {
        child->PreUpdate(context);
    }

    TPoolTreeElement::PreUpdate(context);
}

void TPoolTreeCompositeElement::BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context)
{
    PostUpdateAttributes_.UnschedulableOperationsResourceUsage = TJobResources();
    SchedulableChildren_.clear();

    ResetSchedulableCounters();
    auto updateSchedulableCounters = [&] (const TPoolTreeElementPtr& child) {
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
        std::vector<TPoolTreeOperationElement*> sortedChildren;
        for (const auto& child : EnabledChildren_) {
            YT_VERIFY(child->IsOperation());
            sortedChildren.push_back(dynamic_cast<TPoolTreeOperationElement*>(child.Get()));
        }

        std::sort(
            sortedChildren.begin(),
            sortedChildren.end(),
            [&] (const TPoolTreeOperationElement* lhs, const TPoolTreeOperationElement* rhs) {
                return lhs->Attributes().FifoIndex < rhs->Attributes().FifoIndex;
            });

        for (auto* child : sortedChildren) {
            child->BuildSchedulableChildrenLists(context);
            if (child->IsSchedulable()) {
                bool shouldSkip = SchedulableElementCount_ >= *maxSchedulableElementCount &&
                    Dominates(TResourceVector::Epsilon(), child->Attributes().FairShare.Total);
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

void TPoolTreeCompositeElement::ComputeSatisfactionRatioAtUpdate()
{
    TPoolTreeElement::ComputeSatisfactionRatioAtUpdate();

    auto isBetterChild = [&] (const TPoolTreeElement* lhs, const TPoolTreeElement* rhs) {
        return lhs->PostUpdateAttributes().SatisfactionRatio < rhs->PostUpdateAttributes().SatisfactionRatio;
    };

    TPoolTreeElement* bestChild = nullptr;
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
}

void TPoolTreeCompositeElement::BuildElementMapping(TFairSharePostUpdateContext* context)
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

void TPoolTreeCompositeElement::IncreaseOperationCount(int delta)
{
    DoIncreaseOperationCount(delta, &TPoolTreeCompositeElement::OperationCount_);
}

void TPoolTreeCompositeElement::IncreaseRunningOperationCount(int delta)
{
    DoIncreaseOperationCount(delta, &TPoolTreeCompositeElement::RunningOperationCount_);
}

void TPoolTreeCompositeElement::IncreaseLightweightRunningOperationCount(int delta)
{
    DoIncreaseOperationCount(delta, &TPoolTreeCompositeElement::LightweightRunningOperationCount_);
}

void TPoolTreeCompositeElement::DoIncreaseOperationCount(int delta, int TPoolTreeCompositeElement::* operationCounter)
{
    auto* current = this;
    while (current) {
        current->*operationCounter += delta;
        current = current->GetMutableParent();
    }
}

bool TPoolTreeCompositeElement::IsSchedulable() const
{
    return IsRoot() || !SchedulableChildren_.empty();
}

bool TPoolTreeCompositeElement::IsExplicit() const
{
    return false;
}

void TPoolTreeCompositeElement::AddChild(TPoolTreeElement* child, bool enabled)
{
    YT_VERIFY(Mutable_);

    if (enabled) {
        child->PersistentAttributes_.ResetOnElementEnabled();
    }

    if (child->IsOperation()) {
        auto* childOperation = static_cast<TPoolTreeOperationElement*>(child);
        childOperation->UpdatePoolAttributes(IsDefaultConfigured());
    }

    auto& map = enabled ? EnabledChildToIndex_ : DisabledChildToIndex_;
    auto& list = enabled ? EnabledChildren_ : DisabledChildren_;
    AddChild(&map, &list, child);
}

void TPoolTreeCompositeElement::EnableChild(const TPoolTreeElementPtr& child)
{
    YT_VERIFY(Mutable_);

    child->PersistentAttributes_.ResetOnElementEnabled();

    RemoveChild(&DisabledChildToIndex_, &DisabledChildren_, child);
    AddChild(&EnabledChildToIndex_, &EnabledChildren_, child);
}

void TPoolTreeCompositeElement::DisableChild(const TPoolTreeElementPtr& child)
{
    YT_VERIFY(Mutable_);

    if (EnabledChildToIndex_.find(child) == EnabledChildToIndex_.end()) {
        return;
    }

    RemoveChild(&EnabledChildToIndex_, &EnabledChildren_, child);
    AddChild(&DisabledChildToIndex_, &DisabledChildren_, child);
}

void TPoolTreeCompositeElement::RemoveChild(TPoolTreeElement* child)
{
    YT_VERIFY(Mutable_);

    bool enabled = ContainsChild(EnabledChildToIndex_, child);
    auto& map = enabled ? EnabledChildToIndex_ : DisabledChildToIndex_;
    auto& list = enabled ? EnabledChildren_ : DisabledChildren_;
    RemoveChild(&map, &list, child);
}

bool TPoolTreeCompositeElement::IsEnabledChild(TPoolTreeElement* child)
{
    return ContainsChild(EnabledChildToIndex_, child);
}

bool TPoolTreeCompositeElement::IsEmpty() const
{
    return EnabledChildren_.empty() && DisabledChildren_.empty();
}

void TPoolTreeCompositeElement::CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const
{
    for (const auto& child : EnabledChildren_) {
        child->CollectResourceTreeOperationElements(elements);
    }
}

NVectorHdrf::TElement* TPoolTreeCompositeElement::GetChild(int index)
{
    return EnabledChildren_[index].Get();
}

const NVectorHdrf::TElement* TPoolTreeCompositeElement::GetChild(int index) const
{
    return EnabledChildren_[index].Get();
}

int TPoolTreeCompositeElement::GetChildCount() const
{
    return EnabledChildren_.size();
}

std::vector<TPoolTreeOperationElement*> TPoolTreeCompositeElement::GetChildOperations() const
{
    std::vector<TPoolTreeOperationElement*> result;
    result.reserve(std::size(EnabledChildren_) + std::size(DisabledChildren_));

    for (const auto& child : EnabledChildren_) {
        if (child->IsOperation()) {
            result.push_back(static_cast<TPoolTreeOperationElement*>(child.Get()));
        }
    }
    for (const auto& child : DisabledChildren_) {
        if (child->IsOperation()) {
            result.push_back(static_cast<TPoolTreeOperationElement*>(child.Get()));
        }
    }

    return result;
}

int TPoolTreeCompositeElement::GetChildOperationCount() const noexcept
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

int TPoolTreeCompositeElement::GetChildPoolCount() const noexcept
{
    int count = 0;
    for (const auto& child : EnabledChildren_) {
        if (!child->IsOperation()) {
            ++count;
        }
    }
    return count;
}

ESchedulingMode TPoolTreeCompositeElement::GetMode() const
{
    return Mode_;
}

bool TPoolTreeCompositeElement::HasHigherPriorityInFifoMode(const NVectorHdrf::TElement* lhs, const NVectorHdrf::TElement* rhs) const
{
    const auto* lhsElement = dynamic_cast<const TPoolTreeOperationElement*>(lhs);
    const auto* rhsElement = dynamic_cast<const TPoolTreeOperationElement*>(rhs);

    YT_VERIFY(lhsElement);
    YT_VERIFY(rhsElement);

    return HasHigherPriorityInFifoMode(lhsElement, rhsElement);
}

bool TPoolTreeCompositeElement::IsStepFunctionForGangOperationsEnabled() const
{
    return true;
}

const std::vector<TPoolTreeElementPtr>& TPoolTreeCompositeElement::EnabledChildren() const
{
    return EnabledChildren_;
}

void TPoolTreeCompositeElement::AddChild(
    TChildMap* map,
    TChildList* list,
    const TPoolTreeElementPtr& child)
{
    list->push_back(child);
    YT_VERIFY(map->emplace(child, list->size() - 1).second);
}

void TPoolTreeCompositeElement::RemoveChild(
    TChildMap* map,
    TChildList* list,
    const TPoolTreeElementPtr& child)
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

bool TPoolTreeCompositeElement::ContainsChild(
    const TChildMap& map,
    const TPoolTreeElementPtr& child)
{
    return map.find(child) != map.end();
}

bool TPoolTreeCompositeElement::HasHigherPriorityInFifoMode(const TPoolTreeOperationElement* lhs, const TPoolTreeOperationElement* rhs) const
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

int TPoolTreeCompositeElement::GetAvailableRunningOperationCount() const
{
    return std::max(GetMaxRunningOperationCount() - RunningOperationCount_, 0);
}

bool TPoolTreeCompositeElement::GetEffectiveLightweightOperationsEnabled() const
{
    return AreLightweightOperationsEnabled() && Mode_ == ESchedulingMode::Fifo;
}

TResourceVolume TPoolTreeCompositeElement::GetIntegralPoolCapacity() const
{
    return TResourceVolume(TotalResourceLimits_ * Attributes_.ResourceFlowRatio, TreeConfig_->IntegralGuarantees->PoolCapacitySaturationPeriod);
}

void TPoolTreeCompositeElement::UpdateRecursiveAttributes()
{
    YT_VERIFY(Mutable_);

    TPoolTreeElement::UpdateRecursiveAttributes();

    for (const auto& child : EnabledChildren_) {
        child->UpdateRecursiveAttributes();
    }
}

void TPoolTreeCompositeElement::UpdateStarvationStatuses(TInstant now, bool enablePoolStarvation)
{
    YT_VERIFY(Mutable_);

    TPoolTreeElement::UpdateStarvationStatuses(now, enablePoolStarvation);

    for (const auto& child : EnabledChildren_) {
        child->UpdateStarvationStatuses(now, enablePoolStarvation);
    }
}

TYPath TPoolTreeCompositeElement::GetFullPath(bool explicitOnly, bool withTreeId) const
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

std::optional<std::string> TPoolTreeCompositeElement::GetRedirectToCluster() const
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TPoolTreePoolElementFixedState::TPoolTreePoolElementFixedState(TString id, NObjectClient::TObjectId objectId)
    : Id_(std::move(id))
    , ObjectId_(objectId)
{ }

////////////////////////////////////////////////////////////////////////////////

TPoolTreePoolElement::TPoolTreePoolElement(
    IStrategyHost* strategyHost,
    IPoolTreeElementHost* treeElementHost,
    const TString& id,
    TGuid objectId,
    TPoolConfigPtr config,
    bool defaultConfigured,
    TStrategyTreeConfigPtr treeConfig,
    const std::string& treeId,
    const NLogging::TLogger& logger)
    : TPoolTreeCompositeElement(
        strategyHost,
        treeElementHost,
        std::move(treeConfig),
        treeId,
        id,
        EResourceTreeElementKind::Pool,
        logger.WithTag("Pool: %v, SchedulingMode: %v",
            id,
            config->Mode))
    , TPoolTreePoolElementFixedState(id, objectId)
{
    DoSetConfig(std::move(config));
    DefaultConfigured_ = defaultConfigured;
}

TPoolTreePoolElement::TPoolTreePoolElement(const TPoolTreePoolElement& other, TPoolTreeCompositeElement* clonedParent)
    : TPoolTreeCompositeElement(other, clonedParent)
    , TPoolTreePoolElementFixedState(other)
    , Config_(other.Config_)
{ }

bool TPoolTreePoolElement::IsDefaultConfigured() const
{
    return DefaultConfigured_;
}

bool TPoolTreePoolElement::IsEphemeralInDefaultParentPool() const
{
    return EphemeralInDefaultParentPool_;
}

void TPoolTreePoolElement::SetUserName(const std::optional<std::string>& userName)
{
    UserName_ = userName;
}

const std::optional<std::string>& TPoolTreePoolElement::GetUserName() const
{
    return UserName_;
}

TPoolConfigPtr TPoolTreePoolElement::GetConfig() const
{
    return Config_;
}

void TPoolTreePoolElement::SetConfig(TPoolConfigPtr config)
{
    YT_VERIFY(Mutable_);

    DoSetConfig(std::move(config));
    DefaultConfigured_ = false;

    PropagatePoolAttributesToOperations();
}

void TPoolTreePoolElement::SetDefaultConfig()
{
    YT_VERIFY(Mutable_);

    DoSetConfig(New<TPoolConfig>());
    DefaultConfigured_ = true;

    PropagatePoolAttributesToOperations();
}

void TPoolTreePoolElement::SetObjectId(NObjectClient::TObjectId objectId)
{
    YT_VERIFY(Mutable_);

    ObjectId_ = objectId;
}

void TPoolTreePoolElement::SetEphemeralInDefaultParentPool()
{
    YT_VERIFY(Mutable_);

    EphemeralInDefaultParentPool_ = true;
}

bool TPoolTreePoolElement::IsExplicit() const
{
    // NB: This is no coincidence.
    return !DefaultConfigured_;
}

std::optional<bool> TPoolTreePoolElement::IsAggressiveStarvationEnabled() const
{
    return Config_->EnableAggressiveStarvation;
}

TJobResourcesConfigPtr TPoolTreePoolElement::GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const
{
    return Config_->NonPreemptibleResourceUsageThreshold;
}

std::optional<TDuration> TPoolTreePoolElement::GetSpecifiedWaitingForResourcesOnNodeTimeout() const
{
    return Config_->WaitingForResourcesOnNodeTimeout;
}

TString TPoolTreePoolElement::GetId() const
{
    return Id_;
}

std::optional<double> TPoolTreePoolElement::GetSpecifiedWeight() const
{
    return Config_->Weight;
}

const NVectorHdrf::TJobResourcesConfig* TPoolTreePoolElement::GetStrongGuaranteeResourcesConfig() const
{
    return Config_->StrongGuaranteeResources.Get();
}

EIntegralGuaranteeType TPoolTreePoolElement::GetIntegralGuaranteeType() const
{
    return Config_->IntegralGuarantees->GuaranteeType;
}

const TIntegralResourcesState& TPoolTreePoolElement::IntegralResourcesState() const
{
    return PersistentAttributes_.IntegralResourcesState;
}

TIntegralResourcesState& TPoolTreePoolElement::IntegralResourcesState()
{
    return PersistentAttributes_.IntegralResourcesState;
}

ESchedulableStatus TPoolTreePoolElement::GetStatus() const
{
    return TPoolTreeElement::GetStatusImpl(EffectiveFairShareStarvationTolerance_);
}

void TPoolTreePoolElement::UpdateRecursiveAttributes()
{
    YT_VERIFY(Mutable_);

    EffectiveFairShareStarvationTolerance_ = GetSpecifiedFairShareStarvationTolerance().value_or(
        Parent_->GetEffectiveFairShareStarvationTolerance());

    EffectiveFairShareStarvationTimeout_ = GetSpecifiedFairShareStarvationTimeout().value_or(
        Parent_->GetEffectiveFairShareStarvationTimeout());

    EffectiveAggressiveStarvationEnabled_ = IsAggressiveStarvationEnabled().value_or(
        Parent_->GetEffectiveAggressiveStarvationEnabled());

    EffectiveNonPreemptibleResourceUsageThresholdConfig_ = Parent_->EffectiveNonPreemptibleResourceUsageThresholdConfig();
    if (const auto& specifiedConfig = GetSpecifiedNonPreemptibleResourceUsageThresholdConfig()) {
        EffectiveNonPreemptibleResourceUsageThresholdConfig_ = specifiedConfig;
    }

    EffectiveWaitingForResourcesOnNodeTimeout_ = Parent_->GetEffectiveWaitingForResourcesOnNodeTimeout();
    if (auto specifiedTimeout = GetSpecifiedWaitingForResourcesOnNodeTimeout()) {
        EffectiveWaitingForResourcesOnNodeTimeout_ = specifiedTimeout;
    }

    TPoolTreeCompositeElement::UpdateRecursiveAttributes();
}

std::optional<double> TPoolTreePoolElement::GetSpecifiedFairShareStarvationTolerance() const
{
    return Config_->FairShareStarvationTolerance;
}

std::optional<TDuration> TPoolTreePoolElement::GetSpecifiedFairShareStarvationTimeout() const
{
    return Config_->FairShareStarvationTimeout;
}

void TPoolTreePoolElement::SetStarvationStatus(EStarvationStatus starvationStatus, TInstant now)
{
    YT_VERIFY(Mutable_);

    if (starvationStatus != GetStarvationStatus()) {
        YT_LOG_INFO("Pool starvation status changed (Current: %v, New: %v)",
            GetStarvationStatus(),
            starvationStatus);
    }
    TPoolTreeElement::SetStarvationStatus(starvationStatus, now);
}

void TPoolTreePoolElement::CheckForStarvation(TInstant now)
{
    YT_VERIFY(Mutable_);

    TPoolTreeElement::CheckForStarvationImpl(
        EffectiveFairShareStarvationTimeout_,
        TreeConfig_->FairShareAggressiveStarvationTimeout,
        now);
}

const TSchedulingTagFilter& TPoolTreePoolElement::GetSchedulingTagFilter() const
{
    return SchedulingTagFilter_;
}

int TPoolTreePoolElement::GetMaxRunningOperationCount() const
{
    return Config_->MaxRunningOperationCount.value_or(TreeConfig_->MaxRunningOperationCountPerPool);
}

bool TPoolTreePoolElement::AreLightweightOperationsEnabled() const
{
    return Config_->EnableLightweightOperations;
}

int TPoolTreePoolElement::GetMaxOperationCount() const
{
    return Config_->MaxOperationCount.value_or(TreeConfig_->MaxOperationCountPerPool);
}

bool TPoolTreePoolElement::IsPriorityStrongGuaranteeAdjustmentEnabled() const
{
    return Config_->EnablePriorityStrongGuaranteeAdjustment;
}

bool TPoolTreePoolElement::IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled() const
{
    return Config_->EnablePriorityStrongGuaranteeAdjustmentDonorship;
}

TPoolIntegralGuaranteesConfigPtr TPoolTreePoolElement::GetIntegralGuaranteesConfig() const
{
    return Config_->IntegralGuarantees;
}

TJobResources TPoolTreePoolElement::GetSpecifiedBurstGuaranteeResources() const
{
    return ToJobResources(Config_->IntegralGuarantees->BurstGuaranteeResources, {});
}

TJobResources TPoolTreePoolElement::GetSpecifiedResourceFlow() const
{
    return ToJobResources(Config_->IntegralGuarantees->ResourceFlow, {});
}

std::vector<EFifoSortParameter> TPoolTreePoolElement::GetFifoSortParameters() const
{
    return FifoSortParameters_;
}

bool TPoolTreePoolElement::AreImmediateOperationsForbidden() const
{
    return Config_->ForbidImmediateOperations;
}

bool TPoolTreePoolElement::AreGangOperationsAllowed() const
{
    auto mode = [&] {
        if (Config_->CreateEphemeralSubpools) {
            return Config_->EphemeralSubpoolConfig
                ? (*Config_->EphemeralSubpoolConfig)->Mode
                : ESchedulingMode::FairShare;
        }

        return Mode_;
    }();
    return mode == ESchedulingMode::Fifo || !TreeConfig_->AllowGangOperationsOnlyInFifoPools || Config_->AlwaysAllowGangOperations;
}

bool TPoolTreePoolElement::IsEphemeralHub() const
{
    return Config_->CreateEphemeralSubpools;
}

THashSet<TString> TPoolTreePoolElement::GetAllowedProfilingTags() const
{
    return Config_->AllowedProfilingTags;
}

bool TPoolTreePoolElement::IsStepFunctionForGangOperationsEnabled() const
{
    return Config_->EnableStepFunctionForGangOperations;
}

bool TPoolTreePoolElement::ShouldComputePromisedGuaranteeFairShare() const
{
    return Config_->ComputePromisedGuaranteeFairShare;
}

std::optional<TDuration> TPoolTreePoolElement::GetMaybeHistoricUsageAggregatorPeriod() const
{
    return Config_->HistoricUsageAggregationPeriod;
}

void TPoolTreePoolElement::BuildResourceMetering(
    const std::optional<TMeteringKey>& lowestMeteredAncestorKey,
    const THashMap<TString, TResourceVolume>& poolResourceUsages,
    TMeteringMap* meteringMap) const
{
    YT_VERIFY(lowestMeteredAncestorKey);

    std::optional<TMeteringKey> key;
    if (Config_->Abc) {
        key = TMeteringKey{
            .AbcId  = Config_->Abc->Id,
            .TreeId = GetTreeId(),
            .PoolId = GetId(),
            .MeteringTags = Config_->MeteringTags,
        };

        auto accumulatedResourceUsageVolume = GetOrDefault(poolResourceUsages, GetId(), TResourceVolume{});
        // NB(eshcherbin): Integral guarantees are included even if the pool is not integral.
        auto meteringStatistics = TMeteringStatistics(
            GetSpecifiedStrongGuaranteeResources(),
            GetSpecifiedResourceFlow(),
            GetSpecifiedBurstGuaranteeResources(),
            GetResourceUsageAtUpdate(),
            accumulatedResourceUsageVolume);

        EmplaceOrCrash(*meteringMap, *key, meteringStatistics);
        GetOrCrash(*meteringMap, *lowestMeteredAncestorKey).DiscountChild(meteringStatistics);
    }

    for (const auto& child : EnabledChildren_) {
        child->BuildResourceMetering(
            key ? key : lowestMeteredAncestorKey,
            poolResourceUsages,
            meteringMap);
    }
}

TPoolTreeElementPtr TPoolTreePoolElement::Clone(TPoolTreeCompositeElement* clonedParent)
{
    return New<TPoolTreePoolElement>(*this, clonedParent);
}

ESchedulerElementType TPoolTreePoolElement::GetType() const
{
    return ESchedulerElementType::Pool;
}

void TPoolTreePoolElement::AttachParent(TPoolTreeCompositeElement* parent)
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

const TPoolTreeCompositeElement* TPoolTreePoolElement::GetNearestAncestorWithResourceLimits(const TPoolTreeCompositeElement* element) const
{
    do {
        if (element->PersistentAttributes().AppliedSpecifiedResourceLimits) {
            return element;
        }
        element = element->GetParent();
    } while (element);

    return nullptr;
}

void TPoolTreePoolElement::ChangeParent(TPoolTreeCompositeElement* newParent)
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

void TPoolTreePoolElement::DetachParent()
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

void TPoolTreePoolElement::DoSetConfig(TPoolConfigPtr newConfig)
{
    YT_VERIFY(Mutable_);

    Config_ = std::move(newConfig);
    FifoSortParameters_ = Config_->FifoSortParameters;
    Mode_ = Config_->Mode;
    SchedulingTagFilter_ = TSchedulingTagFilter(Config_->SchedulingTagFilter);
}

TJobResourcesConfigPtr TPoolTreePoolElement::GetSpecifiedResourceLimitsConfig() const
{
    return Config_->ResourceLimits;
}

TJobResourcesConfigPtr TPoolTreePoolElement::GetSpecifiedResourceLimitsOvercommitToleranceConfig() const
{
    return Config_->ResourceLimitsOvercommitTolerance;
}

void TPoolTreePoolElement::BuildElementMapping(TFairSharePostUpdateContext* context)
{
    context->PoolNameToElement.emplace(GetId(), this);
    TPoolTreeCompositeElement::BuildElementMapping(context);
}

double TPoolTreePoolElement::GetSpecifiedBurstRatio() const
{
    if (Config_->IntegralGuarantees->GuaranteeType == EIntegralGuaranteeType::None) {
        return 0.0;
    }
    return GetMaxResourceRatio(GetSpecifiedBurstGuaranteeResources(), TotalResourceLimits_);
}

double TPoolTreePoolElement::GetSpecifiedResourceFlowRatio() const
{
    if (Config_->IntegralGuarantees->GuaranteeType == EIntegralGuaranteeType::None) {
        return 0.0;
    }
    return GetMaxResourceRatio(GetSpecifiedResourceFlow(), TotalResourceLimits_);
}

TResourceVector TPoolTreePoolElement::GetIntegralShareLimitForRelaxedPool() const
{
    YT_VERIFY(GetIntegralGuaranteeType() == EIntegralGuaranteeType::Relaxed);
    auto multiplier = Config_->IntegralGuarantees->RelaxedShareMultiplierLimit.value_or(TreeConfig_->IntegralGuarantees->RelaxedShareMultiplierLimit);
    return TResourceVector::FromDouble(Attributes_.ResourceFlowRatio) * multiplier;
}

bool TPoolTreePoolElement::CanAcceptFreeVolume() const
{
    return Config_->IntegralGuarantees->CanAcceptFreeVolume;
}

bool TPoolTreePoolElement::ShouldDistributeFreeVolumeAmongChildren() const
{
    return Config_->IntegralGuarantees->ShouldDistributeFreeVolumeAmongChildren.value_or(
        TreeConfig_->ShouldDistributeFreeVolumeAmongChildren);
}

bool TPoolTreePoolElement::AreDetailedLogsEnabled() const
{
    return Config_->EnableDetailedLogs;
}

TGuid TPoolTreePoolElement::GetObjectId() const
{
    return ObjectId_;
}

const TOffloadingSettings& TPoolTreePoolElement::GetOffloadingSettings() const
{
    return Config_->OffloadingSettings;
}

std::optional<bool> TPoolTreePoolElement::IsIdleCpuPolicyAllowed() const
{
    if (Config_->AllowIdleCpuPolicy.has_value()) {
        return *Config_->AllowIdleCpuPolicy;
    }

    return Parent_->IsIdleCpuPolicyAllowed();
}

std::optional<std::string> TPoolTreePoolElement::GetRedirectToCluster() const
{
    return Config_->RedirectToCluster
        ? Config_->RedirectToCluster
        : Parent_->GetRedirectToCluster();
}

void TPoolTreePoolElement::PropagatePoolAttributesToOperations()
{
    for (auto* child : GetChildOperations()) {
        child->UpdatePoolAttributes(DefaultConfigured_);
    }
}

////////////////////////////////////////////////////////////////////////////////

TPoolTreeOperationElementFixedState::TPoolTreeOperationElementFixedState(
    IOperationPtr operation,
    TStrategyOperationControllerConfigPtr controllerConfig,
    TSchedulingTagFilter schedulingTagFilter)
    : OperationId_(operation->GetId())
    , Operation_(std::move(operation))
    , ControllerConfig_(std::move(controllerConfig))
    , UserName_(Operation_->GetAuthenticatedUser())
    , Type_(Operation_->GetType())
    , TrimmedAnnotations_(Operation_->GetTrimmedAnnotations())
    , SchedulingTagFilter_(std::move(schedulingTagFilter))
    , StartTime_(Operation_->GetStartTime())
{ }

////////////////////////////////////////////////////////////////////////////////

TPoolTreeOperationElement::TPoolTreeOperationElement(
    TStrategyTreeConfigPtr treeConfig,
    TStrategyOperationSpecPtr spec,
    TOperationOptionsPtr operationOptions,
    TOperationPoolTreeRuntimeParametersPtr runtimeParameters,
    TOperationControllerPtr controller,
    TStrategyOperationControllerConfigPtr controllerConfig,
    TStrategyOperationStatePtr state,
    IStrategyHost* strategyHost,
    IPoolTreeElementHost* treeElementHost,
    IOperationPtr operation,
    const std::string& treeId,
    const NLogging::TLogger& logger)
    : TPoolTreeElement(
        strategyHost,
        treeElementHost,
        std::move(treeConfig),
        treeId,
        ToString(operation->GetId()),
        EResourceTreeElementKind::Operation,
        logger.WithTag("OperationId: %v", operation->GetId()))
    , TPoolTreeOperationElementFixedState(
        std::move(operation),
        std::move(controllerConfig),
        TSchedulingTagFilter(spec->SchedulingTagFilter))
    , Spec_(std::move(spec))
    , OperationOptions_(std::move(operationOptions))
    , RuntimeParameters_(std::move(runtimeParameters))
    , Controller_(std::move(controller))
    , StrategyOperationState_(std::move(state))
{ }

TPoolTreeOperationElement::TPoolTreeOperationElement(
    const TPoolTreeOperationElement& other,
    TPoolTreeCompositeElement* clonedParent)
    : TPoolTreeElement(other, clonedParent)
    , TPoolTreeOperationElementFixedState(other)
    , Spec_(other.Spec_)
    , OperationOptions_(other.OperationOptions_)
    , RuntimeParameters_(other.RuntimeParameters_)
    , Controller_(other.Controller_)
{ }

std::optional<double> TPoolTreeOperationElement::GetSpecifiedFairShareStarvationTolerance() const
{
    return Spec_->FairShareStarvationTolerance;
}

std::optional<TDuration> TPoolTreeOperationElement::GetSpecifiedFairShareStarvationTimeout() const
{
    return Spec_->FairShareStarvationTimeout;
}

void TPoolTreeOperationElement::DisableNonAliveElements()
{ }

void TPoolTreeOperationElement::InitializeUpdate(TInstant now)
{
    YT_VERIFY(Mutable_);

    TotalNeededResources_ = Controller_->GetNeededResources().GetNeededResourcesForTree(TreeId_);
    PendingAllocationCount_ = TotalNeededResources_.GetUserSlots();
    GroupedNeededResources_ = Controller_->GetGroupedNeededResources();
    AggregatedMinNeededAllocationResources_ = Controller_->GetAggregatedMinNeededAllocationResources();
    AggregatedInitialMinNeededAllocationResources_ = Controller_->GetAggregatedInitialMinNeededAllocationResources();
    ScheduleAllocationBackoffCheckEnabled_ = Controller_->ScheduleAllocationBackoffObserved();

    UnschedulableReason_ = ComputeUnschedulableReason();
    Tentative_ = RuntimeParameters_->Tentative;

    InitializeResourceUsageAndDemand();

    TPoolTreeElement::InitializeUpdate(now);

    // NB(eshcherbin): This is a hotfix, see YT-19127.
    if (Spec_->ApplySpecifiedResourceLimitsToDemand && MaybeSpecifiedResourceLimits_) {
        TotalNeededResources_ = Max(
            Min(ResourceDemand_, *MaybeSpecifiedResourceLimits_) - ResourceUsageAtUpdate_,
            TJobResources());
        ResourceDemand_ = ResourceUsageAtUpdate_ + TotalNeededResources_;
        PendingAllocationCount_ = TotalNeededResources_.GetUserSlots();
    }

    for (const auto& [_, allocationGroupResources] : GroupedNeededResources_) {
        for (auto [index, _] : allocationGroupResources.MinNeededResources.DiskQuota().DiskSpacePerMedium) {
            DiskRequestMedia_.insert(index);
        }
    }
}

void TPoolTreeOperationElement::PreUpdate(TFairSharePreUpdateContext* context)
{
    YT_VERIFY(Mutable_);

    TPoolTreeElement::PreUpdate(context);

    if (context->Now >= PersistentAttributes_.LastBestAllocationShareUpdateTime + TreeConfig_->BestAllocationShareUpdatePeriod &&
        AreTotalResourceLimitsStable())
    {
        auto allocationLimits = GetAdjustedResourceLimits(
            ResourceDemand_,
            TotalResourceLimits_,
            GetHost()->GetExecNodeMemoryDistribution(SchedulingTagFilter_ & TreeConfig_->NodeTagFilter));
        PersistentAttributes_.BestAllocationShare = TResourceVector::FromJobResources(allocationLimits, TotalResourceLimits_);
        PersistentAttributes_.LastBestAllocationShareUpdateTime = context->Now;

        YT_LOG_DEBUG("Updated operation best allocation share (AdjustedResourceLimits: %v, TotalResourceLimits: %v, BestAllocationShare: %.6g)",
            FormatResources(allocationLimits),
            FormatResources(TotalResourceLimits_),
            PersistentAttributes_.BestAllocationShare);
    }
}

void TPoolTreeOperationElement::BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context)
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

void TPoolTreeOperationElement::UpdateRecursiveAttributes()
{
    TPoolTreeElement::UpdateRecursiveAttributes();

    // These attributes can be overwritten in operations, but only if the new values are more conservative.
    EffectiveFairShareStarvationTolerance_ = Parent_->GetEffectiveFairShareStarvationTolerance();
    if (const auto& specifiedTolerance = GetSpecifiedFairShareStarvationTolerance()) {
        EffectiveFairShareStarvationTolerance_ = std::min(EffectiveFairShareStarvationTolerance_, *specifiedTolerance);
    }
    EffectiveFairShareStarvationTimeout_ = Parent_->GetEffectiveFairShareStarvationTimeout();
    if (const auto& specifiedTimeout = GetSpecifiedFairShareStarvationTimeout()) {
        EffectiveFairShareStarvationTimeout_ = std::max(EffectiveFairShareStarvationTimeout_, *specifiedTimeout);
    }
    EffectiveNonPreemptibleResourceUsageThresholdConfig_ = Parent_->EffectiveNonPreemptibleResourceUsageThresholdConfig();
    if (const auto& specifiedThresholdConfig = GetSpecifiedNonPreemptibleResourceUsageThresholdConfig()) {
        auto newEffectiveConfig = EffectiveNonPreemptibleResourceUsageThresholdConfig_->Clone();
        TJobResourcesConfig::ForEachResource([&] (auto NVectorHdrf::TJobResourcesConfig::* resourceDataMember, EJobResourceType /*resourceType*/) {
            auto& effectiveThreshold = newEffectiveConfig.Get()->*resourceDataMember;
            auto& specifiedThreshold = specifiedThresholdConfig.Get()->*resourceDataMember;
            if (!effectiveThreshold || (specifiedThreshold && *specifiedThreshold < *effectiveThreshold)) {
                effectiveThreshold = specifiedThreshold;
            }
        });
        EffectiveNonPreemptibleResourceUsageThresholdConfig_ = std::move(newEffectiveConfig);
    }

    // These attributes cannot be overwritten in operations.
    EffectiveAggressiveStarvationEnabled_ = Parent_->GetEffectiveAggressiveStarvationEnabled();

    // TODO(eshcherbin): Currently |WaitingJobTimeout| spec option is applied in controller. Should we do it here instead?
    EffectiveWaitingForResourcesOnNodeTimeout_ = Parent_->GetEffectiveWaitingForResourcesOnNodeTimeout();
}

void TPoolTreeOperationElement::OnFifoSchedulableElementCountLimitReached(TFairSharePostUpdateContext* context)
{
    UnschedulableReason_ = EUnschedulableReason::FifoSchedulableElementCountLimitReached;
    ++context->UnschedulableReasons[*UnschedulableReason_];
    PostUpdateAttributes_.UnschedulableOperationsResourceUsage = GetInstantResourceUsage();
}

void TPoolTreeOperationElement::UpdateControllerConfig(const TStrategyOperationControllerConfigPtr& config)
{
    YT_VERIFY(Mutable_);
    ControllerConfig_ = config;
}

void TPoolTreeOperationElement::BuildLoggingStringAttributes(TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    TPoolTreeElement::BuildLoggingStringAttributes(delimitedBuilder);

    delimitedBuilder->AppendFormat(
        "PendingAllocations: %v, AggregatedMinNeededResources: %v",
        PendingAllocationCount_,
        AggregatedMinNeededAllocationResources_);
}

bool TPoolTreeOperationElement::AreDetailedLogsEnabled() const
{
    bool enabledDueToStarvation = TreeConfig_->EnableDetailedLogsForStarvingOperations &&
        PersistentAttributes_.StarvationStatus != EStarvationStatus::NonStarving;
    bool enabledSinceSingleAllocation = TreeConfig_->EnableDetailedLogsForSingleAllocationVanillaOperations &&
        IsSingleAllocationVanillaOperation();
    return RuntimeParameters_->EnableDetailedLogs || enabledDueToStarvation || enabledSinceSingleAllocation;
}

TString TPoolTreeOperationElement::GetId() const
{
    return ToString(OperationId_);
}

TOperationId TPoolTreeOperationElement::GetOperationId() const
{
    return OperationId_;
}

std::optional<std::string> TPoolTreeOperationElement::GetTitle() const
{
    return Operation_->GetTitle();
}

void TPoolTreeOperationElement::SetRuntimeParameters(TOperationPoolTreeRuntimeParametersPtr runtimeParameters)
{
    RuntimeParameters_ = std::move(runtimeParameters);

    Controller_->SetDetailedLogsEnabled(RuntimeParameters_->EnableDetailedLogs);
}

TOperationPoolTreeRuntimeParametersPtr TPoolTreeOperationElement::GetRuntimeParameters() const
{
    return RuntimeParameters_;
}

void TPoolTreeOperationElement::UpdatePoolAttributes(bool runningInEphemeralPool)
{
    TOperationPoolTreeAttributes attributes;
    attributes.RunningInEphemeralPool = runningInEphemeralPool;
    attributes.RunningInLightweightPool = Parent_->GetEffectiveLightweightOperationsEnabled();

    Operation_->UpdatePoolAttributes(TreeId_, std::move(attributes));
}

TJobResourcesConfigPtr TPoolTreeOperationElement::GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const
{
    return Spec_->NonPreemptibleResourceUsageThreshold;
}

std::optional<double> TPoolTreeOperationElement::GetSpecifiedWeight() const
{
    return RuntimeParameters_->Weight;
}

const NVectorHdrf::TJobResourcesConfig* TPoolTreeOperationElement::GetStrongGuaranteeResourcesConfig() const
{
    return Spec_->StrongGuaranteeResources.Get();
}

const TStrategyOperationStatePtr& TPoolTreeOperationElement::GetStrategyOperationState() const
{
    return StrategyOperationState_;
}

void TPoolTreeOperationElement::SetSchedulingTagFilter(TSchedulingTagFilter schedulingTagFilter)
{
    SchedulingTagFilter_ = std::move(schedulingTagFilter);
}

const TSchedulingTagFilter& TPoolTreeOperationElement::GetSchedulingTagFilter() const
{
    return SchedulingTagFilter_;
}

ESchedulableStatus TPoolTreeOperationElement::GetStatus() const
{
    if (UnschedulableReason_) {
        return ESchedulableStatus::Normal;
    }

    double tolerance = EffectiveFairShareStarvationTolerance_;
    if (Dominates(Attributes_.FairShare.Total + TResourceVector::LargeEpsilon(), Attributes_.DemandShare)) {
        tolerance = 1.0;
    } else if (TreeConfig_->EnableAbsoluteFairShareStarvationTolerance) {
        auto aggregatedMinNeededShare = TResourceVector::FromJobResources(AggregatedMinNeededAllocationResources_, TotalResourceLimits_);

        if (!IsStrictlyDominatesNonBlocked(Attributes_.FairShare.Total - Attributes_.UsageShare, aggregatedMinNeededShare)) {
            return ESchedulableStatus::Normal;
        }
    }

    return TPoolTreeElement::GetStatusImpl(tolerance);
}

void TPoolTreeOperationElement::SetStarvationStatus(EStarvationStatus starvationStatus, TInstant now)
{
    YT_VERIFY(Mutable_);

    auto currentStarvationStatus = GetStarvationStatus();

    if (starvationStatus == currentStarvationStatus) {
        return;
    }

    YT_LOG_INFO("Operation starvation status changed (Current: %v, New: %v)",
        currentStarvationStatus,
        starvationStatus);

    TPoolTreeElement::SetStarvationStatus(starvationStatus, now);

    if (starvationStatus != EStarvationStatus::NonStarving) {
        PersistentAttributes_.StarvingSince = now;
        PersistentAttributes_.FairShareOnStarvationStart = Attributes_.FairShare.Total;
        PersistentAttributes_.UsageOnStarvationStart = Attributes_.UsageShare;
        if (TreeConfig_->EnableDetailedStarvationLogs) {
            YT_LOG_DEBUG(
                "Operation started starving "
                "(StarvationStatus: %v, StarvingSince: %v, CurrentFairShare: %v, CurrentUsage: %v)",
                starvationStatus,
                PersistentAttributes_.StarvingSince,
                Attributes_.FairShare.Total,
                Attributes_.UsageShare);

            NLogging::LogStructuredEventFluently(SchedulerStructuredLogger(), NLogging::ELogLevel::Info)
                .Item("timestamp").Value(TInstant::Now())
                .Item("event_type").Value(ELogEventType::OperationStarvationStarted)
                .Item(EventLogPoolTreeKey).Value(TreeId_)
                .Item("operation_id").Value(GetId())
                .Item("starvation_status").Value(starvationStatus)
                .Item("starving_since").Value(PersistentAttributes_.StarvingSince)
                .Item("current_fair_share").Value(Attributes_.FairShare.Total)
                .Item("current_usage").Value(Attributes_.UsageShare);
        }
        return;
    }

    if (PersistentAttributes_.StarvingSince) {
        auto reason = [&] {
            if (TResourceVector::Near(*PersistentAttributes_.FairShareOnStarvationStart, Attributes_.FairShare.Total, NVectorHdrf::Epsilon)) {
                return EStarvationChangeReason::UsageIncreased;
            }
            if (Dominates(*PersistentAttributes_.FairShareOnStarvationStart, Attributes_.FairShare.Total)) {
                return EStarvationChangeReason::FairShareDecreased;
            }

            return EStarvationChangeReason::UsageIncreased;
        }();

        PostUpdateAttributes_.StarvationInterval.emplace(TStarvationInterval{
            .Duration = now - *PersistentAttributes_.StarvingSince,
            .Reason = reason});

        if (TreeConfig_->EnableDetailedStarvationLogs) {
            YT_LOG_DEBUG(
                "Operation stopped starving "
                "(StarvationStatus: %v, StarvingSince: %v, CurrentFairShare: %v, CurrentUsage: %v, FairShareOnStarvationStart: %v, UsageOnStarvationStart: %v)",
                starvationStatus,
                PersistentAttributes_.StarvingSince,
                Attributes_.FairShare.Total,
                Attributes_.UsageShare,
                PersistentAttributes_.FairShareOnStarvationStart,
                PersistentAttributes_.UsageOnStarvationStart);

            NLogging::LogStructuredEventFluently(SchedulerStructuredLogger(), NLogging::ELogLevel::Info)
                .Item("timestamp").Value(TInstant::Now())
                .Item("event_type").Value(ELogEventType::OperationStarvationFinished)
                .Item(EventLogPoolTreeKey).Value(TreeId_)
                .Item("operation_id").Value(GetId())
                .Item("starvation_status").Value(starvationStatus)
                .Item("starving_since").Value(PersistentAttributes_.StarvingSince)
                .Item("current_fair_share").Value(Attributes_.FairShare.Total)
                .Item("current_usage").Value(Attributes_.UsageShare)
                .Item("fair_share_on_starvation_start").Value(PersistentAttributes_.FairShareOnStarvationStart)
                .Item("usage_on_starvation_start").Value(PersistentAttributes_.UsageOnStarvationStart);
        }



        PersistentAttributes_.StarvingSince.reset();
    }
}

void TPoolTreeOperationElement::CheckForStarvation(TInstant now)
{
    YT_VERIFY(Mutable_);

    auto fairShareStarvationTimeout = EffectiveFairShareStarvationTimeout_;
    auto fairShareAggressiveStarvationTimeout = TreeConfig_->FairShareAggressiveStarvationTimeout;

    double allocationCountRatio = GetPendingAllocationCount() / TreeConfig_->AllocationCountPreemptionTimeoutCoefficient;
    if (allocationCountRatio < 1.0) {
        fairShareStarvationTimeout *= allocationCountRatio;
        fairShareAggressiveStarvationTimeout *= allocationCountRatio;
    }

    TPoolTreeElement::CheckForStarvationImpl(
        fairShareStarvationTimeout,
        fairShareAggressiveStarvationTimeout,
        now);
}

std::optional<TInstant> TPoolTreeOperationElement::GetStarvingSince() const {
    return PersistentAttributes_.StarvingSince;
}

int TPoolTreeOperationElement::GetSlotIndex() const
{
    return SlotIndex_;
}

std::string TPoolTreeOperationElement::GetUserName() const
{
    return UserName_;
}

EOperationType TPoolTreeOperationElement::GetOperationType() const
{
    return Type_;
}

const TYsonString& TPoolTreeOperationElement::GetTrimmedAnnotations() const
{
    return TrimmedAnnotations_;
}

TResourceVector TPoolTreeOperationElement::GetBestAllocationShare() const
{
    return PersistentAttributes_.BestAllocationShare;
}

bool TPoolTreeOperationElement::IsGangLike() const
{
    return IsGang() ||
        (IsSingleAllocationVanillaOperation() && TreeConfig_->ConsiderSingleAllocationVanillaOperationsAsGang);
}

bool TPoolTreeOperationElement::IsGang() const
{
    return Spec_->IsGang;
}

bool TPoolTreeOperationElement::IsDefaultGpuFullHost() const
{
    return Spec_->SchedulingSegment.value_or(ESchedulingSegment::Default) == ESchedulingSegment::Default &&
        AggregatedInitialMinNeededAllocationResources_.GetGpu() == FullHostGpuAllocationGpuDemand;
}


void TPoolTreeOperationElement::BuildElementMapping(TFairSharePostUpdateContext* context)
{
    if (Parent_->IsEnabledChild(this)) {
        context->EnabledOperationIdToElement.emplace(OperationId_, this);
    } else {
        context->DisabledOperationIdToElement.emplace(OperationId_, this);
    }
}

TPoolTreeElementPtr TPoolTreeOperationElement::Clone(TPoolTreeCompositeElement* clonedParent)
{
    return New<TPoolTreeOperationElement>(*this, clonedParent);
}

ESchedulerElementType TPoolTreeOperationElement::GetType() const
{
    return ESchedulerElementType::Operation;
}

TInstant TPoolTreeOperationElement::GetStartTime() const
{
    return StartTime_;
}

bool TPoolTreeOperationElement::IsSchedulable() const
{
    return !UnschedulableReason_;
}

std::optional<EUnschedulableReason> TPoolTreeOperationElement::ComputeUnschedulableReason() const
{
    auto result = Operation_->CheckUnschedulable(TreeId_);
    if (!result && IsMaxScheduleAllocationCallsViolated()) {
        result = EUnschedulableReason::MaxScheduleAllocationCallsViolated;
    }
    return result;
}

TControllerEpoch TPoolTreeOperationElement::GetControllerEpoch() const
{
    return Controller_->GetEpoch();
}

void TPoolTreeOperationElement::OnScheduleAllocationStarted(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext)
{
    Controller_->OnScheduleAllocationStarted(schedulingHeartbeatContext);
}

void TPoolTreeOperationElement::OnScheduleAllocationFinished(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext)
{
    Controller_->OnScheduleAllocationFinished(schedulingHeartbeatContext);
}

bool TPoolTreeOperationElement::IsMaxScheduleAllocationCallsViolated() const
{
    return Controller_->CheckMaxScheduleAllocationCallsOverdraft(
        Spec_->MaxConcurrentControllerScheduleAllocationCalls.value_or(
            ControllerConfig_->MaxConcurrentControllerScheduleAllocationCalls));
}

bool TPoolTreeOperationElement::IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext) const
{
    return Controller_->IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(schedulingHeartbeatContext);
}

bool TPoolTreeOperationElement::IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(
    const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext) const
{
    return Controller_->IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(schedulingHeartbeatContext);
}

bool TPoolTreeOperationElement::HasRecentScheduleAllocationFailure(NProfiling::TCpuInstant now) const
{
    return Controller_->HasRecentScheduleAllocationFailure(now);
}

bool TPoolTreeOperationElement::IsSaturatedInTentativeTree(
    NProfiling::TCpuInstant now,
    const std::string& treeId,
    TDuration saturationDeactivationTimeout) const
{
    return Controller_->IsSaturatedInTentativeTree(now, treeId, saturationDeactivationTimeout);
}

TControllerScheduleAllocationResultPtr TPoolTreeOperationElement::ScheduleAllocation(
    const ISchedulingHeartbeatContextPtr& context,
    const TJobResources& availableResources,
    const TDiskResources& availableDiskResources,
    TDuration timeLimit,
    const std::string& treeId)
{
    return Controller_->ScheduleAllocation(
        context,
        availableResources,
        availableDiskResources,
        timeLimit,
        treeId,
        GetParent()->GetFullPath(/*explicitOnly*/ false),
        EffectiveWaitingForResourcesOnNodeTimeout_);
}

void TPoolTreeOperationElement::OnScheduleAllocationFailed(
    TCpuInstant now,
    const std::string& treeId,
    const TControllerScheduleAllocationResultPtr& scheduleAllocationResult)
{
    Controller_->OnScheduleAllocationFailed(now, treeId, scheduleAllocationResult);
}

void TPoolTreeOperationElement::AbortAllocation(
    TAllocationId allocationId,
    EAbortReason abortReason,
    TControllerEpoch allocationEpoch)
{
    Controller_->AbortAllocation(allocationId, abortReason, allocationEpoch);
}

TAllocationGroupResourcesMap TPoolTreeOperationElement::GetInitialGroupedNeededResources() const
{
    return Controller_->GetInitialGroupedNeededResources();
}

TJobResources TPoolTreeOperationElement::GetAggregatedInitialMinNeededResources() const
{
    return Controller_->GetAggregatedInitialMinNeededAllocationResources();
}

EResourceTreeIncreaseResult TPoolTreeOperationElement::TryIncreaseHierarchicalResourceUsagePrecommit(
    const TJobResources& delta,
    bool allowLimitsOvercommit,
    const std::optional<TJobResources>& additionalLocalResourceLimits,
    TJobResources* availableResourceLimitsOutput)
{
    return TreeElementHost_->GetResourceTree()->TryIncreaseHierarchicalResourceUsagePrecommit(
        ResourceTreeElement_,
        delta,
        allowLimitsOvercommit,
        additionalLocalResourceLimits,
        availableResourceLimitsOutput);
}

void TPoolTreeOperationElement::IncreaseHierarchicalResourceUsage(const TJobResources& delta)
{
    TreeElementHost_->GetResourceTree()->IncreaseHierarchicalResourceUsage(ResourceTreeElement_, delta);
}

void TPoolTreeOperationElement::DecreaseHierarchicalResourceUsagePrecommit(const TJobResources& precommittedResources)
{
    TreeElementHost_->GetResourceTree()->IncreaseHierarchicalResourceUsagePrecommit(ResourceTreeElement_, -precommittedResources);
}

void TPoolTreeOperationElement::CommitHierarchicalResourceUsage(const TJobResources& resourceUsage, const TJobResources& precommittedResources)
{
    TreeElementHost_->GetResourceTree()->CommitHierarchicalResourceUsage(ResourceTreeElement_, resourceUsage, precommittedResources);
}

void TPoolTreeOperationElement::ReleaseResources(bool markAsNonAlive)
{
    TreeElementHost_->GetResourceTree()->ReleaseResources(ResourceTreeElement_, markAsNonAlive);
}

EResourceTreeIncreasePreemptedResult TPoolTreeOperationElement::TryIncreaseHierarchicalPreemptedResourceUsagePrecommit(const TJobResources& delta, std::string* violatedIdOutput)
{
    return TreeElementHost_->GetResourceTree()->TryIncreaseHierarchicalPreemptedResourceUsagePrecommit(
        ResourceTreeElement_,
        delta,
        violatedIdOutput);
}

bool TPoolTreeOperationElement::CommitHierarchicalPreemptedResourceUsage(const TJobResources& delta)
{
    return TreeElementHost_->GetResourceTree()->CommitHierarchicalPreemptedResourceUsage(ResourceTreeElement_, delta);
}

void TPoolTreeOperationElement::InitializeResourceUsageAndDemand()
{
    auto detailedResourceUsage = GetInstantDetailedResourceUsage();

    ResourceUsageAtUpdate_ = detailedResourceUsage.Base;

    auto maybeUnschedulableReason = Operation_->CheckUnschedulable(TreeId_);
    if (maybeUnschedulableReason == EUnschedulableReason::IsNotRunning || maybeUnschedulableReason == EUnschedulableReason::Suspended) {
        ResourceDemand_ = ResourceUsageAtUpdate_;
        return;
    }
    // Explanation of the reason to consider resource usage _with precommit_ in the demand computation.
    //
    // In the current scheme the exact demand of operation is not known due to asynchronous nature of total needed resources
    // that are reported to scheduler periodically. The value of total needed resources corresponds to the total demand of operation
    // without already scheduled jobs and some jobs that have been scheduled inside operation controller but have not been reported
    // to scheduler yet. And such jobs correspond to the part of precommited resource usage.
    //
    // Note that the total needed resources usually decreases in time, therefore using resource usage with precommit here
    // allows us to nearly guarantee that calculated demand is greater than or equal to exact value of demand.
    ResourceDemand_ = detailedResourceUsage.Base + detailedResourceUsage.Precommit + TotalNeededResources_;
}

TJobResourcesConfigPtr TPoolTreeOperationElement::GetSpecifiedResourceLimitsConfig() const
{
    return RuntimeParameters_->ResourceLimits;
}

void TPoolTreeOperationElement::AttachParent(TPoolTreeCompositeElement* newParent, int slotIndex)
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

void TPoolTreeOperationElement::ChangeParent(TPoolTreeCompositeElement* parent, int slotIndex)
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

void TPoolTreeOperationElement::DetachParent()
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

void TPoolTreeOperationElement::MarkOperationRunningInPool()
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

bool TPoolTreeOperationElement::IsOperationRunningInPool() const
{
    return RunningInThisPoolTree_;
}

// NB(eshcherbin): Lightweight operations are a special kind of operations which aren't counted in pool's running operation count,
// therefore their count is not as limited as for regular operations. From strategy's point of view, there operations should be
// quick to be fully scheduled and not consume much precious time during scheduling heartbeat.
// Operation is lightweight iff it is eligible and it's running in a pool where lightweight operations are enabled.
// Currently, only vanilla operations are considered eligible.
bool TPoolTreeOperationElement::IsLightweightEligible() const
{
    // TODO(eshcherbin): Do we want to restrict this to only vanilla operations with a single job?
    return Type_ == EOperationType::Vanilla;
}

bool TPoolTreeOperationElement::IsLightweight() const
{
    return IsLightweightEligible() && Parent_->GetEffectiveLightweightOperationsEnabled();
}

void TPoolTreeOperationElement::MarkPendingBy(TPoolTreeCompositeElement* violatedPool)
{
    violatedPool->PendingOperationIds().push_back(OperationId_);
    PendingByPool_ = violatedPool->GetId();

    YT_LOG_DEBUG("Operation is pending since max running operation count is violated (OperationId: %v, Pool: %v, Limit: %v)",
        OperationId_,
        violatedPool->GetId(),
        violatedPool->GetMaxRunningOperationCount());
}

std::optional<TString> TPoolTreeOperationElement::GetCustomProfilingTag() const
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

bool TPoolTreeOperationElement::IsLimitingAncestorCheckEnabled() const
{
    return Spec_->EnableLimitingAncestorCheck;
}

void TPoolTreeOperationElement::CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const
{
    elements->push_back(ResourceTreeElement_);
}

bool TPoolTreeOperationElement::IsIdleCpuPolicyAllowed() const
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

bool TPoolTreeOperationElement::IsSingleAllocationVanillaOperation() const
{
    const auto& maybeVanillaTaskSpecs = Operation_->GetMaybeBriefVanillaTaskSpecs();
    return maybeVanillaTaskSpecs &&
        (size(*maybeVanillaTaskSpecs) == 1) &&
        (maybeVanillaTaskSpecs->begin()->second.JobCount == 1);
}

TDuration TPoolTreeOperationElement::GetEffectiveAllocationPreemptionTimeout() const
{
    return OperationOptions_->AllocationPreemptionTimeout.value_or(TreeConfig_->AllocationPreemptionTimeout);
}

TDuration TPoolTreeOperationElement::GetEffectiveAllocationGracefulPreemptionTimeout() const
{
    return OperationOptions_->AllocationGracefulPreemptionTimeout.value_or(TreeConfig_->AllocationGracefulPreemptionTimeout);
}

////////////////////////////////////////////////////////////////////////////////

TPoolTreeRootElement::TPoolTreeRootElement(
    IStrategyHost* strategyHost,
    IPoolTreeElementHost* treeElementHost,
    TStrategyTreeConfigPtr treeConfig,
    const std::string& treeId,
    const NLogging::TLogger& logger)
    : TPoolTreeCompositeElement(
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

TPoolTreeRootElement::TPoolTreeRootElement(const TPoolTreeRootElement& other)
    : TPoolTreeCompositeElement(other, nullptr)
    , TPoolTreeRootElementFixedState(other)
{ }

void TPoolTreeRootElement::InitializeFairShareUpdate(TInstant now)
{
    YT_VERIFY(Mutable_);

    TForbidContextSwitchGuard contextSwitchGuard;

    DisableNonAliveElements();

    InitializeUpdate(now);
}

/// Steps of fair share post update:
///
/// 1. Publish the computed fair share to the shared resource tree and update the operations' preemptible allocation lists.
///
/// 2. Update dynamic attributes based on the calculated fair share (for orchid).
void TPoolTreeRootElement::PostUpdate(TFairSharePostUpdateContext* postUpdateContext)
{
    YT_ASSERT_INVOKER_AFFINITY(StrategyHost_->GetFairShareUpdateInvoker());

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

void TPoolTreeRootElement::UpdateRecursiveAttributes()
{
    YT_VERIFY(Mutable_);

    YT_VERIFY(GetSpecifiedFairShareStarvationTolerance());
    EffectiveFairShareStarvationTolerance_ = *GetSpecifiedFairShareStarvationTolerance();

    YT_VERIFY(GetSpecifiedFairShareStarvationTimeout());
    EffectiveFairShareStarvationTimeout_ = *GetSpecifiedFairShareStarvationTimeout();

    YT_VERIFY(IsAggressiveStarvationEnabled());
    EffectiveAggressiveStarvationEnabled_ = *IsAggressiveStarvationEnabled();

    YT_VERIFY(GetSpecifiedNonPreemptibleResourceUsageThresholdConfig());
    EffectiveNonPreemptibleResourceUsageThresholdConfig_ = GetSpecifiedNonPreemptibleResourceUsageThresholdConfig();

    // NB: May be null.
    EffectiveWaitingForResourcesOnNodeTimeout_ = GetSpecifiedWaitingForResourcesOnNodeTimeout();

    TPoolTreeCompositeElement::UpdateRecursiveAttributes();
}

const TSchedulingTagFilter& TPoolTreeRootElement::GetSchedulingTagFilter() const
{
    return EmptySchedulingTagFilter;
}

TString TPoolTreeRootElement::GetId() const
{
    return RootPoolName;
}

std::optional<double> TPoolTreeRootElement::GetSpecifiedWeight() const
{
    return std::nullopt;
}

TJobResources TPoolTreeRootElement::GetSpecifiedStrongGuaranteeResources() const
{
    return TotalResourceLimits_;
}

std::optional<double> TPoolTreeRootElement::GetSpecifiedFairShareStarvationTolerance() const
{
    return TreeConfig_->FairShareStarvationTolerance;
}

std::optional<TDuration> TPoolTreeRootElement::GetSpecifiedFairShareStarvationTimeout() const
{
    return TreeConfig_->FairShareStarvationTimeout;
}

std::optional<bool> TPoolTreeRootElement::IsAggressiveStarvationEnabled() const
{
    return TreeConfig_->EnableAggressiveStarvation;
}

TJobResourcesConfigPtr TPoolTreeRootElement::GetSpecifiedNonPreemptibleResourceUsageThresholdConfig() const
{
    return TreeConfig_->NonPreemptibleResourceUsageThreshold;
}

std::optional<TDuration> TPoolTreeRootElement::GetSpecifiedWaitingForResourcesOnNodeTimeout() const
{
    return TreeConfig_->WaitingForResourcesOnNodeTimeout;
}

void TPoolTreeRootElement::BuildPoolSatisfactionDigests(TFairSharePostUpdateContext* postUpdateContext)
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

void TPoolTreeRootElement::CheckForStarvation(TInstant /*now*/)
{ }

int TPoolTreeRootElement::GetMaxRunningOperationCount() const
{
    return TreeConfig_->MaxRunningOperationCount;
}

int TPoolTreeRootElement::GetMaxOperationCount() const
{
    return TreeConfig_->MaxOperationCount;
}

bool TPoolTreeRootElement::AreLightweightOperationsEnabled() const
{
    return false;
}

TPoolIntegralGuaranteesConfigPtr TPoolTreeRootElement::GetIntegralGuaranteesConfig() const
{
    return New<TPoolIntegralGuaranteesConfig>();
}

std::vector<EFifoSortParameter> TPoolTreeRootElement::GetFifoSortParameters() const
{
    YT_ABORT();
}

bool TPoolTreeRootElement::AreImmediateOperationsForbidden() const
{
    return TreeConfig_->ForbidImmediateOperationsInRoot;
}

bool TPoolTreeRootElement::AreGangOperationsAllowed() const
{
    return !TreeConfig_->AllowGangOperationsOnlyInFifoPools;
}

bool TPoolTreeRootElement::IsEphemeralHub() const
{
    return false;
}

THashSet<TString> TPoolTreeRootElement::GetAllowedProfilingTags() const
{
    return {};
}

bool TPoolTreeRootElement::ShouldComputePromisedGuaranteeFairShare() const
{
    return false;
}

bool TPoolTreeRootElement::IsPriorityStrongGuaranteeAdjustmentEnabled() const
{
    return false;
}

bool TPoolTreeRootElement::IsPriorityStrongGuaranteeAdjustmentDonorshipEnabled() const
{
    return false;
}

bool TPoolTreeRootElement::CanAcceptFreeVolume() const
{
    // This value is not used.
    return false;
}

bool TPoolTreeRootElement::ShouldDistributeFreeVolumeAmongChildren() const
{
    return false;
}

TJobResourcesConfigPtr TPoolTreeRootElement::GetSpecifiedResourceLimitsConfig() const
{
    return {};
}

std::optional<TDuration> TPoolTreeRootElement::GetMaybeHistoricUsageAggregatorPeriod() const
{
    return {};
}

void TPoolTreeRootElement::BuildResourceMetering(
    const std::optional<TMeteringKey>& /*lowestMeteredAncestorKey*/,
    const THashMap<TString, TResourceVolume>& poolResourceUsages,
    TMeteringMap* meteringMap) const
{
    auto key = TMeteringKey{
        .AbcId = StrategyHost_->GetDefaultAbcId(),
        .TreeId = GetTreeId(),
        .PoolId = GetId(),
    };

    auto accumulatedResourceUsageVolume = GetOrDefault(poolResourceUsages, GetId(), TResourceVolume{});

    TJobResources TotalStrongGuaranteeResources;
    TJobResources TotalResourceFlow;
    TJobResources TotalBurstGuaranteeResources;
    for (const auto& child : EnabledChildren_) {
        TotalStrongGuaranteeResources += child->GetSpecifiedStrongGuaranteeResources();
        TotalResourceFlow += child->GetSpecifiedResourceFlow();
        TotalBurstGuaranteeResources += child->GetSpecifiedBurstGuaranteeResources();
    }

    EmplaceOrCrash(
        *meteringMap,
        key,
        TMeteringStatistics(
            TotalStrongGuaranteeResources,
            TotalResourceFlow,
            TotalBurstGuaranteeResources,
            GetResourceUsageAtUpdate(),
            accumulatedResourceUsageVolume));

    for (const auto& child : EnabledChildren_) {
        child->BuildResourceMetering(/*parentKey*/ key, poolResourceUsages, meteringMap);
    }
}

TPoolTreeElementPtr TPoolTreeRootElement::Clone(TPoolTreeCompositeElement* /*clonedParent*/)
{
    YT_ABORT();
}

TPoolTreeRootElementPtr TPoolTreeRootElement::Clone()
{
    return New<TPoolTreeRootElement>(*this);
}

ESchedulerElementType TPoolTreeRootElement::GetType() const
{
    return ESchedulerElementType::Root;
}

bool TPoolTreeRootElement::IsDefaultConfigured() const
{
    return false;
}

TResourceDistributionInfo TPoolTreeRootElement::GetResourceDistributionInfo() const
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

void TPoolTreeRootElement::BuildResourceDistributionInfo(TFluentMap fluent) const
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

double TPoolTreeRootElement::GetSpecifiedBurstRatio() const
{
    return 0.0;
}

double TPoolTreeRootElement::GetSpecifiedResourceFlowRatio() const
{
    return 0.0;
}

TGuid TPoolTreeRootElement::GetObjectId() const
{
    return {};
}

const TOffloadingSettings& TPoolTreeRootElement::GetOffloadingSettings() const
{
    return EmptyOffloadingSettings;
}

std::optional<bool> TPoolTreeRootElement::IsIdleCpuPolicyAllowed() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
