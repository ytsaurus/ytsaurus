#include "fair_share_tree_job_scheduler.h"
#include "fair_share_tree.h"
#include "scheduling_context.h"

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/string_builder.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const TJobWithPreemptionInfoSet EmptyJobWithPreemptionInfoSet;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

int SchedulingIndexToProfilingRangeIndex(int schedulingIndex)
{
    return std::min(
        static_cast<int>((schedulingIndex == 0) ? 0 : (MostSignificantBit(schedulingIndex) + 1)),
        SchedulingIndexProfilingRangeCount);
}

TString FormatProfilingRangeIndex(int rangeIndex)
{
    switch (rangeIndex) {
        case 0:
        case 1:
            return ToString(rangeIndex);
        case SchedulingIndexProfilingRangeCount:
            return Format("%v-inf", 1 << (SchedulingIndexProfilingRangeCount - 1));
        default:
            return Format("%v-%v", 1 << (rangeIndex - 1), (1 << rangeIndex) - 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TJobWithPreemptionInfo> GetJobPreemptionInfos(
    const std::vector<TJobPtr>& jobs,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    std::vector<TJobWithPreemptionInfo> jobInfos;
    for (const auto& job : jobs) {
        auto* operationElement = treeSnapshot->FindEnabledOperationElement(job->GetOperationId());
        const auto& operationSharedState = operationElement
            ? treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(operationElement)
            : nullptr;
        if (!operationElement || !operationSharedState->IsJobKnown(job->GetId())) {
            const auto& Logger = StrategyLogger;

            YT_LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v, TreeId: %v)",
                job->GetId(),
                job->GetOperationId(),
                treeSnapshot->RootElement()->GetTreeId());
            continue;
        }
        jobInfos.push_back(TJobWithPreemptionInfo{
            .Job = job,
            .PreemptionStatus = operationSharedState->GetJobPreemptionStatus(job->GetId()),
            .OperationElement = operationElement,
        });
    }

    return jobInfos;
}

std::vector<TJobWithPreemptionInfo> CollectRunningJobsWithPreemptionInfo(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    return GetJobPreemptionInfos(schedulingContext->RunningJobs(), treeSnapshot);
}

void SortJobsWithPreemptionInfo(std::vector<TJobWithPreemptionInfo>* jobInfos)
{
    std::sort(
        jobInfos->begin(),
        jobInfos->end(),
        [&] (const TJobWithPreemptionInfo& lhs, const TJobWithPreemptionInfo& rhs) {
            if (lhs.PreemptionStatus != rhs.PreemptionStatus) {
                return lhs.PreemptionStatus < rhs.PreemptionStatus;
            }

            if (lhs.PreemptionStatus != EJobPreemptionStatus::Preemptible) {
                auto hasCpuGap = [] (const TJobWithPreemptionInfo& jobWithPreemptionInfo) {
                    return jobWithPreemptionInfo.Job->ResourceUsage().GetCpu() <
                        jobWithPreemptionInfo.Job->ResourceLimits().GetCpu();
                };

                // Save jobs without cpu gap.
                bool lhsHasCpuGap = hasCpuGap(lhs);
                bool rhsHasCpuGap = hasCpuGap(rhs);
                if (lhsHasCpuGap != rhsHasCpuGap) {
                    return lhsHasCpuGap < rhsHasCpuGap;
                }
            }

            return lhs.Job->GetStartTime() < rhs.Job->GetStartTime();
        }
    );
}

////////////////////////////////////////////////////////////////////////////////

std::optional<EJobPreemptionStatus> GetCachedJobPreemptionStatus(
    const TJobPtr& job,
    const TCachedJobPreemptionStatuses& jobPreemptionStatuses)
{
    if (!jobPreemptionStatuses.Value) {
        // Tree snapshot is missing.
        return {};
    }

    auto operationIt = jobPreemptionStatuses.Value->find(job->GetOperationId());
    if (operationIt == jobPreemptionStatuses.Value->end()) {
        return {};
    }

    const auto& jobIdToStatus = operationIt->second;
    auto jobIt = jobIdToStatus.find(job->GetId());
    return jobIt != jobIdToStatus.end() ? std::make_optional(jobIt->second) : std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

bool IsEligibleForSsdPriorityPreemption(
    const THashSet<int>& diskRequestMedia,
    const THashSet<int>& ssdPriorityPreemptionMedia)
{
    for (auto medium : diskRequestMedia) {
        if (ssdPriorityPreemptionMedia.contains(medium)) {
            return true;
        }
    }

    return false;
}

EOperationPreemptionPriority GetOperationPreemptionPriority(
    const TSchedulerOperationElement* operationElement,
    EOperationPreemptionPriorityScope scope,
    bool ssdPriorityPreemptionEnabled,
    const THashSet<int>& ssdPriorityPreemptionMedia)
{
    if (!operationElement->IsSchedulable()) {
        return EOperationPreemptionPriority::None;
    }

    bool isEligibleForAggressivePreemption;
    bool isEligibleForPreemption;
    switch (scope) {
        case EOperationPreemptionPriorityScope::OperationOnly:
            isEligibleForAggressivePreemption = operationElement->GetLowestAggressivelyStarvingAncestor() == operationElement;
            isEligibleForPreemption = operationElement->GetLowestStarvingAncestor() == operationElement;
            break;
        case EOperationPreemptionPriorityScope::OperationAndAncestors:
            isEligibleForAggressivePreemption = operationElement->GetLowestAggressivelyStarvingAncestor() != nullptr;
            isEligibleForPreemption = operationElement->GetLowestStarvingAncestor() != nullptr;
            break;
        default:
            YT_ABORT();
    }

    bool isEligibleForSsdPriorityPreemption = ssdPriorityPreemptionEnabled &&
        IsEligibleForSsdPriorityPreemption(operationElement->DiskRequestMedia(), ssdPriorityPreemptionMedia);
    if (isEligibleForAggressivePreemption) {
        return isEligibleForSsdPriorityPreemption
            ? EOperationPreemptionPriority::SsdAggressive
            : EOperationPreemptionPriority::Aggressive;
    }
    if (isEligibleForPreemption) {
        return isEligibleForSsdPriorityPreemption
            ? EOperationPreemptionPriority::SsdNormal
            : EOperationPreemptionPriority::Normal;
    }

    return EOperationPreemptionPriority::None;
}

////////////////////////////////////////////////////////////////////////////////

std::optional<bool> IsAggressivePreemptionAllowed(const TSchedulerElement* element)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Root:
            return true;
        case ESchedulerElementType::Pool:
            return static_cast<const TSchedulerPoolElement*>(element)->GetConfig()->AllowAggressivePreemption;
        case ESchedulerElementType::Operation: {
            const auto* operationElement = static_cast<const TSchedulerOperationElement*>(element);
            if (operationElement->IsGang() && !operationElement->TreeConfig()->AllowAggressivePreemptionForGangOperations) {
                return false;
            }
            return {};
        }
    }
}

bool IsRegularPreemptionAllowed(const TSchedulerElement* element)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
            return static_cast<const TSchedulerPoolElement*>(element)->GetConfig()->AllowRegularPreemption;
        default:
            return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSchedulableChildSet::TSchedulableChildSet(
    const TSchedulerCompositeElement* owningElement,
    TNonOwningElementList children,
    TDynamicAttributesList* dynamicAttributesList,
    bool useHeap)
    : OwningElement_(owningElement)
    , DynamicAttributesList_(dynamicAttributesList)
    , UseFifoSchedulingOrder_(OwningElement_->ShouldUseFifoSchedulingOrder())
    , UseHeap_(useHeap)
    , Children_(std::move(children))
{
    InitializeChildrenOrder();
}

const TNonOwningElementList& TSchedulableChildSet::GetChildren() const
{
    return Children_;
}

TSchedulerElement* TSchedulableChildSet::GetBestActiveChild() const
{
    if (Children_.empty()) {
        return nullptr;
    }

    auto* bestChild = Children_.front();
    return DynamicAttributesList_->AttributesOf(bestChild).Active
        ? bestChild
        : nullptr;
}

void TSchedulableChildSet::OnChildAttributesUpdatedHeap(int childIndex)
{
    AdjustHeapItem(
        Children_.begin(),
        Children_.end(),
        Children_.begin() + childIndex,
        [&] (const TSchedulerElement* lhs, const TSchedulerElement* rhs) {
            return Comparator(lhs, rhs);
        },
        [&] (size_t offset) {
            DynamicAttributesList_->AttributesOf(Children_[offset]).SchedulableChildSetIndex = offset;
        });
}

void TSchedulableChildSet::OnChildAttributesUpdatedSimple(int childIndex)
{
    if (childIndex == 0) {
        MoveBestChildToFront();
        return;
    }

    auto& frontChild = Children_.front();
    auto& candidateChild = Children_[childIndex];
    if (Comparator(frontChild, candidateChild)) {
        std::swap(
            DynamicAttributesList_->AttributesOf(frontChild).SchedulableChildSetIndex,
            DynamicAttributesList_->AttributesOf(candidateChild).SchedulableChildSetIndex);
        std::swap(frontChild, candidateChild);
    }
}

void TSchedulableChildSet::OnChildAttributesUpdated(const TSchedulerElement* child)
{
    int childIndex = DynamicAttributesList_->AttributesOf(child).SchedulableChildSetIndex;

    YT_VERIFY(childIndex != InvalidSchedulableChildSetIndex);
    YT_VERIFY(childIndex < std::ssize(Children_));

    if (UseHeap_) {
        OnChildAttributesUpdatedHeap(childIndex);
    } else {
        OnChildAttributesUpdatedSimple(childIndex);
    }
}

bool TSchedulableChildSet::UsesHeapInTest() const
{
    return UseHeap_;
}

bool TSchedulableChildSet::Comparator(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const
{
    const auto& lhsAttributes = DynamicAttributesList_->AttributesOf(lhs);
    const auto& rhsAttributes = DynamicAttributesList_->AttributesOf(rhs);

    if (lhsAttributes.Active != rhsAttributes.Active) {
        return rhsAttributes.Active < lhsAttributes.Active;
    }

    if (UseFifoSchedulingOrder_) {
        return OwningElement_->HasHigherPriorityInFifoMode(lhs, rhs);
    }

    return lhsAttributes.SatisfactionRatio < rhsAttributes.SatisfactionRatio;
}

void TSchedulableChildSet::MoveBestChildToFront()
{
    if (Children_.empty()) {
        return;
    }

    auto& frontChild = Children_.front();
    auto& bestChild = *std::min_element(
        Children_.begin(),
        Children_.end(),
        [&] (const TSchedulerElement* lhs, const TSchedulerElement* rhs) {
            return Comparator(lhs, rhs);
        });
    std::swap(
        DynamicAttributesList_->AttributesOf(frontChild).SchedulableChildSetIndex,
        DynamicAttributesList_->AttributesOf(bestChild).SchedulableChildSetIndex);
    std::swap(frontChild, bestChild);
}

void TSchedulableChildSet::InitializeChildrenOrder()
{
    for (int index = 0; index < std::ssize(Children_); ++index) {
        DynamicAttributesList_->AttributesOf(Children_[index]).SchedulableChildSetIndex = index;
    }

    if (UseHeap_) {
        MakeHeap(
            Children_.begin(),
            Children_.end(),
            [&] (const TSchedulerElement* lhs, const TSchedulerElement* rhs) {
                return Comparator(lhs, rhs);
            },
            [&] (size_t offset) {
                DynamicAttributesList_->AttributesOf(Children_[offset]).SchedulableChildSetIndex = offset;
            });
    } else {
        MoveBestChildToFront();
    }
}

////////////////////////////////////////////////////////////////////////////////

TStaticAttributes& TStaticAttributesList::AttributesOf(const TSchedulerElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

const TStaticAttributes& TStaticAttributesList::AttributesOf(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

////////////////////////////////////////////////////////////////////////////////

TDynamicAttributesList::TDynamicAttributesList(int size)
    : std::vector<TDynamicAttributes>(size)
{ }

TDynamicAttributes& TDynamicAttributesList::AttributesOf(const TSchedulerElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

const TDynamicAttributes& TDynamicAttributesList::AttributesOf(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

////////////////////////////////////////////////////////////////////////////////

TDynamicAttributesListSnapshot::TDynamicAttributesListSnapshot(TDynamicAttributesList value)
    : Value(std::move(value))
{ }

////////////////////////////////////////////////////////////////////////////////

TDynamicAttributesList TDynamicAttributesManager::BuildDynamicAttributesListFromSnapshot(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot,
    TCpuInstant now)
{
    auto* rootElement = treeSnapshot->RootElement().Get();
    TDynamicAttributesList attributesList(rootElement->SchedulableElementCount());

    TFillResourceUsageContext context{
        .TreeSnapshot = treeSnapshot,
        .ResourceUsageSnapshot = resourceUsageSnapshot,
        .Now = now,
        .AttributesList = &attributesList,
    };
    FillResourceUsage(rootElement, &context);

    return attributesList;
}

TDynamicAttributesManager::TDynamicAttributesManager(TFairShareTreeSchedulingSnapshotPtr schedulingSnapshot, int size)
    : SchedulingSnapshot_(std::move(schedulingSnapshot))
    , AttributesList_(size)
{ }

void TDynamicAttributesManager::SetAttributesList(TDynamicAttributesList attributesList)
{
    AttributesList_ = std::move(attributesList);
}

TDynamicAttributes& TDynamicAttributesManager::AttributesOf(const TSchedulerElement* element)
{
    return AttributesList_.AttributesOf(element);
}

const TDynamicAttributes& TDynamicAttributesManager::AttributesOf(const TSchedulerElement* element) const
{
    return AttributesList_.AttributesOf(element);
}

void TDynamicAttributesManager::InitializeAttributesAtCompositeElement(
    TSchedulerCompositeElement* element,
    std::optional<TNonOwningElementList> consideredSchedulableChildren,
    bool useChildHeap)
{
    // COMPAT(eshcherbin)
    if (useChildHeap && !consideredSchedulableChildren) {
        consideredSchedulableChildren.emplace();
        consideredSchedulableChildren->reserve(element->SchedulableChildren().size());
        for (const auto& child : element->SchedulableChildren()) {
            consideredSchedulableChildren->push_back(child.Get());
        }
    }

    if (consideredSchedulableChildren) {
        AttributesOf(element).SchedulableChildSet.emplace(
            element,
            std::move(*consideredSchedulableChildren),
            &AttributesList_,
            useChildHeap);
    }

    UpdateAttributesAtCompositeElement(element);
}

void TDynamicAttributesManager::InitializeAttributesAtOperation(
    TSchedulerOperationElement* element,
    bool isActive)
{
    AttributesOf(element).Active = isActive;

    if (isActive) {
        UpdateAttributesAtOperation(element);
    }
}

void TDynamicAttributesManager::InitializeResourceUsageAtPostUpdate(const TSchedulerElement* element, const TJobResources& resourceUsage)
{
    YT_VERIFY(element->GetMutable());

    auto& attributes = AttributesOf(element);
    SetResourceUsage(element, &attributes, resourceUsage);
}

void TDynamicAttributesManager::ActivateOperation(TSchedulerOperationElement* element)
{
    AttributesOf(element).Active = true;
    UpdateAttributesHierarchically(element, /*deltaResourceUsage*/ {}, /*checkAncestorsActiveness*/ false);
}

void TDynamicAttributesManager::DeactivateOperation(TSchedulerOperationElement* element)
{
    AttributesOf(element).Active = false;
    UpdateAttributesHierarchically(element);
}

void TDynamicAttributesManager::UpdateOperationResourceUsage(TSchedulerOperationElement* element, TCpuInstant now)
{
    if (!element->IsSchedulable()) {
        return;
    }

    auto& attributes = AttributesOf(element);
    auto resourceUsageBeforeUpdate = attributes.ResourceUsage;
    const auto& operationSharedState = SchedulingSnapshot_->GetEnabledOperationSharedState(element);
    DoUpdateOperationResourceUsage(element, &attributes, operationSharedState, now);

    auto resourceUsageDelta = attributes.ResourceUsage - resourceUsageBeforeUpdate;
    UpdateAttributesHierarchically(element, resourceUsageDelta);
}

void TDynamicAttributesManager::Clear()
{
    for (auto& attributes : AttributesList_) {
        attributes.Active = false;
        attributes.SchedulableChildSet.reset();
    }

    CompositeElementDeactivationCount_ = 0;
}

int TDynamicAttributesManager::GetCompositeElementDeactivationCount() const
{
    return CompositeElementDeactivationCount_;
}

bool TDynamicAttributesManager::ShouldCheckLiveness() const
{
    return SchedulingSnapshot_ != nullptr;
}

void TDynamicAttributesManager::UpdateAttributesHierarchically(
    TSchedulerOperationElement* element,
    const TJobResources& resourceUsageDelta,
    bool checkAncestorsActiveness)
{
    UpdateAttributes(element);

    auto* ancestor = element->GetMutableParent();
    while (ancestor) {
        if (checkAncestorsActiveness) {
            YT_VERIFY(AttributesOf(ancestor).Active);
        }

        auto& ancestorAttributes = AttributesOf(ancestor);
        IncreaseResourceUsage(ancestor, &ancestorAttributes, resourceUsageDelta);
        UpdateAttributes(ancestor);

        ancestor = ancestor->GetMutableParent();
    }
}

void TDynamicAttributesManager::UpdateAttributes(TSchedulerElement* element)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            UpdateAttributesAtCompositeElement(static_cast<TSchedulerCompositeElement*>(element));
            break;
        case ESchedulerElementType::Operation:
            UpdateAttributesAtOperation(static_cast<TSchedulerOperationElement*>(element));
            break;
        default:
            YT_ABORT();
    }

    if (const auto* parent = element->GetParent()) {
        if (auto& childSet = AttributesOf(parent).SchedulableChildSet) {
            childSet->OnChildAttributesUpdated(element);
        }
    }
}

void TDynamicAttributesManager::UpdateAttributesAtCompositeElement(TSchedulerCompositeElement* element)
{
    auto& attributes = AttributesOf(element);
    auto finallyGuard = Finally([&, activeBefore = attributes.Active] {
        bool activeAfter = attributes.Active;
        if (activeBefore && !activeAfter) {
            ++CompositeElementDeactivationCount_;
        }
    });

    if (ShouldCheckLiveness() && !element->IsAlive()) {
        attributes.Active = false;
        return;
    }

    // Satisfaction ratio of a composite element is the minimum of its children's satisfaction ratios.

    if (const auto* bestChild = GetBestActiveChild(element)) {
        const auto& bestChildAttributes = AttributesOf(bestChild);
        attributes.Active = true;
        attributes.BestLeafDescendant = bestChildAttributes.BestLeafDescendant;
        attributes.SatisfactionRatio = bestChildAttributes.SatisfactionRatio;

        if (element->GetEffectiveUsePoolSatisfactionForScheduling()) {
            attributes.SatisfactionRatio = std::min(attributes.SatisfactionRatio, attributes.LocalSatisfactionRatio);
        }
    } else {
        // Declare the element passive if all children are passive.
        attributes.Active = false;
        attributes.BestLeafDescendant = nullptr;
        // NB(eshcherbin): We use pool's local satisfaction ratio as a fallback value for smoother diagnostics.
        // This value will not influence scheduling decisions.
        attributes.SatisfactionRatio = attributes.LocalSatisfactionRatio;
    }
}

void TDynamicAttributesManager::UpdateAttributesAtOperation(TSchedulerOperationElement* element)
{
    auto& attributes = AttributesOf(element);
    attributes.SatisfactionRatio = attributes.LocalSatisfactionRatio;
    attributes.BestLeafDescendant = element;
}

TSchedulerElement* TDynamicAttributesManager::GetBestActiveChild(TSchedulerCompositeElement* element) const
{
    if (const auto& childSet = AttributesOf(element).SchedulableChildSet) {
        return childSet->GetBestActiveChild();
    }

    // COMPAT(eshcherbin)
    if (element->ShouldUseFifoSchedulingOrder()) {
        return GetBestActiveChildFifo(element);
    }

    return GetBestActiveChildFairShare(element);
}

TSchedulerElement* TDynamicAttributesManager::GetBestActiveChildFifo(TSchedulerCompositeElement* element) const
{
    TSchedulerElement* bestChild = nullptr;
    for (const auto& child : element->SchedulableChildren()) {
        if (!AttributesOf(child.Get()).Active) {
            continue;
        }

        if (!bestChild || element->HasHigherPriorityInFifoMode(child.Get(), bestChild)) {
            bestChild = child.Get();
        }
    }
    return bestChild;
}

TSchedulerElement* TDynamicAttributesManager::GetBestActiveChildFairShare(TSchedulerCompositeElement* element) const
{
    TSchedulerElement* bestChild = nullptr;
    double bestChildSatisfactionRatio = InfiniteSatisfactionRatio;
    for (const auto& child : element->SchedulableChildren()) {
        if (!AttributesOf(child.Get()).Active) {
            continue;
        }

        double childSatisfactionRatio = AttributesOf(child.Get()).SatisfactionRatio;
        if (!bestChild || childSatisfactionRatio < bestChildSatisfactionRatio) {
            bestChild = child.Get();
            bestChildSatisfactionRatio = childSatisfactionRatio;
        }
    }
    return bestChild;
}

void TDynamicAttributesManager::SetResourceUsage(
    const TSchedulerElement* element,
    TDynamicAttributes* attributes,
    const TJobResources& resourceUsage,
    std::optional<TCpuInstant> updateTime)
{
    attributes->ResourceUsage = resourceUsage;
    attributes->LocalSatisfactionRatio = element->ComputeLocalSatisfactionRatio(attributes->ResourceUsage);
    if (updateTime) {
        attributes->ResourceUsageUpdateTime = *updateTime;
    }
}

void TDynamicAttributesManager::IncreaseResourceUsage(
    const TSchedulerElement* element,
    TDynamicAttributes* attributes,
    const TJobResources& resourceUsageDelta,
    std::optional<TCpuInstant> updateTime)
{
    attributes->ResourceUsage += resourceUsageDelta;
    attributes->LocalSatisfactionRatio = element->ComputeLocalSatisfactionRatio(attributes->ResourceUsage);
    if (updateTime) {
        attributes->ResourceUsageUpdateTime = *updateTime;
    }
}

void TDynamicAttributesManager::DoUpdateOperationResourceUsage(
    const TSchedulerOperationElement* element,
    TDynamicAttributes* operationAttributes,
    const TFairShareTreeJobSchedulerOperationSharedStatePtr& operationSharedState,
    TCpuInstant now)
{
    bool alive = element->IsAlive();
    auto resourceUsage = (alive && operationSharedState->IsEnabled())
        ? element->GetInstantResourceUsage()
        : TJobResources();
    SetResourceUsage(element, operationAttributes, resourceUsage, now);
    operationAttributes->Alive = alive;
}

TJobResources TDynamicAttributesManager::FillResourceUsage(const TSchedulerElement* element, TFillResourceUsageContext* context)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            return FillResourceUsageAtCompositeElement(static_cast<const TSchedulerCompositeElement*>(element), context);
        case ESchedulerElementType::Operation:
            return FillResourceUsageAtOperation(static_cast<const TSchedulerOperationElement*>(element), context);
        default:
            YT_ABORT();
    }
}

TJobResources TDynamicAttributesManager::FillResourceUsageAtCompositeElement(const TSchedulerCompositeElement* element, TFillResourceUsageContext* context)
{
    auto& attributes = context->AttributesList->AttributesOf(element);

    auto resourceUsage = element->PostUpdateAttributes().UnschedulableOperationsResourceUsage;
    for (const auto& child : element->SchedulableChildren()) {
        resourceUsage += FillResourceUsage(child.Get(), context);
    }
    SetResourceUsage(element, &attributes, resourceUsage);

    return attributes.ResourceUsage;
}

TJobResources TDynamicAttributesManager::FillResourceUsageAtOperation(const TSchedulerOperationElement* element, TFillResourceUsageContext* context)
{
    auto& attributes = context->AttributesList->AttributesOf(element);
    if (context->ResourceUsageSnapshot) {
        auto operationId = element->GetOperationId();
        auto it = context->ResourceUsageSnapshot->OperationIdToResourceUsage.find(operationId);
        const auto& resourceUsage = it != context->ResourceUsageSnapshot->OperationIdToResourceUsage.end()
            ? it->second
            : TJobResources();
        SetResourceUsage(
            element,
            &attributes,
            resourceUsage,
            context->ResourceUsageSnapshot->BuildTime);
        attributes.Alive = context->ResourceUsageSnapshot->AliveOperationIds.contains(operationId);
    } else {
        DoUpdateOperationResourceUsage(
            element,
            &attributes,
            context->TreeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element),
            context->Now);
    }

    return attributes.ResourceUsage;
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeSchedulingSnapshot::TFairShareTreeSchedulingSnapshot(
    TStaticAttributesList staticAttributesList,
    TOperationElementsBySchedulingPriority schedulableOperationsPerPriority,
    THashSet<int> ssdPriorityPreemptionMedia,
    TCachedJobPreemptionStatuses cachedJobPreemptionStatuses,
    std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
    TOperationCountsByPreemptionPriorityParameters operationCountsByPreemptionPriorityParameters,
    TFairShareTreeJobSchedulerOperationStateMap operationIdToState,
    TFairShareTreeJobSchedulerSharedOperationStateMap operationIdToSharedState)
    : StaticAttributesList_(std::move(staticAttributesList))
    , SchedulableOperationsPerPriority_(std::move(schedulableOperationsPerPriority))
    , SsdPriorityPreemptionMedia_(std::move(ssdPriorityPreemptionMedia))
    , CachedJobPreemptionStatuses_(std::move(cachedJobPreemptionStatuses))
    , KnownSchedulingTagFilters_(std::move(knownSchedulingTagFilters))
    , OperationCountsByPreemptionPriorityParameters_(std::move(operationCountsByPreemptionPriorityParameters))
    , OperationIdToState_(std::move(operationIdToState))
    , OperationIdToSharedState_(std::move(operationIdToSharedState))
{ }

const TFairShareTreeJobSchedulerOperationStatePtr& TFairShareTreeSchedulingSnapshot::GetOperationState(const TSchedulerOperationElement* element) const
{
    return GetOrCrash(OperationIdToState_, element->GetOperationId());
}

const TFairShareTreeJobSchedulerOperationSharedStatePtr& TFairShareTreeSchedulingSnapshot::GetOperationSharedState(const TSchedulerOperationElement* element) const
{
    return GetOrCrash(OperationIdToSharedState_, element->GetOperationId());
}

const TFairShareTreeJobSchedulerOperationStatePtr& TFairShareTreeSchedulingSnapshot::GetEnabledOperationState(const TSchedulerOperationElement* element) const
{
    const auto& operationState = StaticAttributesList_.AttributesOf(element).OperationState;
    YT_ASSERT(operationState);
    return operationState;
}

const TFairShareTreeJobSchedulerOperationSharedStatePtr& TFairShareTreeSchedulingSnapshot::GetEnabledOperationSharedState(const TSchedulerOperationElement* element) const
{
    const auto& operationSharedState = StaticAttributesList_.AttributesOf(element).OperationSharedState;
    YT_ASSERT(operationSharedState);
    return operationSharedState;
}

TDynamicAttributesListSnapshotPtr TFairShareTreeSchedulingSnapshot::GetDynamicAttributesListSnapshot() const
{
    return DynamicAttributesListSnapshot_.Acquire();
}

void TFairShareTreeSchedulingSnapshot::UpdateDynamicAttributesListSnapshot(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot)
{
    if (!resourceUsageSnapshot) {
        DynamicAttributesListSnapshot_.Reset();
        return;
    }

    auto attributesSnapshot = New<TDynamicAttributesListSnapshot>(
        TDynamicAttributesManager::BuildDynamicAttributesListFromSnapshot(
            treeSnapshot,
            resourceUsageSnapshot,
            NProfiling::GetCpuInstant()));
    DynamicAttributesListSnapshot_.Store(std::move(attributesSnapshot));
}

////////////////////////////////////////////////////////////////////////////////

TSchedulingStageProfilingCounters::TSchedulingStageProfilingCounters(
    const NProfiling::TProfiler& profiler)
    : PrescheduleJobCount(profiler.Counter("/preschedule_job_count"))
    , UselessPrescheduleJobCount(profiler.Counter("/useless_preschedule_job_count"))
    , PrescheduleJobTime(profiler.Timer("/preschedule_job_time"))
    , TotalControllerScheduleJobTime(profiler.Timer("/controller_schedule_job_time/total"))
    , ExecControllerScheduleJobTime(profiler.Timer("/controller_schedule_job_time/exec"))
    , StrategyScheduleJobTime(profiler.Timer("/strategy_schedule_job_time"))
    , PackingRecordHeartbeatTime(profiler.Timer("/packing_record_heartbeat_time"))
    , PackingCheckTime(profiler.Timer("/packing_check_time"))
    , AnalyzeJobsTime(profiler.Timer("/analyze_jobs_time"))
    , CumulativePrescheduleJobTime(profiler.TimeCounter("/cumulative_preschedule_job_time"))
    , CumulativeTotalControllerScheduleJobTime(profiler.TimeCounter("/cumulative_controller_schedule_job_time/total"))
    , CumulativeExecControllerScheduleJobTime(profiler.TimeCounter("/cumulative_controller_schedule_job_time/exec"))
    , CumulativeStrategyScheduleJobTime(profiler.TimeCounter("/cumulative_strategy_schedule_job_time"))
    , CumulativeAnalyzeJobsTime(profiler.TimeCounter("/cumulative_analyze_jobs_time"))
    , ScheduleJobAttemptCount(profiler.Counter("/schedule_job_attempt_count"))
    , ScheduleJobFailureCount(profiler.Counter("/schedule_job_failure_count"))
    , ControllerScheduleJobCount(profiler.Counter("/controller_schedule_job_count"))
    , ControllerScheduleJobTimedOutCount(profiler.Counter("/controller_schedule_job_timed_out_count"))
    , ActiveTreeSize(profiler.Summary("/active_tree_size"))
    , ActiveOperationCount(profiler.Summary("/active_operation_count"))
{
    for (auto reason : TEnumTraits<NControllerAgent::EScheduleJobFailReason>::GetDomainValues()) {
        ControllerScheduleJobFail[reason] = profiler
            .WithTag("reason", FormatEnum(reason))
            .Counter("/controller_schedule_job_fail");
    }
    for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
        DeactivationCount[reason] = profiler
            .WithTag("reason", FormatEnum(reason))
            .Counter("/deactivation_count");
    }
    for (int rangeIndex = 0; rangeIndex <= SchedulingIndexProfilingRangeCount; ++rangeIndex) {
        SchedulingIndexCounters[rangeIndex] = profiler
            .WithTag("scheduling_index", FormatProfilingRangeIndex(rangeIndex))
            .Counter("/operation_scheduling_index_attempt_count");
        MaxSchedulingIndexCounters[rangeIndex] = profiler
            .WithTag("scheduling_index", FormatProfilingRangeIndex(rangeIndex))
            .Counter("/max_operation_scheduling_index");
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TJobWithPreemptionInfo& jobInfo, TStringBuf /*format*/)
{
    builder->AppendFormat("{JobId: %v, PreemptionStatus: %v, OperationId: %v}",
        jobInfo.Job->GetId(),
        jobInfo.PreemptionStatus,
        jobInfo.OperationElement->GetId());
}

TString ToString(const TJobWithPreemptionInfo& jobInfo)
{
    return ToStringViaBuilder(jobInfo);
}

////////////////////////////////////////////////////////////////////////////////

TScheduleJobsContext::TScheduleJobsContext(
    ISchedulingContextPtr schedulingContext,
    TFairShareTreeSnapshotPtr treeSnapshot,
    const TFairShareTreeJobSchedulerNodeState* nodeState,
    bool schedulingInfoLoggingEnabled,
    ISchedulerStrategyHost* strategyHost,
    const NProfiling::TCounter& scheduleJobsDeadlineReachedCounter,
    const NLogging::TLogger& logger)
    : SchedulingContext_(std::move(schedulingContext))
    , TreeSnapshot_(std::move(treeSnapshot))
    , SsdPriorityPreemptionEnabled_(TreeSnapshot_->TreeConfig()->SsdPriorityPreemption->Enable &&
        SchedulingContext_->CanSchedule(TreeSnapshot_->TreeConfig()->SsdPriorityPreemption->NodeTagFilter))
    , SchedulingDeadline_(SchedulingContext_->GetNow() + DurationToCpuDuration(TreeSnapshot_->ControllerConfig()->ScheduleJobsTimeout))
    , NodeSchedulingSegment_(nodeState->SchedulingSegment)
    , OperationCountByPreemptionPriority_(GetOrCrash(
        TreeSnapshot_->SchedulingSnapshot()->OperationCountsByPreemptionPriorityParameters(),
        TOperationPreemptionPriorityParameters{
            TreeSnapshot_->TreeConfig()->SchedulingPreemptionPriorityScope,
            SsdPriorityPreemptionEnabled_,
        }))
    , SsdPriorityPreemptionMedia_(TreeSnapshot_->SchedulingSnapshot()->SsdPriorityPreemptionMedia())
    , SchedulingInfoLoggingEnabled_(schedulingInfoLoggingEnabled)
    , DynamicAttributesListSnapshot_(TreeSnapshot_->TreeConfig()->EnableResourceUsageSnapshot
        ? TreeSnapshot_->SchedulingSnapshot()->GetDynamicAttributesListSnapshot()
        : nullptr)
    , StrategyHost_(strategyHost)
    , ScheduleJobsDeadlineReachedCounter_(scheduleJobsDeadlineReachedCounter)
    , Logger(logger)
    , DynamicAttributesManager_(TreeSnapshot_->SchedulingSnapshot())
{
    YT_LOG_DEBUG_IF(DynamicAttributesListSnapshot_ && SchedulingInfoLoggingEnabled_,
        "Using dynamic attributes snapshot for job scheduling");

    SchedulingStatistics_.ResourceUsage = SchedulingContext_->ResourceUsage();
    SchedulingStatistics_.ResourceLimits = SchedulingContext_->ResourceLimits();
    SchedulingStatistics_.SsdPriorityPreemptionEnabled = SsdPriorityPreemptionEnabled_;
    SchedulingStatistics_.SsdPriorityPreemptionMedia = SsdPriorityPreemptionMedia_;
    SchedulingStatistics_.OperationCountByPreemptionPriority = OperationCountByPreemptionPriority_;
}

void TScheduleJobsContext::PrepareForScheduling()
{
    YT_VERIFY(StageState_);
    YT_VERIFY(!StageState_->PrescheduleExecuted);

    if (!Initialized_) {
        Initialized_ = true;

        const auto& knownSchedulingTagFilters = TreeSnapshot_->SchedulingSnapshot()->KnownSchedulingTagFilters();
        CanSchedule_.reserve(knownSchedulingTagFilters.size());
        for (const auto& filter : knownSchedulingTagFilters) {
            CanSchedule_.push_back(SchedulingContext_->CanSchedule(filter));
        }

        auto dynamicAttributesList = DynamicAttributesListSnapshot_
            ? DynamicAttributesListSnapshot_->Value
            : TDynamicAttributesManager::BuildDynamicAttributesListFromSnapshot(
                TreeSnapshot_,
                /*resourceUsageSnapshot*/ nullptr,
                SchedulingContext_->GetNow());
        DynamicAttributesManager_.SetAttributesList(std::move(dynamicAttributesList));
    } else {
        DynamicAttributesManager_.Clear();
        ConsideredSchedulableChildrenPerPool_.clear();
    }
}

void TScheduleJobsContext::PrescheduleJob(
    const std::optional<TNonOwningOperationElementList>& consideredSchedulableOperations,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    TWallTimer prescheduleTimer;

    CollectConsideredSchedulableChildrenPerPool(consideredSchedulableOperations);
    PrescheduleJobAtCompositeElement(TreeSnapshot_->RootElement().Get(), targetOperationPreemptionPriority);

    StageState_->PrescheduleDuration = prescheduleTimer.GetElapsedTime();
    StageState_->PrescheduleExecuted = true;
}

bool TScheduleJobsContext::ShouldContinueScheduling(const std::optional<TJobResources>& customMinSpareJobResources) const
{
    return SchedulingContext_->CanStartMoreJobs(customMinSpareJobResources) &&
        SchedulingContext_->GetNow() < SchedulingDeadline_;
}

TScheduleJobsContext::TFairShareScheduleJobResult TScheduleJobsContext::ScheduleJob(bool ignorePacking)
{
    ++StageState_->ScheduleJobAttemptCount;

    auto* bestOperation = FindBestOperationForScheduling();
    if (!bestOperation) {
        return TFairShareScheduleJobResult{
            .Finished = true,
            .Scheduled = false,
        };
    }

    bool scheduled = ScheduleJob(bestOperation, ignorePacking);

    if (scheduled) {
        ReactivateBadPackingOperations();
    }

    if (SchedulingContext_->GetNow() >= SchedulingDeadline_) {
        ScheduleJobsDeadlineReachedCounter_.Increment();
    }

    return TFairShareScheduleJobResult{
        .Finished = false,
        .Scheduled = scheduled,
    };
}

bool TScheduleJobsContext::ScheduleJobInTest(TSchedulerOperationElement* element, bool ignorePacking)
{
    return ScheduleJob(element, ignorePacking);
}

int TScheduleJobsContext::GetOperationWithPreemptionPriorityCount(EOperationPreemptionPriority priority) const
{
    return OperationCountByPreemptionPriority_[priority];
}

void TScheduleJobsContext::AnalyzePreemptibleJobs(
    EOperationPreemptionPriority targetOperationPreemptionPriority,
    EJobPreemptionLevel minJobPreemptionLevel,
    std::vector<TJobWithPreemptionInfo>* unconditionallyPreemptibleJobs,
    TNonOwningJobSet* forcefullyPreemptibleJobs)
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();

    YT_LOG_TRACE("Looking for preemptible jobs (MinJobPreemptionLevel: %v)", minJobPreemptionLevel);

    int totalConditionallyPreemptibleJobCount = 0;
    int maxConditionallyPreemptibleJobCountInPool = 0;

    NProfiling::TWallTimer timer;

    auto jobInfos = CollectRunningJobsWithPreemptionInfo(SchedulingContext_, TreeSnapshot_);
    for (const auto& jobInfo : jobInfos) {
        const auto& [job, preemptionStatus, operationElement] = jobInfo;

        bool isJobForcefullyPreemptible = !IsSchedulingSegmentCompatibleWithNode(operationElement);
        if (isJobForcefullyPreemptible) {
            const auto& operationState = TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationState(operationElement);

            YT_ELEMENT_LOG_DETAILED(operationElement,
                "Job is forcefully preemptible because it is running on a node in a different scheduling segment or module "
                "(JobId: %v, OperationId: %v, OperationSegment: %v, NodeSegment: %v, Address: %v, Module: %v)",
                job->GetId(),
                operationElement->GetId(),
                operationState->SchedulingSegment,
                NodeSchedulingSegment_,
                SchedulingContext_->GetNodeDescriptor().Address,
                SchedulingContext_->GetNodeDescriptor().DataCenter);

            forcefullyPreemptibleJobs->insert(job.Get());
        }

        bool isJobPreemptible = isJobForcefullyPreemptible || (GetJobPreemptionLevel(jobInfo) >= minJobPreemptionLevel);
        if (!isJobPreemptible) {
            continue;
        }

        auto preemptionBlockingAncestor = FindPreemptionBlockingAncestor(operationElement, targetOperationPreemptionPriority);
        bool isUnconditionalPreemptionAllowed = isJobForcefullyPreemptible || preemptionBlockingAncestor == nullptr;
        bool isConditionalPreemptionAllowed = treeConfig->EnableConditionalPreemption &&
            !isUnconditionalPreemptionAllowed &&
            preemptionStatus == EJobPreemptionStatus::Preemptible &&
            preemptionBlockingAncestor != operationElement;

        if (isUnconditionalPreemptionAllowed) {
            const auto* parent = operationElement->GetParent();
            while (parent) {
                LocalUnconditionalUsageDiscountMap_[parent->GetTreeIndex()] += job->ResourceUsage();
                parent = parent->GetParent();
            }
            SchedulingContext_->UnconditionalResourceUsageDiscount() += job->ResourceUsage();
            unconditionallyPreemptibleJobs->push_back(jobInfo);
        } else if (isConditionalPreemptionAllowed) {
            ConditionallyPreemptibleJobSetMap_[preemptionBlockingAncestor->GetTreeIndex()].insert(jobInfo);
            ++totalConditionallyPreemptibleJobCount;
        }
    }

    TPrepareConditionalUsageDiscountsContext context{.TargetOperationPreemptionPriority = targetOperationPreemptionPriority};
    PrepareConditionalUsageDiscountsAtCompositeElement(TreeSnapshot_->RootElement().Get(), &context);
    for (const auto& [_, jobSet] : ConditionallyPreemptibleJobSetMap_) {
        maxConditionallyPreemptibleJobCountInPool = std::max(
            maxConditionallyPreemptibleJobCountInPool,
            static_cast<int>(jobSet.size()));
    }

    StageState_->AnalyzeJobsDuration += timer.GetElapsedTime();

    SchedulingStatistics_.UnconditionallyPreemptibleJobCount = unconditionallyPreemptibleJobs->size();
    SchedulingStatistics_.UnconditionalResourceUsageDiscount = SchedulingContext_->UnconditionalResourceUsageDiscount();
    SchedulingStatistics_.MaxConditionalResourceUsageDiscount = SchedulingContext_->GetMaxConditionalUsageDiscount();
    SchedulingStatistics_.TotalConditionallyPreemptibleJobCount = totalConditionallyPreemptibleJobCount;
    SchedulingStatistics_.MaxConditionallyPreemptibleJobCountInPool = maxConditionallyPreemptibleJobCountInPool;
}

void TScheduleJobsContext::PreemptJobsAfterScheduling(
    EOperationPreemptionPriority targetOperationPreemptionPriority,
    std::vector<TJobWithPreemptionInfo> preemptibleJobs,
    const TNonOwningJobSet& forcefullyPreemptibleJobs,
    const TJobPtr& jobStartedUsingPreemption)
{
    // Collect conditionally preemptible jobs.
    EOperationPreemptionPriority preemptorOperationLocalPreemptionPriority;
    TJobWithPreemptionInfoSet conditionallyPreemptibleJobs;
    if (jobStartedUsingPreemption) {
        auto* operationElement = TreeSnapshot_->FindEnabledOperationElement(jobStartedUsingPreemption->GetOperationId());
        YT_VERIFY(operationElement);

        preemptorOperationLocalPreemptionPriority = GetOperationPreemptionPriority(operationElement, EOperationPreemptionPriorityScope::OperationOnly);

        auto* parent = operationElement->GetParent();
        while (parent) {
            const auto& parentConditionallyPreemptibleJobs = GetConditionallyPreemptibleJobsInPool(parent);
            conditionallyPreemptibleJobs.insert(
                parentConditionallyPreemptibleJobs.begin(),
                parentConditionallyPreemptibleJobs.end());

            parent = parent->GetParent();
        }
    }

    preemptibleJobs.insert(preemptibleJobs.end(), conditionallyPreemptibleJobs.begin(), conditionallyPreemptibleJobs.end());
    SortJobsWithPreemptionInfo(&preemptibleJobs);
    std::reverse(preemptibleJobs.begin(), preemptibleJobs.end());

    // Reset discounts.
    SchedulingContext_->ResetUsageDiscounts();
    LocalUnconditionalUsageDiscountMap_.clear();
    ConditionallyPreemptibleJobSetMap_.clear();

    auto findPoolWithViolatedLimitsForJob = [&] (const TJobPtr& job) -> const TSchedulerCompositeElement* {
        auto* operationElement = TreeSnapshot_->FindEnabledOperationElement(job->GetOperationId());
        if (!operationElement) {
            return nullptr;
        }

        auto* parent = operationElement->GetParent();
        while (parent) {
            if (parent->AreResourceLimitsViolated()) {
                return parent;
            }
            parent = parent->GetParent();
        }
        return nullptr;
    };

    // TODO(eshcherbin): Use a separate tag for specifying preemptive scheduling stage.
    // Bloating |EJobPreemptionReason| is unwise.
    auto preemptionReason = [&] {
        switch (targetOperationPreemptionPriority) {
            case EOperationPreemptionPriority::Normal:
                return EJobPreemptionReason::Preemption;
            case EOperationPreemptionPriority::SsdNormal:
                return EJobPreemptionReason::SsdPreemption;
            case EOperationPreemptionPriority::Aggressive:
                return EJobPreemptionReason::AggressivePreemption;
            case EOperationPreemptionPriority::SsdAggressive:
                return EJobPreemptionReason::SsdAggressivePreemption;
            default:
                YT_ABORT();
        }
    }();

    int currentJobIndex = 0;
    for (; currentJobIndex < std::ssize(preemptibleJobs); ++currentJobIndex) {
        if (Dominates(SchedulingContext_->ResourceLimits(), SchedulingContext_->ResourceUsage())) {
            break;
        }

        const auto& jobInfo = preemptibleJobs[currentJobIndex];
        const auto& [job, preemptionStatus, operationElement] = jobInfo;

        if (!IsJobKnown(operationElement, job->GetId())) {
            // Job may have been terminated concurrently with scheduling, e.g. operation aborted by user request. See: YT-16429.
            YT_LOG_DEBUG("Job preemption skipped, since the job is already terminated (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());

            continue;
        }

        if (jobStartedUsingPreemption) {
            TStringBuilder preemptionReasonBuilder;
            preemptionReasonBuilder.AppendFormat(
                "Preempted to start job %v of operation %v; "
                "this job had status %Qlv and level %Qlv, preemptor operation local priority was %Qlv, "
                "and scheduling stage target priority was %Qlv",
                jobStartedUsingPreemption->GetId(),
                jobStartedUsingPreemption->GetOperationId(),
                preemptionStatus,
                GetJobPreemptionLevel(jobInfo),
                preemptorOperationLocalPreemptionPriority,
                targetOperationPreemptionPriority);
            if (forcefullyPreemptibleJobs.contains(job.Get())) {
                preemptionReasonBuilder.AppendString(
                    "; this job was forcefully preemptible, because its node was moved to other scheduling segment");
            }
            if (conditionallyPreemptibleJobs.contains(jobInfo)) {
                preemptionReasonBuilder.AppendString("; this job was conditionally preemptible");
            }

            job->SetPreemptionReason(preemptionReasonBuilder.Flush());

            job->SetPreemptedFor(TPreemptedFor{
                .JobId = jobStartedUsingPreemption->GetId(),
                .OperationId = jobStartedUsingPreemption->GetOperationId(),
            });

            job->SetPreemptedForProperlyStarvingOperation(
                targetOperationPreemptionPriority == preemptorOperationLocalPreemptionPriority);
        } else {
            job->SetPreemptionReason(Format("Node resource limits violated"));
        }
        PreemptJob(job, operationElement, preemptionReason);
    }

    // NB(eshcherbin): Specified resource limits can be violated in two cases:
    // 1. A job has just been scheduled with preemption over the limit.
    // 2. The limit has been reduced in the config.
    // Note that in the second case any job, which is considered preemptible at least in some stage,
    // may be preempted (e.g. an aggressively preemptible job can be preempted without scheduling any new jobs).
    // This is one of the reasons why we advise against specified resource limits.
    for (; currentJobIndex < std::ssize(preemptibleJobs); ++currentJobIndex) {
        const auto& jobInfo = preemptibleJobs[currentJobIndex];
        if (conditionallyPreemptibleJobs.contains(jobInfo)) {
            // Only unconditionally preemptible jobs can be preempted to recover violated resource limits.
            continue;
        }

        const auto& [job, _, operationElement] = jobInfo;
        if (!IsJobKnown(operationElement, job->GetId())) {
            // Job may have been terminated concurrently with scheduling, e.g. operation aborted by user request. See: YT-16429, YT-17913.
            YT_LOG_DEBUG("Job preemption skipped, since the job is already terminated (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());

            continue;
        }

        if (!Dominates(operationElement->GetResourceLimits(), operationElement->GetInstantResourceUsage())) {
            job->SetPreemptionReason(Format("Preempted due to violation of resource limits of operation %v",
                operationElement->GetId()));
            PreemptJob(job, operationElement, EJobPreemptionReason::ResourceLimitsViolated);
            continue;
        }

        if (auto violatedPool = findPoolWithViolatedLimitsForJob(job)) {
            job->SetPreemptionReason(Format("Preempted due to violation of limits on pool %Qv",
                violatedPool->GetId()));
            PreemptJob(job, operationElement, EJobPreemptionReason::ResourceLimitsViolated);
        }
    }

    if (!Dominates(SchedulingContext_->ResourceLimits(), SchedulingContext_->ResourceUsage())) {
        YT_LOG_INFO("Resource usage exceeds node resource limits even after preemption (ResourceLimits: %v, ResourceUsage: %v, NodeId: %v, Address: %v)",
            FormatResources(SchedulingContext_->ResourceLimits()),
            FormatResources(SchedulingContext_->ResourceUsage()),
            SchedulingContext_->GetNodeDescriptor().Id,
            SchedulingContext_->GetNodeDescriptor().Address);
    }
}

void TScheduleJobsContext::AbortJobsSinceResourcesOvercommit() const
{
    YT_LOG_DEBUG("Interrupting jobs on node since resources are overcommitted (NodeId: %v, Address: %v)",
        SchedulingContext_->GetNodeDescriptor().Id,
        SchedulingContext_->GetNodeDescriptor().Address);

    auto jobInfos = CollectRunningJobsWithPreemptionInfo(SchedulingContext_, TreeSnapshot_);
    SortJobsWithPreemptionInfo(&jobInfos);

    TJobResources currentResources;
    for (const auto& jobInfo : jobInfos) {
        if (!Dominates(SchedulingContext_->ResourceLimits(), currentResources + jobInfo.Job->ResourceUsage())) {
            YT_LOG_DEBUG("Interrupt job since node resources are overcommitted (JobId: %v, OperationId: %v, NodeAddress: %v)",
                jobInfo.Job->GetId(),
                jobInfo.OperationElement->GetId(),
                SchedulingContext_->GetNodeDescriptor().Address);

            jobInfo.Job->SetPreemptionReason("Preempted due to node resource ovecommit");
            PreemptJob(jobInfo.Job, jobInfo.OperationElement, EJobPreemptionReason::ResourceOvercommit);
        } else {
            currentResources += jobInfo.Job->ResourceUsage();
        }
    }
}

void TScheduleJobsContext::PreemptJob(
    const TJobPtr& job,
    TSchedulerOperationElement* element,
    EJobPreemptionReason preemptionReason) const
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();

    SchedulingContext_->ResourceUsage() -= job->ResourceUsage();
    job->ResourceUsage() = TJobResources();

    const auto& operationSharedState = TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    auto delta = operationSharedState->SetJobResourceUsage(job->GetId(), TJobResources());
    element->IncreaseHierarchicalResourceUsage(delta);
    operationSharedState->UpdatePreemptibleJobsList(element);

    SchedulingContext_->PreemptJob(job, treeConfig->JobInterruptTimeout, preemptionReason);
}

TNonOwningOperationElementList TScheduleJobsContext::ExtractBadPackingOperations()
{
    TNonOwningOperationElementList badPackingOperations;
    std::swap(BadPackingOperations_, badPackingOperations);

    return badPackingOperations;
}

void TScheduleJobsContext::StartStage(EJobSchedulingStage stage, TSchedulingStageProfilingCounters* profilingCounters)
{
    YT_VERIFY(!StageState_);

    StageState_.emplace(TStageState{
        .Stage = stage,
        .ProfilingCounters = profilingCounters,
    });
}

void TScheduleJobsContext::FinishStage()
{
    YT_VERIFY(StageState_);

    StageState_->DeactivationReasons[EDeactivationReason::NoBestLeafDescendant] = DynamicAttributesManager_.GetCompositeElementDeactivationCount();
    SchedulingStatistics_.ScheduleJobAttemptCountPerStage[GetStageType()] = StageState_->ScheduleJobAttemptCount;
    ProfileAndLogStatisticsOfStage();

    StageState_.reset();
}

int TScheduleJobsContext::GetStageMaxSchedulingIndex() const
{
    return StageState_->MaxSchedulingIndex;
}

bool TScheduleJobsContext::GetStagePrescheduleExecuted() const
{
    return StageState_->PrescheduleExecuted;
}

const TSchedulerElement* TScheduleJobsContext::FindPreemptionBlockingAncestor(
    const TSchedulerOperationElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority) const
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();
    const auto& spec = element->Spec();

    if (spec->PreemptionMode == EPreemptionMode::Graceful) {
        return element;
    }

    const TSchedulerElement* current = element;
    while (current) {
        // NB: This option is intended only for testing purposes.
        if (!IsRegularPreemptionAllowed(current)) {
            UpdateOperationPreemptionStatusStatistics(element, EOperationPreemptionStatus::ForbiddenInAncestorConfig);
            return element;
        }

        current = current->GetParent();
    }

    current = element;
    while (current && !current->IsRoot()) {
        // NB(eshcherbin): A bit strange that we check for starvation here and then for satisfaction later.
        // Maybe just satisfaction is enough?
        if (treeConfig->PreemptionCheckStarvation && current->GetStarvationStatus() != EStarvationStatus::NonStarving) {
            UpdateOperationPreemptionStatusStatistics(
                element,
                current == element
                    ? EOperationPreemptionStatus::ForbiddenSinceStarving
                    : EOperationPreemptionStatus::AllowedConditionally);
            return current;
        }

        bool useAggressiveThreshold = StaticAttributesOf(current).EffectiveAggressivePreemptionAllowed &&
            targetOperationPreemptionPriority >= EOperationPreemptionPriority::Aggressive;
        auto threshold = useAggressiveThreshold
            ? treeConfig->AggressivePreemptionSatisfactionThreshold
            : treeConfig->PreemptionSatisfactionThreshold;

        // NB: We want to use *local* satisfaction ratio here.
        double localSatisfactionRatio = current->ComputeLocalSatisfactionRatio(GetCurrentResourceUsage(current));
        if (treeConfig->PreemptionCheckSatisfaction && localSatisfactionRatio < threshold + NVectorHdrf::RatioComparisonPrecision) {
            UpdateOperationPreemptionStatusStatistics(
                element,
                current == element
                    ? EOperationPreemptionStatus::ForbiddenSinceUnsatisfied
                    : EOperationPreemptionStatus::AllowedConditionally);
            return current;
        }

        current = current->GetParent();
    }


    UpdateOperationPreemptionStatusStatistics(element, EOperationPreemptionStatus::AllowedUnconditionally);
    return {};
}

void TScheduleJobsContext::PrepareConditionalUsageDiscounts(const TSchedulerElement* element, TPrepareConditionalUsageDiscountsContext* context)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            PrepareConditionalUsageDiscountsAtCompositeElement(static_cast<const TSchedulerCompositeElement*>(element), context);
            break;
        case ESchedulerElementType::Operation:
            PrepareConditionalUsageDiscountsAtOperation(static_cast<const TSchedulerOperationElement*>(element), context);
            break;
        default:
            YT_ABORT();
    }
}

const TJobWithPreemptionInfoSet& TScheduleJobsContext::GetConditionallyPreemptibleJobsInPool(const TSchedulerCompositeElement* element) const
{
    auto it = ConditionallyPreemptibleJobSetMap_.find(element->GetTreeIndex());
    return it != ConditionallyPreemptibleJobSetMap_.end() ? it->second : EmptyJobWithPreemptionInfoSet;
}

const TDynamicAttributes& TScheduleJobsContext::DynamicAttributesOf(const TSchedulerElement* element) const
{
    YT_ASSERT(Initialized_);

    return DynamicAttributesManager_.AttributesOf(element);
}

void TScheduleJobsContext::DeactivateOperationInTest(TSchedulerOperationElement* element)
{
    return DynamicAttributesManager_.DeactivateOperation(element);
}

const TStaticAttributes& TScheduleJobsContext::StaticAttributesOf(const TSchedulerElement* element) const
{
    return TreeSnapshot_->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element);
}

bool TScheduleJobsContext::IsActive(const TSchedulerElement* element) const
{
    return DynamicAttributesManager_.AttributesOf(element).Active;
}

TJobResources TScheduleJobsContext::GetCurrentResourceUsage(const TSchedulerElement* element) const
{
    if (element->IsSchedulable()) {
        return DynamicAttributesOf(element).ResourceUsage;
    } else {
        return element->PostUpdateAttributes().UnschedulableOperationsResourceUsage;
    }
}

TJobResources TScheduleJobsContext::GetHierarchicalAvailableResources(const TSchedulerElement* element) const
{
    auto availableResources = TJobResources::Infinite();
    while (element) {
        availableResources = Min(availableResources, GetLocalAvailableResourceLimits(element));
        element = element->GetParent();
    }

    return availableResources;
}

TJobResources TScheduleJobsContext::GetLocalAvailableResourceLimits(const TSchedulerElement* element) const
{
    if (element->GetHasSpecifiedResourceLimits()) {
        return ComputeAvailableResources(
            element->ResourceLimits(),
            element->GetResourceUsageWithPrecommit(),
            GetLocalUnconditionalUsageDiscount(element));
    }
    return TJobResources::Infinite();
}

TJobResources TScheduleJobsContext::GetLocalUnconditionalUsageDiscount(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_VERIFY(index != UnassignedTreeIndex);

    auto it = LocalUnconditionalUsageDiscountMap_.find(index);
    return it != LocalUnconditionalUsageDiscountMap_.end() ? it->second : TJobResources{};
}

void TScheduleJobsContext::CollectConsideredSchedulableChildrenPerPool(
    const std::optional<TNonOwningOperationElementList>& consideredSchedulableOperations)
{
    // NB: This means all schedulable operations are considered, so no need for extra work,
    // because full lists of schedulable children have been precomputed during post update.
    if (!consideredSchedulableOperations) {
        return;
    }

    // NB: In case there are no considered operations.
    EmplaceOrCrash(
        ConsideredSchedulableChildrenPerPool_,
        TreeSnapshot_->RootElement().Get(),
        TNonOwningElementList{});
    for (auto* operationElement : *consideredSchedulableOperations) {
        TSchedulerElement* element = operationElement;
        while (auto* parent = element->GetMutableParent()) {
            auto [it, firstVisit] = ConsideredSchedulableChildrenPerPool_.try_emplace(parent);
            auto& parentSchedulableChildren = it->second;
            parentSchedulableChildren.push_back(element);

            if (!firstVisit) {
                break;
            }

            element = parent;
        }
    }
}

std::optional<TNonOwningElementList> TScheduleJobsContext::GetConsideredSchedulableChildrenForPool(const TSchedulerCompositeElement* element)
{
    auto it = ConsideredSchedulableChildrenPerPool_.find(element);
    return it != ConsideredSchedulableChildrenPerPool_.end()
        ? std::make_optional(std::move(it->second))
        : std::nullopt;
}

void TScheduleJobsContext::PrescheduleJob(
    TSchedulerElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            PrescheduleJobAtCompositeElement(static_cast<TSchedulerCompositeElement*>(element), targetOperationPreemptionPriority);
            break;
        case ESchedulerElementType::Operation:
            PrescheduleJobAtOperation(static_cast<TSchedulerOperationElement*>(element), targetOperationPreemptionPriority);
            break;
        default:
            YT_ABORT();
    }
}

void TScheduleJobsContext::PrescheduleJobAtCompositeElement(
    TSchedulerCompositeElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    auto onDeactivated = [&] (EDeactivationReason deactivationReason) {
        ++StageState_->DeactivationReasons[deactivationReason];
        YT_VERIFY(!DynamicAttributesOf(element).Active);
    };

    if (!element->IsAlive()) {
        onDeactivated(EDeactivationReason::IsNotAlive);
        return;
    }

    if (TreeSnapshot_->TreeConfig()->EnableSchedulingTags && !CanSchedule(StaticAttributesOf(element).SchedulingTagFilterIndex)) {
        onDeactivated(EDeactivationReason::UnmatchedSchedulingTag);
        return;
    }

    auto consideredSchedulableChildren = GetConsideredSchedulableChildrenForPool(element);
    if (consideredSchedulableChildren) {
        for (auto* child : *consideredSchedulableChildren) {
            PrescheduleJob(child, targetOperationPreemptionPriority);
        }
    } else {
        // COMPAT(eshcherbin): Leave old code as it is for now to have a fallback option.
        for (const auto& child : element->SchedulableChildren()) {
            PrescheduleJob(child.Get(), targetOperationPreemptionPriority);
        }
    }

    bool useChildHeap = false;
    int schedulableChildrenCount = consideredSchedulableChildren
        ? std::ssize(*consideredSchedulableChildren)
        : std::ssize(element->SchedulableChildren());
    if (schedulableChildrenCount >= TreeSnapshot_->TreeConfig()->MinChildHeapSize) {
        useChildHeap = true;
        StageState_->TotalHeapElementCount += schedulableChildrenCount;
    }

    DynamicAttributesManager_.InitializeAttributesAtCompositeElement(element, std::move(consideredSchedulableChildren), useChildHeap);

    if (DynamicAttributesOf(element).Active) {
        ++StageState_->ActiveTreeSize;
    }
}

void TScheduleJobsContext::PrescheduleJobAtOperation(
    TSchedulerOperationElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    bool isActive = CheckForDeactivation(element, targetOperationPreemptionPriority);
    DynamicAttributesManager_.InitializeAttributesAtOperation(element, isActive);

    if (isActive) {
        ++StageState_->ActiveTreeSize;
        ++StageState_->ActiveOperationCount;
    }
}

TSchedulerOperationElement* TScheduleJobsContext::FindBestOperationForScheduling()
{
    const auto& attributes = DynamicAttributesOf(TreeSnapshot_->RootElement().Get());
    TSchedulerOperationElement* bestLeafDescendant = nullptr;
    TSchedulerOperationElement* lastConsideredBestLeafDescendant = nullptr;
    while (!bestLeafDescendant) {
        if (!attributes.Active) {
            return nullptr;
        }

        bestLeafDescendant = attributes.BestLeafDescendant;
        if (!bestLeafDescendant->IsAlive() || !IsOperationEnabled(bestLeafDescendant)) {
            DeactivateOperation(bestLeafDescendant, EDeactivationReason::IsNotAlive);
            bestLeafDescendant = nullptr;
            continue;
        }
        if (lastConsideredBestLeafDescendant != bestLeafDescendant && IsOperationResourceUsageOutdated(bestLeafDescendant)) {
            UpdateOperationResourceUsage(bestLeafDescendant);
            lastConsideredBestLeafDescendant = bestLeafDescendant;
            bestLeafDescendant = nullptr;
            continue;
        }
    }

    return bestLeafDescendant;
}

bool TScheduleJobsContext::ScheduleJob(TSchedulerOperationElement* element, bool ignorePacking)
{
    YT_VERIFY(IsActive(element));

    YT_ELEMENT_LOG_DETAILED(element,
        "Trying to schedule job "
        "(SatisfactionRatio: %v, NodeId: %v, NodeResourceUsage: %v, "
        "UsageDiscount: {Total: %v, Unconditional: %v, Conditional: %v}, StageType: %v)",
        DynamicAttributesOf(element).SatisfactionRatio,
        SchedulingContext_->GetNodeDescriptor().Id,
        FormatResourceUsage(SchedulingContext_->ResourceUsage(), SchedulingContext_->ResourceLimits()),
        FormatResources(SchedulingContext_->UnconditionalResourceUsageDiscount() +
            SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
        FormatResources(SchedulingContext_->UnconditionalResourceUsageDiscount()),
        FormatResources(SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
        GetStageType());

    auto deactivateOperationElement = [&] (EDeactivationReason reason) {
        YT_ELEMENT_LOG_DETAILED(element,
            "Failed to schedule job, operation deactivated "
            "(DeactivationReason: %v, NodeResourceUsage: %v)",
            FormatEnum(reason),
            FormatResourceUsage(SchedulingContext_->ResourceUsage(), SchedulingContext_->ResourceLimits()));

        DeactivateOperation(element, reason);
    };

    auto recordPackingHeartbeatWithTimer = [&] (const auto& heartbeatSnapshot) {
        NProfiling::TWallTimer timer;
        RecordPackingHeartbeat(element, heartbeatSnapshot);
        StageState_->PackingRecordHeartbeatDuration += timer.GetElapsedTime();
    };

    auto decreaseHierarchicalResourceUsagePrecommit = [&] (const TJobResources& precommittedResources, int scheduleJobEpoch) {
        if (IsOperationEnabled(element) && scheduleJobEpoch == element->GetControllerEpoch()) {
            element->DecreaseHierarchicalResourceUsagePrecommit(precommittedResources);
        }
    };

    int schedulingIndex = StaticAttributesOf(element).SchedulingIndex;
    YT_VERIFY(schedulingIndex != UndefinedSchedulingIndex);
    ++StageState_->SchedulingIndexToScheduleJobAttemptCount[schedulingIndex];
    StageState_->MaxSchedulingIndex = std::max(StageState_->MaxSchedulingIndex, schedulingIndex);

    if (auto blockedReason = CheckBlocked(element)) {
        deactivateOperationElement(*blockedReason);
        return false;
    }

    if (!IsOperationEnabled(element)) {
        deactivateOperationElement(EDeactivationReason::IsNotAlive);
        return false;
    }

    if (!HasJobsSatisfyingResourceLimits(element)) {
        YT_ELEMENT_LOG_DETAILED(element,
            "No pending jobs can satisfy available resources on node ("
            "FreeResources: %v, DiscountResources: {Total: %v, Unconditional: %v, Conditional: %v}, "
            "MinNeededResources: %v, DetailedMinNeededResources: %v, "
            "Address: %v)",
            FormatResources(SchedulingContext_->GetNodeFreeResourcesWithoutDiscount()),
            FormatResources(SchedulingContext_->UnconditionalResourceUsageDiscount() +
                SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
            FormatResources(SchedulingContext_->UnconditionalResourceUsageDiscount()),
            FormatResources(SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
            FormatResources(element->AggregatedMinNeededJobResources()),
            MakeFormattableView(
                element->DetailedMinNeededJobResources(),
                [&] (TStringBuilderBase* builder, const TJobResourcesWithQuota& resources) {
                    builder->AppendFormat("%v", StrategyHost_->FormatResources(resources));
                }),
            SchedulingContext_->GetNodeDescriptor().Address);

        OnMinNeededResourcesUnsatisfied(
            element,
            SchedulingContext_->GetNodeFreeResourcesWithDiscountForOperation(element->GetOperationId()),
            element->AggregatedMinNeededJobResources());
        deactivateOperationElement(EDeactivationReason::MinNeededResourcesUnsatisfied);
        return false;
    }

    TJobResources precommittedResources;
    TJobResources availableResources;

    int scheduleJobEpoch = element->GetControllerEpoch();

    auto deactivationReason = TryStartScheduleJob(
        element,
        &precommittedResources,
        &availableResources);
    if (deactivationReason) {
        deactivateOperationElement(*deactivationReason);
        return false;
    }

    std::optional<TPackingHeartbeatSnapshot> heartbeatSnapshot;
    if (GetPackingConfig()->Enable && !ignorePacking) {
        heartbeatSnapshot = CreateHeartbeatSnapshot(SchedulingContext_);

        bool acceptPacking;
        {
            NProfiling::TWallTimer timer;
            acceptPacking = CheckPacking(element, *heartbeatSnapshot);
            StageState_->PackingCheckDuration += timer.GetElapsedTime();
        }

        if (!acceptPacking) {
            recordPackingHeartbeatWithTimer(*heartbeatSnapshot);
            decreaseHierarchicalResourceUsagePrecommit(precommittedResources, scheduleJobEpoch);
            deactivateOperationElement(EDeactivationReason::BadPacking);
            BadPackingOperations_.push_back(element);
            FinishScheduleJob(element);

            return false;
        }
    }

    TControllerScheduleJobResultPtr scheduleJobResult;
    {
        NProfiling::TWallTimer timer;

        scheduleJobResult = DoScheduleJob(element, availableResources, &precommittedResources);

        auto scheduleJobDuration = timer.GetElapsedTime();
        StageState_->TotalScheduleJobDuration += scheduleJobDuration;
        StageState_->ExecScheduleJobDuration += scheduleJobResult->Duration;
    }

    if (!scheduleJobResult->StartDescriptor) {
        for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
            StageState_->FailedScheduleJob[reason] += scheduleJobResult->Failed[reason];
        }

        ++StageState_->ScheduleJobFailureCount;
        deactivateOperationElement(EDeactivationReason::ScheduleJobFailed);

        element->OnScheduleJobFailed(
            SchedulingContext_->GetNow(),
            element->GetTreeId(),
            scheduleJobResult);

        decreaseHierarchicalResourceUsagePrecommit(precommittedResources, scheduleJobEpoch);
        FinishScheduleJob(element);

        return false;
    }

    const auto& startDescriptor = *scheduleJobResult->StartDescriptor;

    const auto& operationSharedState = TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    bool onJobStartedSuccess = operationSharedState->OnJobStarted(
        element,
        startDescriptor.Id,
        startDescriptor.ResourceLimits,
        precommittedResources,
        scheduleJobEpoch);
    if (!onJobStartedSuccess) {
        element->AbortJob(
            startDescriptor.Id,
            EAbortReason::SchedulingOperationDisabled,
            scheduleJobResult->ControllerEpoch);
        deactivateOperationElement(EDeactivationReason::OperationDisabled);
        decreaseHierarchicalResourceUsagePrecommit(precommittedResources, scheduleJobEpoch);
        FinishScheduleJob(element);

        return false;
    }

    SchedulingContext_->StartJob(
        element->GetTreeId(),
        element->GetOperationId(),
        scheduleJobResult->IncarnationId,
        scheduleJobResult->ControllerEpoch,
        startDescriptor,
        element->Spec()->PreemptionMode,
        schedulingIndex,
        GetStageType());

    UpdateOperationResourceUsage(element);

    if (heartbeatSnapshot) {
        recordPackingHeartbeatWithTimer(*heartbeatSnapshot);
    }

    FinishScheduleJob(element);

    YT_ELEMENT_LOG_DETAILED(element,
        "Scheduled a job (SatisfactionRatio: %v, NodeId: %v, JobId: %v, JobResourceLimits: %v)",
        DynamicAttributesOf(element).SatisfactionRatio,
        SchedulingContext_->GetNodeDescriptor().Id,
        startDescriptor.Id,
        StrategyHost_->FormatResources(startDescriptor.ResourceLimits));

    return true;
}

void TScheduleJobsContext::PrepareConditionalUsageDiscountsAtCompositeElement(
    const TSchedulerCompositeElement* element,
    TPrepareConditionalUsageDiscountsContext* context)
{
    TJobResources deltaConditionalDiscount;
    for (const auto& jobInfo : GetConditionallyPreemptibleJobsInPool(element)) {
        deltaConditionalDiscount += jobInfo.Job->ResourceUsage();
    }

    context->CurrentConditionalDiscount += deltaConditionalDiscount;
    for (const auto& child : element->SchedulableChildren()) {
        PrepareConditionalUsageDiscounts(child.Get(), context);
    }
    context->CurrentConditionalDiscount -= deltaConditionalDiscount;
}

void TScheduleJobsContext::PrepareConditionalUsageDiscountsAtOperation(
    const TSchedulerOperationElement* element,
    TPrepareConditionalUsageDiscountsContext* context)
{
    if (GetOperationPreemptionPriority(element) != context->TargetOperationPreemptionPriority) {
        return;
    }

    SchedulingContext_->SetConditionalDiscountForOperation(element->GetOperationId(), context->CurrentConditionalDiscount);
}

std::optional<EDeactivationReason> TScheduleJobsContext::TryStartScheduleJob(
    TSchedulerOperationElement* element,
    TJobResources* precommittedResourcesOutput,
    TJobResources* availableResourcesOutput)
{
    const auto& minNeededResources = element->AggregatedMinNeededJobResources();

    // Do preliminary checks to avoid the overhead of updating and reverting precommit usage.
    if (!Dominates(GetHierarchicalAvailableResources(element), minNeededResources)) {
        return EDeactivationReason::ResourceLimitsExceeded;
    }
    if (!element->CheckAvailableDemand(minNeededResources)) {
        return EDeactivationReason::NoAvailableDemand;
    }

    TJobResources availableResourceLimits;
    auto increaseResult = element->TryIncreaseHierarchicalResourceUsagePrecommit(
        minNeededResources,
        &availableResourceLimits);

    if (increaseResult == EResourceTreeIncreaseResult::ResourceLimitExceeded) {
        return EDeactivationReason::ResourceLimitsExceeded;
    }
    if (increaseResult == EResourceTreeIncreaseResult::ElementIsNotAlive) {
        return EDeactivationReason::IsNotAlive;
    }

    element->IncreaseConcurrentScheduleJobCalls(SchedulingContext_);
    element->IncreaseScheduleJobCallsSinceLastUpdate(SchedulingContext_);

    *precommittedResourcesOutput = minNeededResources;
    *availableResourcesOutput = Min(
        availableResourceLimits,
        SchedulingContext_->GetNodeFreeResourcesWithDiscountForOperation(element->GetOperationId()));
    return {};
}

TControllerScheduleJobResultPtr TScheduleJobsContext::DoScheduleJob(
    TSchedulerOperationElement* element,
    const TJobResources& availableResources,
    TJobResources* precommittedResources)
{
    ++SchedulingStatistics_.ControllerScheduleJobCount;

    auto scheduleJobResult = element->ScheduleJob(
        SchedulingContext_,
        availableResources,
        TreeSnapshot_->ControllerConfig()->ScheduleJobTimeLimit,
        element->GetTreeId(),
        TreeSnapshot_->TreeConfig());

    MaybeDelay(element->Spec()->TestingOperationOptions->ScheduleJobDelay);

    // Discard the job in case of resource overcommit.
    if (scheduleJobResult->StartDescriptor) {
        const auto& startDescriptor = *scheduleJobResult->StartDescriptor;
        // Note: |resourceDelta| might be negative.
        const auto resourceDelta = startDescriptor.ResourceLimits.ToJobResources() - *precommittedResources;
        // NB: If the element is disabled, we still choose the success branch. This is kind of a hotfix. See: YT-16070.
        auto increaseResult = EResourceTreeIncreaseResult::Success;
        if (IsOperationEnabled(element)) {
            increaseResult = element->TryIncreaseHierarchicalResourceUsagePrecommit(resourceDelta);
        }
        switch (increaseResult) {
            case EResourceTreeIncreaseResult::Success: {
                *precommittedResources += resourceDelta;
                break;
            }
            case EResourceTreeIncreaseResult::ResourceLimitExceeded: {
                auto jobId = scheduleJobResult->StartDescriptor->Id;
                // NB(eshcherbin): GetHierarchicalAvailableResource will never return infinite resources here,
                // because ResourceLimitExceeded could only be triggered if there's an ancestor with specified limits.
                auto availableDelta = GetHierarchicalAvailableResources(element);
                YT_LOG_DEBUG("Aborting job with resource overcommit (JobId: %v, Limits: %v, JobResources: %v)",
                    jobId,
                    FormatResources(*precommittedResources + availableDelta),
                    FormatResources(startDescriptor.ResourceLimits.ToJobResources()));

                element->AbortJob(
                    jobId,
                    EAbortReason::SchedulingResourceOvercommit,
                    scheduleJobResult->ControllerEpoch);

                // Reset result.
                scheduleJobResult = New<TControllerScheduleJobResult>();
                scheduleJobResult->RecordFail(EScheduleJobFailReason::ResourceOvercommit);
                break;
            }
            case EResourceTreeIncreaseResult::ElementIsNotAlive: {
                auto jobId = scheduleJobResult->StartDescriptor->Id;
                YT_LOG_DEBUG("Aborting job as operation is not alive in tree anymore (JobId: %v)", jobId);

                element->AbortJob(
                    jobId,
                    EAbortReason::SchedulingOperationIsNotAlive,
                    scheduleJobResult->ControllerEpoch);

                scheduleJobResult = New<TControllerScheduleJobResult>();
                scheduleJobResult->RecordFail(EScheduleJobFailReason::OperationIsNotAlive);
                break;
            }
            default:
                YT_ABORT();
        }
    } else if (scheduleJobResult->Failed[EScheduleJobFailReason::Timeout] > 0) {
        YT_LOG_WARNING("Job scheduling timed out");

        ++SchedulingStatistics_.ControllerScheduleJobTimedOutCount;

        YT_UNUSED_FUTURE(StrategyHost_->SetOperationAlert(
            element->GetOperationId(),
            EOperationAlertType::ScheduleJobTimedOut,
            TError("Job scheduling timed out: either scheduler is under heavy load or operation is too heavy"),
            TreeSnapshot_->ControllerConfig()->ScheduleJobTimeoutAlertResetTime));
    }

    return scheduleJobResult;
}

void TScheduleJobsContext::FinishScheduleJob(TSchedulerOperationElement* element)
{
    element->DecreaseConcurrentScheduleJobCalls(SchedulingContext_);
}

EOperationPreemptionPriority TScheduleJobsContext::GetOperationPreemptionPriority(
    const TSchedulerOperationElement* operationElement,
    EOperationPreemptionPriorityScope scope) const
{
    return NScheduler::GetOperationPreemptionPriority(
        operationElement,
        scope,
        SsdPriorityPreemptionEnabled_,
        SsdPriorityPreemptionMedia_);
}

bool TScheduleJobsContext::CheckForDeactivation(
    TSchedulerOperationElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();

    if (!DynamicAttributesOf(element).Alive) {
        OnOperationDeactivated(element, EDeactivationReason::IsNotAlive);
        return false;
    }

    if (targetOperationPreemptionPriority != EOperationPreemptionPriority::None &&
        targetOperationPreemptionPriority != GetOperationPreemptionPriority(element, treeConfig->SchedulingPreemptionPriorityScope))
    {
        auto deactivationReason = [&] {
            YT_VERIFY(targetOperationPreemptionPriority != EOperationPreemptionPriority::None);

            // TODO(eshcherbin): We can filter out all ineligible operations with the new preschedule.
            switch (targetOperationPreemptionPriority) {
                case EOperationPreemptionPriority::Normal:
                    return EDeactivationReason::IsNotEligibleForPreemptiveScheduling;
                case EOperationPreemptionPriority::SsdNormal:
                    return EDeactivationReason::IsNotEligibleForSsdPreemptiveScheduling;
                case EOperationPreemptionPriority::Aggressive:
                    return EDeactivationReason::IsNotEligibleForAggressivelyPreemptiveScheduling;
                case EOperationPreemptionPriority::SsdAggressive:
                    return EDeactivationReason::IsNotEligibleForSsdAggressivelyPreemptiveScheduling;
                default:
                    YT_ABORT();
            }
        }();
        OnOperationDeactivated(element, deactivationReason, /*considerInOperationCounter*/ false);
        return false;
    }

    if (TreeSnapshot_->TreeConfig()->CheckOperationForLivenessInPreschedule && !element->IsAlive()) {
        OnOperationDeactivated(element, EDeactivationReason::IsNotAlive);
        return false;
    }

    if (auto blockedReason = CheckBlocked(element)) {
        OnOperationDeactivated(element, *blockedReason);
        return false;
    }

    if (element->Spec()->PreemptionMode == EPreemptionMode::Graceful &&
        element->GetStatus() == ESchedulableStatus::Normal)
    {
        OnOperationDeactivated(element, EDeactivationReason::FairShareExceeded);
        return false;
    }

    if (treeConfig->EnableSchedulingTags && !CanSchedule(StaticAttributesOf(element).SchedulingTagFilterIndex)) {
        OnOperationDeactivated(element, EDeactivationReason::UnmatchedSchedulingTag);
        return false;
    }

    if (!IsSchedulingSegmentCompatibleWithNode(element)) {
        OnOperationDeactivated(element, EDeactivationReason::IncompatibleSchedulingSegment);
        return false;
    }

    if (SsdPriorityPreemptionEnabled_ &&
        !IsEligibleForSsdPriorityPreemption(element->DiskRequestMedia()) &&
        !StaticAttributesOf(element).AreRegularJobsOnSsdNodesAllowed)
    {
        OnOperationDeactivated(element, EDeactivationReason::RegularJobOnSsdNodeForbidden);
        return false;
    }

    if (element->GetTentative() &&
        element->IsSaturatedInTentativeTree(
            SchedulingContext_->GetNow(),
            element->GetTreeId(),
            treeConfig->TentativeTreeSaturationDeactivationPeriod))
    {
        OnOperationDeactivated(element, EDeactivationReason::SaturatedInTentativeTree);
        return false;
    }

    return true;
}

void TScheduleJobsContext::ActivateOperation(TSchedulerOperationElement* element)
{
    YT_VERIFY(!DynamicAttributesOf(element).Active);
    DynamicAttributesManager_.ActivateOperation(element);
}

void TScheduleJobsContext::DeactivateOperation(TSchedulerOperationElement* element, EDeactivationReason reason)
{
    YT_VERIFY(DynamicAttributesOf(element).Active);
    DynamicAttributesManager_.DeactivateOperation(element);
    OnOperationDeactivated(element, reason, /*considerInOperationCounter*/ true);
}

void TScheduleJobsContext::OnOperationDeactivated(
    TSchedulerOperationElement* element,
    EDeactivationReason reason,
    bool considerInOperationCounter)
{
    ++StageState_->DeactivationReasons[reason];
    if (considerInOperationCounter) {
        TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->OnOperationDeactivated(SchedulingContext_, reason);
    }
}

std::optional<EDeactivationReason> TScheduleJobsContext::CheckBlocked(const TSchedulerOperationElement* element) const
{
    if (element->IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(SchedulingContext_)) {
        return EDeactivationReason::MaxConcurrentScheduleJobCallsPerNodeShardViolated;
    }

    if (element->ScheduleJobBackoffCheckEnabled() &&
        element->HasRecentScheduleJobFailure(SchedulingContext_->GetNow()))
    {
        return EDeactivationReason::RecentScheduleJobFailed;
    }

    return std::nullopt;
}

bool TScheduleJobsContext::IsSchedulingSegmentCompatibleWithNode(const TSchedulerOperationElement* element) const
{
    if (TreeSnapshot_->TreeConfig()->SchedulingSegments->Mode == ESegmentedSchedulingMode::Disabled) {
        return true;
    }

    YT_VERIFY(TreeSnapshot_->IsElementEnabled(element));

    const auto& operationState = TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationState(element);

    YT_VERIFY(operationState->SchedulingSegment);

    const auto& nodeModule = TSchedulingSegmentManager::GetNodeModule(
        SchedulingContext_->GetNodeDescriptor(),
        TreeSnapshot_->TreeConfig()->SchedulingSegments->ModuleType);
    if (IsModuleAwareSchedulingSegment(*operationState->SchedulingSegment)) {
        if (!operationState->SchedulingSegmentModule) {
            // We have not decided on the operation's module yet.
            return false;
        }

        return operationState->SchedulingSegment == NodeSchedulingSegment_ &&
            operationState->SchedulingSegmentModule == nodeModule;
    }

    YT_VERIFY(!operationState->SchedulingSegmentModule);

    return operationState->SchedulingSegment == NodeSchedulingSegment_;
}

bool TScheduleJobsContext::IsOperationResourceUsageOutdated(const TSchedulerOperationElement* element) const
{
    auto now = SchedulingContext_->GetNow();
    auto updateTime = DynamicAttributesOf(element).ResourceUsageUpdateTime;
    return updateTime + DurationToCpuDuration(TreeSnapshot_->TreeConfig()->AllowedResourceUsageStaleness) < now;
}

void TScheduleJobsContext::UpdateOperationResourceUsage(TSchedulerOperationElement* element)
{
    DynamicAttributesManager_.UpdateOperationResourceUsage(element, SchedulingContext_->GetNow());
}

bool TScheduleJobsContext::HasJobsSatisfyingResourceLimits(const TSchedulerOperationElement* element) const
{
    for (const auto& jobResources : element->DetailedMinNeededJobResources()) {
        if (SchedulingContext_->CanStartJobForOperation(jobResources, element->GetOperationId())) {
            return true;
        }
    }
    return false;
}

TFairShareStrategyPackingConfigPtr TScheduleJobsContext::GetPackingConfig() const
{
    return TreeSnapshot_->TreeConfig()->Packing;
}

bool TScheduleJobsContext::CheckPacking(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot) const
{
    // NB: We expect DetailedMinNeededResources_ to be of size 1 most of the time.
    TJobResourcesWithQuota packingJobResourcesWithQuota;
    if (element->DetailedMinNeededJobResources().empty()) {
        // Refuse packing if no information about resource requirements is provided.
        return false;
    } else if (element->DetailedMinNeededJobResources().size() == 1) {
        packingJobResourcesWithQuota = element->DetailedMinNeededJobResources()[0];
    } else {
        auto idx = RandomNumber<ui32>(static_cast<ui32>(element->DetailedMinNeededJobResources().size()));
        packingJobResourcesWithQuota = element->DetailedMinNeededJobResources()[idx];
    }

    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->CheckPacking(
        element,
        heartbeatSnapshot,
        packingJobResourcesWithQuota,
        TreeSnapshot_->RootElement()->GetTotalResourceLimits(),
        GetPackingConfig());
}

void TScheduleJobsContext::ReactivateBadPackingOperations()
{
    for (auto* operation : BadPackingOperations_) {
        // TODO(antonkikh): multiple activations can be implemented more efficiently.
        ActivateOperation(operation);
    }
    BadPackingOperations_.clear();
}

void TScheduleJobsContext::RecordPackingHeartbeat(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot)
{
    TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->RecordPackingHeartbeat(heartbeatSnapshot, GetPackingConfig());
}

bool TScheduleJobsContext::IsJobKnown(const TSchedulerOperationElement* element, TJobId jobId) const
{
    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->IsJobKnown(jobId);
}

bool TScheduleJobsContext::IsOperationEnabled(const TSchedulerOperationElement* element) const
{
    // NB(eshcherbin): Operation may have been disabled since last fair share update.
    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->IsEnabled();
}

void TScheduleJobsContext::OnMinNeededResourcesUnsatisfied(
    const TSchedulerOperationElement* element,
    const TJobResources& availableResources,
    const TJobResources& minNeededResources) const
{
    TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->OnMinNeededResourcesUnsatisfied(
        SchedulingContext_,
        availableResources,
        minNeededResources);
}

void TScheduleJobsContext::UpdateOperationPreemptionStatusStatistics(
    const TSchedulerOperationElement* element,
    EOperationPreemptionStatus status) const
{
    TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->UpdatePreemptionStatusStatistics(status);
}

int TScheduleJobsContext::GetOperationRunningJobCount(const TSchedulerOperationElement* element) const
{
    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->GetRunningJobCount();
}

bool TScheduleJobsContext::CanSchedule(int schedulingTagFilterIndex) const
{
    return schedulingTagFilterIndex == EmptySchedulingTagFilterIndex ||
        CanSchedule_[schedulingTagFilterIndex];
}

EJobSchedulingStage TScheduleJobsContext::GetStageType() const
{
    return StageState_->Stage;
}

void TScheduleJobsContext::ProfileAndLogStatisticsOfStage()
{
    YT_VERIFY(StageState_);

    StageState_->TotalDuration = StageState_->Timer.GetElapsedTime();

    ProfileStageStatistics();

    if (StageState_->ScheduleJobAttemptCount > 0 && SchedulingInfoLoggingEnabled_) {
        LogStageStatistics();
    }
}

void TScheduleJobsContext::ProfileStageStatistics()
{
    if (!Initialized_) {
        return;
    }

    YT_VERIFY(StageState_);

    auto* profilingCounters = StageState_->ProfilingCounters;

    profilingCounters->PrescheduleJobTime.Record(StageState_->PrescheduleDuration);
    profilingCounters->CumulativePrescheduleJobTime.Add(StageState_->PrescheduleDuration);

    if (StageState_->PrescheduleExecuted) {
        profilingCounters->PrescheduleJobCount.Increment();
        if (StageState_->ScheduleJobAttemptCount == 0) {
            profilingCounters->UselessPrescheduleJobCount.Increment();
        }
    }

    auto strategyScheduleJobDuration = StageState_->TotalDuration
        - StageState_->PrescheduleDuration
        - StageState_->TotalScheduleJobDuration;
    profilingCounters->StrategyScheduleJobTime.Record(strategyScheduleJobDuration);
    profilingCounters->CumulativeStrategyScheduleJobTime.Add(strategyScheduleJobDuration);

    profilingCounters->TotalControllerScheduleJobTime.Record(StageState_->TotalScheduleJobDuration);
    profilingCounters->CumulativeTotalControllerScheduleJobTime.Add(StageState_->TotalScheduleJobDuration);
    profilingCounters->ExecControllerScheduleJobTime.Record(StageState_->ExecScheduleJobDuration);
    profilingCounters->CumulativeExecControllerScheduleJobTime.Add(StageState_->ExecScheduleJobDuration);
    profilingCounters->PackingRecordHeartbeatTime.Record(StageState_->PackingRecordHeartbeatDuration);
    profilingCounters->PackingCheckTime.Record(StageState_->PackingCheckDuration);
    profilingCounters->AnalyzeJobsTime.Record(StageState_->AnalyzeJobsDuration);
    profilingCounters->CumulativeAnalyzeJobsTime.Add(StageState_->AnalyzeJobsDuration);

    profilingCounters->ScheduleJobAttemptCount.Increment(StageState_->ScheduleJobAttemptCount);
    profilingCounters->ScheduleJobFailureCount.Increment(StageState_->ScheduleJobFailureCount);
    profilingCounters->ControllerScheduleJobCount.Increment(SchedulingStatistics().ControllerScheduleJobCount);
    profilingCounters->ControllerScheduleJobTimedOutCount.Increment(SchedulingStatistics().ControllerScheduleJobTimedOutCount);

    for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
        profilingCounters->ControllerScheduleJobFail[reason].Increment(StageState_->FailedScheduleJob[reason]);
    }
    for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
        profilingCounters->DeactivationCount[reason].Increment(StageState_->DeactivationReasons[reason]);
    }

    for (auto [schedulingIndex, count] : StageState_->SchedulingIndexToScheduleJobAttemptCount) {
        int rangeIndex = SchedulingIndexToProfilingRangeIndex(schedulingIndex);
        profilingCounters->SchedulingIndexCounters[rangeIndex].Increment(count);
    }
    if (StageState_->MaxSchedulingIndex >= 0) {
        profilingCounters->MaxSchedulingIndexCounters[SchedulingIndexToProfilingRangeIndex(StageState_->MaxSchedulingIndex)].Increment();
    }

    profilingCounters->ActiveTreeSize.Record(StageState_->ActiveTreeSize);
    profilingCounters->ActiveOperationCount.Record(StageState_->ActiveOperationCount);
}

void TScheduleJobsContext::LogStageStatistics()
{
    if (!Initialized_) {
        return;
    }

    YT_VERIFY(StageState_);

    YT_LOG_DEBUG(
        "Scheduling statistics (SchedulingStage: %v, ActiveTreeSize: %v, ActiveOperationCount: %v, TotalHeapElementCount: %v, "
        "DeactivationReasons: %v, CanStartMoreJobs: %v, Address: %v, SchedulingSegment: %v, MaxSchedulingIndex: %v)",
        StageState_->Stage,
        StageState_->ActiveTreeSize,
        StageState_->ActiveOperationCount,
        StageState_->TotalHeapElementCount,
        StageState_->DeactivationReasons,
        SchedulingContext_->CanStartMoreJobs(),
        SchedulingContext_->GetNodeDescriptor().Address,
        NodeSchedulingSegment_,
        StageState_->MaxSchedulingIndex);
}

EJobPreemptionLevel TScheduleJobsContext::GetJobPreemptionLevel(const TJobWithPreemptionInfo& jobWithPreemptionInfo) const
{
    const auto& [job, preemptionStatus, operationElement] = jobWithPreemptionInfo;

    bool isEligibleForSsdPriorityPreemption = SsdPriorityPreemptionEnabled_ &&
        IsEligibleForSsdPriorityPreemption(GetDiskQuotaMedia(job->DiskQuota()));
    auto aggressivePreemptionAllowed = StaticAttributesOf(operationElement).EffectiveAggressivePreemptionAllowed;
    switch (preemptionStatus) {
        case EJobPreemptionStatus::NonPreemptible:
            return isEligibleForSsdPriorityPreemption
                ? EJobPreemptionLevel::SsdNonPreemptible
                : EJobPreemptionLevel::NonPreemptible;
        case EJobPreemptionStatus::AggressivelyPreemptible:
            if (aggressivePreemptionAllowed) {
                return isEligibleForSsdPriorityPreemption
                    ? EJobPreemptionLevel::SsdAggressivelyPreemptible
                    : EJobPreemptionLevel::AggressivelyPreemptible;
            } else {
                return isEligibleForSsdPriorityPreemption
                    ? EJobPreemptionLevel::SsdNonPreemptible
                    : EJobPreemptionLevel::NonPreemptible;
            }
        case EJobPreemptionStatus::Preemptible:
            return EJobPreemptionLevel::Preemptible;
        default:
            YT_ABORT();
    }
}

bool TScheduleJobsContext::IsEligibleForSsdPriorityPreemption(const THashSet<int>& diskRequestMedia) const
{
    return NScheduler::IsEligibleForSsdPriorityPreemption(diskRequestMedia, SsdPriorityPreemptionMedia_);
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeJobScheduler::TFairShareTreeJobScheduler(
    TString treeId,
    NLogging::TLogger logger,
    TWeakPtr<IFairShareTreeJobSchedulerHost> host,
    IFairShareTreeHost* treeHost,
    ISchedulerStrategyHost* strategyHost,
    TFairShareStrategyTreeConfigPtr config,
    NProfiling::TProfiler profiler)
    : TreeId_(std::move(treeId))
    , Logger(std::move(logger))
    , Host_(std::move(host))
    , TreeHost_(treeHost)
    , StrategyHost_(strategyHost)
    , Config_(std::move(config))
    , Profiler_(std::move(profiler))
    , CumulativeScheduleJobsTime_(Profiler_.TimeCounter("/cumulative_schedule_jobs_time"))
    , ScheduleJobsTime_(Profiler_.Timer("/schedule_jobs_time"))
    , GracefulPreemptionTime_(Profiler_.Timer("/graceful_preemption_time"))
    , ScheduleJobsDeadlineReachedCounter_(Profiler_.Counter("/schedule_jobs_deadline_reached"))
    , OperationCountByPreemptionPriorityBufferedProducer_(New<TBufferedProducer>())
    , SchedulingSegmentManager_(TreeId_, Config_->SchedulingSegments, Logger, Profiler_)
{
    InitSchedulingProfilingCounters();

    Profiler_.AddProducer("/operation_count_by_preemption_priority", OperationCountByPreemptionPriorityBufferedProducer_);

    SchedulingSegmentsManagementExecutor_ = New<TPeriodicExecutor>(
        StrategyHost_->GetControlInvoker(EControlQueue::FairShareStrategy),
        BIND(&TFairShareTreeJobScheduler::ManageSchedulingSegments, MakeWeak(this)),
        Config_->SchedulingSegments->ManagePeriod);
    SchedulingSegmentsManagementExecutor_->Start();
}

void TFairShareTreeJobScheduler::RegisterNode(TNodeId nodeId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ESchedulingSegment initialSchedulingSegment = ESchedulingSegment::Default;
    if (auto maybeState = FindInitialNodePersistentState(nodeId)) {
        initialSchedulingSegment = maybeState->Segment;

        YT_LOG_DEBUG(
            "Revived node's scheduling segment "
            "(NodeId: %v, SchedulingSegment: %v)",
            nodeId,
            initialSchedulingSegment);
    }

    auto nodeShardId = StrategyHost_->GetNodeShardId(nodeId);
    const auto& nodeShardInvoker = StrategyHost_->GetNodeShardInvokers()[nodeShardId];
    nodeShardInvoker->Invoke(BIND([this, this_ = MakeStrong(this), nodeId, nodeShardId, initialSchedulingSegment] {
        EmplaceOrCrash(
            NodeStateShards_[nodeShardId].NodeIdToState,
            nodeId,
            TFairShareTreeJobSchedulerNodeState{
                .SchedulingSegment = initialSchedulingSegment,
            });
    }));
}

void TFairShareTreeJobScheduler::UnregisterNode(TNodeId nodeId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto nodeShardId = StrategyHost_->GetNodeShardId(nodeId);
    const auto& nodeShardInvoker = StrategyHost_->GetNodeShardInvokers()[nodeShardId];
    nodeShardInvoker->Invoke(BIND([this, this_ = MakeStrong(this), nodeId, nodeShardId] {
        EraseOrCrash(NodeStateShards_[nodeShardId].NodeIdToState, nodeId);
    }));
}

void TFairShareTreeJobScheduler::ProcessSchedulingHeartbeat(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    bool skipScheduleJobs)
{
    auto nodeId = schedulingContext->GetNodeDescriptor().Id;
    auto* nodeState = FindNodeState(nodeId);
    if (!nodeState) {
        YT_LOG_DEBUG("Skipping scheduling heartbeat because node is not registered in tree (NodeId: %v, NodeAddress: %v)",
            nodeId,
            schedulingContext->GetNodeDescriptor().Address);

        return;
    }

    const auto& treeConfig = treeSnapshot->TreeConfig();
    bool shouldUpdateRunningJobStatistics = !nodeState->LastRunningJobStatisticsUpdateTime ||
        schedulingContext->GetNow() > *nodeState->LastRunningJobStatisticsUpdateTime + DurationToCpuDuration(treeConfig->RunningJobStatisticsUpdatePeriod);
    if (shouldUpdateRunningJobStatistics) {
        nodeState->RunningJobStatistics = ComputeRunningJobStatistics(schedulingContext, treeSnapshot);
        nodeState->LastRunningJobStatisticsUpdateTime = schedulingContext->GetNow();
    }

    nodeState->Descriptor = schedulingContext->GetNodeDescriptor();
    nodeState->SpecifiedSchedulingSegment = [&] () -> std::optional<ESchedulingSegment> {
        const auto& schedulingOptions = nodeState->Descriptor->SchedulingOptions;
        if (!schedulingOptions) {
            return {};
        }

        // TODO(eshcherbin): Improve error handing. Ideally scheduling options parsing error should lead to a scheduler alert.
        try {
            return schedulingOptions->Find<ESchedulingSegment>("scheduling_segment");
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to parse specified scheduling segment (NodeId: %v, NodeAddress: %v)",
                nodeState->Descriptor->Id,
                nodeState->Descriptor->Address);

            return {};
        }
    }();

    PreemptJobsGracefully(schedulingContext, treeSnapshot);

    if (!skipScheduleJobs) {
        bool enableSchedulingInfoLogging = false;
        auto now = schedulingContext->GetNow();
        const auto& config = treeSnapshot->TreeConfig();
        if (LastSchedulingInformationLoggedTime_ + DurationToCpuDuration(config->HeartbeatTreeSchedulingInfoLogBackoff) < now) {
            enableSchedulingInfoLogging = true;
            LastSchedulingInformationLoggedTime_ = now;
        }

        TScheduleJobsContext context(
            schedulingContext,
            treeSnapshot,
            nodeState,
            enableSchedulingInfoLogging,
            StrategyHost_,
            ScheduleJobsDeadlineReachedCounter_,
            Logger);
        ScheduleJobs(&context);
    }
}

void TFairShareTreeJobScheduler::ScheduleJobs(TScheduleJobsContext* context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    NProfiling::TWallTimer scheduleJobsTimer;

    DoRegularJobScheduling(context);
    DoPreemptiveJobScheduling(context);

    // Interrupt some jobs if usage is greater that limit.
    if (context->SchedulingContext()->ShouldAbortJobsSinceResourcesOvercommit()) {
        context->AbortJobsSinceResourcesOvercommit();
    }

    context->SchedulingContext()->SetSchedulingStatistics(context->SchedulingStatistics());

    auto elapsedTime = scheduleJobsTimer.GetElapsedTime();
    CumulativeScheduleJobsTime_.Add(elapsedTime);
    ScheduleJobsTime_.Record(elapsedTime);
}

void TFairShareTreeJobScheduler::PreemptJobsGracefully(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    NProfiling::TEventTimerGuard eventTimerGuard(GracefulPreemptionTime_);

    const auto& treeConfig = treeSnapshot->TreeConfig();

    YT_LOG_TRACE("Looking for gracefully preemptible jobs");

    std::vector<TJobPtr> candidates;
    for (const auto& job : schedulingContext->RunningJobs()) {
        if (job->GetPreemptionMode() == EPreemptionMode::Graceful && !job->IsInterrupted()) {
            candidates.push_back(job);
        }
    }

    auto jobInfos = GetJobPreemptionInfos(candidates, treeSnapshot);
    for (const auto& [job, preemptionStatus, _] : jobInfos) {
        if (preemptionStatus == EJobPreemptionStatus::Preemptible) {
            schedulingContext->PreemptJob(job, treeConfig->JobGracefulInterruptTimeout, EJobPreemptionReason::GracefulPreemption);
        }
    }
}

void TFairShareTreeJobScheduler::RegisterOperation(const TSchedulerOperationElement* element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto operationId = element->GetOperationId();
    auto operationState = New<TFairShareTreeJobSchedulerOperationState>(
        element->Spec(),
        element->IsGang());
    if (auto maybeState = FindInitialOperationPersistentState(operationId)) {
        operationState->SchedulingSegmentModule = std::move(maybeState->Module);

        YT_LOG_DEBUG(
            "Revived operation's scheduling segment module assignment "
            "(OperationId: %v, SchedulingSegmentModule: %v)",
            operationId,
            operationState->SchedulingSegmentModule);
    }

    EmplaceOrCrash(
        OperationIdToState_,
        operationId,
        std::move(operationState));
    EmplaceOrCrash(
        OperationIdToSharedState_,
        operationId,
        New<TFairShareTreeJobSchedulerOperationSharedState>(
            StrategyHost_,
            element->Spec()->UpdatePreemptibleJobsListLoggingPeriod,
            Logger.WithTag("OperationId: %v", operationId)));
}

void TFairShareTreeJobScheduler::UnregisterOperation(const TSchedulerOperationElement* element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EraseOrCrash(OperationIdToState_, element->GetOperationId());
    EraseOrCrash(OperationIdToSharedState_, element->GetOperationId());
}

void TFairShareTreeJobScheduler::OnOperationMaterialized(const TSchedulerOperationElement* element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& operationState = GetOperationState(element->GetOperationId());
    operationState->InitialAggregatedMinNeededResources = element->GetInitialAggregatedMinNeededResources();

    SchedulingSegmentManager_.InitOrUpdateOperationSchedulingSegment(operationState);
}

TError TFairShareTreeJobScheduler::CheckOperationSchedulingInSeveralTreesAllowed(const TSchedulerOperationElement* element) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& operationState = GetOperationState(element->GetOperationId());
    auto segment = operationState->SchedulingSegment;
    if (IsModuleAwareSchedulingSegment(*segment)) {
        // NB: This error will be propagated to operation's failure only if operation is launched in several trees.
        return TError(
            "Scheduling in several trees is forbidden for operations in module-aware scheduling segments, "
            "specify a single tree or use the \"schedule_in_single_tree\" spec option")
            << TErrorAttribute("segment", segment);
    }

    return TError();
}

void TFairShareTreeJobScheduler::EnableOperation(const TSchedulerOperationElement* element) const
{
    auto operationId = element->GetOperationId();
    GetOperationSharedState(operationId)->Enable();
}

void TFairShareTreeJobScheduler::DisableOperation(TSchedulerOperationElement* element, bool markAsNonAlive) const
{
    auto operationId = element->GetOperationId();
    GetOperationSharedState(operationId)->Disable();
    element->ReleaseResources(markAsNonAlive);
}

void TFairShareTreeJobScheduler::RegisterJobsFromRevivedOperation(TSchedulerOperationElement* element, std::vector<TJobPtr> jobs) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SortBy(jobs, [] (const TJobPtr& job) {
        return job->GetStartTime();
    });

    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    for (const auto& job : jobs) {
        TJobResourcesWithQuota resourceUsageWithQuota = job->ResourceUsage();
        resourceUsageWithQuota.SetDiskQuota(job->DiskQuota());
        operationSharedState->OnJobStarted(
            element,
            job->GetId(),
            resourceUsageWithQuota,
            /*precommittedResources*/ {},
            // NB: |scheduleJobEpoch| is ignored in case |force| is true.
            /*scheduleJobEpoch*/ 0,
            /*force*/ true);
    }
}

void TFairShareTreeJobScheduler::ProcessUpdatedJob(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TSchedulerOperationElement* element,
    TJobId jobId,
    const TJobResources& jobResources,
    const std::optional<TString>& jobDataCenter,
    const std::optional<TString>& jobInfinibandCluster,
    bool* shouldAbortJob) const
{
    const auto& operationState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationState(element);
    const auto& operationSharedState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element);

    auto delta = operationSharedState->SetJobResourceUsage(jobId, jobResources);
    element->IncreaseHierarchicalResourceUsage(delta);
    operationSharedState->UpdatePreemptibleJobsList(element);

    const auto& operationSchedulingSegment = operationState->SchedulingSegment;
    if (operationSchedulingSegment && IsModuleAwareSchedulingSegment(*operationSchedulingSegment)) {
        const auto& operationModule = operationState->SchedulingSegmentModule;
        const auto& jobModule = TSchedulingSegmentManager::GetNodeModule(
            jobDataCenter,
            jobInfinibandCluster,
            element->TreeConfig()->SchedulingSegments->ModuleType);
        bool jobIsRunningInTheRightModule = operationModule && (operationModule == jobModule);
        if (!jobIsRunningInTheRightModule) {
            *shouldAbortJob = true;

            YT_LOG_DEBUG(
                "Requested to abort job because it is running in a wrong module "
                "(OperationId: %v, JobId: %v, OperationModule: %v, JobModule: %v)",
                element->GetOperationId(),
                jobId,
                operationModule,
                jobModule);
        }
    }
}

void TFairShareTreeJobScheduler::ProcessFinishedJob(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TSchedulerOperationElement* element,
    TJobId jobId) const
{
    const auto& operationSharedState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    operationSharedState->OnJobFinished(element, jobId);
}

void TFairShareTreeJobScheduler::BuildSchedulingAttributesStringForNode(TNodeId nodeId, TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    const auto* nodeState = FindNodeState(nodeId);
    if (!nodeState) {
        return;
    }

    delimitedBuilder->AppendFormat("SchedulingSegment: %v, RunningJobStatistics: %v",
        nodeState->SchedulingSegment,
        nodeState->RunningJobStatistics);
}

void TFairShareTreeJobScheduler::BuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const
{
    const auto* nodeState = FindNodeState(nodeId);
    if (!nodeState) {
        return;
    }

    fluent
        .Item("scheduling_segment").Value(nodeState->SchedulingSegment)
        .Item("running_job_statistics").Value(nodeState->RunningJobStatistics);
}

void TFairShareTreeJobScheduler::BuildSchedulingAttributesStringForOngoingJobs(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const std::vector<TJobPtr>& jobs,
    TInstant now,
    TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    auto cachedJobPreemptionStatuses = treeSnapshot
        ? treeSnapshot->SchedulingSnapshot()->CachedJobPreemptionStatuses()
        : TCachedJobPreemptionStatuses{.UpdateTime = now};

    TEnumIndexedVector<EJobPreemptionStatus, std::vector<TJobId>> jobIdsByPreemptionStatus;
    std::vector<TJobId> unknownStatusJobIds;
    for (const auto& job : jobs) {
        if (auto status = GetCachedJobPreemptionStatus(job, cachedJobPreemptionStatuses)) {
            jobIdsByPreemptionStatus[*status].push_back(job->GetId());
        } else {
            unknownStatusJobIds.push_back(job->GetId());
        }
    }

    delimitedBuilder->AppendFormat("JobIdsByPreemptionStatus: %v, UnknownStatusJobIds: %v, TimeSinceLastPreemptionStatusUpdateSeconds: %v",
        jobIdsByPreemptionStatus,
        unknownStatusJobIds,
        (now - cachedJobPreemptionStatuses.UpdateTime).SecondsFloat());
}

TError TFairShareTreeJobScheduler::CheckOperationIsHung(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TSchedulerOperationElement* element,
    TInstant now,
    TInstant activationTime,
    TDuration safeTimeout,
    int minScheduleJobCallAttempts,
    const THashSet<EDeactivationReason>& deactivationReasons)
{
    if (element->PersistentAttributes().StarvationStatus == EStarvationStatus::NonStarving) {
        return TError();
    }

    YT_VERIFY(treeSnapshot->IsElementEnabled(element));

    const auto& operationSharedState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    {
        int deactivationCount = 0;
        auto deactivationReasonToCount = operationSharedState->GetDeactivationReasonsFromLastNonStarvingTime();
        for (auto reason : deactivationReasons) {
            deactivationCount += deactivationReasonToCount[reason];
        }

        auto lastScheduleJobSuccessTime = operationSharedState->GetLastScheduleJobSuccessTime();
        if (activationTime + safeTimeout < now &&
            lastScheduleJobSuccessTime + safeTimeout < now &&
            element->GetLastNonStarvingTime() + safeTimeout < now &&
            operationSharedState->GetRunningJobCount() == 0 &&
            deactivationCount > minScheduleJobCallAttempts)
        {
            return TError("Operation has no successful scheduled jobs for a long period")
                << TErrorAttribute("period", safeTimeout)
                << TErrorAttribute("deactivation_count", deactivationCount)
                << TErrorAttribute("last_schedule_job_success_time", lastScheduleJobSuccessTime)
                << TErrorAttribute("last_non_starving_time", element->GetLastNonStarvingTime());
        }
    }

    // NB(eshcherbin): See YT-14393.
    const auto& operationState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationState(element);
    {
        const auto& segment = operationState->SchedulingSegment;
        const auto& schedulingSegmentModule = operationState->SchedulingSegmentModule;
        if (segment && IsModuleAwareSchedulingSegment(*segment) && schedulingSegmentModule && !element->GetSchedulingTagFilter().IsEmpty()) {
            auto tagFilter = element->GetSchedulingTagFilter().GetBooleanFormula().GetFormula();
            bool isModuleFilter = false;
            for (const auto& possibleModule : treeSnapshot->TreeConfig()->SchedulingSegments->GetModules()) {
                auto moduleTag = TSchedulingSegmentManager::GetNodeTagFromModuleName(
                    possibleModule,
                    treeSnapshot->TreeConfig()->SchedulingSegments->ModuleType);
                // NB(eshcherbin): This doesn't cover all the cases, only the most usual.
                // Don't really want to check boolean formula satisfiability here.
                if (tagFilter == moduleTag) {
                    isModuleFilter = true;
                    break;
                }
            }

            auto operationModuleTag = TSchedulingSegmentManager::GetNodeTagFromModuleName(
                *schedulingSegmentModule,
                treeSnapshot->TreeConfig()->SchedulingSegments->ModuleType);
            if (isModuleFilter && tagFilter != operationModuleTag) {
                return TError(
                    "Operation has a module specified in the scheduling tag filter, which causes scheduling problems; "
                    "use \"scheduling_segment_modules\" spec option instead")
                    << TErrorAttribute("scheduling_tag_filter", tagFilter)
                    << TErrorAttribute("available_modules", treeSnapshot->TreeConfig()->SchedulingSegments->GetModules());
            }
        }
    }

    return TError();
}

void TFairShareTreeJobScheduler::BuildOperationProgress(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TSchedulerOperationElement* element,
    ISchedulerStrategyHost* const strategyHost,
    TFluentMap fluent)
{
    bool isEnabled = treeSnapshot->IsElementEnabled(element);
    const auto& operationState = isEnabled
        ? treeSnapshot->SchedulingSnapshot()->GetEnabledOperationState(element)
        : treeSnapshot->SchedulingSnapshot()->GetOperationState(element);
    const auto& operationSharedState = isEnabled
        ? treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element)
        : treeSnapshot->SchedulingSnapshot()->GetOperationSharedState(element);
    const auto& attributes = isEnabled
        ? treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element)
        : TStaticAttributes{};
    fluent
        .Item("preemptible_job_count").Value(operationSharedState->GetPreemptibleJobCount())
        .Item("aggressively_preemptible_job_count").Value(operationSharedState->GetAggressivelyPreemptibleJobCount())
        .Item("scheduling_index").Value(attributes.SchedulingIndex)
        .Item("deactivation_reasons").Value(operationSharedState->GetDeactivationReasons())
        .Item("min_needed_resources_unsatisfied_count").Value(operationSharedState->GetMinNeededResourcesUnsatisfiedCount())
        .Item("disk_quota_usage").BeginMap()
            .Do([&] (TFluentMap fluent) {
                strategyHost->SerializeDiskQuota(operationSharedState->GetTotalDiskQuota(), fluent.GetConsumer());
            })
        .EndMap()
        .Item("are_regular_jobs_on_ssd_nodes_allowed").Value(attributes.AreRegularJobsOnSsdNodesAllowed)
        .Item("scheduling_segment").Value(operationState->SchedulingSegment)
        .Item("scheduling_segment_module").Value(operationState->SchedulingSegmentModule);
}

void TFairShareTreeJobScheduler::BuildElementYson(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TSchedulerElement* element,
    const TFieldsFilter& filter,
    TFluentMap fluent)
{
    const auto& attributes = treeSnapshot->IsElementEnabled(element)
        ? treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element)
        : TStaticAttributes{};
    fluent
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "aggressive_preemption_allowed", IsAggressivePreemptionAllowed(element))
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
            filter,
            "effective_aggressive_preemption_allowed",
            attributes.EffectiveAggressivePreemptionAllowed);
}

TJobSchedulerPostUpdateContext TFairShareTreeJobScheduler::CreatePostUpdateContext(TSchedulerRootElement* rootElement)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetControlInvoker(EControlQueue::FairShareStrategy));

    // NB(eshcherbin): We cannot update SSD media in the constructor, because initial pool trees update
    // in the registration pipeline is done before medium directory sync. That's why we do the initial update
    // during the first fair share update.
    if (!SsdPriorityPreemptionMedia_) {
        UpdateSsdPriorityPreemptionMedia();
    }

    return TJobSchedulerPostUpdateContext{
        .RootElement = rootElement,
        .SsdPriorityPreemptionMedia = SsdPriorityPreemptionMedia_.value_or(THashSet<int>()),
        .OperationIdToState = GetOperationStateMapSnapshot(),
        .OperationIdToSharedState = OperationIdToSharedState_,
    };
}

void TFairShareTreeJobScheduler::PostUpdate(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetFairShareUpdateInvoker());

    InitializeStaticAttributes(fairSharePostUpdateContext, postUpdateContext);

    PublishFairShareAndUpdatePreemptionAttributes(postUpdateContext->RootElement, postUpdateContext);

    ProcessUpdatedStarvationStatuses(fairSharePostUpdateContext, postUpdateContext);

    auto cachedJobPreemptionStatusesUpdateDeadline =
        CachedJobPreemptionStatuses_.UpdateTime + fairSharePostUpdateContext->TreeConfig->CachedJobPreemptionStatusesUpdatePeriod;
    if (fairSharePostUpdateContext->Now > cachedJobPreemptionStatusesUpdateDeadline) {
        UpdateCachedJobPreemptionStatuses(fairSharePostUpdateContext, postUpdateContext);
    }

    TDynamicAttributesManager dynamicAttributesManager(/*schedulingSnapshot*/ {}, postUpdateContext->RootElement->GetTreeSize());
    ComputeDynamicAttributesAtUpdateRecursively(postUpdateContext->RootElement, &dynamicAttributesManager);
    BuildSchedulableIndices(&dynamicAttributesManager, postUpdateContext);

    CollectSchedulableOperationsPerPriority(fairSharePostUpdateContext, postUpdateContext);

    CollectKnownSchedulingTagFilters(fairSharePostUpdateContext, postUpdateContext);

    UpdateSsdNodeSchedulingAttributes(fairSharePostUpdateContext, postUpdateContext);

    CountOperationsByPreemptionPriority(fairSharePostUpdateContext, postUpdateContext);
}

TFairShareTreeSchedulingSnapshotPtr TFairShareTreeJobScheduler::CreateSchedulingSnapshot(TJobSchedulerPostUpdateContext* postUpdateContext)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetControlInvoker(EControlQueue::FairShareStrategy));

    return New<TFairShareTreeSchedulingSnapshot>(
        std::move(postUpdateContext->StaticAttributesList),
        std::move(postUpdateContext->SchedulableOperationsPerPriority),
        std::move(postUpdateContext->SsdPriorityPreemptionMedia),
        CachedJobPreemptionStatuses_,
        std::move(postUpdateContext->KnownSchedulingTagFilters),
        std::move(postUpdateContext->OperationCountsByPreemptionPriorityParameters),
        std::move(postUpdateContext->OperationIdToState),
        std::move(postUpdateContext->OperationIdToSharedState));
}

void TFairShareTreeJobScheduler::OnResourceUsageSnapshotUpdate(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const
{
    treeSnapshot->SchedulingSnapshot()->UpdateDynamicAttributesListSnapshot(treeSnapshot, resourceUsageSnapshot);
}

void TFairShareTreeJobScheduler::UpdateConfig(TFairShareStrategyTreeConfigPtr config)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto oldConfig = std::move(Config_);
    Config_ = std::move(config);

    SchedulingSegmentsManagementExecutor_->SetPeriod(Config_->SchedulingSegments->ManagePeriod);
    SchedulingSegmentManager_.UpdateConfig(Config_->SchedulingSegments);
    if (oldConfig->SchedulingSegments->Mode != Config_->SchedulingSegments->Mode) {
        for (const auto& [_, operationState] : OperationIdToState_) {
            SchedulingSegmentManager_.InitOrUpdateOperationSchedulingSegment(operationState);
        }
    }

    UpdateSsdPriorityPreemptionMedia();
}

void TFairShareTreeJobScheduler::BuildElementLoggingStringAttributes(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TSchedulerElement* element,
    TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    if (element->GetType() == ESchedulerElementType::Operation) {
        const auto* operationElement = static_cast<const TSchedulerOperationElement*>(element);
        const auto& operationState = treeSnapshot->IsElementEnabled(operationElement)
            ? treeSnapshot->SchedulingSnapshot()->GetEnabledOperationState(operationElement)
            : treeSnapshot->SchedulingSnapshot()->GetOperationState(operationElement);
        const auto& operationSharedState = treeSnapshot->IsElementEnabled(operationElement)
            ? treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(operationElement)
            : treeSnapshot->SchedulingSnapshot()->GetOperationSharedState(operationElement);
        const auto& attributes = treeSnapshot->IsElementEnabled(element)
            ? treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element)
            : TStaticAttributes{};
        delimitedBuilder->AppendFormat(
            "PreemptibleRunningJobs: %v, AggressivelyPreemptibleRunningJobs: %v, PreemptionStatusStatistics: %v, "
            "SchedulingIndex: %v, DeactivationReasons: %v, MinNeededResourcesUnsatisfiedCount: %v, "
            "SchedulingSegment: %v, SchedulingSegmentModule: %v",
            operationSharedState->GetPreemptibleJobCount(),
            operationSharedState->GetAggressivelyPreemptibleJobCount(),
            operationSharedState->GetPreemptionStatusStatistics(),
            attributes.SchedulingIndex,
            operationSharedState->GetDeactivationReasons(),
            operationSharedState->GetMinNeededResourcesUnsatisfiedCount(),
            operationState->SchedulingSegment,
            operationState->SchedulingSegmentModule);
    }
}

void TFairShareTreeJobScheduler::InitPersistentState(INodePtr persistentState)
{
    if (persistentState) {
        try {
            InitialPersistentState_ = ConvertTo<TPersistentFairShareTreeJobSchedulerStatePtr>(persistentState);
        } catch (const std::exception& ex) {
            InitialPersistentState_ = New<TPersistentFairShareTreeJobSchedulerState>();

            // TODO(eshcherbin): Should we set scheduler alert instead? It'll be more visible this way,
            // but it'll have to be removed manually
            YT_LOG_WARNING(ex, "Failed to deserialize strategy state; will ignore it");
        }
    } else {
        InitialPersistentState_ = New<TPersistentFairShareTreeJobSchedulerState>();
    }

    InitialPersistentSchedulingSegmentNodeStates_ = InitialPersistentState_->SchedulingSegmentsState->NodeStates;
    InitialPersistentSchedulingSegmentOperationStates_ = InitialPersistentState_->SchedulingSegmentsState->OperationStates;

    auto now = TInstant::Now();
    SchedulingSegmentsInitializationDeadline_ = now + Config_->SchedulingSegments->InitializationTimeout;
    SchedulingSegmentManager_.SetInitializationDeadline(SchedulingSegmentsInitializationDeadline_);
}

INodePtr TFairShareTreeJobScheduler::BuildPersistentState() const
{
    auto persistentState = PersistentState_
        ? PersistentState_
        : InitialPersistentState_;
    return ConvertToNode(persistentState);
}

void TFairShareTreeJobScheduler::OnJobStartedInTest(
    TSchedulerOperationElement* element,
    TJobId jobId,
    const TJobResourcesWithQuota& resourceUsage)
{
    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    operationSharedState->OnJobStarted(
        element,
        jobId,
        resourceUsage,
        /*precommitedResources*/ {},
        /*scheduleJobEpoch*/ 0);
}

void TFairShareTreeJobScheduler::ProcessUpdatedJobInTest(
    TSchedulerOperationElement* element,
    TJobId jobId,
    const TJobResources& jobResources)
{
    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    auto delta = operationSharedState->SetJobResourceUsage(jobId, jobResources);
    element->IncreaseHierarchicalResourceUsage(delta);
    operationSharedState->UpdatePreemptibleJobsList(element);
}

EJobPreemptionStatus TFairShareTreeJobScheduler::GetJobPreemptionStatusInTest(const TSchedulerOperationElement* element, TJobId jobId) const
{
    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    return operationSharedState->GetJobPreemptionStatus(jobId);
}

void TFairShareTreeJobScheduler::InitSchedulingProfilingCounters()
{
    for (auto stage : TEnumTraits<EJobSchedulingStage>::GetDomainValues()) {
        SchedulingStageProfilingCounters_[stage] = TSchedulingStageProfilingCounters(Profiler_.WithTag("scheduling_stage", FormatEnum(stage)));
    }
}

TRunningJobStatistics TFairShareTreeJobScheduler::ComputeRunningJobStatistics(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    auto cachedJobPreemptionStatuses = treeSnapshot->SchedulingSnapshot()->CachedJobPreemptionStatuses();
    auto now = CpuInstantToInstant(schedulingContext->GetNow());

    TRunningJobStatistics runningJobStatistics;
    for (const auto& job : schedulingContext->RunningJobs()) {
        // Technically it's an overestimation of the job's duration, however, we feel it's more fair this way.
        auto duration = (now - job->GetStartTime()).SecondsFloat();
        auto jobCpuTime = static_cast<double>(job->ResourceLimits().GetCpu()) * duration;
        auto jobGpuTime = job->ResourceLimits().GetGpu() * duration;

        runningJobStatistics.TotalCpuTime += jobCpuTime;
        runningJobStatistics.TotalGpuTime += jobGpuTime;

        // TODO(eshcherbin): Do we really still need to use cached preemption statuses?
        // Now that this code has been moved to job scheduler, we can use operation shared state directly.
        if (GetCachedJobPreemptionStatus(job, cachedJobPreemptionStatuses) == EJobPreemptionStatus::Preemptible) {
            runningJobStatistics.PreemptibleCpuTime += jobCpuTime;
            runningJobStatistics.PreemptibleGpuTime += jobGpuTime;
        }
    }

    return runningJobStatistics;
}

void TFairShareTreeJobScheduler::DoRegularJobScheduling(TScheduleJobsContext* context)
{
    auto runRegularSchedulingStage = [&] (EJobSchedulingStage stageType, const TRegularSchedulingParameters& parameters = {}) {
        context->StartStage(stageType, &SchedulingStageProfilingCounters_[stageType]);
        RunRegularSchedulingStage(parameters, context);
        context->FinishStage();
    };

    if (const auto& priorityConfig = context->TreeSnapshot()->TreeConfig()->PrioritizedRegularScheduling) {
        const auto& schedulableOperationsPerPriority = context->TreeSnapshot()->SchedulingSnapshot()->SchedulableOperationsPerPriority();
        runRegularSchedulingStage(
            EJobSchedulingStage::RegularMediumPriority,
            TRegularSchedulingParameters{
                .ConsideredOperations = schedulableOperationsPerPriority[EOperationSchedulingPriority::Medium],
            });

        auto customLowPriorityMinSpareJobResources = !context->SchedulingContext()->StartedJobs().empty()
            ? std::make_optional(ToJobResources(priorityConfig->LowPriorityFallbackMinSpareJobResources, TJobResources{}))
            : std::nullopt;
        runRegularSchedulingStage(
            EJobSchedulingStage::RegularLowPriority,
            TRegularSchedulingParameters{
                .ConsideredOperations = schedulableOperationsPerPriority[EOperationSchedulingPriority::Medium],
                .CustomMinSpareJobResources = customLowPriorityMinSpareJobResources,
            });
    } else {
        runRegularSchedulingStage(EJobSchedulingStage::RegularMediumPriority);
    }

    auto badPackingOperations = context->ExtractBadPackingOperations();
    bool needPackingFallback = context->SchedulingContext()->StartedJobs().empty() && !badPackingOperations.empty();
    if (needPackingFallback) {
        runRegularSchedulingStage(
            EJobSchedulingStage::RegularPackingFallback,
            TRegularSchedulingParameters{
                .ConsideredOperations = badPackingOperations,
                .IgnorePacking = true,
                .OneJobOnly = true,
            });
    }
}

void TFairShareTreeJobScheduler::DoPreemptiveJobScheduling(TScheduleJobsContext* context)
{
    bool scheduleJobsWithPreemption = [&] {
        NLogging::TLogger Logger("TestDebug");

        auto nodeId = context->SchedulingContext()->GetNodeDescriptor().Id;
        auto nodeShardId = StrategyHost_->GetNodeShardId(nodeId);
        auto& nodeIdToLastPreemptiveSchedulingTime = NodeStateShards_[nodeShardId].NodeIdToLastPreemptiveSchedulingTime;

        auto now = context->SchedulingContext()->GetNow();
        auto [it, wasMissing] = nodeIdToLastPreemptiveSchedulingTime.emplace(nodeId, now);

        auto deadline = it->second + DurationToCpuDuration(context->TreeSnapshot()->TreeConfig()->PreemptiveSchedulingBackoff);
        if (!wasMissing && now > deadline) {
            it->second = now;
            return true;
        }

        return wasMissing;
    }();

    context->SchedulingStatistics().ScheduleWithPreemption = scheduleJobsWithPreemption;
    if (!scheduleJobsWithPreemption) {
        YT_LOG_DEBUG("Skip preemptive scheduling");
        return;
    }

    for (const auto& [stage, parameters] : BuildPreemptiveSchedulingStageList(context)) {
        // We allow to schedule at most one job using preemption.
        if (context->SchedulingStatistics().ScheduledDuringPreemption > 0) {
            break;
        }

        context->StartStage(stage, &SchedulingStageProfilingCounters_[stage]);
        RunPreemptiveSchedulingStage(parameters, context);
        context->FinishStage();
    }
}

TPreemptiveStageWithParametersList TFairShareTreeJobScheduler::BuildPreemptiveSchedulingStageList(TScheduleJobsContext* context)
{
    TPreemptiveStageWithParametersList stages;

    if (context->GetSsdPriorityPreemptionEnabled()) {
        stages.emplace_back(
            EJobSchedulingStage::PreemptiveSsdAggressive,
            TPreemptiveSchedulingParameters{
                .TargetOperationPreemptionPriority = EOperationPreemptionPriority::SsdAggressive,
                .MinJobPreemptionLevel = EJobPreemptionLevel::SsdAggressivelyPreemptible,
            });
        stages.emplace_back(
            EJobSchedulingStage::PreemptiveSsdNormal,
            TPreemptiveSchedulingParameters{
                .TargetOperationPreemptionPriority = EOperationPreemptionPriority::SsdNormal,
                .MinJobPreemptionLevel = EJobPreemptionLevel::NonPreemptible,
            });
    }

    stages.emplace_back(
        EJobSchedulingStage::PreemptiveAggressive,
        TPreemptiveSchedulingParameters{
            .TargetOperationPreemptionPriority = EOperationPreemptionPriority::Aggressive,
            .MinJobPreemptionLevel = EJobPreemptionLevel::AggressivelyPreemptible,
        });
    stages.emplace_back(
        EJobSchedulingStage::PreemptiveNormal,
        TPreemptiveSchedulingParameters{
            .TargetOperationPreemptionPriority = EOperationPreemptionPriority::Normal,
            .MinJobPreemptionLevel = EJobPreemptionLevel::Preemptible,
            .ForcePreemptionAttempt = true,
        });

    return stages;
}

void TFairShareTreeJobScheduler::RunRegularSchedulingStage(const TRegularSchedulingParameters& parameters, TScheduleJobsContext* context)
{
    while (context->ShouldContinueScheduling(parameters.CustomMinSpareJobResources)) {
        if (!context->GetStagePrescheduleExecuted()) {
            context->PrepareForScheduling();
            context->PrescheduleJob(parameters.ConsideredOperations);
        }

        auto scheduleJobResult = context->ScheduleJob(parameters.IgnorePacking);
        if (scheduleJobResult.Finished || (parameters.OneJobOnly && scheduleJobResult.Scheduled)) {
            break;
        }
    }

    context->SchedulingStatistics().MaxNonPreemptiveSchedulingIndex = context->GetStageMaxSchedulingIndex();
}

void TFairShareTreeJobScheduler::RunPreemptiveSchedulingStage(const TPreemptiveSchedulingParameters& parameters, TScheduleJobsContext* context)
{
    YT_VERIFY(parameters.TargetOperationPreemptionPriority != EOperationPreemptionPriority::None);

    // NB(eshcherbin): We might want to analyze jobs and attempt preemption even if there are no candidate operations of target priority.
    // For example, we preempt jobs in pools or operations which exceed their specified resource limits.
    auto operationWithPreemptionPriorityCount = context->GetOperationWithPreemptionPriorityCount(
        parameters.TargetOperationPreemptionPriority);
    bool shouldAttemptScheduling = operationWithPreemptionPriorityCount > 0;
    bool shouldAttemptPreemption = parameters.ForcePreemptionAttempt || shouldAttemptScheduling;
    if (!shouldAttemptPreemption) {
        return;
    }

    // NB: This method achieves 2 goals relevant for scheduling with preemption:
    // 1. Reset |Active| attribute after scheduling without preemption (this is necessary for PrescheduleJob correctness).
    // 2. Initialize dynamic attributes and calculate local resource usages if scheduling without preemption was skipped.
    context->PrepareForScheduling();

    std::vector<TJobWithPreemptionInfo> unconditionallyPreemptibleJobs;
    TNonOwningJobSet forcefullyPreemptibleJobs;
    context->AnalyzePreemptibleJobs(
        parameters.TargetOperationPreemptionPriority,
        parameters.MinJobPreemptionLevel,
        &unconditionallyPreemptibleJobs,
        &forcefullyPreemptibleJobs);

    int startedBeforePreemption = context->SchedulingContext()->StartedJobs().size();

    // NB: Schedule at most one job with preemption.
    TJobPtr jobStartedUsingPreemption;
    if (shouldAttemptScheduling) {
        YT_LOG_TRACE(
            "Scheduling new jobs with preemption "
            "(UnconditionallyPreemptibleJobs: %v, UnconditionalResourceUsageDiscount: %v, TargetOperationPreemptionPriority: %v)",
            unconditionallyPreemptibleJobs,
            FormatResources(context->SchedulingContext()->UnconditionalResourceUsageDiscount()),
            parameters.TargetOperationPreemptionPriority);

        while (context->ShouldContinueScheduling()) {
            if (!context->GetStagePrescheduleExecuted()) {
                context->PrescheduleJob(
                    /*consideredSchedulableOperations*/ std::nullopt,
                    parameters.TargetOperationPreemptionPriority);
            }

            auto scheduleJobResult = context->ScheduleJob(/*ignorePacking*/ true);
            if (scheduleJobResult.Scheduled) {
                jobStartedUsingPreemption = context->SchedulingContext()->StartedJobs().back();
                break;
            }
            if (scheduleJobResult.Finished) {
                break;
            }
        }
    }

    int startedAfterPreemption = context->SchedulingContext()->StartedJobs().size();
    context->SchedulingStatistics().ScheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

    context->PreemptJobsAfterScheduling(
        parameters.TargetOperationPreemptionPriority,
        std::move(unconditionallyPreemptibleJobs),
        forcefullyPreemptibleJobs,
        jobStartedUsingPreemption);
}

const TFairShareTreeJobSchedulerOperationStatePtr& TFairShareTreeJobScheduler::GetOperationState(TOperationId operationId) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return GetOrCrash(OperationIdToState_, operationId);
}

const TFairShareTreeJobSchedulerOperationSharedStatePtr& TFairShareTreeJobScheduler::GetOperationSharedState(TOperationId operationId) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return GetOrCrash(OperationIdToSharedState_, operationId);
}

std::optional<TPersistentNodeSchedulingSegmentState> TFairShareTreeJobScheduler::FindInitialNodePersistentState(TNodeId nodeId)
{
    std::optional<TPersistentNodeSchedulingSegmentState> maybeState;
    if (TInstant::Now() <= SchedulingSegmentsInitializationDeadline_) {
        auto it = InitialPersistentSchedulingSegmentNodeStates_.find(nodeId);
        if (it != InitialPersistentSchedulingSegmentNodeStates_.end()) {
            maybeState = std::move(it->second);
            InitialPersistentSchedulingSegmentNodeStates_.erase(it);
        }
    } else if (!InitialPersistentSchedulingSegmentNodeStates_.empty()) {
        InitialPersistentSchedulingSegmentNodeStates_.clear();
    }

    return maybeState;
}

std::optional<TPersistentOperationSchedulingSegmentState> TFairShareTreeJobScheduler::FindInitialOperationPersistentState(TOperationId operationId)
{
    std::optional<TPersistentOperationSchedulingSegmentState> maybeState;
    if (TInstant::Now() <= SchedulingSegmentsInitializationDeadline_) {
        auto it = InitialPersistentSchedulingSegmentOperationStates_.find(operationId);
        if (it != InitialPersistentSchedulingSegmentOperationStates_.end()) {
            maybeState = std::move(it->second);
            InitialPersistentSchedulingSegmentOperationStates_.erase(it);
        }
    } else if (!InitialPersistentSchedulingSegmentOperationStates_.empty()) {
        InitialPersistentSchedulingSegmentOperationStates_.clear();
    }

    return maybeState;
}

void TFairShareTreeJobScheduler::UpdateSsdPriorityPreemptionMedia()
{
    THashSet<int> media;
    std::vector<TString> unknownNames;
    for (const auto& mediumName : Config_->SsdPriorityPreemption->MediumNames) {
        if (auto mediumIndex = StrategyHost_->FindMediumIndexByName(mediumName)) {
            media.insert(*mediumIndex);
        } else {
            unknownNames.push_back(mediumName);
        }
    }

    if (unknownNames.empty()) {
        if (SsdPriorityPreemptionMedia_ != media) {
            YT_LOG_INFO("Updated SSD priority preemption media (OldSsdPriorityPreemptionMedia: %v, NewSsdPriorityPreemptionMedia: %v)",
                SsdPriorityPreemptionMedia_,
                media);

            SsdPriorityPreemptionMedia_.emplace(std::move(media));

            StrategyHost_->SetSchedulerAlert(ESchedulerAlertType::UpdateSsdPriorityPreemptionMedia, TError());
        }
    } else {
        auto error = TError("Config contains unknown SSD priority preemption media")
            << TErrorAttribute("unknown_medium_names", std::move(unknownNames));
        StrategyHost_->SetSchedulerAlert(ESchedulerAlertType::UpdateSsdPriorityPreemptionMedia, error);
    }
}

void TFairShareTreeJobScheduler::InitializeStaticAttributes(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    postUpdateContext->StaticAttributesList.resize(postUpdateContext->RootElement->GetTreeSize());

    for (const auto& [operationId, operationElement] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(operationElement);
        attributes.OperationState = GetOrCrash(postUpdateContext->OperationIdToState, operationId);
        attributes.OperationSharedState = GetOrCrash(postUpdateContext->OperationIdToSharedState, operationId);
    }
}

void TFairShareTreeJobScheduler::CollectSchedulableOperationsPerPriority(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    const auto& priorityConfig = fairSharePostUpdateContext->TreeConfig->PrioritizedRegularScheduling;
    if (!priorityConfig) {
        return;
    }

    TNonOwningOperationElementList sortedOperationElements;
    sortedOperationElements.reserve(postUpdateContext->RootElement->SchedulableElementCount());
    for (const auto& [_, operationElement] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        if (operationElement->IsSchedulable()) {
            sortedOperationElements.push_back(operationElement);
        }
    }

    SortBy(sortedOperationElements, [&] (const TSchedulerOperationElement* element) {
        return postUpdateContext->StaticAttributesList.AttributesOf(element).SchedulingIndex;
    });

    auto& operationsPerPriority = postUpdateContext->SchedulableOperationsPerPriority;
    for (auto* operationElement : sortedOperationElements) {
        int highPriorityCount = std::ssize(operationsPerPriority[EOperationSchedulingPriority::Medium]);
        auto priority = highPriorityCount < priorityConfig->MediumPriorityOperationCountLimit
            ? EOperationSchedulingPriority::Medium
            : EOperationSchedulingPriority::Low;
        operationsPerPriority[priority].push_back(operationElement);
    }
}

void TFairShareTreeJobScheduler::PublishFairShareAndUpdatePreemptionAttributes(
    TSchedulerElement* element,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(element);
    auto isAggressivePreemptionAllowed = IsAggressivePreemptionAllowed(element);
    if (element->IsRoot()) {
        YT_VERIFY(isAggressivePreemptionAllowed);
        attributes.EffectiveAggressivePreemptionAllowed = *isAggressivePreemptionAllowed;
    } else {
        const auto* parent = element->GetParent();
        YT_VERIFY(parent);
        const auto& parentAttributes = postUpdateContext->StaticAttributesList.AttributesOf(parent);

        attributes.EffectiveAggressivePreemptionAllowed = isAggressivePreemptionAllowed
            .value_or(parentAttributes.EffectiveAggressivePreemptionAllowed);
    }

    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            PublishFairShareAndUpdatePreemptionAttributesAtCompositeElement(static_cast<TSchedulerCompositeElement*>(element), postUpdateContext);
            break;
        case ESchedulerElementType::Operation:
            PublishFairShareAndUpdatePreemptionAttributesAtOperation(static_cast<TSchedulerOperationElement*>(element), postUpdateContext);
            break;
        default:
            YT_ABORT();
    }
}

void TFairShareTreeJobScheduler::PublishFairShareAndUpdatePreemptionAttributesAtCompositeElement(
    TSchedulerCompositeElement* element,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    for (const auto& child : element->EnabledChildren()) {
        PublishFairShareAndUpdatePreemptionAttributes(child.Get(), postUpdateContext);
    }
}

void TFairShareTreeJobScheduler::PublishFairShareAndUpdatePreemptionAttributesAtOperation(
    TSchedulerOperationElement* element,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    // If fair share ratio equals demand ratio then we want to explicitly disable preemption.
    // It is necessary since some job's resource usage may increase before the next fair share update,
    // and in this case we don't want any jobs to become preemptible
    bool isDominantFairShareEqualToDominantDemandShare =
        TResourceVector::Near(element->Attributes().FairShare.Total, element->Attributes().DemandShare, NVectorHdrf::RatioComparisonPrecision) &&
            !Dominates(TResourceVector::Epsilon(), element->Attributes().DemandShare);
    bool currentPreemptibleValue = !isDominantFairShareEqualToDominantDemandShare;

    const auto& operationSharedState = postUpdateContext->StaticAttributesList.AttributesOf(element).OperationSharedState;
    operationSharedState->PublishFairShare(element->Attributes().FairShare.Total);
    operationSharedState->SetPreemptible(currentPreemptibleValue);
    operationSharedState->UpdatePreemptibleJobsList(element);
}

void TFairShareTreeJobScheduler::ProcessUpdatedStarvationStatuses(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext)
{
    auto processUpdatedStarvationStatuses = [&] (const auto& operationMap) {
        for (const auto& [operationId, operationElement] : operationMap) {
            GetOrCrash(postUpdateContext->OperationIdToSharedState, operationId)->ProcessUpdatedStarvationStatus(operationElement->GetStarvationStatus());
        }
    };

    processUpdatedStarvationStatuses(fairSharePostUpdateContext->EnabledOperationIdToElement);
    processUpdatedStarvationStatuses(fairSharePostUpdateContext->DisabledOperationIdToElement);
}

void TFairShareTreeJobScheduler::UpdateCachedJobPreemptionStatuses(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext)
{
    auto jobPreemptionStatuses = New<TRefCountedJobPreemptionStatusMapPerOperation>();
    auto collectJobPreemptionStatuses = [&] (const auto& operationMap) {
        for (const auto& [operationId, operationElement] : operationMap) {
            // NB: We cannot use operation shared state from static attributes list, because disabled operations don't have a tree index.
            EmplaceOrCrash(
                *jobPreemptionStatuses,
                operationId,
                GetOrCrash(postUpdateContext->OperationIdToSharedState, operationId)->GetJobPreemptionStatusMap());
        }
    };

    collectJobPreemptionStatuses(fairSharePostUpdateContext->EnabledOperationIdToElement);
    collectJobPreemptionStatuses(fairSharePostUpdateContext->DisabledOperationIdToElement);

    CachedJobPreemptionStatuses_ = TCachedJobPreemptionStatuses{
        .Value = std::move(jobPreemptionStatuses),
        .UpdateTime = fairSharePostUpdateContext->Now,
    };
}

void TFairShareTreeJobScheduler::ComputeDynamicAttributesAtUpdateRecursively(
    TSchedulerElement* element,
    TDynamicAttributesManager* dynamicAttributesManager) const
{
    dynamicAttributesManager->InitializeResourceUsageAtPostUpdate(element, element->ResourceUsageAtUpdate());
    if (element->IsOperation()) {
        dynamicAttributesManager->InitializeAttributesAtOperation(static_cast<TSchedulerOperationElement*>(element));
    } else {
        auto* compositeElement = static_cast<TSchedulerCompositeElement*>(element);
        TNonOwningElementList children;
        children.reserve(compositeElement->SchedulableChildren().size());
        for (const auto& child : compositeElement->SchedulableChildren()) {
            ComputeDynamicAttributesAtUpdateRecursively(child.Get(), dynamicAttributesManager);
            children.push_back(child.Get());
        }

        dynamicAttributesManager->InitializeAttributesAtCompositeElement(compositeElement, std::move(children));
    }
}

void TFairShareTreeJobScheduler::BuildSchedulableIndices(
    TDynamicAttributesManager* dynamicAttributesManager,
    TJobSchedulerPostUpdateContext* context) const
{
    const auto& dynamicAttributes = const_cast<const TDynamicAttributesManager*>(dynamicAttributesManager)->AttributesOf(context->RootElement);
    int schedulingIndex = 0;
    while (dynamicAttributes.Active) {
        auto* bestLeafDescendant = dynamicAttributes.BestLeafDescendant;
        context->StaticAttributesList.AttributesOf(bestLeafDescendant).SchedulingIndex = schedulingIndex++;
        dynamicAttributesManager->DeactivateOperation(bestLeafDescendant);
    }
}

void TFairShareTreeJobScheduler::CollectKnownSchedulingTagFilters(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    THashMap<TSchedulingTagFilter, int> schedulingTagFilterToIndex;
    auto getTagFilterIndex = [&] (const TSchedulingTagFilter& filter) {
        if (filter.IsEmpty()) {
            return EmptySchedulingTagFilterIndex;
        }

        auto it = schedulingTagFilterToIndex.find(filter);
        if (it != schedulingTagFilterToIndex.end()) {
            return it->second;
        }

        int index = std::ssize(postUpdateContext->KnownSchedulingTagFilters);
        EmplaceOrCrash(schedulingTagFilterToIndex, filter, index);
        postUpdateContext->KnownSchedulingTagFilters.push_back(filter);
        return index;
    };

    for (const auto& [_, operationElement] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(operationElement);
        attributes.SchedulingTagFilterIndex = getTagFilterIndex(operationElement->GetSchedulingTagFilter());
    }
    for (const auto& [_, poolElement] : fairSharePostUpdateContext->PoolNameToElement) {
        auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(poolElement);
        attributes.SchedulingTagFilterIndex = getTagFilterIndex(poolElement->GetSchedulingTagFilter());
    }
}

void TFairShareTreeJobScheduler::UpdateSsdNodeSchedulingAttributes(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    for (const auto& [_, element] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(element);
        const TSchedulerCompositeElement* current = element->GetParent();
        while (current) {
            if (current->GetType() == ESchedulerElementType::Pool &&
                !static_cast<const TSchedulerPoolElement*>(current)->GetConfig()->AllowRegularJobsOnSsdNodes)
            {
                attributes.AreRegularJobsOnSsdNodesAllowed = false;
                break;
            }

            current = current->GetParent();
        }
    }
}

void TFairShareTreeJobScheduler::CountOperationsByPreemptionPriority(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    TOperationCountsByPreemptionPriorityParameters operationCountsByPreemptionPriorityParameters;
    for (const auto& [_, element] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        for (auto scope : TEnumTraits<EOperationPreemptionPriorityScope>::GetDomainValues()) {
            for (bool ssdPriorityPreemptionEnabled : {false, true}) {
                TOperationPreemptionPriorityParameters parameters{scope, ssdPriorityPreemptionEnabled};
                auto priority = NScheduler::GetOperationPreemptionPriority(
                    element,
                    scope,
                    ssdPriorityPreemptionEnabled,
                    postUpdateContext->SsdPriorityPreemptionMedia);
                ++operationCountsByPreemptionPriorityParameters[parameters][priority];
            }
        }
    }

    TSensorBuffer sensorBuffer;
    for (auto scope : TEnumTraits<EOperationPreemptionPriorityScope>::GetDomainValues()) {
        TWithTagGuard scopeTagGuard(&sensorBuffer, "scope", FormatEnum(scope));
        for (bool ssdPriorityPreemptionEnabled : {false, true}) {
            TWithTagGuard ssdTagGuard(&sensorBuffer, "ssd_priority_preemption_enabled", TString(FormatBool(ssdPriorityPreemptionEnabled)));
            TOperationPreemptionPriorityParameters parameters{scope, ssdPriorityPreemptionEnabled};
            const auto& operationCountByPreemptionPriority = operationCountsByPreemptionPriorityParameters[parameters];
            for (auto priority : TEnumTraits<EOperationPreemptionPriority>::GetDomainValues()) {
                TWithTagGuard priorityTagGuard(&sensorBuffer, "priority", FormatEnum(priority));
                sensorBuffer.AddGauge(/*name*/ "", operationCountByPreemptionPriority[priority]);
            }
        }
    }
    OperationCountByPreemptionPriorityBufferedProducer_->Update(std::move(sensorBuffer));

    postUpdateContext->OperationCountsByPreemptionPriorityParameters = std::move(operationCountsByPreemptionPriorityParameters);
}

const TFairShareTreeJobSchedulerNodeState* TFairShareTreeJobScheduler::FindNodeState(TNodeId nodeId) const
{
    return const_cast<const TFairShareTreeJobSchedulerNodeState*>(const_cast<TFairShareTreeJobScheduler*>(this)->FindNodeState(nodeId));
}

TFairShareTreeJobSchedulerNodeState* TFairShareTreeJobScheduler::FindNodeState(TNodeId nodeId)
{
    auto nodeShardId = StrategyHost_->GetNodeShardId(nodeId);

    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetNodeShardInvokers()[nodeShardId]);

    auto& nodeStates = NodeStateShards_[nodeShardId].NodeIdToState;
    auto it = nodeStates.find(nodeId);
    return it != nodeStates.end() ? &it->second : nullptr;
}

TFairShareTreeJobSchedulerOperationStateMap TFairShareTreeJobScheduler::GetOperationStateMapSnapshot() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TFairShareTreeJobSchedulerOperationStateMap operationStateMapSnapshot;
    operationStateMapSnapshot.reserve(OperationIdToState_.size());
    for (const auto& [operationId, operationState] : OperationIdToState_) {
        operationStateMapSnapshot.emplace(operationId, New<TFairShareTreeJobSchedulerOperationState>(*operationState));
    }

    return operationStateMapSnapshot;
}

TFairShareTreeJobSchedulerNodeStateMap TFairShareTreeJobScheduler::GetNodeStateMapSnapshot() const
{
    const auto& nodeShardInvokers = StrategyHost_->GetNodeShardInvokers();
    std::vector<TFuture<TFairShareTreeJobSchedulerNodeStateMap>> futures;
    for (int shardId = 0; shardId < std::ssize(nodeShardInvokers); ++shardId) {
        const auto& invoker = nodeShardInvokers[shardId];
        futures.push_back(
            BIND([&, this_ = MakeStrong(this), shardId] {
                return NodeStateShards_[shardId].NodeIdToState;
            })
            .AsyncVia(invoker)
            .Run());
    }
    auto shardResults = WaitFor(AllSucceeded(std::move(futures)))
        .ValueOrThrow();

    TFairShareTreeJobSchedulerNodeStateMap nodeStates;
    for (auto&& shardNodeStates : shardResults) {
        for (auto&& [nodeId, nodeState] : shardNodeStates) {
            // NB(eshcherbin): Descriptor may be missing if the node has only just registered and we haven't processed any heartbeats from it.
            if (nodeState.Descriptor) {
                EmplaceOrCrash(nodeStates, nodeId, std::move(nodeState));
            }
        }
    }

    return nodeStates;
}

void TFairShareTreeJobScheduler::ApplyOperationSchedulingSegmentsChanges(const TFairShareTreeJobSchedulerOperationStateMap& changedOperationStates)
{
    for (const auto& [operationId, changedOperationState] : changedOperationStates) {
        const auto& operationState = GetOperationState(operationId);
        operationState->SchedulingSegmentModule = changedOperationState->SchedulingSegmentModule;
        operationState->FailingToScheduleAtModuleSince = changedOperationState->FailingToScheduleAtModuleSince;
    }
}

void TFairShareTreeJobScheduler::ApplyNodeSchedulingSegmentsChanges(const TSetNodeSchedulingSegmentOptionsList& movedNodes)
{
    if (movedNodes.empty()) {
        return;
    }

    YT_LOG_DEBUG("Moving nodes to new scheduling segments (TotalMovedNodeCount: %v)",
        movedNodes.size());

    std::array<TSetNodeSchedulingSegmentOptionsList, MaxNodeShardCount> movedNodesPerNodeShard;
    for (auto [nodeId, newSegment] : movedNodes) {
        auto shardId = StrategyHost_->GetNodeShardId(nodeId);
        movedNodesPerNodeShard[shardId].push_back(TSetNodeSchedulingSegmentOptions{
            .NodeId = nodeId,
            .Segment = newSegment,
        });
    }

    const auto& nodeShardInvokers = StrategyHost_->GetNodeShardInvokers();
    std::vector<TFuture<void>> futures;
    for (int shardId = 0; shardId < std::ssize(nodeShardInvokers); ++shardId) {
        futures.push_back(BIND(
            [&, this_ = MakeStrong(this), shardId, movedNodes = std::move(movedNodesPerNodeShard[shardId])] {
                auto& nodeStates = NodeStateShards_[shardId].NodeIdToState;
                std::vector<std::pair<TNodeId, ESchedulingSegment>> missingNodeIdsWithSegments;
                for (auto [nodeId, newSegment] : movedNodes) {
                    auto it = nodeStates.find(nodeId);
                    if (it == nodeStates.end()) {
                        missingNodeIdsWithSegments.emplace_back(nodeId, newSegment);
                        continue;
                    }
                    auto& node = it->second;

                    YT_VERIFY(node.SchedulingSegment != newSegment);

                    YT_LOG_DEBUG("Setting new scheduling segment for node (Address: %v, Segment: %v)",
                        node.Descriptor->Address,
                        newSegment);

                    node.SchedulingSegment = newSegment;
                }

                YT_LOG_DEBUG_UNLESS(missingNodeIdsWithSegments.empty(),
                    "Trying to set scheduling segments for missing nodes (MissingNodeIdsWithSegments: %v)",
                    missingNodeIdsWithSegments);
            })
            .AsyncVia(nodeShardInvokers[shardId])
            .Run());
    }

    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

void TFairShareTreeJobScheduler::ManageSchedulingSegments()
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetControlInvoker(EControlQueue::FairShareStrategy));

    auto host = Host_.Lock();
    if (!host) {
        return;
    }

    if (!TreeHost_->IsConnected()) {
        return;
    }

    // NB(eshcherbin): Tree snapshot is used only for attributes which are unavailable otherwise.
    // Cloned operation states and tree config from the snapshot are NOT used.
    auto treeSnapshot = host->GetTreeSnapshot();
    if (!treeSnapshot) {
        YT_LOG_DEBUG("Tree snapshot is missing, skipping scheduling segments management");

        return;
    }

    YT_LOG_DEBUG("Started managing scheduling segments");

    auto nodeStates = GetNodeStateMapSnapshot();

    TSetNodeSchedulingSegmentOptionsList movedNodes;
    {
        TForbidContextSwitchGuard guard;

        auto now = TInstant::Now();
        auto operationStates = GetOperationStateMapSnapshot();
        TUpdateSchedulingSegmentsContext context{
            .Now = now,
            .TreeSnapshot = treeSnapshot,
            .OperationStates = std::move(operationStates),
            .NodeStates = std::move(nodeStates),
        };

        context.OperationStates = GetOperationStateMapSnapshot();

        SchedulingSegmentManager_.UpdateSchedulingSegments(&context);

        ApplyOperationSchedulingSegmentsChanges(context.OperationStates);
        movedNodes = std::move(context.MovedNodes);

        TreeHost_->SetSchedulerTreeAlert(TreeId_, ESchedulerAlertType::ManageSchedulingSegments, context.Error);

        if (now > SchedulingSegmentsInitializationDeadline_) {
            PersistentState_ = New<TPersistentFairShareTreeJobSchedulerState>();
            PersistentState_->SchedulingSegmentsState = std::move(context.PersistentState);

            YT_LOG_DEBUG("Saved new persistent scheduling segments state");
        } else {
            YT_LOG_DEBUG(
                "Skipped saving persistent scheduling segments state, "
                "because initialization deadline has not passed yet "
                "(Now: %v, Deadline: %v)",
                now,
                SchedulingSegmentsInitializationDeadline_);
        }
    }

    ApplyNodeSchedulingSegmentsChanges(movedNodes);

    YT_LOG_DEBUG("Finished managing scheduling segments");
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NScheduler
