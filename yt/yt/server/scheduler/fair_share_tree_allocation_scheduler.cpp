#include "fair_share_tree_allocation_scheduler.h"

#include "fair_share_tree.h"
#include "scheduling_context.h"

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/string_builder.h>

#include <yt/yt/core/actions/new_with_offloaded_dtor.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const TAllocationWithPreemptionInfoSet EmptyAllocationWithPreemptionInfoSet;

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

std::vector<TAllocationWithPreemptionInfo> GetAllocationPreemptionInfos(
    const std::vector<TAllocationPtr>& allocations,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    std::vector<TAllocationWithPreemptionInfo> allocationInfos;
    for (const auto& allocation : allocations) {
        auto* operationElement = treeSnapshot->FindEnabledOperationElement(allocation->GetOperationId());
        const auto& operationSharedState = operationElement
            ? treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(operationElement)
            : nullptr;
        if (!operationElement || !operationSharedState->IsAllocationKnown(allocation->GetId())) {
            const auto& Logger = StrategyLogger;

            YT_LOG_DEBUG(
                "Dangling running allocation found (AllocationId: %v, OperationId: %v, TreeId: %v)",
                allocation->GetId(),
                allocation->GetOperationId(),
                treeSnapshot->RootElement()->GetTreeId());
            continue;
        }
        allocationInfos.push_back(TAllocationWithPreemptionInfo{
            .Allocation = allocation,
            .PreemptionStatus = operationSharedState->GetAllocationPreemptionStatus(allocation->GetId()),
            .OperationElement = operationElement,
        });
    }

    return allocationInfos;
}

std::vector<TAllocationWithPreemptionInfo> CollectRunningAllocationsWithPreemptionInfo(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    return GetAllocationPreemptionInfos(schedulingContext->RunningAllocations(), treeSnapshot);
}

void SortAllocationsWithPreemptionInfo(std::vector<TAllocationWithPreemptionInfo>* allocationInfos)
{
    std::sort(
        allocationInfos->begin(),
        allocationInfos->end(),
        [&] (const TAllocationWithPreemptionInfo& lhs, const TAllocationWithPreemptionInfo& rhs) {
            if (lhs.PreemptionStatus != rhs.PreemptionStatus) {
                return lhs.PreemptionStatus < rhs.PreemptionStatus;
            }

            if (lhs.PreemptionStatus != EAllocationPreemptionStatus::Preemptible) {
                auto hasCpuGap = [] (const TAllocationWithPreemptionInfo& allocationWithPreemptionInfo) {
                    return allocationWithPreemptionInfo.Allocation->ResourceUsage().GetCpu() <
                        allocationWithPreemptionInfo.Allocation->ResourceLimits().GetCpu();
                };

                // Save allocations without cpu gap.
                bool lhsHasCpuGap = hasCpuGap(lhs);
                bool rhsHasCpuGap = hasCpuGap(rhs);
                if (lhsHasCpuGap != rhsHasCpuGap) {
                    return lhsHasCpuGap < rhsHasCpuGap;
                }
            }

            return lhs.Allocation->GetStartTime() < rhs.Allocation->GetStartTime();
        }
    );
}

////////////////////////////////////////////////////////////////////////////////

std::optional<EAllocationPreemptionStatus> GetCachedAllocationPreemptionStatus(
    const TAllocationPtr& allocation,
    const TCachedAllocationPreemptionStatuses& allocationPreemptionStatuses)
{
    if (!allocationPreemptionStatuses.Value) {
        // Tree snapshot is missing.
        return {};
    }

    auto operationIt = allocationPreemptionStatuses.Value->find(allocation->GetOperationId());
    if (operationIt == allocationPreemptionStatuses.Value->end()) {
        return {};
    }

    const auto& allocationIdToStatus = operationIt->second;
    auto allocationIt = allocationIdToStatus.find(allocation->GetId());
    return allocationIt != allocationIdToStatus.end() ? std::optional(allocationIt->second) : std::nullopt;
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

bool IsNormalPreemptionAllowed(const TSchedulerElement* element)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
            return static_cast<const TSchedulerPoolElement*>(element)->GetConfig()->AllowNormalPreemption;
        default:
            return true;
    }
}

std::optional<bool> IsPrioritySchedulingSegmentModuleAssignmentEnabled(const TSchedulerElement* element)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Root:
            return false;
        case ESchedulerElementType::Operation:
            return {};
        case ESchedulerElementType::Pool:
            return static_cast<const TSchedulerPoolElement*>(element)->GetConfig()->EnablePrioritySchedulingSegmentModuleAssignment;
    }
}

////////////////////////////////////////////////////////////////////////////////

EAllocationSchedulingStage GetRegularSchedulingStageByPriority(EOperationSchedulingPriority priority) {
    switch (priority) {
        case EOperationSchedulingPriority::High:
            return EAllocationSchedulingStage::RegularHighPriority;
        case EOperationSchedulingPriority::Medium:
            return EAllocationSchedulingStage::RegularMediumPriority;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

bool IsSsdAllocationPreemptionLevel(EAllocationPreemptionLevel allocationPreemptionLevel)
{
    switch (allocationPreemptionLevel) {
        case EAllocationPreemptionLevel::SsdNonPreemptible:
        case EAllocationPreemptionLevel::SsdAggressivelyPreemptible:
            return true;
        default:
            return false;
    }
}

bool IsSsdOperationPreemptionPriority(EOperationPreemptionPriority operationPreemptionPriority)
{
    switch (operationPreemptionPriority) {
        case EOperationPreemptionPriority::SsdNormal:
        case EOperationPreemptionPriority::SsdAggressive:
            return true;
        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Builds the subtree induced by |operationSubset| and returns the list of children in this subtree for each pool.
//! Children lists are returned in a vector indexed by elements' tree indexes, like dynamic attributes list (for performance reasons).
// TODO(eshcherbin): Add a class which would represent a subtree.
std::vector<TNonOwningElementList> BuildOperationSubsetInducedSubtree(
    TSchedulerRootElement* rootElement,
    const TNonOwningOperationElementList& operationSubset)
{
    //! NB(eshcherbin): All operations in |operationSubset| must be schedulable.
    std::vector<TNonOwningElementList> subtreeChildrenPerPool(rootElement->SchedulableElementCount());
    for (auto* operationElement : operationSubset) {
        TSchedulerElement* element = operationElement;
        while (auto* parent = element->GetMutableParent()) {
            auto& parentSubtreeChildren = GetSchedulerElementAttributesFromVector(subtreeChildrenPerPool, parent);

            bool firstVisit = false;
            if (parentSubtreeChildren.empty()) {
                // For fewer reallocations.
                parentSubtreeChildren.reserve(parent->SchedulableChildren().size());
                firstVisit = true;
            }

            parentSubtreeChildren.push_back(element);

            if (!firstVisit) {
                break;
            }

            element = parent;
        }
    }

    return subtreeChildrenPerPool;
}

////////////////////////////////////////////////////////////////////////////////

using EOperationSchedulingPriorityList = TCompactVector<EOperationSchedulingPriority, TEnumTraits<EOperationSchedulingPriority>::GetDomainSize()>;
const EOperationSchedulingPriorityList& GetDescendingSchedulingPriorities()
{
    static const EOperationSchedulingPriorityList DescendingSchedulingPriorities = {
        EOperationSchedulingPriority::High,
        EOperationSchedulingPriority::Medium,
    };

    return DescendingSchedulingPriorities;
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
        consideredSchedulableChildren = element->SchedulableChildren();
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
    for (auto* child : element->SchedulableChildren()) {
        if (!AttributesOf(child).Active) {
            continue;
        }

        if (!bestChild || element->HasHigherPriorityInFifoMode(child, bestChild)) {
            bestChild = child;
        }
    }
    return bestChild;
}

TSchedulerElement* TDynamicAttributesManager::GetBestActiveChildFairShare(TSchedulerCompositeElement* element) const
{
    TSchedulerElement* bestChild = nullptr;
    double bestChildSatisfactionRatio = InfiniteSatisfactionRatio;
    for (auto* child : element->SchedulableChildren()) {
        if (!AttributesOf(child).Active) {
            continue;
        }

        double childSatisfactionRatio = AttributesOf(child).SatisfactionRatio;
        if (!bestChild || childSatisfactionRatio < bestChildSatisfactionRatio) {
            bestChild = child;
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
    const TFairShareTreeAllocationSchedulerOperationSharedStatePtr& operationSharedState,
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
    for (auto* child : element->SchedulableChildren()) {
        resourceUsage += FillResourceUsage(child, context);
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

TSchedulingStageProfilingCounters::TSchedulingStageProfilingCounters(
    const NProfiling::TProfiler& profiler)
    : PrescheduleAllocationCount(profiler.Counter("/preschedule_job_count"))
    , UselessPrescheduleAllocationCount(profiler.Counter("/useless_preschedule_job_count"))
    , PrescheduleAllocationTime(profiler.Timer("/preschedule_job_time"))
    , TotalControllerScheduleAllocationTime(profiler.Timer("/controller_schedule_job_time/total"))
    , ExecControllerScheduleAllocationTime(profiler.Timer("/controller_schedule_job_time/exec"))
    , StrategyScheduleAllocationTime(profiler.Timer("/strategy_schedule_job_time"))
    , PackingRecordHeartbeatTime(profiler.Timer("/packing_record_heartbeat_time"))
    , PackingCheckTime(profiler.Timer("/packing_check_time"))
    , AnalyzeAllocationsTime(profiler.Timer("/analyze_jobs_time"))
    , CumulativePrescheduleAllocationTime(profiler.TimeCounter("/cumulative_preschedule_job_time"))
    , CumulativeTotalControllerScheduleAllocationTime(profiler.TimeCounter("/cumulative_controller_schedule_job_time/total"))
    , CumulativeExecControllerScheduleAllocationTime(profiler.TimeCounter("/cumulative_controller_schedule_job_time/exec"))
    , CumulativeStrategyScheduleAllocationTime(profiler.TimeCounter("/cumulative_strategy_schedule_job_time"))
    , CumulativeAnalyzeAllocationsTime(profiler.TimeCounter("/cumulative_analyze_jobs_time"))
    , ScheduleAllocationAttemptCount(profiler.Counter("/schedule_job_attempt_count"))
    , ScheduleAllocationFailureCount(profiler.Counter("/schedule_job_failure_count"))
    , ControllerScheduleAllocationCount(profiler.Counter("/controller_schedule_job_count"))
    , ControllerScheduleAllocationTimedOutCount(profiler.Counter("/controller_schedule_job_timed_out_count"))
    , ActiveTreeSize(profiler.Summary("/active_tree_size"))
    , ActiveOperationCount(profiler.Summary("/active_operation_count"))
{
    for (auto reason : TEnumTraits<NControllerAgent::EScheduleAllocationFailReason>::GetDomainValues()) {
        ControllerScheduleAllocationFail[reason] = profiler
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

    for (int stageAttemptIndex = 0; stageAttemptIndex < std::ssize(StageAttemptCount); ++stageAttemptIndex) {
        // NB(eshcherbin): We use sparse to avoid creating many unneeded and noisy sensors.
        StageAttemptCount[stageAttemptIndex] = profiler
            .WithSparse()
            .WithTag("attempt_index", ToString(stageAttemptIndex))
            .Counter("/stage_attempt_count");
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TAllocationWithPreemptionInfo& allocationInfo, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{AllocationId: %v, PreemptionStatus: %v, OperationId: %v}",
        allocationInfo.Allocation->GetId(),
        allocationInfo.PreemptionStatus,
        allocationInfo.OperationElement->GetId());
}

TString ToString(const TAllocationWithPreemptionInfo& allocationInfo)
{
    return ToStringViaBuilder(allocationInfo);
}

////////////////////////////////////////////////////////////////////////////////

TScheduleAllocationsContext::TScheduleAllocationsContext(
    ISchedulingContextPtr schedulingContext,
    TFairShareTreeSnapshotPtr treeSnapshot,
    const TFairShareTreeAllocationSchedulerNodeState* nodeState,
    bool schedulingInfoLoggingEnabled,
    ISchedulerStrategyHost* strategyHost,
    const NProfiling::TCounter& scheduleAllocationsDeadlineReachedCounter,
    const NLogging::TLogger& logger)
    : SchedulingContext_(std::move(schedulingContext))
    , TreeSnapshot_(std::move(treeSnapshot))
    , SsdPriorityPreemptionEnabled_(TreeSnapshot_->TreeConfig()->SsdPriorityPreemption->Enable &&
        SchedulingContext_->CanSchedule(TreeSnapshot_->TreeConfig()->SsdPriorityPreemption->NodeTagFilter))
    , SchedulingDeadline_(SchedulingContext_->GetNow() + DurationToCpuDuration(TreeSnapshot_->ControllerConfig()->ScheduleAllocationsTimeout))
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
    , ScheduleAllocationsDeadlineReachedCounter_(scheduleAllocationsDeadlineReachedCounter)
    , Logger(logger)
    , DynamicAttributesManager_(TreeSnapshot_->SchedulingSnapshot())
{
    YT_LOG_DEBUG_IF(
        DynamicAttributesListSnapshot_ && SchedulingInfoLoggingEnabled_,
        "Using dynamic attributes snapshot for allocation scheduling");

    SchedulingStatistics_.ResourceUsage = SchedulingContext_->ResourceUsage();
    SchedulingStatistics_.ResourceLimits = SchedulingContext_->ResourceLimits();
    SchedulingStatistics_.SsdPriorityPreemptionEnabled = SsdPriorityPreemptionEnabled_;
    SchedulingStatistics_.SsdPriorityPreemptionMedia = SsdPriorityPreemptionMedia_;
    SchedulingStatistics_.OperationCountByPreemptionPriority = OperationCountByPreemptionPriority_;
}

void TScheduleAllocationsContext::PrepareForScheduling()
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
        ConsideredSchedulableChildrenPerPool_.reset();
    }
}

void TScheduleAllocationsContext::PrescheduleAllocation(
    const std::optional<TNonOwningOperationElementList>& consideredSchedulableOperations,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    TWallTimer prescheduleTimer;

    CollectConsideredSchedulableChildrenPerPool(consideredSchedulableOperations);
    PrescheduleAllocationAtCompositeElement(TreeSnapshot_->RootElement().Get(), targetOperationPreemptionPriority);

    StageState_->PrescheduleDuration = prescheduleTimer.GetElapsedTime();
    StageState_->PrescheduleExecuted = true;
}

bool TScheduleAllocationsContext::ShouldContinueScheduling(const std::optional<TJobResources>& customMinSpareAllocationResources) const
{
    return SchedulingContext_->CanStartMoreAllocations(customMinSpareAllocationResources) &&
        SchedulingContext_->GetNow() < SchedulingDeadline_;
}

TScheduleAllocationsContext::TFairShareScheduleAllocationResult TScheduleAllocationsContext::ScheduleAllocation(bool ignorePacking)
{
    ++StageState_->ScheduleAllocationAttemptCount;

    auto* bestOperation = FindBestOperationForScheduling();
    if (!bestOperation) {
        return TFairShareScheduleAllocationResult{
            .Finished = true,
            .Scheduled = false,
        };
    }

    bool scheduled = ScheduleAllocation(bestOperation, ignorePacking);

    if (scheduled) {
        ReactivateBadPackingOperations();
    }

    if (SchedulingContext_->GetNow() >= SchedulingDeadline_) {
        ScheduleAllocationsDeadlineReachedCounter_.Increment();
    }

    return TFairShareScheduleAllocationResult{
        .Finished = false,
        .Scheduled = scheduled,
    };
}

bool TScheduleAllocationsContext::ScheduleAllocationInTest(TSchedulerOperationElement* element, bool ignorePacking)
{
    return ScheduleAllocation(element, ignorePacking);
}

int TScheduleAllocationsContext::GetOperationWithPreemptionPriorityCount(EOperationPreemptionPriority priority) const
{
    return OperationCountByPreemptionPriority_[priority];
}

void TScheduleAllocationsContext::AnalyzePreemptibleAllocations(
    EOperationPreemptionPriority targetOperationPreemptionPriority,
    EAllocationPreemptionLevel minAllocationPreemptionLevel,
    std::vector<TAllocationWithPreemptionInfo>* unconditionallyPreemptibleAllocations,
    TNonOwningAllocationSet* forcefullyPreemptibleAllocations)
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();

    YT_LOG_TRACE("Looking for preemptible allocations (MinAllocationPreemptionLevel: %v)", minAllocationPreemptionLevel);

    int totalConditionallyPreemptibleAllocationCount = 0;
    int maxConditionallyPreemptibleAllocationCountInPool = 0;

    NProfiling::TWallTimer timer;

    auto allocationInfos = CollectRunningAllocationsWithPreemptionInfo(SchedulingContext_, TreeSnapshot_);
    for (const auto& allocationInfo : allocationInfos) {
        const auto& [allocation, preemptionStatus, operationElement] = allocationInfo;

        bool isAllocationForcefullyPreemptible = !IsSchedulingSegmentCompatibleWithNode(operationElement);
        if (isAllocationForcefullyPreemptible) {
            const auto& operationState = TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationState(operationElement);

            YT_ELEMENT_LOG_DETAILED(
                operationElement,
                "Allocation is forcefully preemptible because it is running on a node in a different scheduling segment or module "
                "(AllocationId: %v, OperationId: %v, OperationSegment: %v, NodeSegment: %v, Address: %v, Module: %v)",
                allocation->GetId(),
                operationElement->GetId(),
                operationState->SchedulingSegment,
                NodeSchedulingSegment_,
                SchedulingContext_->GetNodeDescriptor()->Address,
                SchedulingContext_->GetNodeDescriptor()->DataCenter);

            forcefullyPreemptibleAllocations->insert(allocation.Get());
        }

        auto allocationPreemptionLevel = GetAllocationPreemptionLevel(allocationInfo);
        bool isAllocationPreemptible = isAllocationForcefullyPreemptible || (allocationPreemptionLevel >= minAllocationPreemptionLevel);
        if (!isAllocationPreemptible) {
            continue;
        }

        auto preemptionBlockingAncestor = FindPreemptionBlockingAncestor(operationElement, allocationPreemptionLevel, targetOperationPreemptionPriority);
        bool isUnconditionalPreemptionAllowed = isAllocationForcefullyPreemptible || preemptionBlockingAncestor == nullptr;
        bool isConditionalPreemptionAllowed = treeConfig->EnableConditionalPreemption &&
            !isUnconditionalPreemptionAllowed &&
            preemptionStatus == EAllocationPreemptionStatus::Preemptible &&
            preemptionBlockingAncestor != operationElement;

        if (isUnconditionalPreemptionAllowed) {
            const auto* parent = operationElement->GetParent();
            while (parent) {
                LocalUnconditionalUsageDiscountMap_[parent->GetTreeIndex()] += allocation->ResourceUsage();
                parent = parent->GetParent();
            }
            SchedulingContext_->IncreaseUnconditionalDiscount(TJobResourcesWithQuota(allocation->ResourceUsage(), allocation->DiskQuota()));
            unconditionallyPreemptibleAllocations->push_back(allocationInfo);
        } else if (isConditionalPreemptionAllowed) {
            ConditionallyPreemptibleAllocationSetMap_[preemptionBlockingAncestor->GetTreeIndex()].insert(allocationInfo);
            ++totalConditionallyPreemptibleAllocationCount;
        }
    }

    TPrepareConditionalUsageDiscountsContext context{.TargetOperationPreemptionPriority = targetOperationPreemptionPriority};
    PrepareConditionalUsageDiscountsAtCompositeElement(TreeSnapshot_->RootElement().Get(), &context);
    for (const auto& [_, allocationSet] : ConditionallyPreemptibleAllocationSetMap_) {
        maxConditionallyPreemptibleAllocationCountInPool = std::max(
            maxConditionallyPreemptibleAllocationCountInPool,
            static_cast<int>(allocationSet.size()));
    }

    StageState_->AnalyzeAllocationsDuration += timer.GetElapsedTime();

    SchedulingStatistics_.UnconditionallyPreemptibleAllocationCount = unconditionallyPreemptibleAllocations->size();
    SchedulingStatistics_.UnconditionalResourceUsageDiscount = SchedulingContext_->UnconditionalDiscount().ToJobResources();
    SchedulingStatistics_.MaxConditionalResourceUsageDiscount = SchedulingContext_->GetMaxConditionalDiscount().ToJobResources();
    SchedulingStatistics_.TotalConditionallyPreemptibleAllocationCount = totalConditionallyPreemptibleAllocationCount;
    SchedulingStatistics_.MaxConditionallyPreemptibleAllocationCountInPool = maxConditionallyPreemptibleAllocationCountInPool;
}

void TScheduleAllocationsContext::PreemptAllocationsAfterScheduling(
    EOperationPreemptionPriority targetOperationPreemptionPriority,
    std::vector<TAllocationWithPreemptionInfo> preemptibleAllocations,
    const TNonOwningAllocationSet& forcefullyPreemptibleAllocations,
    const TAllocationPtr& allocationStartedUsingPreemption)
{
    // Collect conditionally preemptible allocations.
    EOperationPreemptionPriority preemptorOperationLocalPreemptionPriority;
    TAllocationWithPreemptionInfoSet conditionallyPreemptibleAllocations;
    if (allocationStartedUsingPreemption) {
        auto* operationElement = TreeSnapshot_->FindEnabledOperationElement(allocationStartedUsingPreemption->GetOperationId());
        YT_VERIFY(operationElement);

        preemptorOperationLocalPreemptionPriority = GetOperationPreemptionPriority(operationElement, EOperationPreemptionPriorityScope::OperationOnly);

        auto* parent = operationElement->GetParent();
        while (parent) {
            const auto& parentConditionallyPreemptibleAllocations = GetConditionallyPreemptibleAllocationsInPool(parent);
            conditionallyPreemptibleAllocations.insert(
                parentConditionallyPreemptibleAllocations.begin(),
                parentConditionallyPreemptibleAllocations.end());

            parent = parent->GetParent();
        }
    }

    preemptibleAllocations.insert(preemptibleAllocations.end(), conditionallyPreemptibleAllocations.begin(), conditionallyPreemptibleAllocations.end());
    SortAllocationsWithPreemptionInfo(&preemptibleAllocations);
    std::reverse(preemptibleAllocations.begin(), preemptibleAllocations.end());

    // Reset discounts.
    SchedulingContext_->ResetDiscounts();
    LocalUnconditionalUsageDiscountMap_.clear();
    ConditionallyPreemptibleAllocationSetMap_.clear();

    auto findPoolWithViolatedLimitsForAllocation = [&] (const TAllocationPtr& allocation) -> const TSchedulerCompositeElement* {
        auto* operationElement = TreeSnapshot_->FindEnabledOperationElement(allocation->GetOperationId());
        if (!operationElement) {
            return nullptr;
        }

        auto* parent = operationElement->GetParent();
        while (parent) {
            if (parent->AreSpecifiedResourceLimitsViolated()) {
                return parent;
            }
            parent = parent->GetParent();
        }
        return nullptr;
    };

    // TODO(eshcherbin): Use a separate tag for specifying preemptive scheduling stage.
    // Bloating |EAllocationPreemptionReason| is unwise.
    auto preemptionReason = [&] {
        switch (targetOperationPreemptionPriority) {
            case EOperationPreemptionPriority::Normal:
                return EAllocationPreemptionReason::Preemption;
            case EOperationPreemptionPriority::SsdNormal:
                return EAllocationPreemptionReason::SsdPreemption;
            case EOperationPreemptionPriority::Aggressive:
                return EAllocationPreemptionReason::AggressivePreemption;
            case EOperationPreemptionPriority::SsdAggressive:
                return EAllocationPreemptionReason::SsdAggressivePreemption;
            default:
                YT_ABORT();
        }
    }();

    int currentAllocationIndex = 0;
    for (; currentAllocationIndex < std::ssize(preemptibleAllocations); ++currentAllocationIndex) {
        if (Dominates(SchedulingContext_->ResourceLimits(), SchedulingContext_->ResourceUsage()) &&
            CanSatisfyDiskQuotaRequests(SchedulingContext_->DiskResources(), SchedulingContext_->DiskRequests()))
        {
            break;
        }

        const auto& allocationInfo = preemptibleAllocations[currentAllocationIndex];
        const auto& [allocation, preemptionStatus, operationElement] = allocationInfo;

        if (!IsAllocationKnown(operationElement, allocation->GetId())) {
            // Allocation may have been terminated concurrently with scheduling, e.g. operation aborted by user request. See: YT-16429.
            YT_LOG_DEBUG(
                "Allocation preemption skipped, since the allocation is already terminated (AllocationId: %v, OperationId: %v)",
                allocation->GetId(),
                allocation->GetOperationId());

            continue;
        }

        if (allocationStartedUsingPreemption) {
            TStringBuilder preemptionReasonBuilder;
            preemptionReasonBuilder.AppendFormat(
                "Preempted to start allocation %v of operation %v; "
                "this allocation had status %Qlv and level %Qlv, preemptor operation local priority was %Qlv, "
                "and scheduling stage target priority was %Qlv",
                allocationStartedUsingPreemption->GetId(),
                allocationStartedUsingPreemption->GetOperationId(),
                preemptionStatus,
                GetAllocationPreemptionLevel(allocationInfo),
                preemptorOperationLocalPreemptionPriority,
                targetOperationPreemptionPriority);
            if (forcefullyPreemptibleAllocations.contains(allocation.Get())) {
                preemptionReasonBuilder.AppendString(
                    "; this allocation was forcefully preemptible, because its node was moved to other scheduling segment");
            }
            if (conditionallyPreemptibleAllocations.contains(allocationInfo)) {
                preemptionReasonBuilder.AppendString("; this allocation was conditionally preemptible");
            }

            allocation->SetPreemptionReason(preemptionReasonBuilder.Flush());

            allocation->SetPreemptedFor(TPreemptedFor{
                .AllocationId = allocationStartedUsingPreemption->GetId(),
                .OperationId = allocationStartedUsingPreemption->GetOperationId(),
            });

            allocation->SetPreemptedForProperlyStarvingOperation(
                targetOperationPreemptionPriority == preemptorOperationLocalPreemptionPriority);
        } else {
            allocation->SetPreemptionReason(Format("Node resource limits violated"));
        }
        PreemptAllocation(allocation, operationElement, preemptionReason);
    }

    // NB(eshcherbin): Specified resource limits can be violated in two cases:
    // 1. An allocation has just been scheduled with preemption over the limit.
    // 2. The limit has been reduced in the config.
    // Note that in the second case any allocation, which is considered preemptible at least in some stage,
    // may be preempted (e.g. an aggressively preemptible allocation can be preempted without scheduling any new allocations).
    // This is one of the reasons why we advise against specified resource limits.
    for (; currentAllocationIndex < std::ssize(preemptibleAllocations); ++currentAllocationIndex) {
        const auto& allocationInfo = preemptibleAllocations[currentAllocationIndex];
        if (conditionallyPreemptibleAllocations.contains(allocationInfo)) {
            // Only unconditionally preemptible allocations can be preempted to recover violated resource limits.
            continue;
        }

        const auto& [allocation, _, operationElement] = allocationInfo;
        if (!IsAllocationKnown(operationElement, allocation->GetId())) {
            // Allocation may have been terminated concurrently with scheduling, e.g. operation aborted by user request. See: YT-16429, YT-17913.
            YT_LOG_DEBUG(
                "Allocation preemption skipped, since the allocation is already terminated (AllocationId: %v, OperationId: %v)",
                allocation->GetId(),
                allocation->GetOperationId());

            continue;
        }

        if (operationElement->AreSpecifiedResourceLimitsViolated()) {
            allocation->SetPreemptionReason(
                Format("Preempted due to violation of resource limits of operation %v",
                operationElement->GetId()));
            PreemptAllocation(allocation, operationElement, EAllocationPreemptionReason::ResourceLimitsViolated);
            continue;
        }

        if (auto violatedPool = findPoolWithViolatedLimitsForAllocation(allocation)) {
            allocation->SetPreemptionReason(
                Format("Preempted due to violation of limits on pool %Qv",
                violatedPool->GetId()));
            PreemptAllocation(allocation, operationElement, EAllocationPreemptionReason::ResourceLimitsViolated);
        }
    }

    if (!Dominates(SchedulingContext_->ResourceLimits(), SchedulingContext_->ResourceUsage())) {
        YT_LOG_INFO("Resource usage exceeds node resource limits even after preemption (ResourceLimits: %v, ResourceUsage: %v, NodeId: %v, Address: %v)",
            FormatResources(SchedulingContext_->ResourceLimits()),
            FormatResources(SchedulingContext_->ResourceUsage()),
            SchedulingContext_->GetNodeDescriptor()->Id,
            SchedulingContext_->GetNodeDescriptor()->Address);
    }
}

void TScheduleAllocationsContext::AbortAllocationsSinceResourcesOvercommit() const
{
    YT_LOG_DEBUG(
        "Preempting allocations on node since resources are overcommitted (NodeId: %v, Address: %v, ResourceLimits: %v, ResourceUsage: %v)",
        FormatResources(SchedulingContext_->ResourceLimits()),
        FormatResources(SchedulingContext_->ResourceUsage()),
        SchedulingContext_->GetNodeDescriptor()->Id,
        SchedulingContext_->GetNodeDescriptor()->Address);

    auto allocationInfos = CollectRunningAllocationsWithPreemptionInfo(SchedulingContext_, TreeSnapshot_);
    SortAllocationsWithPreemptionInfo(&allocationInfos);

    TJobResources currentResources;
    for (const auto& allocationInfo : allocationInfos) {
        if (!Dominates(SchedulingContext_->ResourceLimits(), currentResources + allocationInfo.Allocation->ResourceUsage())) {
            YT_LOG_DEBUG(
                "Preempting allocation since node resources are overcommitted "
                "(ResourceLimits: %v, CurrentResourceUsage: %v, AllocationResourceUsage: %v, AllocationId: %v, OperationId: %v, NodeAddress: %v)",
                FormatResources(SchedulingContext_->ResourceLimits()),
                FormatResources(currentResources),
                FormatResources(allocationInfo.Allocation->ResourceUsage()),
                allocationInfo.Allocation->GetId(),
                allocationInfo.OperationElement->GetId(),
                SchedulingContext_->GetNodeDescriptor()->Address);

            allocationInfo.Allocation->SetPreemptionReason("Preempted due to node resource ovecommit");
            PreemptAllocation(allocationInfo.Allocation, allocationInfo.OperationElement, EAllocationPreemptionReason::ResourceOvercommit);
        } else {
            currentResources += allocationInfo.Allocation->ResourceUsage();
        }
    }
}

void TScheduleAllocationsContext::PreemptAllocation(
    const TAllocationPtr& allocation,
    TSchedulerOperationElement* element,
    EAllocationPreemptionReason preemptionReason) const
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();

    SchedulingContext_->ResourceUsage() -= allocation->ResourceUsage();
    allocation->ResourceUsage() = TJobResources();

    const auto& operationSharedState = TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    auto delta = operationSharedState->SetAllocationResourceUsage(allocation->GetId(), TJobResources());
    element->IncreaseHierarchicalResourceUsage(delta);
    operationSharedState->UpdatePreemptibleAllocationsList(element);

    SchedulingContext_->PreemptAllocation(allocation, treeConfig->AllocationPreemptionTimeout, preemptionReason);
}

TNonOwningOperationElementList TScheduleAllocationsContext::ExtractBadPackingOperations()
{
    TNonOwningOperationElementList badPackingOperations;
    std::swap(BadPackingOperations_, badPackingOperations);

    return badPackingOperations;
}

void TScheduleAllocationsContext::StartStage(
    EAllocationSchedulingStage stage,
    TSchedulingStageProfilingCounters* profilingCounters,
    int stageAttemptIndex)
{
    YT_VERIFY(!StageState_);

    StageState_.emplace(TStageState{
        .Stage = stage,
        .ProfilingCounters = profilingCounters,
        .StageAttemptIndex = stageAttemptIndex,
    });

    if (stageAttemptIndex < std::ssize(profilingCounters->StageAttemptCount)) {
        profilingCounters->StageAttemptCount[stageAttemptIndex].Increment();
    }
}

void TScheduleAllocationsContext::FinishStage()
{
    YT_VERIFY(StageState_);

    StageState_->DeactivationReasons[EDeactivationReason::NoBestLeafDescendant] = DynamicAttributesManager_.GetCompositeElementDeactivationCount();
    SchedulingStatistics_.ScheduleAllocationAttemptCountPerStage[GetStageType()] = StageState_->ScheduleAllocationAttemptCount;
    ProfileAndLogStatisticsOfStage();

    StageState_.reset();
}

int TScheduleAllocationsContext::GetStageMaxSchedulingIndex() const
{
    return StageState_->MaxSchedulingIndex;
}

bool TScheduleAllocationsContext::GetStagePrescheduleExecuted() const
{
    return StageState_->PrescheduleExecuted;
}

// TODO(eshcherbin): Reconsider this logic.
const TSchedulerElement* TScheduleAllocationsContext::FindPreemptionBlockingAncestor(
    const TSchedulerOperationElement* element,
    EAllocationPreemptionLevel allocationPreemptionLevel,
    EOperationPreemptionPriority targetOperationPreemptionPriority) const
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();
    const auto& spec = element->Spec();

    if (spec->PreemptionMode == EPreemptionMode::Graceful) {
        return element;
    }

    // NB(eshcherbin): We ignore preemption blocking ancestors for allocations with SSD priority preemption.
    if (IsSsdOperationPreemptionPriority(targetOperationPreemptionPriority) && !IsSsdAllocationPreemptionLevel(allocationPreemptionLevel)) {
        return {};
    }

    if (targetOperationPreemptionPriority == EOperationPreemptionPriority::Normal) {
        const TSchedulerElement* current = element;
        while (current) {
            // NB: This option is intended only for testing purposes.
            if (!IsNormalPreemptionAllowed(current)) {
                UpdateOperationPreemptionStatusStatistics(element, EOperationPreemptionStatus::ForbiddenInAncestorConfig);
                return element;
            }

            current = current->GetParent();
        }
    }

    const TSchedulerElement* current = element;
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

void TScheduleAllocationsContext::PrepareConditionalUsageDiscounts(
    const TSchedulerElement* element,
    TPrepareConditionalUsageDiscountsContext* context)
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

const TAllocationWithPreemptionInfoSet& TScheduleAllocationsContext::GetConditionallyPreemptibleAllocationsInPool(
    const TSchedulerCompositeElement* element) const
{
    auto it = ConditionallyPreemptibleAllocationSetMap_.find(element->GetTreeIndex());
    return it != ConditionallyPreemptibleAllocationSetMap_.end() ? it->second : EmptyAllocationWithPreemptionInfoSet;
}

const TDynamicAttributes& TScheduleAllocationsContext::DynamicAttributesOf(const TSchedulerElement* element) const
{
    YT_ASSERT(Initialized_);

    return DynamicAttributesManager_.AttributesOf(element);
}

void TScheduleAllocationsContext::DeactivateOperationInTest(TSchedulerOperationElement* element)
{
    return DynamicAttributesManager_.DeactivateOperation(element);
}

const TStaticAttributes& TScheduleAllocationsContext::StaticAttributesOf(const TSchedulerElement* element) const
{
    return TreeSnapshot_->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element);
}

bool TScheduleAllocationsContext::IsActive(const TSchedulerElement* element) const
{
    return DynamicAttributesManager_.AttributesOf(element).Active;
}

TJobResources TScheduleAllocationsContext::GetCurrentResourceUsage(const TSchedulerElement* element) const
{
    if (element->IsSchedulable()) {
        return DynamicAttributesOf(element).ResourceUsage;
    } else {
        return element->PostUpdateAttributes().UnschedulableOperationsResourceUsage;
    }
}

TJobResources TScheduleAllocationsContext::GetHierarchicalAvailableResources(const TSchedulerElement* element) const
{
    auto availableResources = TJobResources::Infinite();
    while (element) {
        availableResources = Min(availableResources, GetLocalAvailableResourceLimits(element));
        element = element->GetParent();
    }

    return availableResources;
}

TJobResources TScheduleAllocationsContext::GetLocalAvailableResourceLimits(const TSchedulerElement* element) const
{
    if (element->MaybeSpecifiedResourceLimits()) {
        return ComputeAvailableResources(
            element->ResourceLimits(),
            element->GetResourceUsageWithPrecommit(),
            GetLocalUnconditionalUsageDiscount(element));
    }
    return TJobResources::Infinite();
}

TJobResources TScheduleAllocationsContext::GetLocalUnconditionalUsageDiscount(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_VERIFY(index != UnassignedTreeIndex);

    auto it = LocalUnconditionalUsageDiscountMap_.find(index);
    return it != LocalUnconditionalUsageDiscountMap_.end() ? it->second : TJobResources{};
}

void TScheduleAllocationsContext::CollectConsideredSchedulableChildrenPerPool(
    const std::optional<TNonOwningOperationElementList>& consideredSchedulableOperations)
{
    // NB: This means all schedulable operations are considered, so no need for extra work,
    // because full lists of schedulable children have been precomputed during post update.
    if (!consideredSchedulableOperations) {
        return;
    }

    ConsideredSchedulableChildrenPerPool_ = BuildOperationSubsetInducedSubtree(
        TreeSnapshot_->RootElement().Get(),
        *consideredSchedulableOperations);
}

void TScheduleAllocationsContext::PrescheduleAllocation(
    TSchedulerElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            PrescheduleAllocationAtCompositeElement(static_cast<TSchedulerCompositeElement*>(element), targetOperationPreemptionPriority);
            break;
        case ESchedulerElementType::Operation:
            PrescheduleAllocationAtOperation(static_cast<TSchedulerOperationElement*>(element), targetOperationPreemptionPriority);
            break;
        default:
            YT_ABORT();
    }
}

void TScheduleAllocationsContext::PrescheduleAllocationAtCompositeElement(
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

    std::optional<TNonOwningElementList> consideredSchedulableChildren;
    if (ConsideredSchedulableChildrenPerPool_) {
        // NB(eshcherbin): We move children list out of |ConsideredSchedulableChildrenPerPool_| to avoid extra copying.
        // This is the only place |ConsideredSchedulableChildrenPerPool_| is accessed after it is initialized.
        consideredSchedulableChildren = std::move(GetSchedulerElementAttributesFromVector(*ConsideredSchedulableChildrenPerPool_, element));
    }

    // COMPAT(eshcherbin): Use all of element's schedulable children if we don't compute considered subset.
    const TNonOwningElementList& children = consideredSchedulableChildren
        ? *consideredSchedulableChildren
        : element->SchedulableChildren();
    for (auto* child : children) {
        PrescheduleAllocation(child, targetOperationPreemptionPriority);
    }

    bool useChildHeap = false;
    int schedulableChildrenCount = std::ssize(children);
    if (schedulableChildrenCount >= TreeSnapshot_->TreeConfig()->MinChildHeapSize) {
        useChildHeap = true;
        StageState_->TotalHeapElementCount += schedulableChildrenCount;
    }

    DynamicAttributesManager_.InitializeAttributesAtCompositeElement(element, std::move(consideredSchedulableChildren), useChildHeap);

    if (DynamicAttributesOf(element).Active) {
        ++StageState_->ActiveTreeSize;
    }
}

void TScheduleAllocationsContext::PrescheduleAllocationAtOperation(
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

TSchedulerOperationElement* TScheduleAllocationsContext::FindBestOperationForScheduling()
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

bool TScheduleAllocationsContext::ScheduleAllocation(TSchedulerOperationElement* element, bool ignorePacking)
{
    YT_VERIFY(IsActive(element));

    YT_ELEMENT_LOG_DETAILED(
        element,
        "Trying to schedule allocation "
        "(SatisfactionRatio: %v, NodeId: %v, NodeResourceUsage: %v, "
        "UsageDiscount: {Total: %v, Unconditional: %v, Conditional: %v}, StageType: %v)",
        DynamicAttributesOf(element).SatisfactionRatio,
        SchedulingContext_->GetNodeDescriptor()->Id,
        FormatResourceUsage(SchedulingContext_->ResourceUsage(), SchedulingContext_->ResourceLimits()),
        FormatResources(SchedulingContext_->UnconditionalDiscount() +
            SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
        FormatResources(SchedulingContext_->UnconditionalDiscount()),
        FormatResources(SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
        GetStageType());

    auto deactivateOperationElement = [&] (EDeactivationReason reason) {
        YT_ELEMENT_LOG_DETAILED(
            element,
            "Failed to schedule allocation, operation deactivated "
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

    auto decreaseHierarchicalResourceUsagePrecommit = [&] (const TJobResources& precommittedResources, TControllerEpoch scheduleAllocationEpoch) {
        if (IsOperationEnabled(element) && scheduleAllocationEpoch == element->GetControllerEpoch()) {
            element->DecreaseHierarchicalResourceUsagePrecommit(precommittedResources);
        }
    };

    int schedulingIndex = StaticAttributesOf(element).SchedulingIndex;
    YT_VERIFY(schedulingIndex != UndefinedSchedulingIndex);
    ++StageState_->SchedulingIndexToScheduleAllocationAttemptCount[schedulingIndex];
    StageState_->MaxSchedulingIndex = std::max(StageState_->MaxSchedulingIndex, schedulingIndex);
    IncrementOperationScheduleAllocationAttemptCount(element);

    if (auto blockedReason = CheckBlocked(element)) {
        deactivateOperationElement(*blockedReason);
        return false;
    }

    if (!IsOperationEnabled(element)) {
        deactivateOperationElement(EDeactivationReason::IsNotAlive);
        return false;
    }

    if (!HasAllocationsSatisfyingResourceLimits(element)) {
        YT_ELEMENT_LOG_DETAILED(
            element,
            "No pending allocations can satisfy available resources on node ("
            "FreeAllocationResources: %v, DiskResources: %v, DiscountResources: {Total: %v, Unconditional: %v, Conditional: %v}, "
            "MinNeededResources: %v, DetailedMinNeededResources: %v, "
            "Address: %v)",
            FormatResources(SchedulingContext_->GetNodeFreeResourcesWithoutDiscount()),
            SchedulingContext_->DiskResources(),
            FormatResources(SchedulingContext_->UnconditionalDiscount() + SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
            FormatResources(SchedulingContext_->UnconditionalDiscount()),
            FormatResources(SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
            FormatResources(element->AggregatedMinNeededAllocationResources()),
            MakeFormattableView(
                element->DetailedMinNeededAllocationResources(),
                [&] (TStringBuilderBase* builder, const TJobResourcesWithQuota& resources) {
                    builder->AppendFormat("%v", StrategyHost_->FormatResources(resources));
                }),
            SchedulingContext_->GetNodeDescriptor()->Address);

        OnMinNeededResourcesUnsatisfied(
            element,
            SchedulingContext_->GetNodeFreeResourcesWithDiscountForOperation(element->GetOperationId()),
            element->AggregatedMinNeededAllocationResources());
        deactivateOperationElement(EDeactivationReason::MinNeededResourcesUnsatisfied);
        return false;
    }

    TJobResources precommittedResources;
    TJobResources availableAllocationResources;
    TDiskResources availableDiskResources;

    auto scheduleAllocationEpoch = element->GetControllerEpoch();

    auto deactivationReason = TryStartScheduleAllocation(
        element,
        &precommittedResources,
        &availableAllocationResources,
        &availableDiskResources);
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
            decreaseHierarchicalResourceUsagePrecommit(precommittedResources, scheduleAllocationEpoch);
            deactivateOperationElement(EDeactivationReason::BadPacking);
            BadPackingOperations_.push_back(element);
            FinishScheduleAllocation(element);

            return false;
        }
    }

    TControllerScheduleAllocationResultPtr scheduleAllocationResult;
    {
        NProfiling::TWallTimer timer;

        scheduleAllocationResult = DoScheduleAllocation(element, availableAllocationResources, availableDiskResources, &precommittedResources);

        auto scheduleAllocationDuration = timer.GetElapsedTime();
        StageState_->TotalScheduleAllocationDuration += scheduleAllocationDuration;
        StageState_->ExecScheduleAllocationDuration += scheduleAllocationResult->Duration;
    }

    if (!scheduleAllocationResult->StartDescriptor) {
        for (auto reason : TEnumTraits<EScheduleAllocationFailReason>::GetDomainValues()) {
            StageState_->FailedScheduleAllocation[reason] += scheduleAllocationResult->Failed[reason];
        }

        ++StageState_->ScheduleAllocationFailureCount;
        deactivateOperationElement(EDeactivationReason::ScheduleAllocationFailed);

        element->OnScheduleAllocationFailed(
            SchedulingContext_->GetNow(),
            element->GetTreeId(),
            scheduleAllocationResult);

        decreaseHierarchicalResourceUsagePrecommit(precommittedResources, scheduleAllocationEpoch);
        FinishScheduleAllocation(element);

        return false;
    }

    const auto& startDescriptor = *scheduleAllocationResult->StartDescriptor;

    const auto& operationSharedState = TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    bool onAllocationStartedSuccess = operationSharedState->OnAllocationStarted(
        element,
        startDescriptor.Id,
        startDescriptor.ResourceLimits,
        precommittedResources,
        scheduleAllocationEpoch);
    if (!onAllocationStartedSuccess) {
        element->AbortAllocation(
            startDescriptor.Id,
            EAbortReason::SchedulingOperationDisabled,
            scheduleAllocationResult->ControllerEpoch);
        deactivateOperationElement(EDeactivationReason::OperationDisabled);
        decreaseHierarchicalResourceUsagePrecommit(precommittedResources, scheduleAllocationEpoch);
        FinishScheduleAllocation(element);

        return false;
    }

    SchedulingContext_->StartAllocation(
        element->GetTreeId(),
        element->GetOperationId(),
        scheduleAllocationResult->IncarnationId,
        scheduleAllocationResult->ControllerEpoch,
        startDescriptor,
        element->Spec()->PreemptionMode,
        schedulingIndex,
        GetStageType());

    UpdateOperationResourceUsage(element);

    if (heartbeatSnapshot) {
        recordPackingHeartbeatWithTimer(*heartbeatSnapshot);
    }

    FinishScheduleAllocation(element);

    YT_ELEMENT_LOG_DETAILED(
        element,
        "Scheduled an allocation (SatisfactionRatio: %v, NodeId: %v, AllocationId: %v, AllocationResourceLimits: %v)",
        DynamicAttributesOf(element).SatisfactionRatio,
        SchedulingContext_->GetNodeDescriptor()->Id,
        startDescriptor.Id,
        StrategyHost_->FormatResources(startDescriptor.ResourceLimits));

    return true;
}

void TScheduleAllocationsContext::PrepareConditionalUsageDiscountsAtCompositeElement(
    const TSchedulerCompositeElement* element,
    TPrepareConditionalUsageDiscountsContext* context)
{
    TJobResourcesWithQuota deltaConditionalDiscount;
    for (const auto& allocationInfo : GetConditionallyPreemptibleAllocationsInPool(element)) {
        deltaConditionalDiscount += TJobResourcesWithQuota(allocationInfo.Allocation->ResourceUsage(), allocationInfo.Allocation->DiskQuota());
    }

    context->CurrentConditionalDiscount += deltaConditionalDiscount;
    for (auto* child : element->SchedulableChildren()) {
        PrepareConditionalUsageDiscounts(child, context);
    }
    context->CurrentConditionalDiscount -= deltaConditionalDiscount;
}

void TScheduleAllocationsContext::PrepareConditionalUsageDiscountsAtOperation(
    const TSchedulerOperationElement* element,
    TPrepareConditionalUsageDiscountsContext* context)
{
    if (GetOperationPreemptionPriority(element) != context->TargetOperationPreemptionPriority) {
        return;
    }
    SchedulingContext_->SetConditionalDiscountForOperation(element->GetOperationId(), context->CurrentConditionalDiscount);
}

std::optional<EDeactivationReason> TScheduleAllocationsContext::TryStartScheduleAllocation(
    TSchedulerOperationElement* element,
    TJobResources* precommittedResourcesOutput,
    TJobResources* availableResourcesOutput,
    TDiskResources* availableDiskResourcesOutput)
{
    const auto& minNeededResources = element->AggregatedMinNeededAllocationResources();

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

    element->OnScheduleAllocationStarted(SchedulingContext_);

    *precommittedResourcesOutput = minNeededResources;
    *availableResourcesOutput = Min(
        availableResourceLimits,
        SchedulingContext_->GetNodeFreeResourcesWithDiscountForOperation(element->GetOperationId()));
    *availableDiskResourcesOutput = SchedulingContext_->GetDiskResourcesWithDiscountForOperation(element->GetOperationId());
    return {};
}

TControllerScheduleAllocationResultPtr TScheduleAllocationsContext::DoScheduleAllocation(
    TSchedulerOperationElement* element,
    const TJobResources& availableResources,
    const TDiskResources& availableDiskResources,
    TJobResources* precommittedResources)
{
    ++SchedulingStatistics_.ControllerScheduleAllocationCount;

    auto scheduleAllocationResult = element->ScheduleAllocation(
        SchedulingContext_,
        availableResources,
        availableDiskResources,
        TreeSnapshot_->ControllerConfig()->ScheduleAllocationTimeLimit,
        element->GetTreeId(),
        TreeSnapshot_->TreeConfig());

    MaybeDelay(element->Spec()->TestingOperationOptions->ScheduleAllocationDelayScheduler);

    // Discard the allocation in case of resource overcommit.
    if (scheduleAllocationResult->StartDescriptor) {
        const auto& startDescriptor = *scheduleAllocationResult->StartDescriptor;
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
                auto allocationId = scheduleAllocationResult->StartDescriptor->Id;
                // NB(eshcherbin): GetHierarchicalAvailableResource will never return infinite resources here,
                // because ResourceLimitExceeded could only be triggered if there's an ancestor with specified limits.
                auto availableDelta = GetHierarchicalAvailableResources(element);
                YT_LOG_DEBUG(
                    "Aborting allocation with resource overcommit (AllocationId: %v, Limits: %v, AllocationResources: %v)",
                    allocationId,
                    FormatResources(*precommittedResources + availableDelta),
                    FormatResources(startDescriptor.ResourceLimits.ToJobResources()));

                element->AbortAllocation(
                    allocationId,
                    EAbortReason::SchedulingResourceOvercommit,
                    scheduleAllocationResult->ControllerEpoch);

                // Reset result.
                scheduleAllocationResult = New<TControllerScheduleAllocationResult>();
                scheduleAllocationResult->RecordFail(EScheduleAllocationFailReason::ResourceOvercommit);
                break;
            }
            case EResourceTreeIncreaseResult::ElementIsNotAlive: {
                auto allocationId = scheduleAllocationResult->StartDescriptor->Id;
                YT_LOG_DEBUG("Aborting allocation as operation is not alive in tree anymore (AllocationId: %v)", allocationId);

                element->AbortAllocation(
                    allocationId,
                    EAbortReason::SchedulingOperationIsNotAlive,
                    scheduleAllocationResult->ControllerEpoch);

                scheduleAllocationResult = New<TControllerScheduleAllocationResult>();
                scheduleAllocationResult->RecordFail(EScheduleAllocationFailReason::OperationIsNotAlive);
                break;
            }
            default:
                YT_ABORT();
        }
    } else if (scheduleAllocationResult->Failed[EScheduleAllocationFailReason::Timeout] > 0) {
        YT_LOG_WARNING("Allocation scheduling timed out");

        ++SchedulingStatistics_.ControllerScheduleAllocationTimedOutCount;

        YT_UNUSED_FUTURE(StrategyHost_->SetOperationAlert(
            element->GetOperationId(),
            EOperationAlertType::ScheduleJobTimedOut,
            TError("Allocation scheduling timed out: either scheduler is under heavy load or operation is too heavy"),
            TreeSnapshot_->ControllerConfig()->ScheduleAllocationTimeoutAlertResetTime));
    }

    return scheduleAllocationResult;
}

void TScheduleAllocationsContext::FinishScheduleAllocation(TSchedulerOperationElement* element)
{
    element->OnScheduleAllocationFinished(SchedulingContext_);
}

EOperationPreemptionPriority TScheduleAllocationsContext::GetOperationPreemptionPriority(
    const TSchedulerOperationElement* operationElement,
    EOperationPreemptionPriorityScope scope) const
{
    return NScheduler::GetOperationPreemptionPriority(
        operationElement,
        scope,
        SsdPriorityPreemptionEnabled_,
        SsdPriorityPreemptionMedia_);
}

bool TScheduleAllocationsContext::CheckForDeactivation(
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
        !StaticAttributesOf(element).AreRegularAllocationsOnSsdNodesAllowed)
    {
        OnOperationDeactivated(element, EDeactivationReason::RegularAllocationOnSsdNodeForbidden);
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

void TScheduleAllocationsContext::ActivateOperation(TSchedulerOperationElement* element)
{
    YT_VERIFY(!DynamicAttributesOf(element).Active);
    DynamicAttributesManager_.ActivateOperation(element);
}

void TScheduleAllocationsContext::DeactivateOperation(TSchedulerOperationElement* element, EDeactivationReason reason)
{
    YT_VERIFY(DynamicAttributesOf(element).Active);
    DynamicAttributesManager_.DeactivateOperation(element);
    OnOperationDeactivated(element, reason, /*considerInOperationCounter*/ true);
}

void TScheduleAllocationsContext::OnOperationDeactivated(
    TSchedulerOperationElement* element,
    EDeactivationReason reason,
    bool considerInOperationCounter)
{
    ++StageState_->DeactivationReasons[reason];
    if (considerInOperationCounter) {
        TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->OnOperationDeactivated(SchedulingContext_, reason);
    }
}

std::optional<EDeactivationReason> TScheduleAllocationsContext::CheckBlocked(const TSchedulerOperationElement* element) const
{
    if (element->IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(SchedulingContext_)) {
        return EDeactivationReason::MaxConcurrentScheduleAllocationCallsPerNodeShardViolated;
    }

    if (element->IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(SchedulingContext_)) {
        return EDeactivationReason::MaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated;
    }

    if (element->ScheduleAllocationBackoffCheckEnabled() &&
        element->HasRecentScheduleAllocationFailure(SchedulingContext_->GetNow()))
    {
        return EDeactivationReason::RecentScheduleAllocationFailed;
    }

    return std::nullopt;
}

bool TScheduleAllocationsContext::IsSchedulingSegmentCompatibleWithNode(const TSchedulerOperationElement* element) const
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

bool TScheduleAllocationsContext::IsOperationResourceUsageOutdated(const TSchedulerOperationElement* element) const
{
    auto now = SchedulingContext_->GetNow();
    auto updateTime = DynamicAttributesOf(element).ResourceUsageUpdateTime;
    return updateTime + DurationToCpuDuration(TreeSnapshot_->TreeConfig()->AllowedResourceUsageStaleness) < now;
}

void TScheduleAllocationsContext::UpdateOperationResourceUsage(TSchedulerOperationElement* element)
{
    DynamicAttributesManager_.UpdateOperationResourceUsage(element, SchedulingContext_->GetNow());
}

bool TScheduleAllocationsContext::HasAllocationsSatisfyingResourceLimits(const TSchedulerOperationElement* element) const
{
    for (const auto& allocationResources : element->DetailedMinNeededAllocationResources()) {
        if (SchedulingContext_->CanStartAllocationForOperation(allocationResources, element->GetOperationId())) {
            return true;
        }
    }
    return false;
}

TFairShareStrategyPackingConfigPtr TScheduleAllocationsContext::GetPackingConfig() const
{
    return TreeSnapshot_->TreeConfig()->Packing;
}

bool TScheduleAllocationsContext::CheckPacking(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot) const
{
    // NB: We expect DetailedMinNeededResources_ to be of size 1 most of the time.
    TJobResourcesWithQuota packingAllocationResourcesWithQuota;
    if (element->DetailedMinNeededAllocationResources().empty()) {
        // Refuse packing if no information about resource requirements is provided.
        return false;
    } else if (element->DetailedMinNeededAllocationResources().size() == 1) {
        packingAllocationResourcesWithQuota = element->DetailedMinNeededAllocationResources()[0];
    } else {
        auto idx = RandomNumber<ui32>(static_cast<ui32>(element->DetailedMinNeededAllocationResources().size()));
        packingAllocationResourcesWithQuota = element->DetailedMinNeededAllocationResources()[idx];
    }

    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->CheckPacking(
        element,
        heartbeatSnapshot,
        packingAllocationResourcesWithQuota,
        TreeSnapshot_->RootElement()->GetTotalResourceLimits(),
        GetPackingConfig());
}

void TScheduleAllocationsContext::ReactivateBadPackingOperations()
{
    for (auto* operation : BadPackingOperations_) {
        // TODO(antonkikh): multiple activations can be implemented more efficiently.
        ActivateOperation(operation);
    }
    BadPackingOperations_.clear();
}

void TScheduleAllocationsContext::RecordPackingHeartbeat(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot)
{
    TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->RecordPackingHeartbeat(heartbeatSnapshot, GetPackingConfig());
}

bool TScheduleAllocationsContext::IsAllocationKnown(const TSchedulerOperationElement* element, TAllocationId allocationId) const
{
    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->IsAllocationKnown(allocationId);
}

bool TScheduleAllocationsContext::IsOperationEnabled(const TSchedulerOperationElement* element) const
{
    // NB(eshcherbin): Operation may have been disabled since last fair share update.
    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->IsEnabled();
}

void TScheduleAllocationsContext::OnMinNeededResourcesUnsatisfied(
    const TSchedulerOperationElement* element,
    const TJobResources& availableResources,
    const TJobResources& minNeededResources) const
{
    TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->OnMinNeededResourcesUnsatisfied(
        SchedulingContext_,
        availableResources,
        minNeededResources);
}

void TScheduleAllocationsContext::UpdateOperationPreemptionStatusStatistics(
    const TSchedulerOperationElement* element,
    EOperationPreemptionStatus status) const
{
    TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->UpdatePreemptionStatusStatistics(status);
}

void TScheduleAllocationsContext::IncrementOperationScheduleAllocationAttemptCount(const TSchedulerOperationElement* element) const
{
    TreeSnapshot_
        ->SchedulingSnapshot()
        ->GetEnabledOperationSharedState(element)
        ->IncrementOperationScheduleAllocationAttemptCount(SchedulingContext_);
}

int TScheduleAllocationsContext::GetOperationRunningAllocationCount(const TSchedulerOperationElement* element) const
{
    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->GetRunningAllocationCount();
}

bool TScheduleAllocationsContext::CanSchedule(int schedulingTagFilterIndex) const
{
    return schedulingTagFilterIndex == EmptySchedulingTagFilterIndex ||
        CanSchedule_[schedulingTagFilterIndex];
}

EAllocationSchedulingStage TScheduleAllocationsContext::GetStageType() const
{
    return StageState_->Stage;
}

void TScheduleAllocationsContext::ProfileAndLogStatisticsOfStage()
{
    YT_VERIFY(StageState_);

    StageState_->TotalDuration = StageState_->Timer.GetElapsedTime();

    ProfileStageStatistics();

    if (StageState_->ScheduleAllocationAttemptCount > 0 && SchedulingInfoLoggingEnabled_) {
        LogStageStatistics();
    }
}

void TScheduleAllocationsContext::ProfileStageStatistics()
{
    if (!Initialized_) {
        return;
    }

    YT_VERIFY(StageState_);

    auto* profilingCounters = StageState_->ProfilingCounters;

    profilingCounters->PrescheduleAllocationTime.Record(StageState_->PrescheduleDuration);
    profilingCounters->CumulativePrescheduleAllocationTime.Add(StageState_->PrescheduleDuration);

    if (StageState_->PrescheduleExecuted) {
        profilingCounters->PrescheduleAllocationCount.Increment();
        if (StageState_->ScheduleAllocationAttemptCount == 0) {
            profilingCounters->UselessPrescheduleAllocationCount.Increment();
        }
    }

    auto strategyScheduleAllocationDuration = StageState_->TotalDuration
        - StageState_->PrescheduleDuration
        - StageState_->TotalScheduleAllocationDuration;
    profilingCounters->StrategyScheduleAllocationTime.Record(strategyScheduleAllocationDuration);
    profilingCounters->CumulativeStrategyScheduleAllocationTime.Add(strategyScheduleAllocationDuration);

    profilingCounters->TotalControllerScheduleAllocationTime.Record(StageState_->TotalScheduleAllocationDuration);
    profilingCounters->CumulativeTotalControllerScheduleAllocationTime.Add(StageState_->TotalScheduleAllocationDuration);
    profilingCounters->ExecControllerScheduleAllocationTime.Record(StageState_->ExecScheduleAllocationDuration);
    profilingCounters->CumulativeExecControllerScheduleAllocationTime.Add(StageState_->ExecScheduleAllocationDuration);
    profilingCounters->PackingRecordHeartbeatTime.Record(StageState_->PackingRecordHeartbeatDuration);
    profilingCounters->PackingCheckTime.Record(StageState_->PackingCheckDuration);
    profilingCounters->AnalyzeAllocationsTime.Record(StageState_->AnalyzeAllocationsDuration);
    profilingCounters->CumulativeAnalyzeAllocationsTime.Add(StageState_->AnalyzeAllocationsDuration);

    profilingCounters->ScheduleAllocationAttemptCount.Increment(StageState_->ScheduleAllocationAttemptCount);
    profilingCounters->ScheduleAllocationFailureCount.Increment(StageState_->ScheduleAllocationFailureCount);
    profilingCounters->ControllerScheduleAllocationCount.Increment(SchedulingStatistics().ControllerScheduleAllocationCount);
    profilingCounters->ControllerScheduleAllocationTimedOutCount.Increment(SchedulingStatistics().ControllerScheduleAllocationTimedOutCount);

    for (auto reason : TEnumTraits<EScheduleAllocationFailReason>::GetDomainValues()) {
        profilingCounters->ControllerScheduleAllocationFail[reason].Increment(StageState_->FailedScheduleAllocation[reason]);
    }
    for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
        profilingCounters->DeactivationCount[reason].Increment(StageState_->DeactivationReasons[reason]);
    }

    for (auto [schedulingIndex, count] : StageState_->SchedulingIndexToScheduleAllocationAttemptCount) {
        int rangeIndex = SchedulingIndexToProfilingRangeIndex(schedulingIndex);
        profilingCounters->SchedulingIndexCounters[rangeIndex].Increment(count);
    }
    if (StageState_->MaxSchedulingIndex >= 0) {
        profilingCounters->MaxSchedulingIndexCounters[SchedulingIndexToProfilingRangeIndex(StageState_->MaxSchedulingIndex)].Increment();
    }

    profilingCounters->ActiveTreeSize.Record(StageState_->ActiveTreeSize);
    profilingCounters->ActiveOperationCount.Record(StageState_->ActiveOperationCount);
}

void TScheduleAllocationsContext::LogStageStatistics()
{
    if (!Initialized_) {
        return;
    }

    YT_VERIFY(StageState_);

    YT_LOG_DEBUG(
        "Scheduling statistics (SchedulingStage: %v, StageAttemptIndex: %v, ActiveTreeSize: %v, ActiveOperationCount: %v, TotalHeapElementCount: %v, "
        "DeactivationReasons: %v, CanStartMoreAllocations: %v, Address: %v, SchedulingSegment: %v, MaxSchedulingIndex: %v)",
        StageState_->Stage,
        StageState_->StageAttemptIndex,
        StageState_->ActiveTreeSize,
        StageState_->ActiveOperationCount,
        StageState_->TotalHeapElementCount,
        StageState_->DeactivationReasons,
        SchedulingContext_->CanStartMoreAllocations(),
        SchedulingContext_->GetNodeDescriptor()->Address,
        NodeSchedulingSegment_,
        StageState_->MaxSchedulingIndex);
}

EAllocationPreemptionLevel TScheduleAllocationsContext::GetAllocationPreemptionLevel(
    const TAllocationWithPreemptionInfo& allocationWithPreemptionInfo) const
{
    const auto& [allocation, preemptionStatus, operationElement] = allocationWithPreemptionInfo;
    bool isEligibleForSsdPriorityPreemption = SsdPriorityPreemptionEnabled_ &&
        IsEligibleForSsdPriorityPreemption(GetDiskQuotaMedia(allocation->DiskQuota()));
    auto aggressivePreemptionAllowed = StaticAttributesOf(operationElement).EffectiveAggressivePreemptionAllowed;

    switch (preemptionStatus) {
        case EAllocationPreemptionStatus::NonPreemptible:
            return isEligibleForSsdPriorityPreemption
                ? EAllocationPreemptionLevel::SsdNonPreemptible
                : EAllocationPreemptionLevel::NonPreemptible;
        case EAllocationPreemptionStatus::AggressivelyPreemptible:
            if (aggressivePreemptionAllowed) {
                return isEligibleForSsdPriorityPreemption
                    ? EAllocationPreemptionLevel::SsdAggressivelyPreemptible
                    : EAllocationPreemptionLevel::AggressivelyPreemptible;
            } else {
                return isEligibleForSsdPriorityPreemption
                    ? EAllocationPreemptionLevel::SsdNonPreemptible
                    : EAllocationPreemptionLevel::NonPreemptible;
            }
        case EAllocationPreemptionStatus::Preemptible:
            return EAllocationPreemptionLevel::Preemptible;
        default:
            YT_ABORT();
    }
}

bool TScheduleAllocationsContext::IsEligibleForSsdPriorityPreemption(const THashSet<int>& diskRequestMedia) const
{
    return NScheduler::IsEligibleForSsdPriorityPreemption(diskRequestMedia, SsdPriorityPreemptionMedia_);
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeAllocationScheduler::TFairShareTreeAllocationScheduler(
    TString treeId,
    NLogging::TLogger logger,
    TWeakPtr<IFairShareTreeAllocationSchedulerHost> host,
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
    , CumulativeScheduleAllocationsTime_(Profiler_.TimeCounter("/cumulative_schedule_jobs_time"))
    , ScheduleAllocationsTime_(Profiler_.Timer("/schedule_jobs_time"))
    , GracefulPreemptionTime_(Profiler_.Timer("/graceful_preemption_time"))
    , ScheduleAllocationsDeadlineReachedCounter_(Profiler_.Counter("/schedule_jobs_deadline_reached"))
    , OperationCountByPreemptionPriorityBufferedProducer_(New<TBufferedProducer>())
    , SchedulingSegmentManager_(TreeId_, Config_->SchedulingSegments, Logger, Profiler_)
{
    InitSchedulingProfilingCounters();

    Profiler_.AddProducer("/operation_count_by_preemption_priority", OperationCountByPreemptionPriorityBufferedProducer_);

    SchedulingSegmentsManagementExecutor_ = New<TPeriodicExecutor>(
        StrategyHost_->GetControlInvoker(EControlQueue::FairShareStrategy),
        BIND(&TFairShareTreeAllocationScheduler::ManageSchedulingSegments, MakeWeak(this)),
        Config_->SchedulingSegments->ManagePeriod);
    SchedulingSegmentsManagementExecutor_->Start();
}

void TFairShareTreeAllocationScheduler::RegisterNode(TNodeId nodeId)
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
            TFairShareTreeAllocationSchedulerNodeState{
                .SchedulingSegment = initialSchedulingSegment,
            });
    }));
}

void TFairShareTreeAllocationScheduler::UnregisterNode(TNodeId nodeId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto nodeShardId = StrategyHost_->GetNodeShardId(nodeId);
    const auto& nodeShardInvoker = StrategyHost_->GetNodeShardInvokers()[nodeShardId];
    nodeShardInvoker->Invoke(BIND([this, this_ = MakeStrong(this), nodeId, nodeShardId] {
        EraseOrCrash(NodeStateShards_[nodeShardId].NodeIdToState, nodeId);
    }));
}

void TFairShareTreeAllocationScheduler::ProcessSchedulingHeartbeat(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    bool skipScheduleAllocations)
{
    auto nodeId = schedulingContext->GetNodeDescriptor()->Id;
    auto* nodeState = FindNodeState(nodeId);
    if (!nodeState) {
        YT_LOG_DEBUG("Skipping scheduling heartbeat because node is not registered in tree (NodeId: %v, NodeAddress: %v)",
            nodeId,
            schedulingContext->GetNodeDescriptor()->Address);

        return;
    }

    const auto& treeConfig = treeSnapshot->TreeConfig();
    bool shouldUpdateRunningAllocationStatistics = nodeState->ForceRunningAllocationStatisticsUpdate ||
        !nodeState->LastRunningAllocationStatisticsUpdateTime ||
        schedulingContext->GetNow() > *nodeState->LastRunningAllocationStatisticsUpdateTime + DurationToCpuDuration(treeConfig->RunningAllocationStatisticsUpdatePeriod);
    if (shouldUpdateRunningAllocationStatistics) {
        nodeState->RunningAllocationStatistics = ComputeRunningAllocationStatistics(nodeState, schedulingContext, treeSnapshot);
        nodeState->LastRunningAllocationStatisticsUpdateTime = schedulingContext->GetNow();
        nodeState->ForceRunningAllocationStatisticsUpdate = false;
    }

    bool hasUserSlotsBefore = !nodeState->Descriptor || nodeState->Descriptor->ResourceLimits.GetUserSlots() > 0;
    bool hasUserSlotsAfter = schedulingContext->GetNodeDescriptor()->ResourceLimits.GetUserSlots() > 0;
    nodeState->Descriptor = schedulingContext->GetNodeDescriptor();

    YT_LOG_INFO_IF(hasUserSlotsBefore != hasUserSlotsAfter,
        "Node user slots were %v (NodeId: %v, NodeAddress: %v)",
        hasUserSlotsAfter
            ? "enabled"
            : "disabled",
        nodeState->Descriptor->Id,
        nodeState->Descriptor->Address);

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

    PreemptAllocationsGracefully(schedulingContext, treeSnapshot);

    if (!skipScheduleAllocations) {
        bool enableSchedulingInfoLogging = false;
        auto now = schedulingContext->GetNow();
        const auto& config = treeSnapshot->TreeConfig();
        if (LastSchedulingInformationLoggedTime_ + DurationToCpuDuration(config->HeartbeatTreeSchedulingInfoLogBackoff) < now) {
            enableSchedulingInfoLogging = true;
            LastSchedulingInformationLoggedTime_ = now;
        }


        auto context = NewWithOffloadedDtor<TScheduleAllocationsContext>(
            StrategyHost_->GetBackgroundInvoker(),
            schedulingContext,
            treeSnapshot,
            nodeState,
            enableSchedulingInfoLogging,
            StrategyHost_,
            ScheduleAllocationsDeadlineReachedCounter_,
            Logger);
        ScheduleAllocations(context.Get());
    }
}

void TFairShareTreeAllocationScheduler::ScheduleAllocations(TScheduleAllocationsContext* context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    NProfiling::TWallTimer scheduleAllocationsTimer;

    DoRegularAllocationScheduling(context);
    DoPreemptiveAllocationScheduling(context);

    // Preempt some allocations if usage is greater that limit.
    if (context->SchedulingContext()->ShouldAbortAllocationsSinceResourcesOvercommit()) {
        context->AbortAllocationsSinceResourcesOvercommit();
    }

    context->SchedulingContext()->SetSchedulingStatistics(context->SchedulingStatistics());

    auto elapsedTime = scheduleAllocationsTimer.GetElapsedTime();
    CumulativeScheduleAllocationsTime_.Add(elapsedTime);
    ScheduleAllocationsTime_.Record(elapsedTime);
}

void TFairShareTreeAllocationScheduler::PreemptAllocationsGracefully(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    NProfiling::TEventTimerGuard eventTimerGuard(GracefulPreemptionTime_);

    const auto& treeConfig = treeSnapshot->TreeConfig();

    YT_LOG_TRACE("Looking for gracefully preemptible allocations");

    std::vector<TAllocationPtr> candidates;
    for (const auto& allocation : schedulingContext->RunningAllocations()) {
        if (allocation->GetPreemptionMode() == EPreemptionMode::Graceful && !allocation->GetPreempted()) {
            candidates.push_back(allocation);
        }
    }

    auto allocationInfos = GetAllocationPreemptionInfos(candidates, treeSnapshot);
    for (const auto& [allocation, preemptionStatus, _] : allocationInfos) {
        if (preemptionStatus == EAllocationPreemptionStatus::Preemptible) {
            schedulingContext->PreemptAllocation(
                allocation,
                treeConfig->AllocationGracefulPreemptionTimeout,
                EAllocationPreemptionReason::GracefulPreemption);
        }
    }
}

void TFairShareTreeAllocationScheduler::RegisterOperation(const TSchedulerOperationElement* element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto operationId = element->GetOperationId();
    auto operationState = New<TFairShareTreeAllocationSchedulerOperationState>(
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
        New<TFairShareTreeAllocationSchedulerOperationSharedState>(
            StrategyHost_,
            element->Spec()->UpdatePreemptibleAllocationsListLoggingPeriod,
            Logger.WithTag("OperationId: %v", operationId)));
}

void TFairShareTreeAllocationScheduler::UnregisterOperation(const TSchedulerOperationElement* element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EraseOrCrash(OperationIdToState_, element->GetOperationId());
    EraseOrCrash(OperationIdToSharedState_, element->GetOperationId());
}

void TFairShareTreeAllocationScheduler::OnOperationMaterialized(const TSchedulerOperationElement* element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto operationId = element->GetOperationId();
    const auto& operationState = GetOperationState(operationId);
    operationState->AggregatedInitialMinNeededResources = element->GetAggregatedInitialMinNeededResources();

    SchedulingSegmentManager_.InitOrUpdateOperationSchedulingSegment(operationId, operationState);
}

TError TFairShareTreeAllocationScheduler::CheckOperationSchedulingInSeveralTreesAllowed(const TSchedulerOperationElement* element) const
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

void TFairShareTreeAllocationScheduler::EnableOperation(const TSchedulerOperationElement* element) const
{
    auto operationId = element->GetOperationId();
    GetOperationSharedState(operationId)->Enable();
}

void TFairShareTreeAllocationScheduler::DisableOperation(TSchedulerOperationElement* element, bool markAsNonAlive) const
{
    auto operationId = element->GetOperationId();
    GetOperationSharedState(operationId)->Disable();
    element->ReleaseResources(markAsNonAlive);
}

void TFairShareTreeAllocationScheduler::RegisterAllocationsFromRevivedOperation(
    TSchedulerOperationElement* element,
    std::vector<TAllocationPtr> allocations) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SortBy(allocations, [] (const TAllocationPtr& allocation) {
        return allocation->GetStartTime();
    });

    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    for (const auto& allocation : allocations) {
        TJobResourcesWithQuota resourceUsageWithQuota = allocation->ResourceUsage();
        resourceUsageWithQuota.DiskQuota() = allocation->DiskQuota();
        operationSharedState->OnAllocationStarted(
            element,
            allocation->GetId(),
            resourceUsageWithQuota,
            /*precommittedResources*/ {},
            // NB: |scheduleAllocationEpoch| is ignored in case |force| is true.
            /*scheduleAllocationEpoch*/ TControllerEpoch(0),
            /*force*/ true);
    }
}

void TFairShareTreeAllocationScheduler::ProcessUpdatedAllocation(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TSchedulerOperationElement* element,
    TAllocationId allocationId,
    const TJobResources& allocationResources,
    const std::optional<TString>& allocationDataCenter,
    const std::optional<TString>& allocationInfinibandCluster,
    std::optional<EAbortReason>* maybeAbortReason) const
{
    const auto& operationState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationState(element);
    const auto& operationSharedState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element);

    auto delta = operationSharedState->SetAllocationResourceUsage(allocationId, allocationResources);
    element->IncreaseHierarchicalResourceUsage(delta);
    operationSharedState->UpdatePreemptibleAllocationsList(element);

    const auto& operationSchedulingSegment = operationState->SchedulingSegment;
    if (operationSchedulingSegment && IsModuleAwareSchedulingSegment(*operationSchedulingSegment)) {
        const auto& operationModule = operationState->SchedulingSegmentModule;
        const auto& allocationModule = TSchedulingSegmentManager::GetNodeModule(
            allocationDataCenter,
            allocationInfinibandCluster,
            element->TreeConfig()->SchedulingSegments->ModuleType);
        bool allocationIsRunningInTheRightModule = operationModule && (operationModule == allocationModule);
        if (!allocationIsRunningInTheRightModule) {
            *maybeAbortReason = EAbortReason::WrongSchedulingSegmentModule;

            YT_LOG_DEBUG(
                "Requested to abort allocation because it is running in a wrong module "
                "(OperationId: %v, AllocationId: %v, OperationModule: %v, AllocationModule: %v)",
                element->GetOperationId(),
                allocationId,
                operationModule,
                allocationModule);
        }
    }
}

void TFairShareTreeAllocationScheduler::ProcessFinishedAllocation(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TSchedulerOperationElement* element,
    TAllocationId allocationId) const
{
    const auto& operationSharedState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    operationSharedState->OnAllocationFinished(element, allocationId);
}

void TFairShareTreeAllocationScheduler::BuildSchedulingAttributesStringForNode(TNodeId nodeId, TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    const auto* nodeState = FindNodeState(nodeId);
    if (!nodeState) {
        return;
    }

    delimitedBuilder->AppendFormat(
        "SchedulingSegment: %v, RunningAllocationStatistics: %v",
        nodeState->SchedulingSegment,
        nodeState->RunningAllocationStatistics);
}

void TFairShareTreeAllocationScheduler::BuildSchedulingAttributesForNode(TNodeId nodeId, TFluentMap fluent) const
{
    const auto* nodeState = FindNodeState(nodeId);
    if (!nodeState) {
        return;
    }

    fluent
        .Item("scheduling_segment").Value(nodeState->SchedulingSegment)
        .Item("running_job_statistics").Value(nodeState->RunningAllocationStatistics);
}

void TFairShareTreeAllocationScheduler::BuildSchedulingAttributesStringForOngoingAllocations(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const std::vector<TAllocationPtr>& allocations,
    TInstant now,
    TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    const auto& cachedAllocationPreemptionStatuses = treeSnapshot
        ? treeSnapshot->SchedulingSnapshot()->CachedAllocationPreemptionStatuses()
        : TCachedAllocationPreemptionStatuses{.UpdateTime = now};

    TEnumIndexedArray<EAllocationPreemptionStatus, std::vector<TAllocationId>> allocationIdsByPreemptionStatus;
    std::vector<TAllocationId> unknownStatusAllocationIds;
    for (const auto& allocation : allocations) {
        if (auto status = GetCachedAllocationPreemptionStatus(allocation, cachedAllocationPreemptionStatuses)) {
            allocationIdsByPreemptionStatus[*status].push_back(allocation->GetId());
        } else {
            unknownStatusAllocationIds.push_back(allocation->GetId());
        }
    }

    delimitedBuilder->AppendFormat(
        "AllocationIdsByPreemptionStatus: %v, UnknownStatusAllocationIds: %v, TimeSinceLastPreemptionStatusUpdateSeconds: %v",
        allocationIdsByPreemptionStatus,
        unknownStatusAllocationIds,
        (now - cachedAllocationPreemptionStatuses.UpdateTime).SecondsFloat());
}

TError TFairShareTreeAllocationScheduler::CheckOperationIsHung(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TSchedulerOperationElement* element,
    TInstant now,
    TInstant activationTime,
    TDuration safeTimeout,
    int minScheduleAllocationCallAttempts,
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

        auto lastScheduleAllocationSuccessTime = operationSharedState->GetLastScheduleAllocationSuccessTime();
        if (activationTime + safeTimeout < now &&
            lastScheduleAllocationSuccessTime + safeTimeout < now &&
            element->GetLastNonStarvingTime() + safeTimeout < now &&
            operationSharedState->GetRunningAllocationCount() == 0 &&
            deactivationCount > minScheduleAllocationCallAttempts)
        {
            return TError("Operation has no successful scheduled allocations for a long period")
                << TErrorAttribute("period", safeTimeout)
                << TErrorAttribute("deactivation_count", deactivationCount)
                << TErrorAttribute("last_schedule_allocation_success_time", lastScheduleAllocationSuccessTime)
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

void TFairShareTreeAllocationScheduler::BuildOperationProgress(
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
        .Item("preemptible_job_count").Value(operationSharedState->GetPreemptibleAllocationCount())
        .Item("aggressively_preemptible_job_count").Value(operationSharedState->GetAggressivelyPreemptibleAllocationCount())
        .Item("scheduling_index").Value(attributes.SchedulingIndex)
        .Item("scheduling_priority").Value(attributes.SchedulingPriority)
        .Item("deactivation_reasons").Value(operationSharedState->GetDeactivationReasons())
        .Item("min_needed_resources_unsatisfied_count").Value(operationSharedState->GetMinNeededResourcesUnsatisfiedCount())
        .Item("disk_quota_usage").BeginMap()
            .Do([&] (TFluentMap fluent) {
                strategyHost->SerializeDiskQuota(operationSharedState->GetTotalDiskQuota(), fluent.GetConsumer());
            })
        .EndMap()
        .Item("are_regular_jobs_on_ssd_nodes_allowed").Value(attributes.AreRegularAllocationsOnSsdNodesAllowed)
        .Item("scheduling_segment").Value(operationState->SchedulingSegment)
        .Item("scheduling_segment_module").Value(operationState->SchedulingSegmentModule);
}

void TFairShareTreeAllocationScheduler::BuildElementYson(
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

TAllocationSchedulerPostUpdateContext TFairShareTreeAllocationScheduler::CreatePostUpdateContext(TSchedulerRootElement* rootElement)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetControlInvoker(EControlQueue::FairShareStrategy));

    // NB(eshcherbin): We cannot update SSD media in the constructor, because initial pool trees update
    // in the registration pipeline is done before medium directory sync. That's why we do the initial update
    // during the first fair share update.
    if (!SsdPriorityPreemptionMedia_) {
        UpdateSsdPriorityPreemptionMedia();
    }

    return TAllocationSchedulerPostUpdateContext{
        .RootElement = rootElement,
        .SsdPriorityPreemptionMedia = SsdPriorityPreemptionMedia_.value_or(THashSet<int>()),
        .OperationIdToState = GetOperationStateMapSnapshot(),
        .OperationIdToSharedState = OperationIdToSharedState_,
    };
}

void TFairShareTreeAllocationScheduler::PostUpdate(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TAllocationSchedulerPostUpdateContext* postUpdateContext)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetFairShareUpdateInvoker());

    InitializeStaticAttributes(fairSharePostUpdateContext, postUpdateContext);

    PublishFairShareAndUpdatePreemptionAttributes(postUpdateContext->RootElement, postUpdateContext);
    UpdateEffectiveRecursiveAttributes(postUpdateContext->RootElement, postUpdateContext);

    ProcessUpdatedStarvationStatuses(fairSharePostUpdateContext, postUpdateContext);

    auto cachedAllocationPreemptionStatusesUpdateDeadline =
        CachedAllocationPreemptionStatuses_.UpdateTime + fairSharePostUpdateContext->TreeConfig->CachedAllocationPreemptionStatusesUpdatePeriod;
    if (fairSharePostUpdateContext->Now > cachedAllocationPreemptionStatusesUpdateDeadline) {
        UpdateCachedAllocationPreemptionStatuses(fairSharePostUpdateContext, postUpdateContext);
    }

    CollectSchedulableOperationsPerPriority(fairSharePostUpdateContext, postUpdateContext);

    ComputeOperationSchedulingIndexes(fairSharePostUpdateContext, postUpdateContext);

    CollectKnownSchedulingTagFilters(fairSharePostUpdateContext, postUpdateContext);

    UpdateSsdNodeSchedulingAttributes(fairSharePostUpdateContext, postUpdateContext);

    CountOperationsByPreemptionPriority(fairSharePostUpdateContext, postUpdateContext);
}

TFairShareTreeSchedulingSnapshotPtr TFairShareTreeAllocationScheduler::CreateSchedulingSnapshot(TAllocationSchedulerPostUpdateContext* postUpdateContext)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetControlInvoker(EControlQueue::FairShareStrategy));

    return New<TFairShareTreeSchedulingSnapshot>(
        std::move(postUpdateContext->StaticAttributesList),
        std::move(postUpdateContext->SchedulableOperationsPerPriority),
        std::move(postUpdateContext->SsdPriorityPreemptionMedia),
        CachedAllocationPreemptionStatuses_,
        std::move(postUpdateContext->KnownSchedulingTagFilters),
        std::move(postUpdateContext->OperationCountsByPreemptionPriorityParameters),
        std::move(postUpdateContext->OperationIdToState),
        std::move(postUpdateContext->OperationIdToSharedState));
}

void TFairShareTreeAllocationScheduler::OnResourceUsageSnapshotUpdate(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const
{
    UpdateDynamicAttributesListSnapshot(treeSnapshot, resourceUsageSnapshot);
}

void TFairShareTreeAllocationScheduler::ProfileOperation(
    const TSchedulerOperationElement* element,
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    ISensorWriter* writer) const
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetFairShareProfilingInvoker());

    const auto& attributes = treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element);
    writer->AddGauge("/scheduling_index", attributes.SchedulingIndex);

    const auto& operationSharedState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    writer->AddCounter("/schedule_job_attempt_count", operationSharedState->GetOperationScheduleAllocationAttemptCount());
}

void TFairShareTreeAllocationScheduler::UpdateConfig(TFairShareStrategyTreeConfigPtr config)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto oldConfig = std::move(Config_);
    Config_ = std::move(config);

    SchedulingSegmentsManagementExecutor_->SetPeriod(Config_->SchedulingSegments->ManagePeriod);
    SchedulingSegmentManager_.UpdateConfig(Config_->SchedulingSegments);
    if (oldConfig->SchedulingSegments->Mode != Config_->SchedulingSegments->Mode) {
        for (const auto& [operationId, operationState] : OperationIdToState_) {
            SchedulingSegmentManager_.InitOrUpdateOperationSchedulingSegment(operationId, operationState);
        }
    }

    UpdateSsdPriorityPreemptionMedia();
}

void TFairShareTreeAllocationScheduler::BuildElementLoggingStringAttributes(
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
            "PreemptibleRunningAllocations: %v, AggressivelyPreemptibleRunningAllocations: %v, PreemptionStatusStatistics: %v, "
            "SchedulingIndex: %v, SchedulingPriority: %v, DeactivationReasons: %v, MinNeededResourcesUnsatisfiedCount: %v, "
            "SchedulingSegment: %v, SchedulingSegmentModule: %v",
            operationSharedState->GetPreemptibleAllocationCount(),
            operationSharedState->GetAggressivelyPreemptibleAllocationCount(),
            operationSharedState->GetPreemptionStatusStatistics(),
            attributes.SchedulingIndex,
            attributes.SchedulingPriority,
            operationSharedState->GetDeactivationReasons(),
            operationSharedState->GetMinNeededResourcesUnsatisfiedCount(),
            operationState->SchedulingSegment,
            operationState->SchedulingSegmentModule);
    }
}

void TFairShareTreeAllocationScheduler::InitPersistentState(INodePtr persistentState)
{
    if (persistentState) {
        try {
            InitialPersistentState_ = ConvertTo<TPersistentFairShareTreeAllocationSchedulerStatePtr>(persistentState);
        } catch (const std::exception& ex) {
            InitialPersistentState_ = New<TPersistentFairShareTreeAllocationSchedulerState>();

            // TODO(eshcherbin): Should we set scheduler alert instead? It'll be more visible this way,
            // but it'll have to be removed manually
            YT_LOG_WARNING(ex, "Failed to deserialize strategy state; will ignore it");
        }
    } else {
        InitialPersistentState_ = New<TPersistentFairShareTreeAllocationSchedulerState>();
    }

    InitialPersistentSchedulingSegmentNodeStates_ = InitialPersistentState_->SchedulingSegmentsState->NodeStates;
    InitialPersistentSchedulingSegmentOperationStates_ = InitialPersistentState_->SchedulingSegmentsState->OperationStates;

    auto now = TInstant::Now();
    SchedulingSegmentsInitializationDeadline_ = now + Config_->SchedulingSegments->InitializationTimeout;
    SchedulingSegmentManager_.SetInitializationDeadline(SchedulingSegmentsInitializationDeadline_);

    YT_LOG_DEBUG(
        "Initialized tree allocation scheduler persistent state (SchedulingSegmentsInitializationDeadline: %v)",
        SchedulingSegmentsInitializationDeadline_);
}

INodePtr TFairShareTreeAllocationScheduler::BuildPersistentState() const
{
    auto persistentState = PersistentState_
        ? PersistentState_
        : InitialPersistentState_;
    return ConvertToNode(persistentState);
}

void TFairShareTreeAllocationScheduler::OnAllocationStartedInTest(
    TSchedulerOperationElement* element,
    TAllocationId allocationId,
    const TJobResourcesWithQuota& resourceUsage)
{
    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    operationSharedState->OnAllocationStarted(
        element,
        allocationId,
        resourceUsage,
        /*precommitedResources*/ {},
        /*scheduleAllocationEpoch*/ TControllerEpoch(0));
}

void TFairShareTreeAllocationScheduler::ProcessUpdatedAllocationInTest(
    TSchedulerOperationElement* element,
    TAllocationId allocationId,
    const TJobResources& allocationResources)
{
    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    auto delta = operationSharedState->SetAllocationResourceUsage(allocationId, allocationResources);
    element->IncreaseHierarchicalResourceUsage(delta);
    operationSharedState->UpdatePreemptibleAllocationsList(element);
}

EAllocationPreemptionStatus TFairShareTreeAllocationScheduler::GetAllocationPreemptionStatusInTest(
    const TSchedulerOperationElement* element,
    TAllocationId allocationId) const
{
    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    return operationSharedState->GetAllocationPreemptionStatus(allocationId);
}

void TFairShareTreeAllocationScheduler::InitSchedulingProfilingCounters()
{
    for (auto stage : TEnumTraits<EAllocationSchedulingStage>::GetDomainValues()) {
        SchedulingStageProfilingCounters_[stage] = std::make_unique<TSchedulingStageProfilingCounters>(
            Profiler_.WithTag("scheduling_stage", FormatEnum(stage)));
    }
}

TRunningAllocationStatistics TFairShareTreeAllocationScheduler::ComputeRunningAllocationStatistics(
    const TFairShareTreeAllocationSchedulerNodeState* nodeState,
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    const auto& cachedAllocationPreemptionStatuses = treeSnapshot->SchedulingSnapshot()->CachedAllocationPreemptionStatuses();
    auto now = CpuInstantToInstant(schedulingContext->GetNow());

    TRunningAllocationStatistics runningAllocationStatistics;
    for (const auto& allocation : schedulingContext->RunningAllocations()) {
        // Technically it's an overestimation of the allocation's duration, however, we feel it's more fair this way.
        auto duration = (now - allocation->GetStartTime()).SecondsFloat();
        auto allocationCpuTime = static_cast<double>(allocation->ResourceLimits().GetCpu()) * duration;
        auto allocationGpuTime = allocation->ResourceLimits().GetGpu() * duration;

        runningAllocationStatistics.TotalCpuTime += allocationCpuTime;
        runningAllocationStatistics.TotalGpuTime += allocationGpuTime;

        bool preemptible = [&] {
            const auto* operation = treeSnapshot->FindEnabledOperationElement(allocation->GetOperationId());
            if (!operation) {
                return true;
            }

            const auto& operationState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationState(operation);
            if (operationState->SchedulingSegment != nodeState->SchedulingSegment) {
                return true;
            }

            // TODO(eshcherbin): Do we really still need to use cached preemption statuses?
            // Now that this code has been moved to allocation scheduler, we can use operation shared state directly.
            return GetCachedAllocationPreemptionStatus(allocation, cachedAllocationPreemptionStatuses) == EAllocationPreemptionStatus::Preemptible;
        }();

        if (preemptible) {
            runningAllocationStatistics.PreemptibleCpuTime += allocationCpuTime;
            runningAllocationStatistics.PreemptibleGpuTime += allocationGpuTime;
        }
    }

    return runningAllocationStatistics;
}

void TFairShareTreeAllocationScheduler::DoRegularAllocationScheduling(TScheduleAllocationsContext* context)
{
    const auto& treeConfig = context->TreeSnapshot()->TreeConfig();

    auto runRegularSchedulingStage = [&] (
        EAllocationSchedulingStage stageType,
        const TRegularSchedulingParameters& parameters = {},
        int stageAttemptIndex = 0)
    {
        context->StartStage(stageType, SchedulingStageProfilingCounters_[stageType].get(), stageAttemptIndex);
        RunRegularSchedulingStage(parameters, context);
        context->FinishStage();
    };

    auto runRegularSchedulingStageWithBatching = [&] (
        EAllocationSchedulingStage stageType,
        const TNonOwningOperationElementList& operations)
    {
        const auto& batchConfig = treeConfig->BatchOperationScheduling;
        const int batchSize = batchConfig->BatchSize;
        auto batchStart = operations.begin();
        auto getNextOperationBatch = [&] {
            int currentBatchSize = std::min(batchSize, static_cast<int>(std::distance(batchStart, operations.end())));
            auto batchEnd = std::next(batchStart, currentBatchSize);
            TNonOwningOperationElementList batch(batchStart, batchEnd);

            batchStart = batchEnd;

            return batch;
        };

        int stageAttemptIndex = 0;
        int previouslyScheduledAllocationCount = std::ssize(context->SchedulingContext()->StartedAllocations());
        std::optional<TJobResources> customMinSpareAllocationResources;
        while (batchStart != operations.end()) {
            runRegularSchedulingStage(
                stageType,
                TRegularSchedulingParameters{
                    .ConsideredOperations = getNextOperationBatch(),
                    .CustomMinSpareAllocationResources = customMinSpareAllocationResources,
                },
                stageAttemptIndex);

            ++stageAttemptIndex;

            bool hasScheduledAllocations = previouslyScheduledAllocationCount < std::ssize(context->SchedulingContext()->StartedAllocations());
            if (hasScheduledAllocations && !customMinSpareAllocationResources) {
                customMinSpareAllocationResources = ToJobResources(batchConfig->FallbackMinSpareAllocationResources, TJobResources{});
            }
        }
    };

    const auto& schedulableOperationsPerPriority = context->TreeSnapshot()->SchedulingSnapshot()->SchedulableOperationsPerPriority();
    if (treeConfig->EnableGuaranteePriorityScheduling) {
        for (auto priority : GetDescendingSchedulingPriorities()) {
            const auto& operations = schedulableOperationsPerPriority[priority];
            auto stage = GetRegularSchedulingStageByPriority(priority);
            if (treeConfig->BatchOperationScheduling) {
                runRegularSchedulingStageWithBatching(stage, operations);
            } else {
                runRegularSchedulingStage(
                    stage,
                    TRegularSchedulingParameters{
                        .ConsideredOperations = operations,
                    });
            }
        }
    } else if (treeConfig->BatchOperationScheduling) {
        runRegularSchedulingStageWithBatching(
            EAllocationSchedulingStage::RegularMediumPriority,
            schedulableOperationsPerPriority[EOperationSchedulingPriority::Medium]);
    } else {
        runRegularSchedulingStage(EAllocationSchedulingStage::RegularMediumPriority);
    }

    auto badPackingOperations = context->ExtractBadPackingOperations();
    bool needPackingFallback = context->SchedulingContext()->StartedAllocations().empty() && !badPackingOperations.empty();
    if (needPackingFallback) {
        runRegularSchedulingStage(
            EAllocationSchedulingStage::RegularPackingFallback,
            TRegularSchedulingParameters{
                .ConsideredOperations = badPackingOperations,
                .IgnorePacking = true,
                .OneAllocationOnly = true,
            });
    }
}

void TFairShareTreeAllocationScheduler::DoPreemptiveAllocationScheduling(TScheduleAllocationsContext* context)
{
    bool scheduleAllocationsWithPreemption = [&] {
        auto nodeId = context->SchedulingContext()->GetNodeDescriptor()->Id;
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

    context->SchedulingStatistics().ScheduleWithPreemption = scheduleAllocationsWithPreemption;
    if (!scheduleAllocationsWithPreemption) {
        YT_LOG_DEBUG("Skip preemptive scheduling");
        return;
    }

    for (const auto& [stage, parameters] : BuildPreemptiveSchedulingStageList(context)) {
        // We allow to schedule at most one allocation using preemption.
        if (context->SchedulingStatistics().ScheduledDuringPreemption > 0) {
            break;
        }

        context->StartStage(stage, SchedulingStageProfilingCounters_[stage].get());
        RunPreemptiveSchedulingStage(parameters, context);
        context->FinishStage();
    }
}

TPreemptiveStageWithParametersList TFairShareTreeAllocationScheduler::BuildPreemptiveSchedulingStageList(TScheduleAllocationsContext* context)
{
    TPreemptiveStageWithParametersList stages;

    if (context->GetSsdPriorityPreemptionEnabled()) {
        stages.emplace_back(
            EAllocationSchedulingStage::PreemptiveSsdAggressive,
            TPreemptiveSchedulingParameters{
                .TargetOperationPreemptionPriority = EOperationPreemptionPriority::SsdAggressive,
                .MinAllocationPreemptionLevel = EAllocationPreemptionLevel::SsdAggressivelyPreemptible,
            });
        stages.emplace_back(
            EAllocationSchedulingStage::PreemptiveSsdNormal,
            TPreemptiveSchedulingParameters{
                .TargetOperationPreemptionPriority = EOperationPreemptionPriority::SsdNormal,
                .MinAllocationPreemptionLevel = EAllocationPreemptionLevel::NonPreemptible,
            });
    }

    stages.emplace_back(
        EAllocationSchedulingStage::PreemptiveAggressive,
        TPreemptiveSchedulingParameters{
            .TargetOperationPreemptionPriority = EOperationPreemptionPriority::Aggressive,
            .MinAllocationPreemptionLevel = EAllocationPreemptionLevel::AggressivelyPreemptible,
        });
    stages.emplace_back(
        EAllocationSchedulingStage::PreemptiveNormal,
        TPreemptiveSchedulingParameters{
            .TargetOperationPreemptionPriority = EOperationPreemptionPriority::Normal,
            .MinAllocationPreemptionLevel = EAllocationPreemptionLevel::Preemptible,
            .ForcePreemptionAttempt = true,
        });

    return stages;
}

void TFairShareTreeAllocationScheduler::RunRegularSchedulingStage(
    const TRegularSchedulingParameters& parameters,
    TScheduleAllocationsContext* context)
{
    while (context->ShouldContinueScheduling(parameters.CustomMinSpareAllocationResources)) {
        if (!context->GetStagePrescheduleExecuted()) {
            context->PrepareForScheduling();
            context->PrescheduleAllocation(parameters.ConsideredOperations);
        }

        auto scheduleAllocationResult = context->ScheduleAllocation(parameters.IgnorePacking);
        if (scheduleAllocationResult.Finished || (parameters.OneAllocationOnly && scheduleAllocationResult.Scheduled)) {
            break;
        }
    }

    context->SchedulingStatistics().MaxNonPreemptiveSchedulingIndex = context->GetStageMaxSchedulingIndex();
}

void TFairShareTreeAllocationScheduler::RunPreemptiveSchedulingStage(
    const TPreemptiveSchedulingParameters& parameters,
    TScheduleAllocationsContext* context)
{
    YT_VERIFY(parameters.TargetOperationPreemptionPriority != EOperationPreemptionPriority::None);

    // NB(eshcherbin): We might want to analyze allocations and attempt preemption even if there are no candidate operations of target priority.
    // For example, we preempt allocations in pools or operations which exceed their specified resource limits.
    auto operationWithPreemptionPriorityCount = context->GetOperationWithPreemptionPriorityCount(
        parameters.TargetOperationPreemptionPriority);
    bool shouldAttemptScheduling = operationWithPreemptionPriorityCount > 0;
    bool shouldAttemptPreemption = parameters.ForcePreemptionAttempt || shouldAttemptScheduling;
    if (!shouldAttemptPreemption) {
        return;
    }

    // NB: This method achieves 2 goals relevant for scheduling with preemption:
    // 1. Reset |Active| attribute after scheduling without preemption (this is necessary for PrescheduleAllocation correctness).
    // 2. Initialize dynamic attributes and calculate local resource usages if scheduling without preemption was skipped.
    context->PrepareForScheduling();

    std::vector<TAllocationWithPreemptionInfo> unconditionallyPreemptibleAllocations;
    TNonOwningAllocationSet forcefullyPreemptibleAllocations;
    context->AnalyzePreemptibleAllocations(
        parameters.TargetOperationPreemptionPriority,
        parameters.MinAllocationPreemptionLevel,
        &unconditionallyPreemptibleAllocations,
        &forcefullyPreemptibleAllocations);

    int startedBeforePreemption = context->SchedulingContext()->StartedAllocations().size();

    // NB: Schedule at most one allocation with preemption.
    TAllocationPtr allocationStartedUsingPreemption;
    if (shouldAttemptScheduling) {
        YT_LOG_TRACE(
            "Scheduling new allocations with preemption "
            "(UnconditionallyPreemptibleAllocations: %v, UnconditionalResourceUsageDiscount: %v, TargetOperationPreemptionPriority: %v)",
            unconditionallyPreemptibleAllocations,
            FormatResources(context->SchedulingContext()->UnconditionalDiscount()),
            parameters.TargetOperationPreemptionPriority);

        while (context->ShouldContinueScheduling()) {
            if (!context->GetStagePrescheduleExecuted()) {
                context->PrescheduleAllocation(
                    /*consideredSchedulableOperations*/ std::nullopt,
                    parameters.TargetOperationPreemptionPriority);
            }

            auto scheduleAllocationResult = context->ScheduleAllocation(/*ignorePacking*/ true);
            if (scheduleAllocationResult.Scheduled) {
                allocationStartedUsingPreemption = context->SchedulingContext()->StartedAllocations().back();
                break;
            }
            if (scheduleAllocationResult.Finished) {
                break;
            }
        }
    }

    int startedAfterPreemption = context->SchedulingContext()->StartedAllocations().size();
    context->SchedulingStatistics().ScheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

    context->PreemptAllocationsAfterScheduling(
        parameters.TargetOperationPreemptionPriority,
        std::move(unconditionallyPreemptibleAllocations),
        forcefullyPreemptibleAllocations,
        allocationStartedUsingPreemption);
}

const TFairShareTreeAllocationSchedulerOperationStatePtr& TFairShareTreeAllocationScheduler::GetOperationState(TOperationId operationId) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return GetOrCrash(OperationIdToState_, operationId);
}

const TFairShareTreeAllocationSchedulerOperationSharedStatePtr& TFairShareTreeAllocationScheduler::GetOperationSharedState(TOperationId operationId) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return GetOrCrash(OperationIdToSharedState_, operationId);
}

std::optional<TPersistentNodeSchedulingSegmentState> TFairShareTreeAllocationScheduler::FindInitialNodePersistentState(TNodeId nodeId)
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

std::optional<TPersistentOperationSchedulingSegmentState> TFairShareTreeAllocationScheduler::FindInitialOperationPersistentState(TOperationId operationId)
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

void TFairShareTreeAllocationScheduler::UpdateSsdPriorityPreemptionMedia()
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

void TFairShareTreeAllocationScheduler::InitializeStaticAttributes(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TAllocationSchedulerPostUpdateContext* postUpdateContext) const
{
    postUpdateContext->StaticAttributesList.resize(postUpdateContext->RootElement->GetTreeSize());

    for (const auto& [operationId, operationElement] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(operationElement);
        attributes.OperationState = GetOrCrash(postUpdateContext->OperationIdToState, operationId);
        attributes.OperationSharedState = GetOrCrash(postUpdateContext->OperationIdToSharedState, operationId);
    }
}

void TFairShareTreeAllocationScheduler::CollectSchedulableOperationsPerPriority(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TAllocationSchedulerPostUpdateContext* postUpdateContext) const
{
    for (const auto& [_, element] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        if (!element->IsSchedulable()) {
            continue;
        }

        bool useHighPriority = fairSharePostUpdateContext->TreeConfig->EnableGuaranteePriorityScheduling &&
            element->IsStrictlyDominatesNonBlocked(
                element->Attributes().PromisedGuaranteeFairShare.Total,
                element->Attributes().UsageShare);
        auto priority = useHighPriority
            ? EOperationSchedulingPriority::High
            : EOperationSchedulingPriority::Medium;
        postUpdateContext->StaticAttributesList.AttributesOf(element).SchedulingPriority = priority;
        postUpdateContext->SchedulableOperationsPerPriority[priority].push_back(element);
    }
}

void TFairShareTreeAllocationScheduler::PublishFairShareAndUpdatePreemptionAttributes(
    TSchedulerElement* element,
    TAllocationSchedulerPostUpdateContext* postUpdateContext) const
{
    // TODO(omgronny): Move EffectiveAggressivePreemptionAllowed update to UpdateEffectiveRecursiveAttributes.
    auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(element);
    auto aggressivePreemptionAllowed = IsAggressivePreemptionAllowed(element);
    if (element->IsRoot()) {
        YT_VERIFY(aggressivePreemptionAllowed);
        attributes.EffectiveAggressivePreemptionAllowed = *aggressivePreemptionAllowed;
    } else {
        const auto* parent = element->GetParent();
        YT_VERIFY(parent);
        const auto& parentAttributes = postUpdateContext->StaticAttributesList.AttributesOf(parent);

        attributes.EffectiveAggressivePreemptionAllowed = aggressivePreemptionAllowed
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

void TFairShareTreeAllocationScheduler::PublishFairShareAndUpdatePreemptionAttributesAtCompositeElement(
    TSchedulerCompositeElement* element,
    TAllocationSchedulerPostUpdateContext* postUpdateContext) const
{
    for (const auto& child : element->EnabledChildren()) {
        PublishFairShareAndUpdatePreemptionAttributes(child.Get(), postUpdateContext);
    }
}

void TFairShareTreeAllocationScheduler::PublishFairShareAndUpdatePreemptionAttributesAtOperation(
    TSchedulerOperationElement* element,
    TAllocationSchedulerPostUpdateContext* postUpdateContext) const
{
    // If fair share ratio equals demand ratio then we want to explicitly disable preemption.
    // It is necessary since some allocation's resource usage may increase before the next fair share update,
    // and in this case we don't want any allocations to become preemptible
    bool dominantFairShareEqualToDominantDemandShare =
        TResourceVector::Near(element->Attributes().FairShare.Total, element->Attributes().DemandShare, NVectorHdrf::RatioComparisonPrecision) &&
            !Dominates(TResourceVector::Epsilon(), element->Attributes().DemandShare);
    bool currentPreemptibleValue = !dominantFairShareEqualToDominantDemandShare;

    const auto& operationSharedState = postUpdateContext->StaticAttributesList.AttributesOf(element).OperationSharedState;
    operationSharedState->PublishFairShare(element->Attributes().FairShare.Total);
    operationSharedState->SetPreemptible(currentPreemptibleValue);
    operationSharedState->UpdatePreemptibleAllocationsList(element);
}

void TFairShareTreeAllocationScheduler::UpdateEffectiveRecursiveAttributes(
    const TSchedulerElement* element,
    TAllocationSchedulerPostUpdateContext* postUpdateContext)
{
    auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(element);
    auto prioritySchedulingSegmentModuleAssignmentEnabled = IsPrioritySchedulingSegmentModuleAssignmentEnabled(element);
    if (element->IsRoot()) {
        YT_VERIFY(prioritySchedulingSegmentModuleAssignmentEnabled);
        attributes.EffectivePrioritySchedulingSegmentModuleAssignmentEnabled = *prioritySchedulingSegmentModuleAssignmentEnabled;
    } else {
        const auto* parent = element->GetParent();
        YT_VERIFY(parent);
        const auto& parentAttributes = postUpdateContext->StaticAttributesList.AttributesOf(parent);

        attributes.EffectivePrioritySchedulingSegmentModuleAssignmentEnabled = prioritySchedulingSegmentModuleAssignmentEnabled
            .value_or(parentAttributes.EffectivePrioritySchedulingSegmentModuleAssignmentEnabled);
    }

    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            UpdateEffectiveRecursiveAttributesAtCompositeElement(static_cast<const TSchedulerCompositeElement*>(element), postUpdateContext);
            break;
        case ESchedulerElementType::Operation:
            UpdateEffectiveRecursiveAttributesAtOperation(static_cast<const TSchedulerOperationElement*>(element), postUpdateContext);
            break;
        default:
            YT_ABORT();
    }
}

void TFairShareTreeAllocationScheduler::UpdateEffectiveRecursiveAttributesAtCompositeElement(
    const TSchedulerCompositeElement* element,
    TAllocationSchedulerPostUpdateContext* postUpdateContext)
{
    for (const auto& child : element->EnabledChildren()) {
        UpdateEffectiveRecursiveAttributes(child.Get(), postUpdateContext);
    }
}

void TFairShareTreeAllocationScheduler::UpdateEffectiveRecursiveAttributesAtOperation(
    const TSchedulerOperationElement* /*element*/,
    TAllocationSchedulerPostUpdateContext* /*postUpdateContext*/)
{ }

void TFairShareTreeAllocationScheduler::ProcessUpdatedStarvationStatuses(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TAllocationSchedulerPostUpdateContext* postUpdateContext)
{
    auto processUpdatedStarvationStatuses = [&] (const auto& operationMap) {
        for (const auto& [operationId, operationElement] : operationMap) {
            GetOrCrash(postUpdateContext->OperationIdToSharedState, operationId)->ProcessUpdatedStarvationStatus(operationElement->GetStarvationStatus());
        }
    };

    processUpdatedStarvationStatuses(fairSharePostUpdateContext->EnabledOperationIdToElement);
    processUpdatedStarvationStatuses(fairSharePostUpdateContext->DisabledOperationIdToElement);
}

void TFairShareTreeAllocationScheduler::UpdateCachedAllocationPreemptionStatuses(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TAllocationSchedulerPostUpdateContext* postUpdateContext)
{
    auto allocationPreemptionStatuses = New<TRefCountedAllocationPreemptionStatusMapPerOperation>();
    auto collectAllocationPreemptionStatuses = [&] (const auto& operationMap) {
        for (const auto& [operationId, operationElement] : operationMap) {
            // NB: We cannot use operation shared state from static attributes list, because disabled operations don't have a tree index.
            EmplaceOrCrash(
                *allocationPreemptionStatuses,
                operationId,
                GetOrCrash(postUpdateContext->OperationIdToSharedState, operationId)->GetAllocationPreemptionStatusMap());
        }
    };

    collectAllocationPreemptionStatuses(fairSharePostUpdateContext->EnabledOperationIdToElement);
    collectAllocationPreemptionStatuses(fairSharePostUpdateContext->DisabledOperationIdToElement);

    CachedAllocationPreemptionStatuses_ = TCachedAllocationPreemptionStatuses{
        .Value = std::move(allocationPreemptionStatuses),
        .UpdateTime = fairSharePostUpdateContext->Now,
    };
}

void TFairShareTreeAllocationScheduler::ComputeOperationSchedulingIndexes(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TAllocationSchedulerPostUpdateContext* postUpdateContext)
{
    int currentSchedulingIndex = 0;
    auto doComputeOperationSchedulingIndexes = [&] (TNonOwningOperationElementList& operations) {
        TDynamicAttributesManager dynamicAttributesManager(/*schedulingSnapshot*/ {}, postUpdateContext->RootElement->SchedulableElementCount());
        auto consideredSchedulableChildrenPerPool = BuildOperationSubsetInducedSubtree(postUpdateContext->RootElement, operations);
        InitializeDynamicAttributesAtUpdateRecursively(
            postUpdateContext->RootElement,
            &consideredSchedulableChildrenPerPool,
            &dynamicAttributesManager);

        // NB(eshcherbin): For brevity reasons we rely on the fact that reference to the root's attributes is never invalidated.
        const auto& rootDynamicAttributes = const_cast<const TDynamicAttributesManager&>(dynamicAttributesManager).AttributesOf(postUpdateContext->RootElement);
        while (rootDynamicAttributes.Active) {
            auto* bestLeafDescendant = rootDynamicAttributes.BestLeafDescendant;
            postUpdateContext->StaticAttributesList.AttributesOf(bestLeafDescendant).SchedulingIndex = currentSchedulingIndex++;
            dynamicAttributesManager.DeactivateOperation(bestLeafDescendant);
        }

        // NB(eshcherbin): Need to sort operations by scheduling index for batching to work properly.
        SortBy(operations, [&] (TSchedulerOperationElement* element) {
            return postUpdateContext->StaticAttributesList.AttributesOf(element).SchedulingIndex;
        });
    };


    if (fairSharePostUpdateContext->TreeConfig->EnableGuaranteePriorityScheduling) {
        for (auto priority : GetDescendingSchedulingPriorities()) {
            doComputeOperationSchedulingIndexes(postUpdateContext->SchedulableOperationsPerPriority[priority]);
        }
    } else {
        // NB(eshcherbin): If priority scheduling is disabled, all operations have medium priority.
        doComputeOperationSchedulingIndexes(postUpdateContext->SchedulableOperationsPerPriority[EOperationSchedulingPriority::Medium]);
    }
}

void TFairShareTreeAllocationScheduler::InitializeDynamicAttributesAtUpdateRecursively(
    TSchedulerElement* element,
    std::vector<TNonOwningElementList>* consideredSchedulableChildrenPerPool,
    TDynamicAttributesManager* dynamicAttributesManager) const
{
    dynamicAttributesManager->InitializeResourceUsageAtPostUpdate(element, element->ResourceUsageAtUpdate());
    if (element->IsOperation()) {
        dynamicAttributesManager->InitializeAttributesAtOperation(static_cast<TSchedulerOperationElement*>(element));
        return;
    }

    auto* compositeElement = static_cast<TSchedulerCompositeElement*>(element);
    auto& children = GetSchedulerElementAttributesFromVector(*consideredSchedulableChildrenPerPool, element);
    for (auto* child : children) {
        InitializeDynamicAttributesAtUpdateRecursively(child, consideredSchedulableChildrenPerPool, dynamicAttributesManager);
    }

    dynamicAttributesManager->InitializeAttributesAtCompositeElement(compositeElement, std::move(children));
}

void TFairShareTreeAllocationScheduler::CollectKnownSchedulingTagFilters(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TAllocationSchedulerPostUpdateContext* postUpdateContext) const
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

void TFairShareTreeAllocationScheduler::UpdateSsdNodeSchedulingAttributes(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TAllocationSchedulerPostUpdateContext* postUpdateContext) const
{
    for (const auto& [_, element] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(element);
        const TSchedulerCompositeElement* current = element->GetParent();
        while (current) {
            if (current->GetType() == ESchedulerElementType::Pool &&
                !static_cast<const TSchedulerPoolElement*>(current)->GetConfig()->AllowRegularAllocationsOnSsdNodes)
            {
                attributes.AreRegularAllocationsOnSsdNodesAllowed = false;
                break;
            }

            current = current->GetParent();
        }
    }
}

void TFairShareTreeAllocationScheduler::CountOperationsByPreemptionPriority(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TAllocationSchedulerPostUpdateContext* postUpdateContext) const
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

const TFairShareTreeAllocationSchedulerNodeState* TFairShareTreeAllocationScheduler::FindNodeState(TNodeId nodeId) const
{
    return const_cast<const TFairShareTreeAllocationSchedulerNodeState*>(const_cast<TFairShareTreeAllocationScheduler*>(this)->FindNodeState(nodeId));
}

TFairShareTreeAllocationSchedulerNodeState* TFairShareTreeAllocationScheduler::FindNodeState(TNodeId nodeId)
{
    auto nodeShardId = StrategyHost_->GetNodeShardId(nodeId);

    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetNodeShardInvokers()[nodeShardId]);

    auto& nodeStates = NodeStateShards_[nodeShardId].NodeIdToState;
    auto it = nodeStates.find(nodeId);
    return it != nodeStates.end() ? &it->second : nullptr;
}

TFairShareTreeAllocationSchedulerOperationStateMap TFairShareTreeAllocationScheduler::GetOperationStateMapSnapshot() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TFairShareTreeAllocationSchedulerOperationStateMap operationStateMapSnapshot;
    operationStateMapSnapshot.reserve(OperationIdToState_.size());
    for (const auto& [operationId, operationState] : OperationIdToState_) {
        operationStateMapSnapshot.emplace(operationId, New<TFairShareTreeAllocationSchedulerOperationState>(*operationState));
    }

    return operationStateMapSnapshot;
}

TFairShareTreeAllocationSchedulerNodeStateMap TFairShareTreeAllocationScheduler::GetNodeStateMapSnapshot() const
{
    const auto& nodeShardInvokers = StrategyHost_->GetNodeShardInvokers();
    std::vector<TFuture<TFairShareTreeAllocationSchedulerNodeStateMap>> futures;
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

    TFairShareTreeAllocationSchedulerNodeStateMap nodeStates;
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

void TFairShareTreeAllocationScheduler::ApplyOperationSchedulingSegmentsChanges(
    const TFairShareTreeAllocationSchedulerOperationStateMap& changedOperationStates)
{
    for (const auto& [operationId, changedOperationState] : changedOperationStates) {
        const auto& operationState = GetOperationState(operationId);
        operationState->SchedulingSegmentModule = changedOperationState->SchedulingSegmentModule;
        operationState->FailingToScheduleAtModuleSince = changedOperationState->FailingToScheduleAtModuleSince;
        operationState->FailingToAssignToModuleSince = changedOperationState->FailingToAssignToModuleSince;
    }
}

void TFairShareTreeAllocationScheduler::ApplyNodeSchedulingSegmentsChanges(const TSetNodeSchedulingSegmentOptionsList& movedNodes)
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
                    node.ForceRunningAllocationStatisticsUpdate = true;
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

void TFairShareTreeAllocationScheduler::ManageSchedulingSegments()
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
            PersistentState_ = New<TPersistentFairShareTreeAllocationSchedulerState>();
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

void TFairShareTreeAllocationScheduler::UpdateDynamicAttributesListSnapshot(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot)
{
    const auto& schedulingSnapshot = treeSnapshot->SchedulingSnapshot();
    if (!resourceUsageSnapshot) {
        schedulingSnapshot->ResetDynamicAttributesListSnapshot();
        return;
    }

    auto attributesSnapshot = New<TDynamicAttributesListSnapshot>(
        TDynamicAttributesManager::BuildDynamicAttributesListFromSnapshot(
            treeSnapshot,
            resourceUsageSnapshot,
            NProfiling::GetCpuInstant()));
    schedulingSnapshot->SetDynamicAttributesListSnapshot(std::move(attributesSnapshot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
