#include "resource_tree.h"

#include "fair_share_tree_element.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

void TResourceTree::AttachParent(const TSchedulerElementSharedStatePtr& element, const TSchedulerElementSharedStatePtr& parent)
{
    TWriterGuard guard(TreeLock_);
    YCHECK(!element->Parent_);
    YCHECK(element != parent);

    element->Parent_ = parent;
}

void TResourceTree::ChangeParent(const TSchedulerElementSharedStatePtr& element, const TSchedulerElementSharedStatePtr& newParent)
{
    TWriterGuard guard(TreeLock_);

    TWriterGuard resourceUsageLock(element->ResourceUsageLock_);
    YCHECK(element->Parent_);

    CheckCycleAbsence(element, newParent);

    DoIncreaseHierarchicalResourceUsage(element->Parent_.Get(), -element->ResourceUsage_);
    DoIncreaseHierarchicalResourceUsagePrecommit(element->Parent_.Get(), -element->ResourceUsagePrecommit_);

    element->Parent_ = newParent;

    DoIncreaseHierarchicalResourceUsage(newParent, element->ResourceUsage_);
    DoIncreaseHierarchicalResourceUsagePrecommit(newParent, element->ResourceUsagePrecommit_);
}

void TResourceTree::DetachParent(const TSchedulerElementSharedStatePtr& element)
{
    TWriterGuard guard(TreeLock_);
    YCHECK(element->Parent_);
    element->Parent_ = nullptr;
}

void TResourceTree::ReleaseResources(const TSchedulerElementSharedStatePtr& element)
{
    YCHECK(element->Parent_);

    IncreaseHierarchicalResourceUsagePrecommit(element, -element->GetResourceUsagePrecommit());
    IncreaseHierarchicalResourceUsage(element, -element->GetResourceUsage());
}

void TResourceTree::CheckCycleAbsence(const TSchedulerElementSharedStatePtr& element, const TSchedulerElementSharedStatePtr& newParent)
{
    auto current = newParent.Get();
    while (current != nullptr) {
        YCHECK(current != element);
        current = current->Parent_.Get();
    }
}

void TResourceTree::IncreaseHierarchicalResourceUsage(const TSchedulerElementSharedStatePtr& element, const TJobResources& delta)
{
    TReaderGuard guard(TreeLock_);

    DoIncreaseHierarchicalResourceUsage(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsage(const TSchedulerElementSharedStatePtr& element, const TJobResources& delta)
{
    TSchedulerElementSharedState* current = element.Get();
    while (current != nullptr) {
        current->IncreaseLocalResourceUsage(delta);
        current = current->Parent_.Get();
    }
}

void TResourceTree::IncreaseHierarchicalResourceUsagePrecommit(const TSchedulerElementSharedStatePtr& element, const TJobResources& delta)
{
    TReaderGuard guard(TreeLock_);

    DoIncreaseHierarchicalResourceUsagePrecommit(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsagePrecommit(const TSchedulerElementSharedStatePtr& element, const TJobResources& delta)
{
    TSchedulerElementSharedState* current = element.Get();
    while (current != nullptr) {
        current->IncreaseLocalResourceUsagePrecommit(delta);
        current = current->Parent_.Get();
    }
}

bool TResourceTree::TryIncreaseHierarchicalResourceUsagePrecommit(
    const TSchedulerElementSharedStatePtr& element,
    const TJobResources &delta,
    TJobResources *availableResourceLimitsOutput)
{
    TReaderGuard guard(TreeLock_);

    auto availableResourceLimits = TJobResources::Infinite();

    TSchedulerElementSharedState* failedParent = nullptr;

    TSchedulerElementSharedState* currentElement = element.Get();
    while (currentElement) {
        TJobResources localAvailableResourceLimits;
        if (!currentElement->IncreaseLocalResourceUsagePrecommitWithCheck(delta, &localAvailableResourceLimits)) {
            failedParent = currentElement;
            break;
        }
        availableResourceLimits = Min(availableResourceLimits, localAvailableResourceLimits);
        currentElement = currentElement->Parent_.Get();
    }

    if (failedParent) {
        currentElement = element.Get();
        while (currentElement != failedParent) {
            currentElement->IncreaseLocalResourceUsagePrecommit(-delta);
            currentElement = currentElement->Parent_.Get();
        }
        return false;
    }

    if (availableResourceLimitsOutput != nullptr) {
        *availableResourceLimitsOutput = availableResourceLimits;
    }
    return true;
}


void TResourceTree::CommitHierarchicalResourceUsage(
    const TSchedulerElementSharedStatePtr& element,
    const TJobResources& resourceUsageDelta,
    const TJobResources& precommittedResources)
{
    TReaderGuard guard(TreeLock_);

    TSchedulerElementSharedState* current = element.Get();
    while (current != nullptr) {
        current->CommitLocalResourceUsage(resourceUsageDelta, precommittedResources);
        current = current->Parent_.Get();
    }
}

void TResourceTree::ApplyHierarchicalJobMetricsDelta(const TSchedulerElementSharedStatePtr& element, const TJobMetrics& delta)
{
    TReaderGuard guard(TreeLock_);

    TSchedulerElementSharedState* current = element.Get();
    while (current != nullptr) {
        current->ApplyLocalJobMetricsDelta(delta);
        current = current->Parent_.Get();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
