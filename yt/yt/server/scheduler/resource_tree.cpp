#include "resource_tree.h"

#include "resource_tree_element.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

void TResourceTree::AttachParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& parent)
{
    TWriterGuard guard(TreeLock_);
    YT_VERIFY(!element->Parent_);
    YT_VERIFY(element != parent);

    element->Parent_ = parent;
}

void TResourceTree::ChangeParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& newParent)
{
    TWriterGuard guard(TreeLock_);

    TWriterGuard resourceUsageLock(element->ResourceUsageLock_);
    YT_VERIFY(element->Parent_);

    CheckCycleAbsence(element, newParent);

    DoIncreaseHierarchicalResourceUsage(element->Parent_.Get(), -element->ResourceUsage_);
    DoIncreaseHierarchicalResourceUsagePrecommit(element->Parent_.Get(), -element->ResourceUsagePrecommit_);

    element->Parent_ = newParent;

    DoIncreaseHierarchicalResourceUsage(newParent, element->ResourceUsage_);
    DoIncreaseHierarchicalResourceUsagePrecommit(newParent, element->ResourceUsagePrecommit_);
}

void TResourceTree::DetachParent(const TResourceTreeElementPtr& element)
{
    TWriterGuard guard(TreeLock_);
    YT_VERIFY(element->Parent_);
    element->Parent_ = nullptr;
}

void TResourceTree::ReleaseResources(const TResourceTreeElementPtr& element)
{
    YT_VERIFY(element->Parent_);

    IncreaseHierarchicalResourceUsagePrecommit(element, -element->GetResourceUsagePrecommit());
    IncreaseHierarchicalResourceUsage(element, -element->GetResourceUsage());
}

void TResourceTree::CheckCycleAbsence(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& newParent)
{
    auto current = newParent.Get();
    while (current != nullptr) {
        YT_VERIFY(current != element);
        current = current->Parent_.Get();
    }
}

void TResourceTree::IncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    TReaderGuard guard(TreeLock_);

    DoIncreaseHierarchicalResourceUsage(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    TResourceTreeElement* current = element.Get();
    while (current != nullptr) {
        current->IncreaseLocalResourceUsage(delta);
        current = current->Parent_.Get();
    }
}

void TResourceTree::IncreaseHierarchicalResourceUsagePrecommit(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    TReaderGuard guard(TreeLock_);

    DoIncreaseHierarchicalResourceUsagePrecommit(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsagePrecommit(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    TResourceTreeElement* current = element.Get();
    while (current != nullptr) {
        current->IncreaseLocalResourceUsagePrecommit(delta);
        current = current->Parent_.Get();
    }
}

bool TResourceTree::TryIncreaseHierarchicalResourceUsagePrecommit(
    const TResourceTreeElementPtr& element,
    const TJobResources &delta,
    TJobResources *availableResourceLimitsOutput)
{
    TReaderGuard guard(TreeLock_);

    auto availableResourceLimits = TJobResources::Infinite();

    TResourceTreeElement* failedParent = nullptr;

    TResourceTreeElement* currentElement = element.Get();
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
    const TResourceTreeElementPtr& element,
    const TJobResources& resourceUsageDelta,
    const TJobResources& precommittedResources)
{
    TReaderGuard guard(TreeLock_);

    TResourceTreeElement* current = element.Get();
    while (current != nullptr) {
        current->CommitLocalResourceUsage(resourceUsageDelta, precommittedResources);
        current = current->Parent_.Get();
    }
}

void TResourceTree::ApplyHierarchicalJobMetricsDelta(const TResourceTreeElementPtr& element, const TJobMetrics& delta)
{
    TReaderGuard guard(TreeLock_);

    TResourceTreeElement* current = element.Get();
    while (current != nullptr) {
        current->ApplyLocalJobMetricsDelta(delta);
        current = current->Parent_.Get();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
