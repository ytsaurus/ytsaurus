#include "resource_tree.h"
#include "private.h"

#include "resource_tree_element.h"

namespace NYT::NScheduler {

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

void TResourceTree::AttachParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& parent)
{
    // There is no necessity to acquire TreeLock_ since element is newly created and no concurrent operations are possible.
    YT_VERIFY(!element->Initialized_);
    YT_VERIFY(!element->Parent_);
    YT_VERIFY(element != parent);

    element->Parent_ = parent;
    element->Initialized_ = true;
}

void TResourceTree::ChangeParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& newParent)
{
    TWriterGuard guard(TreeLock_);

    Profiler.Increment(TreeLockWriteCount, 1);

    TWriterGuard resourceUsageLock(element->ResourceUsageLock_);
    YT_VERIFY(element->Parent_);
    YT_VERIFY(element->Initialized_);

    CheckCycleAbsence(element, newParent);

    DoIncreaseHierarchicalResourceUsage(element->Parent_.Get(), -element->ResourceUsage_);
    DoIncreaseHierarchicalResourceUsagePrecommit(element->Parent_.Get(), -element->ResourceUsagePrecommit_);

    element->Parent_ = newParent;

    DoIncreaseHierarchicalResourceUsage(newParent, element->ResourceUsage_);
    DoIncreaseHierarchicalResourceUsagePrecommit(newParent, element->ResourceUsagePrecommit_);
}

void TResourceTree::ScheduleDetachParent(const TResourceTreeElementPtr& element)
{
    YT_VERIFY(element->Initialized_);
    ElementsToDetachQueue_.Enqueue(element);
}

void TResourceTree::ReleaseResources(const TResourceTreeElementPtr& element)
{
    YT_VERIFY(element->Parent_);
    YT_VERIFY(element->Initialized_);

    IncreaseHierarchicalResourceUsagePrecommit(element, -element->GetResourceUsagePrecommit());
    IncreaseHierarchicalResourceUsage(element, -element->GetResourceUsage());
}

void TResourceTree::CheckCycleAbsence(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& newParent)
{
    YT_VERIFY(element->Initialized_);
    auto current = newParent.Get();
    while (current != nullptr) {
        YT_VERIFY(current != element);
        current = current->Parent_.Get();
    }
}

void TResourceTree::IncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    TReaderGuard guard(TreeLock_);

    Profiler.Increment(TreeLockReadCount, 1);

    YT_VERIFY(element->Initialized_);

    if (!element->GetAlive()) {
        return;
    }

    DoIncreaseHierarchicalResourceUsage(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    YT_VERIFY(element->Initialized_);

    TResourceTreeElement* current = element.Get();
    while (current != nullptr) {
        current->IncreaseLocalResourceUsage(delta);
        current = current->Parent_.Get();
    }
}

void TResourceTree::IncreaseHierarchicalResourceUsagePrecommit(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    TReaderGuard guard(TreeLock_);

    Profiler.Increment(TreeLockReadCount, 1);

    YT_VERIFY(element->Initialized_);

    if (!element->GetAlive()) {
        return;
    }

    DoIncreaseHierarchicalResourceUsagePrecommit(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsagePrecommit(
    const TResourceTreeElementPtr& element,
    const TJobResources& delta)
{
    YT_VERIFY(element->Initialized_);

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

    Profiler.Increment(TreeLockReadCount, 1);

    YT_VERIFY(element->Initialized_);

    if (!element->GetAlive()) {
        return false;
    }

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

    Profiler.Increment(TreeLockReadCount, 1);

    YT_VERIFY(element->Initialized_);

    TResourceTreeElement* current = element.Get();
    while (current != nullptr) {
        current->CommitLocalResourceUsage(resourceUsageDelta, precommittedResources);
        current = current->Parent_.Get();
    }
}

void TResourceTree::ApplyHierarchicalJobMetricsDelta(const TResourceTreeElementPtr& element, const TJobMetrics& delta)
{
    TReaderGuard guard(TreeLock_);

    Profiler.Increment(TreeLockReadCount, 1);

    YT_VERIFY(element->Initialized_);

    TResourceTreeElement* current = element.Get();
    while (current != nullptr) {
        current->ApplyLocalJobMetricsDelta(delta);
        current = current->Parent_.Get();
    }
}

void TResourceTree::PerformPostponedActions()
{
    TWriterGuard guard(TreeLock_);

    Profiler.Increment(TreeLockReadCount, 1);

    auto elementsToDetach = ElementsToDetachQueue_.DequeueAll();
    for (const auto& element : elementsToDetach) {
        YT_VERIFY(element->Parent_);
        YT_LOG_DEBUG_UNLESS(
            element->GetResourceUsageWithPrecommit() == TJobResources(),
            "Resource tree element has non-zero resources (Id: %v, ResourceUsage: %v, ResourceUsageWithPrecommit: %v)",
            element->GetId(),
            FormatResources(element->GetResourceUsage()),
            FormatResources(element->GetResourceUsageWithPrecommit()));
        YT_VERIFY(element->GetResourceUsageWithPrecommit() == TJobResources());
        element->Parent_ = nullptr;
    }
}
    
void TResourceTree::IncrementResourceUsageLockReadCount()
{
    Profiler.Increment(ResourceUsageLockReadCount, 1);
}

void TResourceTree::IncrementResourceUsageLockWriteCount()
{
    Profiler.Increment(ResourceUsageLockWriteCount, 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
