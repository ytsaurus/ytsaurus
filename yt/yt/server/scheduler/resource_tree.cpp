#include "resource_tree.h"
#include "private.h"

#include "resource_tree_element.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = StrategyLogger;

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

TResourceTree::TResourceTree(const TFairShareStrategyTreeConfigPtr& config)
    : Config_(config)
{ }

void TResourceTree::UpdateConfig(const TFairShareStrategyTreeConfigPtr& config)
{
    EnableStructureLockProfiling.store(config->EnableResourceTreeStructureLockProfiling);
    EnableUsageLockProfiling.store(config->EnableResourceTreeUsageLockProfiling);
}

void TResourceTree::AttachParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& parent)
{
    // There is no necessity to acquire StructureLock_ since element is newly created and no concurrent operations are possible.
    YT_VERIFY(!element->Initialized_);
    YT_VERIFY(!element->Parent_);
    YT_VERIFY(element != parent);

    element->Parent_ = parent;
    element->Initialized_ = true;
}

void TResourceTree::ChangeParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& newParent)
{
    auto structureGuard = WriterGuard(StructureLock_);

    IncrementStructureLockWriteCount();

    auto resourceUsageGuard = WriterGuard(element->ResourceUsageLock_);
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
    YT_LOG_DEBUG("Scheduling element to detach (Id: %v)", element->GetId());
    YT_VERIFY(element->Initialized_);
    ElementsToDetachQueue_.Enqueue(element);
}

void TResourceTree::ReleaseResources(const TResourceTreeElementPtr& element, bool markAsNonAlive)
{
    if (!element->GetAlive()) {
        return;
    }

    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Initialized_);
    YT_VERIFY(element->Parent_);

    if (markAsNonAlive) {
        element->SetNonAlive();
        // No resource usage changes are possible after element is marked as non-alive.

        TJobResources usagePrecommit;
        TJobResources usage;
        element->ReleaseResources(&usagePrecommit, &usage);

        YT_LOG_DEBUG("Strong release of element resources (Id: %v, Usage: %v, UsagePrecommit: %v)",
            element->GetId(),
            FormatResources(usage),
            FormatResources(usagePrecommit));

        DoIncreaseHierarchicalResourceUsagePrecommit(element->Parent_, -usagePrecommit);
        DoIncreaseHierarchicalResourceUsage(element->Parent_, -usage);
    } else {
        // Relaxed way to release resources.
        auto usagePrecommit = element->GetResourceUsagePrecommit();
        auto usage = element->GetResourceUsage();
        YT_LOG_DEBUG("Relaxed release of element resources (Id: %v, Usage: %v, UsagePrecommit: %v)",
            element->GetId(),
            FormatResources(usage),
            FormatResources(usagePrecommit));
        DoIncreaseHierarchicalResourceUsagePrecommit(element, -usagePrecommit);
        DoIncreaseHierarchicalResourceUsage(element, -usage);
    }
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
    if (!element->GetAlive()) {
        return;
    }

    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Initialized_);

    DoIncreaseHierarchicalResourceUsage(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    YT_VERIFY(element->Initialized_);

    TResourceTreeElement* current = element.Get();
    if (!current->IncreaseLocalResourceUsage(delta)) {
        YT_LOG_DEBUG("Local increase of usage failed (Id: %v)", element->GetId());
        return;
    }
    current = current->Parent_.Get();

    while (current != nullptr) {
        auto result = current->IncreaseLocalResourceUsage(delta);
        YT_ASSERT(result);
        current = current->Parent_.Get();
    }
}

void TResourceTree::IncreaseHierarchicalResourceUsagePrecommit(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    if (!element->GetAlive()) {
        return;
    }

    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Initialized_);

    DoIncreaseHierarchicalResourceUsagePrecommit(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsagePrecommit(
    const TResourceTreeElementPtr& element,
    const TJobResources& delta)
{
    YT_VERIFY(element->Initialized_);

    TResourceTreeElement* current = element.Get();
    if (!current->IncreaseLocalResourceUsagePrecommit(delta)) {
        YT_LOG_DEBUG("Local increase of usage precommit failed (Id: %v)", element->GetId());
        return;
    }
    current = current->Parent_.Get();

    while (current != nullptr) {
        auto result = current->IncreaseLocalResourceUsagePrecommit(delta);
        YT_ASSERT(result);
        current = current->Parent_.Get();
    }
}

EResourceTreeIncreaseResult TResourceTree::TryIncreaseHierarchicalResourceUsagePrecommit(
    const TResourceTreeElementPtr& element,
    const TJobResources &delta,
    TJobResources *availableResourceLimitsOutput)
{
    if (!element->GetAlive()) {
        return EResourceTreeIncreaseResult::ElementIsNotAlive;
    }

    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Initialized_);

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
        return EResourceTreeIncreaseResult::ResourceLimitExceeded;
    }

    if (availableResourceLimitsOutput != nullptr) {
        *availableResourceLimitsOutput = availableResourceLimits;
    }

    return EResourceTreeIncreaseResult::Success;
}


void TResourceTree::CommitHierarchicalResourceUsage(
    const TResourceTreeElementPtr& element,
    const TJobResources& resourceUsageDelta,
    const TJobResources& precommittedResources)
{
    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Initialized_);

    TResourceTreeElement* current = element.Get();
    if (!current->CommitLocalResourceUsage(resourceUsageDelta, precommittedResources)) {
        YT_LOG_DEBUG("Local commit of resource usage failed (Id: %v)", current->GetId());
        return;
    }
    current = current->Parent_.Get();

    while (current != nullptr) {
        auto result = current->CommitLocalResourceUsage(resourceUsageDelta, precommittedResources);
        YT_ASSERT(result);
        current = current->Parent_.Get();
    }
}

void TResourceTree::ApplyHierarchicalJobMetricsDelta(const TResourceTreeElementPtr& element, const TJobMetrics& delta)
{
    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Initialized_);

    TResourceTreeElement* current = element.Get();
    while (current != nullptr) {
        current->ApplyLocalJobMetricsDelta(delta);
        current = current->Parent_.Get();
    }
}

void TResourceTree::PerformPostponedActions()
{
    auto guard = WriterGuard(StructureLock_);

    IncrementStructureLockWriteCount();

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

void TResourceTree::IncrementStructureLockReadCount()
{
    if (EnableStructureLockProfiling) {
        StructureLockReadCount_.Increment();
    }
}

void TResourceTree::IncrementStructureLockWriteCount()
{
    if (EnableStructureLockProfiling) {
        StructureLockWriteCount_.Increment();
    }
}

void TResourceTree::IncrementUsageLockReadCount()
{
    if (EnableUsageLockProfiling) {
        UsageLockReadCount_.Increment();
    }
}

void TResourceTree::IncrementUsageLockWriteCount()
{
    if (EnableUsageLockProfiling) {
        UsageLockWriteCount_.Increment();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
