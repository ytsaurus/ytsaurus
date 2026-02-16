#include "resource_tree.h"

#include "private.h"
#include "resource_tree_element.h"

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = StrategyLogger;

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TResourceTree::TResourceTree(
    const TStrategyTreeConfigPtr& config,
    const std::vector<IInvokerPtr>& feasibleInvokers)
    : FeasibleInvokers_(feasibleInvokers)
{
    if (config) {
        UpdateConfig(config);
    }
}

void TResourceTree::UpdateConfig(const TStrategyTreeConfigPtr& config)
{
    YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

    EnableStructureLockProfiling_.store(config->EnableResourceTreeStructureLockProfiling);
    EnableUsageLockProfiling_.store(config->EnableResourceTreeUsageLockProfiling);
    ResourceTreeInitializeResourceUsageDelay_ = config->TestingOptions->ResourceTreeInitializeResourceUsageDelay;
    ResourceTreeReleaseResourcesRandomDelay_ = config->TestingOptions->ResourceTreeReleaseResourcesRandomDelay;
    ResourceTreeIncreaseLocalResourceUsagePrecommitRandomDelay_ = config->TestingOptions->ResourceTreeIncreaseLocalResourceUsagePrecommitRandomDelay;
    ResourceTreeRevertResourceUsagePrecommitRandomDelay_ = config->TestingOptions->ResourceTreeRevertResourceUsagePrecommitRandomDelay;

    if (config->UsePrecommitForPreemption != UsePrecommitForPreemption_) {
        auto structureGuard = WriterGuard(StructureLock_);

        ResetPreemptedResourceUsagePrecommit();

        UsePrecommitForPreemption_.store(config->UsePrecommitForPreemption);
        YT_LOG_DEBUG("Switch precommit preemption setting (Enabled: %v)", UsePrecommitForPreemption_);
    }
}

void TResourceTree::AttachParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& parent)
{
    YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

    // There is no necessity to acquire StructureLock_ since element is newly created and no concurrent operations are possible.
    YT_VERIFY(!element->Initialized_);
    YT_VERIFY(!element->Parent_);
    YT_VERIFY(element != parent);

    element->Parent_ = parent;
    element->Initialized_ = true;

    AliveElements_.insert(element);
}

void TResourceTree::ChangeParent(
    const TResourceTreeElementPtr& element,
    const TResourceTreeElementPtr& newParent,
    const std::optional<std::vector<TResourceTreeElementPtr>>& descendantOperationElements)
{
    YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto structureGuard = WriterGuard(StructureLock_);

    // NB: Provided descendant operation elements indicate that resource usage of the element must be explicitly calculated
    // for correct transfer of ancestor's resource usage.
    bool calculateTransientResourceUsage = descendantOperationElements.has_value();
    if (calculateTransientResourceUsage) {
        DoInitializeResourceUsageFor(element, *descendantOperationElements);
    }

    IncrementStructureLockWriteCount();

    auto resourceUsageGuard = WriterGuard(element->ResourceUsageLock_);
    YT_VERIFY(element->Parent_);
    YT_VERIFY(element->Initialized_);

    CheckCycleAbsence(element, newParent);

    DoIncreaseHierarchicalResourceUsage(element->Parent_.Get(), -element->ResourceUsage_);
    DoIncreaseHierarchicalResourceUsagePrecommit(element->Parent_.Get(), -element->ResourceUsagePrecommit_);
    if (UsePrecommitForPreemption_) {
        DoIncreaseHierarchicalPreemptedResourceUsagePrecommit(element->Parent_.Get(), -element->PreemptedResourceUsagePrecommit_);
    }

    element->Parent_ = newParent;

    DoIncreaseHierarchicalResourceUsage(newParent, element->ResourceUsage_);
    DoIncreaseHierarchicalResourceUsagePrecommit(newParent, element->ResourceUsagePrecommit_);
    if (UsePrecommitForPreemption_) {
        DoIncreaseHierarchicalPreemptedResourceUsagePrecommit(newParent, element->PreemptedResourceUsagePrecommit_);
    }

    if (calculateTransientResourceUsage) {
        element->ResourceUsage_ = TJobResources();
        element->ResourceUsagePrecommit_ = TJobResources();
    }
}

void TResourceTree::ScheduleDetachParent(const TResourceTreeElementPtr& element)
{
    YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

    YT_LOG_DEBUG("Scheduling element to detach (Id: %v)", element->GetId());
    YT_VERIFY(element->Initialized_);
    ElementsToDetachQueue_.Enqueue(element);
}

void TResourceTree::ReleaseResources(const TResourceTreeElementPtr& element, bool markAsNonAlive)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!element->GetAlive()) {
        return;
    }

    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    // TODO(eshcherbin): As a last resort, add a special lock (separate from ResourceUsageLock_) which would guarantee that
    // all hierarchical precommit changes that originate from this operation are finished before resources are released.
    YT_VERIFY(element->Kind_ == EResourceTreeElementKind::Operation);
    YT_VERIFY(element->Initialized_);
    YT_VERIFY(element->Parent_);

    if (markAsNonAlive) {
        element->SetNonAlive();
        // No resource usage changes are possible after element is marked as non-alive.

        TJobResources usagePrecommit;
        TJobResources usage;
        TJobResources preemptedUsagePrecommit;
        element->ReleaseResources(&usagePrecommit, &usage, &preemptedUsagePrecommit);

        YT_LOG_DEBUG("Strong release of element resources (Id: %v, Usage: %v, UsagePrecommit: %v)",
            element->GetId(),
            FormatResources(usage),
            FormatResources(usagePrecommit));

        MaybeDelay(ResourceTreeReleaseResourcesRandomDelay_, EDelayType::Sync);

        DoIncreaseHierarchicalResourceUsagePrecommit(element->Parent_, -usagePrecommit, /*enableDetailedLogs*/ true);
        DoIncreaseHierarchicalResourceUsage(element->Parent_, -usage);

        if (UsePrecommitForPreemption_) {
            DoIncreaseHierarchicalPreemptedResourceUsagePrecommit(element->Parent_, -preemptedUsagePrecommit);
        }
    } else {
        // Relaxed way to release resources.
        auto usagePrecommit = element->GetResourceUsagePrecommit();
        auto usage = element->GetResourceUsage();
        auto preemptedUsagePrecommit = element->GetPreemptedResourceUsagePrecommit();
        YT_LOG_DEBUG("Relaxed release of element resources (Id: %v, Usage: %v, UsagePrecommit: %v)",
            element->GetId(),
            FormatResources(usage),
            FormatResources(usagePrecommit));
        DoIncreaseHierarchicalResourceUsagePrecommit(element, -usagePrecommit);
        DoIncreaseHierarchicalResourceUsage(element, -usage);

        if (UsePrecommitForPreemption_) {
            DoIncreaseHierarchicalPreemptedResourceUsagePrecommit(element, -preemptedUsagePrecommit);
        }
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
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!element->GetAlive()) {
        return;
    }

    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Kind_ == EResourceTreeElementKind::Operation);
    YT_VERIFY(element->Initialized_);

    DoIncreaseHierarchicalResourceUsage(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_VERIFY(element->Initialized_);

    auto increaseLocalResourceUsage = [element] (auto* current, const TJobResources& delta) {
        bool success = current->IncreaseLocalResourceUsage(delta);
        YT_LOG_DEBUG_UNLESS(
            success,
            "Local increase of usage failed (Delta: %v, CurrentElement: %v, SourceElement: %v)",
            delta,
            current->GetId(),
            element->GetId());
        return success;
    };

    if (!increaseLocalResourceUsage(element.Get(), delta)) {
        return;
    }

    auto* current = element->Parent_.Get();
    while (current) {
        bool success = increaseLocalResourceUsage(current, delta);
        YT_ASSERT(success);
        current = current->Parent_.Get();
    }
}

void TResourceTree::IncreaseHierarchicalResourceUsagePrecommit(const TResourceTreeElementPtr& element, const TJobResources& delta)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!element->GetAlive()) {
        YT_LOG_DEBUG(
            "Unable to increase resource usage precommit hierarchically because element is not alive "
            "(Id: %v, Delta: %v)",
            element->GetId(),
            delta);

        return;
    }

    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Initialized_);

    DoIncreaseHierarchicalResourceUsagePrecommit(element, delta);
}

void TResourceTree::DoIncreaseHierarchicalResourceUsagePrecommit(
    const TResourceTreeElementPtr& element,
    const TJobResources& delta,
    bool enableDetailedLogs)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_VERIFY(element->Initialized_);

    auto increaseLocalResourceUsagePrecommit = [element, enableDetailedLogs] (auto* current, const TJobResources& delta) {
        bool success = current->IncreaseLocalResourceUsagePrecommit(delta, enableDetailedLogs);
        YT_LOG_DEBUG_UNLESS(
            success,
            "Local increase of usage precommit failed (Delta: %v, CurrentElement: %v, SourceElement: %v)",
            delta,
            current->GetId(),
            element->GetId());

        return success;
    };

    if (!increaseLocalResourceUsagePrecommit(element.Get(), delta)) {
        return;
    }

    auto* current = element->Parent_.Get();
    while (current) {
        MaybeDelay(ResourceTreeIncreaseLocalResourceUsagePrecommitRandomDelay_, EDelayType::Sync);

        YT_VERIFY(increaseLocalResourceUsagePrecommit(current, delta));
        current = current->Parent_.Get();
    }
}

EResourceTreeIncreaseResult TResourceTree::TryIncreaseHierarchicalResourceUsagePrecommit(
    const TResourceTreeElementPtr& element,
    const TJobResources& delta,
    bool allowLimitsOvercommit,
    const std::optional<TJobResources>& additionalLocalResourceLimits,
    TJobResources* availableResourceLimitsOutput)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!element->GetAlive()) {
        return EResourceTreeIncreaseResult::ElementIsNotAlive;
    }

    auto guard = ReaderGuard(StructureLock_);

    auto elementGuard = element->AcquireWriteLock();

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Kind_ == EResourceTreeElementKind::Operation);
    YT_VERIFY(element->Initialized_);

    auto availableResourceLimits = TJobResources::Infinite();

    TResourceTreeElement* failedParent = nullptr;

    {
        TJobResources localAvailableResourceLimits;
        auto precommitResult = element->IncreaseLocalResourceUsagePrecommitWithCheckUnsafe(
            delta,
            allowLimitsOvercommit,
            additionalLocalResourceLimits,
            &localAvailableResourceLimits);
        if (precommitResult != EResourceTreeIncreaseResult::Success) {
            return precommitResult;
        }
        availableResourceLimits = Min(availableResourceLimits, localAvailableResourceLimits);
    }

    TResourceTreeElement* currentElement = element->Parent_.Get();
    while (currentElement) {
        TJobResources localAvailableResourceLimits;
        auto precommitResult = currentElement->IncreaseLocalResourceUsagePrecommitWithCheck(delta, allowLimitsOvercommit, &localAvailableResourceLimits);
        if (precommitResult != EResourceTreeIncreaseResult::Success) {
            failedParent = currentElement;
            break;
        }
        availableResourceLimits = Min(availableResourceLimits, localAvailableResourceLimits);
        currentElement = currentElement->Parent_.Get();
    }

    if (failedParent) {
        YT_VERIFY(element->IncreaseLocalResourceUsagePrecommitUnsafe(-delta));
        currentElement = element->Parent_.Get();
        while (currentElement != failedParent) {
            if (ResourceTreeRevertResourceUsagePrecommitRandomDelay_) {
                // NB: under RWLock only synchronous sleep is allowed.
                Delay(RandomDuration(*ResourceTreeRevertResourceUsagePrecommitRandomDelay_), EDelayType::Sync);
            }
            YT_VERIFY(currentElement->IncreaseLocalResourceUsagePrecommit(-delta));
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
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Kind_ == EResourceTreeElementKind::Operation);
    YT_VERIFY(element->Initialized_);

    auto commitLocalResourceUsage = [element] (auto* current, const TJobResources& resourceUsageDelta, const TJobResources& precommittedResources) {
        bool success = current->CommitLocalResourceUsage(resourceUsageDelta, precommittedResources);
        YT_LOG_DEBUG_UNLESS(
            success,
            "Local commit of usage failed (ResourceUsageDelta: %v, PrecommittedResources: %v, CurrentElement: %v, SourceElement: %v)",
            resourceUsageDelta,
            precommittedResources,
            current->GetId(),
            element->GetId());
        return success;
    };

    if (!commitLocalResourceUsage(element.Get(), resourceUsageDelta, precommittedResources)) {
        return;
    }

    auto* current = element->Parent_.Get();
    while (current) {
        YT_VERIFY(commitLocalResourceUsage(current, resourceUsageDelta, precommittedResources));
        current = current->Parent_.Get();
    }
}

EResourceTreeIncreasePreemptedResult TResourceTree::TryIncreaseHierarchicalPreemptedResourceUsagePrecommit(
    const TResourceTreeElementPtr& element,
    const TJobResources& delta,
    std::string* violatedIdOutput)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!element->GetAlive()) {
        return EResourceTreeIncreasePreemptedResult::ElementIsNotAlive;
    }

    auto guard = ReaderGuard(StructureLock_);

    if (!UsePrecommitForPreemption_) {
        YT_LOG_DEBUG("Skip increasing precommit preempted usage because it is disabled");
        return EResourceTreeIncreasePreemptedResult::NoResourceLimitsViolation;
    }

    auto elementGuard = element->AcquireWriteLock();

    IncrementStructureLockReadCount();

    YT_VERIFY(element->Kind_ == EResourceTreeElementKind::Operation);
    YT_VERIFY(element->Initialized_);

    TResourceTreeElement* elementWithViolatedLimits = nullptr;

    if (element->AreSpecifiedResourceLimitsViolatedUnsafe(/*considerPreemptedPrecommited*/ true)) {
        elementWithViolatedLimits = element.Get();
    }

    auto increaseLocalPreemptedResourceUsagePrecommitUnsafe = [element] (auto* current, const TJobResources& delta) {
        auto precommitStatus = current->IncreaseLocalPreemptedResourceUsagePrecommitUnsafe(delta);
        YT_LOG_DEBUG_UNLESS(
            precommitStatus,
            "Local increase of precommit preempted resource usage failed "
            "(ResourceUsageDelta: %v, CurrentElement: %v, SourceElement: %v)",
            delta,
            current->GetId(),
            element->GetId());
        return precommitStatus;
    };

    if (!increaseLocalPreemptedResourceUsagePrecommitUnsafe(element.Get(), delta)) {
        return EResourceTreeIncreasePreemptedResult::ElementIsNotAlive;
    }

    TResourceTreeElement* failedParent = nullptr;
    TResourceTreeElement* current = element->Parent_.Get();
    while (current) {
        auto currentGuard = current->AcquireWriteLock();
        if (!elementWithViolatedLimits && current->AreSpecifiedResourceLimitsViolatedUnsafe(/*considerPreemptedPrecommited*/ true)) {
            elementWithViolatedLimits = current;
        }

        auto precommitStatus = increaseLocalPreemptedResourceUsagePrecommitUnsafe(current, delta);
        if (!precommitStatus) {
            failedParent = current;
            break;
        }

        current = current->Parent_.Get();
    }

    if (!elementWithViolatedLimits || failedParent) {
        YT_VERIFY(increaseLocalPreemptedResourceUsagePrecommitUnsafe(element.Get(), -delta));
        current = element->Parent_.Get();

        while (current && current != failedParent) {
            YT_VERIFY(increaseLocalPreemptedResourceUsagePrecommitUnsafe(current, -delta));

            current = current->Parent_.Get();
        }

        return EResourceTreeIncreasePreemptedResult::NoResourceLimitsViolation;
    }

    *violatedIdOutput = elementWithViolatedLimits->GetId();

    return EResourceTreeIncreasePreemptedResult::Success;
}

bool TResourceTree::CommitHierarchicalPreemptedResourceUsage(
    const TResourceTreeElementPtr& element,
    const TJobResources& resourceUsageDelta)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(StructureLock_);

    IncrementStructureLockReadCount();

    if (!UsePrecommitForPreemption_) {
        YT_LOG_DEBUG("Skip commiting precommit preempted usage because UsePrecommitForPreemption is disabled");
        return false;
    }

    YT_VERIFY(element->Kind_ == EResourceTreeElementKind::Operation);
    YT_VERIFY(element->Initialized_);

    auto commitLocalPreemptedResourceUsage = [&] (auto* current) -> bool {
        bool success = current->CommitLocalPreemptedResourceUsage(resourceUsageDelta);
        YT_LOG_DEBUG_UNLESS(
            success,
            "Local commit of preempted usage failed (ResourceUsageDelta: %v, CurrentElement: %v, SourceElement: %v)",
            resourceUsageDelta,
            current->GetId(),
            element->GetId());
        return success;
    };

    if (!commitLocalPreemptedResourceUsage(element.Get())) {
        return false;
    }

    auto* current = element->Parent_.Get();
    while (current) {
        YT_VERIFY(commitLocalPreemptedResourceUsage(current));
        current = current->Parent_.Get();
    }

    return true;
}

void TResourceTree::DoIncreaseHierarchicalPreemptedResourceUsagePrecommit(
    const TResourceTreeElementPtr& element,
    const TJobResources& delta)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_VERIFY(element->Initialized_);

    if (!element->IncreaseLocalPreemptedResourceUsagePrecommit(delta)) {
        return;
    }

    auto* current = element->Parent_.Get();
    while (current) {
        YT_VERIFY(current->IncreaseLocalPreemptedResourceUsagePrecommit(delta));
        current = current->Parent_.Get();
    }
}

void TResourceTree::PerformPostponedActions()
{
    YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

    auto structureGuard = WriterGuard(StructureLock_);

    IncrementStructureLockWriteCount();

    auto elementsToDetach = ElementsToDetachQueue_.DequeueAll();
    for (const auto& element : elementsToDetach) {
        YT_VERIFY(element->Parent_);
        YT_LOG_FATAL_UNLESS(
            element->GetResourceUsageWithPrecommit() == TJobResources(),
            "Resource tree element has non-zero resources (Id: %v, ResourceUsage: %v, ResourceUsageWithPrecommit: %v, ResourceLimitsSpecified: %v)",
            element->GetId(),
            FormatResources(element->GetResourceUsage()),
            FormatResources(element->GetResourceUsageWithPrecommit()),
            element->AreResourceLimitsSpecified());

        element->Parent_ = nullptr;
        EraseOrCrash(AliveElements_, element);
    }
}

void TResourceTree::IncrementStructureLockReadCount()
{
    if (EnableStructureLockProfiling_) {
        StructureLockReadCount_.Increment();
    }
}

void TResourceTree::IncrementStructureLockWriteCount()
{
    if (EnableStructureLockProfiling_) {
        StructureLockWriteCount_.Increment();
    }
}

void TResourceTree::IncrementUsageLockReadCount()
{
    if (EnableUsageLockProfiling_) {
        UsageLockReadCount_.Increment();
    }
}

void TResourceTree::IncrementUsageLockWriteCount()
{
    if (EnableUsageLockProfiling_) {
        UsageLockWriteCount_.Increment();
    }
}

void TResourceTree::DoInitializeResourceUsageFor(
    const TResourceTreeElementPtr& targetElement,
    const std::vector<TResourceTreeElementPtr>& operationElements)
{
    TJobResources newResourceUsage;
    TJobResources newResourceUsagePrecommit;
    TJobResources newPreemptedResourceUsagePrecommit;
    for (auto element : operationElements) {
        auto guard = ReaderGuard(element->ResourceUsageLock_);
        YT_VERIFY(AliveElements_.contains(element));
        newResourceUsage += element->ResourceUsage_;
        newResourceUsagePrecommit += element->ResourceUsagePrecommit_;
        newPreemptedResourceUsagePrecommit += element->PreemptedResourceUsagePrecommit_;
    }

    {
        auto guard = WriterGuard(targetElement->ResourceUsageLock_);
        IncrementUsageLockWriteCount();
        targetElement->ResourceUsage_ = newResourceUsage;
        targetElement->ResourceUsagePrecommit_ = newResourceUsagePrecommit;
        if (UsePrecommitForPreemption_) {
            targetElement->PreemptedResourceUsagePrecommit_ = newPreemptedResourceUsagePrecommit;
        }
    }

    YT_LOG_DEBUG(
        "Resource usage initialized for element in resource tree "
        "(Id: %v, ResourceUsage: %v, ResourceUsagePrecommit: %v, PreemptedResourceUsagePrecommit: %v)",
        targetElement->Id_,
        FormatResources(newResourceUsage),
        FormatResources(newResourceUsagePrecommit),
        FormatResources(newPreemptedResourceUsagePrecommit));
}

TWriterGuard<TReaderWriterSpinLock> TResourceTree::AcquireStructureLock()
{
    return WriterGuard(StructureLock_);
}

void TResourceTree::InitializeResourceUsageFor(
    const TResourceTreeElementPtr& targetElement,
    const std::vector<TResourceTreeElementPtr>& operationElements)
{
    YT_ASSERT_INVOKERS_AFFINITY(FeasibleInvokers_);

    MaybeDelay(ResourceTreeInitializeResourceUsageDelay_, EDelayType::Sync);

    // This method called from Control thread with list of descendant operations elements.
    // All changes of tree structure performed from Control thread, thus we guarantee that
    // all operations are alive.
    DoInitializeResourceUsageFor(targetElement, operationElements);
}

void TResourceTree::ResetPreemptedResourceUsagePrecommit()
{
    for (auto element : AliveElements_) {
        element->ResetLocalPreemptedResourceUsagePrecommit();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
