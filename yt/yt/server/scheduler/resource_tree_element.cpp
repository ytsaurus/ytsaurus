#include "resource_tree_element.h"
#include "resource_tree.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TResourceTreeElement::TResourceTreeElement(TResourceTree* resourceTree, const TString& id)
    : ResourceTree_(resourceTree)
    , Id_(id)
{ }

TJobResources TResourceTreeElement::GetResourceUsage()
{
    NConcurrency::TReaderGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementResourceUsageLockReadCount();

    return ResourceUsage_;
}

TJobResources TResourceTreeElement::GetResourceUsageWithPrecommit()
{
    NConcurrency::TReaderGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementResourceUsageLockReadCount();

    return ResourceUsage_ + ResourceUsagePrecommit_;
}

bool TResourceTreeElement::CheckDemand(
    const TJobResources& delta,
    const TJobResources& resourceDemand,
    const TJobResources& resourceDiscount)
{
    NConcurrency::TReaderGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementResourceUsageLockReadCount();

    auto availableDemand = ComputeAvailableResources(
        resourceDemand,
        ResourceUsage_ + ResourceUsagePrecommit_,
        resourceDiscount);

    return Dominates(availableDemand, delta);
}

void TResourceTreeElement::SetResourceLimits(TJobResources resourceLimits)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementResourceUsageLockWriteCount();

    ResourceLimits_ = resourceLimits;
}

void TResourceTreeElement::IncreaseLocalResourceUsagePrecommit(const TJobResources& delta)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementResourceUsageLockWriteCount();

    ResourceUsagePrecommit_ += delta;
}

void TResourceTreeElement::CommitLocalResourceUsage(
    const TJobResources& resourceUsageDelta,
    const TJobResources& precommittedResources)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementResourceUsageLockWriteCount();

    ResourceUsage_ += resourceUsageDelta;
    ResourceUsagePrecommit_ -= precommittedResources;
}

void TResourceTreeElement::IncreaseLocalResourceUsage(const TJobResources& delta)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementResourceUsageLockWriteCount();

    ResourceUsage_ += delta;
}

TJobResources TResourceTreeElement::GetResourceUsagePrecommit()
{
    NConcurrency::TReaderGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementResourceUsageLockReadCount();

    return ResourceUsagePrecommit_;
}

bool TResourceTreeElement::IncreaseLocalResourceUsagePrecommitWithCheck(
    const TJobResources& delta,
    TJobResources* availableResourceLimitsOutput)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    // NB: Actually tree elements has resource usage discounts (used for scheduling with preemption)
    // that should be considered in this check. But concurrent nature of this shared tree makes hard to consider
    // these discounts here. The only consequence of discounts ignorance is possibly redundant jobs that would
    // be aborted just after being scheduled.
    auto availableResourceLimits = ComputeAvailableResources(
        ResourceLimits_,
        ResourceUsage_ + ResourceUsagePrecommit_,
        {});

    if (!Dominates(availableResourceLimits, delta)) {
        return false;
    }

    ResourceUsagePrecommit_ += delta;

    *availableResourceLimitsOutput = availableResourceLimits;

    return true;
}

void TResourceTreeElement::MarkInitialized()
{
    Initialized_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
