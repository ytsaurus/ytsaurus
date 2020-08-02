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

    ResourceTree_->IncrementUsageLockReadCount();

    return ResourceUsage_;
}

TJobResources TResourceTreeElement::GetResourceUsageWithPrecommit()
{
    NConcurrency::TReaderGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    return ResourceUsage_ + ResourceUsagePrecommit_;
}

bool TResourceTreeElement::CheckDemand(
    const TJobResources& delta,
    const TJobResources& resourceDemand,
    const TJobResources& resourceDiscount)
{
    NConcurrency::TReaderGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    auto availableDemand = ComputeAvailableResources(
        resourceDemand,
        ResourceUsage_ + ResourceUsagePrecommit_,
        resourceDiscount);

    return Dominates(availableDemand, delta);
}

void TResourceTreeElement::SetResourceLimits(TJobResources resourceLimits)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockWriteCount();

    ResourceLimits_ = resourceLimits;
}

bool TResourceTreeElement::IncreaseLocalResourceUsagePrecommit(const TJobResources& delta)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    if (!Alive_) {
        return false;
    }

    ResourceTree_->IncrementUsageLockWriteCount();

    ResourceUsagePrecommit_ += delta;

    return true;
}

bool TResourceTreeElement::CommitLocalResourceUsage(
    const TJobResources& resourceUsageDelta,
    const TJobResources& precommittedResources)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    if (!Alive_) {
        return false;
    }

    ResourceTree_->IncrementUsageLockWriteCount();

    ResourceUsage_ += resourceUsageDelta;
    ResourceUsagePrecommit_ -= precommittedResources;

    return true;
}

bool TResourceTreeElement::IncreaseLocalResourceUsage(const TJobResources& delta)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    if (!Alive_) {
        return false;
    }

    ResourceTree_->IncrementUsageLockWriteCount();

    ResourceUsage_ += delta;

    return true;
}

void TResourceTreeElement::ReleaseResources(TJobResources* usagePrecommit, TJobResources* usage)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    YT_VERIFY(!Alive_);

    *usagePrecommit = ResourceUsagePrecommit_;
    *usage = ResourceUsage_;

    ResourceUsagePrecommit_ = TJobResources();
    ResourceUsage_ = TJobResources();
}

TJobResources TResourceTreeElement::GetResourceUsagePrecommit()
{
    NConcurrency::TReaderGuard guard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    return ResourceUsagePrecommit_;
}

bool TResourceTreeElement::IncreaseLocalResourceUsagePrecommitWithCheck(
    const TJobResources& delta,
    TJobResources* availableResourceLimitsOutput)
{
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);
    
    if (!Alive_) {
        return false;
    }

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
