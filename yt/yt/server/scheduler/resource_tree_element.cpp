#include "resource_tree_element.h"

#include "resource_tree.h"

namespace NYT::NScheduler {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TResourceTreeElement::TResourceTreeElement(
    TResourceTree* resourceTree,
    const TString& id,
    EResourceTreeElementKind elementKind)
    : ResourceTree_(resourceTree)
    , Id_(id)
    , Kind_(elementKind)
{ }

TJobResources TResourceTreeElement::GetResourceUsage()
{
    auto guard = ReaderGuard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    return ResourceUsage_;
}

TJobResources TResourceTreeElement::GetResourceUsageWithPrecommit()
{
    auto guard = ReaderGuard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    return ResourceUsage_ + ResourceUsagePrecommit_;
}

bool TResourceTreeElement::CheckAvailableDemand(
    const TJobResources& delta,
    const TJobResources& resourceDemand)
{
    auto guard = ReaderGuard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    auto availableDemand = ComputeAvailableResources(
        resourceDemand,
        ResourceUsage_ + ResourceUsagePrecommit_,
        /*resourceDiscount*/ {});

    return Dominates(availableDemand, delta);
}

void TResourceTreeElement::SetSpecifiedResourceLimits(
    const std::optional<TJobResources>& specifiedResourceLimits,
    const std::vector<TResourceTreeElementPtr>& descendantOperations)
{
    // NB: This method called from Control thread, tree structure supposed to have no changes.
    bool resourceLimitsSpecifiedBeforeUpdate = ResourceLimitsSpecified_;
    bool resourceLimitsSpecified = specifiedResourceLimits.has_value();
    bool shouldInitializeResourceUsage =
        !resourceLimitsSpecifiedBeforeUpdate &&
        resourceLimitsSpecified &&
        Kind_ != EResourceTreeElementKind::Operation;

    auto setResourceLimits = [&] {
        auto guard = WriterGuard(ResourceUsageLock_);

        ResourceTree_->IncrementUsageLockWriteCount();

        SpecifiedResourceLimits_ = specifiedResourceLimits;
        if (!SpecifiedResourceLimits_ && Kind_ != EResourceTreeElementKind::Operation) {
            ResourceUsagePrecommit_ = TJobResources();
            ResourceUsage_ = TJobResources();
        }

        // NB(eshcherbin): Store this to a separate atomic to be able to check without taking the lock.
        ResourceLimitsSpecified_ = specifiedResourceLimits.has_value();
    };

    if (shouldInitializeResourceUsage) {
        auto guard = ResourceTree_->AcquireStructureLock();
        setResourceLimits();
        ResourceTree_->InitializeResourceUsageFor(this, descendantOperations);
    } else {
        setResourceLimits();
    }
}

bool TResourceTreeElement::AreSpecifiedResourceLimitsViolated() const
{
    if (!ResourceLimitsSpecified_) {
        return false;
    }

    auto guard = ReaderGuard(ResourceUsageLock_);

    return !Dominates(*SpecifiedResourceLimits_, ResourceUsage_);
}

bool TResourceTreeElement::AreResourceLimitsSpecified() const
{
    return ResourceLimitsSpecified_;
}

bool TResourceTreeElement::IncreaseLocalResourceUsagePrecommit(const TJobResources& delta)
{
    if (Kind_ != EResourceTreeElementKind::Operation && !ResourceLimitsSpecified_) {
        return true;
    }

    auto guard = WriterGuard(ResourceUsageLock_);

    if (!Alive_) {
        return false;
    }

    ResourceTree_->IncrementUsageLockWriteCount();

    ResourceUsagePrecommit_ += delta;

    if (Kind_ == EResourceTreeElementKind::Operation) {
        YT_VERIFY(Dominates(ResourceUsagePrecommit_, TJobResources()));
    } else {
        // There is at most MaxNodeShardCount concurrent attempts to updated resource usage precommit hierarchically,
        // we cannot be sure that it is non-negative but we can limit how negative it can be.
        YT_VERIFY(ResourceUsagePrecommit_.GetUserSlots() >= -MaxNodeShardCount);
    }

    return true;
}

bool TResourceTreeElement::CommitLocalResourceUsage(
    const TJobResources& resourceUsageDelta,
    const TJobResources& precommittedResources)
{
    if (!ResourceLimitsSpecified_ && Kind_ != EResourceTreeElementKind::Operation) {
        return true;
    }

    auto guard = WriterGuard(ResourceUsageLock_);

    if (!Alive_) {
        return false;
    }

    ResourceTree_->IncrementUsageLockWriteCount();

    ResourceUsage_ += resourceUsageDelta;
    if (Kind_ == EResourceTreeElementKind::Operation || ResourceLimitsSpecified_) {
        // We can try to subtract some excessive resource usage precommit, if precommit was added before resource limits were set.
        ResourceUsagePrecommit_ = Max(TJobResources(), ResourceUsagePrecommit_ - precommittedResources);
    }

    YT_VERIFY(Dominates(ResourceUsage_, TJobResources()));

    return true;
}

bool TResourceTreeElement::IncreaseLocalResourceUsage(const TJobResources& delta)
{
    if (!ResourceLimitsSpecified_ && Kind_ != EResourceTreeElementKind::Operation) {
        return true;
    }

    auto guard = WriterGuard(ResourceUsageLock_);

    if (!Alive_) {
        return false;
    }

    ResourceTree_->IncrementUsageLockWriteCount();

    ResourceUsage_ += delta;

    YT_VERIFY(Dominates(ResourceUsage_, TJobResources()));

    return true;
}

void TResourceTreeElement::ReleaseResources(TJobResources* usagePrecommit, TJobResources* usage)
{
    YT_VERIFY(Kind_ == EResourceTreeElementKind::Operation);

    auto guard = WriterGuard(ResourceUsageLock_);

    YT_VERIFY(!Alive_);

    *usagePrecommit = ResourceUsagePrecommit_;
    *usage = ResourceUsage_;

    ResourceUsagePrecommit_ = TJobResources();
    ResourceUsage_ = TJobResources();
}

TJobResources TResourceTreeElement::GetResourceUsagePrecommit()
{
    YT_VERIFY(ResourceLimitsSpecified_ || Kind_ == EResourceTreeElementKind::Operation);

    auto guard = ReaderGuard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    return ResourceUsagePrecommit_;
}

bool TResourceTreeElement::IncreaseLocalResourceUsagePrecommitWithCheck(
    const TJobResources& delta,
    TJobResources* availableResourceLimitsOutput)
{
    if (Kind_ != EResourceTreeElementKind::Operation && !ResourceLimitsSpecified_) {
        *availableResourceLimitsOutput = TJobResources::Infinite();
        return true;
    }

    auto guard = WriterGuard(ResourceUsageLock_);

    if (!Alive_) {
        return false;
    }

    // NB: Actually tree elements has resource usage discounts (used for scheduling with preemption)
    // that should be considered in this check. But concurrent nature of this shared tree makes hard to consider
    // these discounts here. The only consequence of discounts ignorance is possibly redundant allocations that would
    // be aborted just after being scheduled.
    *availableResourceLimitsOutput = TJobResources::Infinite();
    if (SpecifiedResourceLimits_) {
        auto availableResourceLimits = ComputeAvailableResources(
            *SpecifiedResourceLimits_,
            ResourceUsage_ + ResourceUsagePrecommit_,
            {});
        if (!Dominates(availableResourceLimits, delta)) {
            return false;
        }

        *availableResourceLimitsOutput = availableResourceLimits;
    }

    ResourceUsagePrecommit_ += delta;

    return true;
}

void TResourceTreeElement::MarkInitialized()
{
    Initialized_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
