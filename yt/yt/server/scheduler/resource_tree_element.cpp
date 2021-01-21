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

bool TResourceTreeElement::CheckDemand(
    const TJobResources& delta,
    const TJobResources& resourceDemand,
    const TJobResources& resourceDiscount)
{
    auto guard = ReaderGuard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    auto availableDemand = ComputeAvailableResources(
        resourceDemand,
        ResourceUsage_ + ResourceUsagePrecommit_,
        resourceDiscount);

    return Dominates(availableDemand, delta);
}

void TResourceTreeElement::SetResourceLimits(
    const TJobResources& resourceLimits,
    const std::vector<TResourceTreeElementPtr>& descendantOperations)
{
    // NB: this method called from Control thread, tree structure supposed to have no changes.
    bool resourceLimitsSpecifiedBeforeUpdate = ResourceLimitsSpecified_;

    {
        auto guard = WriterGuard(ResourceUsageLock_);

        ResourceTree_->IncrementUsageLockWriteCount();

        ResourceLimits_ = resourceLimits;
        ResourceLimitsSpecified_ = (resourceLimits != TJobResources::Infinite());
        if (!ResourceLimitsSpecified_ && Kind_ != EResourceTreeElementKind::Operation) {
            ResourceUsagePrecommit_ = TJobResources();
        }
    }

    // XXX(ignat): is it safe to have this element with incorrect resource usage?
    if (!resourceLimitsSpecifiedBeforeUpdate && ResourceLimitsSpecified_ && Kind_ != EResourceTreeElementKind::Operation) {
        ResourceTree_->InitializeResourceUsageFor(this, descendantOperations);
    }
}

bool TResourceTreeElement::AreResourceLimitsViolated() const
{
    if (!ResourceLimitsSpecified_) {
        return false;
    }

    auto guard = ReaderGuard(ResourceUsageLock_);

    return !Dominates(ResourceLimits_, ResourceUsage_);
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

    return true;
}

bool TResourceTreeElement::CommitLocalResourceUsage(
    const TJobResources& resourceUsageDelta,
    const TJobResources& precommittedResources)
{
    if (!MaintainInstantResourceUsage_ && !ResourceLimitsSpecified_ && Kind_ != EResourceTreeElementKind::Operation) {
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
    if (!MaintainInstantResourceUsage_ && !ResourceLimitsSpecified_ && Kind_ != EResourceTreeElementKind::Operation) {
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
    if (!MaintainInstantResourceUsage_) {
        YT_VERIFY(ResourceLimitsSpecified_ || Kind_ == EResourceTreeElementKind::Operation);
    }

    auto guard = ReaderGuard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    return ResourceUsagePrecommit_;
}

bool TResourceTreeElement::IncreaseLocalResourceUsagePrecommitWithCheck(
    const TJobResources& delta,
    TJobResources* availableResourceLimitsOutput)
{
    if (!MaintainInstantResourceUsage_ && !ResourceLimitsSpecified_ && Kind_ != EResourceTreeElementKind::Operation) {
        *availableResourceLimitsOutput = TJobResources::Infinite();
        return true;
    }

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

void TResourceTreeElement::SetMaintainInstantResourceUsage(bool value)
{
    auto guard = WriterGuard(ResourceUsageLock_);

    MaintainInstantResourceUsage_ = value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
