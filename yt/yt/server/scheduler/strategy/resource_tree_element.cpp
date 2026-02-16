#include "resource_tree_element.h"

#include "resource_tree.h"

#include <yt/yt/server/scheduler/strategy/helpers.h>

namespace NYT::NScheduler::NStrategy {

using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = StrategyLogger;

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

TResourceTreeElement::TDetailedResourceUsage TResourceTreeElement::GetDetailedResourceUsage()
{
    auto guard = ReaderGuard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    return TDetailedResourceUsage{.Base = ResourceUsage_, .Precommit = ResourceUsagePrecommit_};
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
    const TJobResources& overcommitTolerance,
    const std::vector<TResourceTreeElementPtr>& descendantOperations)
{
    // NB: This method is called from Control thread, tree structure supposed to have no changes.
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
        SpecifiedResourceLimitsOvercommitTolerance_ = overcommitTolerance;

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

bool TResourceTreeElement::AreSpecifiedResourceLimitsViolated(bool considerPreemptedPrecommited) const
{
    if (!ResourceLimitsSpecified_) {
        return false;
    }

    auto guard = ReaderGuard(ResourceUsageLock_);

    return AreSpecifiedResourceLimitsViolatedUnsafe(considerPreemptedPrecommited);
}

bool TResourceTreeElement::AreResourceLimitsSpecified() const
{
    return ResourceLimitsSpecified_;
}

// NB(eshcherbin): All detailed logs are temporarily added to find a rare data race with precommit. See: YT-24063.
bool TResourceTreeElement::IncreaseLocalResourceUsagePrecommit(
    const TJobResources& delta,
    bool enableDetailedLogs)
{
    if (Kind_ != EResourceTreeElementKind::Operation && !ResourceLimitsSpecified_) {
        YT_LOG_DEBUG_IF(enableDetailedLogs,
            "Skipping local resource usage precommmit because element has no specified resource limits "
            "(Id: %v, Delta: %v)",
            Id_,
            delta);

        return true;
    }

    auto guard = WriterGuard(ResourceUsageLock_);

    if (!Alive_) {
        YT_LOG_DEBUG_IF(enableDetailedLogs,
            "Unable to increase local resource usage precommit because element is not alive "
            "(Id: %v, Delta: %v)",
            Id_,
            delta);

        return false;
    }

    return IncreaseLocalResourceUsagePrecommitUnsafe(delta, enableDetailedLogs);
}

bool TResourceTreeElement::IncreaseLocalResourceUsagePrecommitUnsafe(
    const TJobResources& delta,
    bool enableDetailedLogs)
{
    ResourceTree_->IncrementUsageLockWriteCount();

    ResourceUsagePrecommit_ += delta;

    YT_LOG_DEBUG_IF(enableDetailedLogs,
        "Successfully increased local resource usage precommit "
        "(Id: %v, Delta: %v, ResourceUsagePrecommit: %v)",
        Id_,
        delta,
        ResourceUsagePrecommit_);

    if (Kind_ == EResourceTreeElementKind::Operation) {
        YT_VERIFY(Dominates(ResourceUsagePrecommit_, TJobResources()));
    } else {
        // There is at most MaxNodeShardCount concurrent attempts to updated resource usage precommit hierarchically,
        // we cannot be sure that it is non-negative but we can limit how negative it can be.
        YT_VERIFY(ResourceUsagePrecommit_.GetUserSlots() >= -MaxNodeShardCount);
    }

    return true;
}

bool TResourceTreeElement::IncreaseLocalPreemptedResourceUsagePrecommit(const TJobResources& delta)
{
    if (Kind_ != EResourceTreeElementKind::Operation && !ResourceLimitsSpecified_) {
        return true;
    }

    auto guard = WriterGuard(ResourceUsageLock_);

    return IncreaseLocalPreemptedResourceUsagePrecommitUnsafe(delta);
}

bool TResourceTreeElement::IncreaseLocalPreemptedResourceUsagePrecommitUnsafe(const TJobResources& delta)
{
    if (Kind_ != EResourceTreeElementKind::Operation && !ResourceLimitsSpecified_) {
        return true;
    }

    if (!Alive_) {
        return false;
    }

    ResourceTree_->IncrementUsageLockWriteCount();

    PreemptedResourceUsagePrecommit_ += delta;

    YT_VERIFY(Dominates(ResourceUsage_, PreemptedResourceUsagePrecommit_));

    return true;
}

void TResourceTreeElement::ResetLocalPreemptedResourceUsagePrecommit()
{
    if (Kind_ != EResourceTreeElementKind::Operation && !ResourceLimitsSpecified_) {
        return;
    }

    auto guard = WriterGuard(ResourceUsageLock_);

    if (!Alive_) {
        return;
    }

    ResourceTree_->IncrementUsageLockWriteCount();

    PreemptedResourceUsagePrecommit_ = TJobResources();

    return;
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

bool TResourceTreeElement::CommitLocalPreemptedResourceUsage(const TJobResources& delta)
{
    if (!ResourceLimitsSpecified_ && Kind_ != EResourceTreeElementKind::Operation) {
        return true;
    }

    auto guard = WriterGuard(ResourceUsageLock_);

    if (!Alive_) {
        return false;
    }

    ResourceTree_->IncrementUsageLockWriteCount();

    ResourceUsage_ -= delta;
    if (Kind_ == EResourceTreeElementKind::Operation || ResourceLimitsSpecified_) {
        PreemptedResourceUsagePrecommit_ -= delta;
    }

    // Sanity check.
    YT_VERIFY(Dominates(ResourceUsage_, TJobResources()));
    YT_VERIFY(Dominates(PreemptedResourceUsagePrecommit_, TJobResources()));

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

void TResourceTreeElement::ReleaseResources(
    TJobResources* usagePrecommit,
    TJobResources* usage,
    TJobResources* preemptedUsagePrecommit)
{
    YT_VERIFY(Kind_ == EResourceTreeElementKind::Operation);

    auto guard = WriterGuard(ResourceUsageLock_);

    YT_VERIFY(!Alive_);

    *usagePrecommit = ResourceUsagePrecommit_;
    *usage = ResourceUsage_;
    *preemptedUsagePrecommit = PreemptedResourceUsagePrecommit_;

    ResourceUsagePrecommit_ = TJobResources();
    ResourceUsage_ = TJobResources();
    PreemptedResourceUsagePrecommit_ = TJobResources();
}

TJobResources TResourceTreeElement::GetResourceUsagePrecommit()
{
    YT_VERIFY(ResourceLimitsSpecified_ || Kind_ == EResourceTreeElementKind::Operation);

    auto guard = ReaderGuard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    return ResourceUsagePrecommit_;
}

TJobResources TResourceTreeElement::GetPreemptedResourceUsagePrecommit()
{
    YT_VERIFY(ResourceLimitsSpecified_ || Kind_ == EResourceTreeElementKind::Operation);

    auto guard = ReaderGuard(ResourceUsageLock_);

    ResourceTree_->IncrementUsageLockReadCount();

    return PreemptedResourceUsagePrecommit_;
}

bool TResourceTreeElement::AreSpecifiedResourceLimitsViolatedUnsafe(bool considerPreemptedPrecommited) const
{
    if (!ResourceLimitsSpecified_) {
        return false;
    }

    if (considerPreemptedPrecommited) {
        return !Dominates(*SpecifiedResourceLimits_, ResourceUsage_ - PreemptedResourceUsagePrecommit_);
    }

    return !Dominates(*SpecifiedResourceLimits_, ResourceUsage_);
}

EResourceTreeIncreaseResult TResourceTreeElement::IncreaseLocalResourceUsagePrecommitWithCheck(
    const TJobResources& delta,
    bool allowLimitsOvercommit,
    TJobResources* availableResourceLimitsOutput)
{
    YT_ASSERT(availableResourceLimitsOutput);
    *availableResourceLimitsOutput = TJobResources::Infinite();

    if (Kind_ != EResourceTreeElementKind::Operation && !ResourceLimitsSpecified_) {
        return EResourceTreeIncreaseResult::Success;
    }

    auto guard = WriterGuard(ResourceUsageLock_);

    return IncreaseLocalResourceUsagePrecommitWithCheckUnsafe(
        delta,
        allowLimitsOvercommit,
        /*additionalLocalResourceLimits*/ std::nullopt,
        availableResourceLimitsOutput);
}

EResourceTreeIncreaseResult TResourceTreeElement::IncreaseLocalResourceUsagePrecommitWithCheckUnsafe(
    const TJobResources& delta,
    bool allowLimitsOvercommit,
    const std::optional<TJobResources>& additionalLocalResourceLimits,
    TJobResources* availableResourceLimitsOutput)
{
    if (!Alive_) {
        return EResourceTreeIncreaseResult::ElementIsNotAlive;
    }

    YT_ASSERT(availableResourceLimitsOutput);
    *availableResourceLimitsOutput = TJobResources::Infinite();
    auto resourceDiscount = allowLimitsOvercommit
        ? SpecifiedResourceLimitsOvercommitTolerance_
        : TJobResources();

    auto checkLimits = [&] (const std::optional<TJobResources>& limits) {
        if (!limits) {
            return true;
        }
        auto availableResourceLimits = ComputeAvailableResources(
            *limits,
            ResourceUsage_ + ResourceUsagePrecommit_,
            resourceDiscount);
        if (!Dominates(availableResourceLimits, delta)) {
            return false;
        }
        *availableResourceLimitsOutput = Min(*availableResourceLimitsOutput, availableResourceLimits);
        return true;
    };

    if (!checkLimits(SpecifiedResourceLimits_)) {
        return EResourceTreeIncreaseResult::ResourceLimitExceeded;
    }
    if (!checkLimits(additionalLocalResourceLimits)) {
        return EResourceTreeIncreaseResult::AdditionalResourceLimitExceeded;
    }

    ResourceUsagePrecommit_ += delta;

    return EResourceTreeIncreaseResult::Success;
}

TWriterGuard<TPaddedReaderWriterSpinLock> TResourceTreeElement::AcquireWriteLock()
{
    return WriterGuard(ResourceUsageLock_);
}

void TResourceTreeElement::MarkInitialized()
{
    Initialized_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
