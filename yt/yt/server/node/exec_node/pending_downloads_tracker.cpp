#include "pending_downloads_tracker.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

TPendingDownloadsTracker::TPendingDownloadsTracker(std::function<TFuture<i64>()> getRemainingSpace)
    : GetRemainingSpace_(std::move(getRemainingSpace))
{ }

void TPendingDownloadsTracker::InitializeStats(i64 remainingSpace)
{
    YT_VERIFY(!LastObservedAt_);
    LastObservedAt_ = TInstant::Now();
    RemainingSpaceLastObserved_ = remainingSpace;
}

TFuture<std::optional<TPendingDownloadsTracker::TPendingModification>> TPendingDownloadsTracker::TryBeginModification(i64 size)
{
    {
        auto guard = Guard(SpinLock_);

        UnconfirmedModifications_ += 1;

        if (UnconfirmedModifications_ == 1 && ModificationsInProgress_ == 0) {
            YT_VERIFY(BytesDownloading_ == 0);
            YT_VERIFY(BytesFreeing_ == 0);

            auto promise = NewPromise<i64>();
            RemainingSpaceFuture_ = promise.ToFuture().ToUncancelable();

            YT_VERIFY(!ObservationInProgress_);
            ObservationInProgress_ = true;

            guard.Release();
            promise.SetFrom(GetRemainingSpace_());
        }
    }

    // Since we increased UnconfirmedModifications_ previously, RemainingSpaceFuture_ won't be modified
    // by another thread. Hence there is no need for guarding this access with a spinlock.
    auto remainingSpaceFuture = RemainingSpaceFuture_;

    return remainingSpaceFuture.Apply(BIND([
        this,
        this_ = MakeStrong(this),
        size
    ] (const TErrorOr<i64>& remainingSpaceOrError) -> std::optional<TPendingModification> {
        auto guard = Guard(SpinLock_);

        UnconfirmedModifications_ -= 1;
        bool isFirstObserver = std::exchange(ObservationInProgress_, false);

        auto remainingSpace = remainingSpaceOrError.ValueOrThrow();

        if (isFirstObserver) {
            LastObservedAt_ = TInstant::Now();
            RemainingSpaceLastObserved_ = remainingSpace;
            AccumulatedDelta_ = 0;
        }

        if (size > 0) {
            if (size > RemainingSpaceLastObserved_ - AccumulatedDelta_ - BytesDownloading_) {
                return std::nullopt;
            }
            BytesDownloading_ += size;
        } else {
            BytesFreeing_ += -size;
        }
        ModificationsInProgress_ += 1;
        return TPendingModification(MakeStrong(this), size);
    }));
}

TFuture<std::optional<TPendingDownloadsTracker::TPendingModification>> TPendingDownloadsTracker::TryBeginDownload(i64 size)
{
    YT_VERIFY(size >= 0);
    return TryBeginModification(size);
}

TFuture<TPendingDownloadsTracker::TPendingModification> TPendingDownloadsTracker::BeginFree(i64 size)
{
    YT_VERIFY(size >= 0);
    return TryBeginModification(-size)
        .AsUnique()
        .Apply(BIND([] (std::optional<TPendingModification>&& download) {
            YT_VERIFY(download.has_value());
            return *std::move(download);
        }));
}

TPendingDownloadsTracker::TStats TPendingDownloadsTracker::GetStats() const
{
    auto guard = Guard(SpinLock_);
    return {
        .ModificationsInProgress = ModificationsInProgress_,
        .RemainingSpaceLastObserved = RemainingSpaceLastObserved_,
        .RemainingSpaceEstimateLow = RemainingSpaceLastObserved_ - AccumulatedDelta_ - BytesDownloading_,
        .RemainingSpaceEstimateHigh = RemainingSpaceLastObserved_ - AccumulatedDelta_ + BytesFreeing_,
        .LastObservedAt = LastObservedAt_,
    };
}

////////////////////////////////////////////////////////////////////////////////

TPendingDownloadsTracker::TPendingModification::TPendingModification(TPendingDownloadsTrackerPtr parent, i64 size)
    : Parent_(std::move(parent))
    , Size_(size)
{ }

void TPendingDownloadsTracker::TPendingModification::Finish(std::optional<i64> actualSizeDelta) &&
{
    YT_VERIFY(Parent_);
    {
        auto guard = Guard(Parent_->SpinLock_);
        Parent_->AccumulatedDelta_ += actualSizeDelta.value_or(Size_);
        Parent_->ModificationsInProgress_ -= 1;
        if (Size_ > 0) {
            Parent_->BytesDownloading_ -= Size_;
        } else {
            Parent_->BytesFreeing_ -= -Size_;
        }
    }
    Parent_ = nullptr;
}

void TPendingDownloadsTracker::TPendingModification::Cancel() &&
{
    YT_VERIFY(Parent_);
    {
        auto guard = Guard(Parent_->SpinLock_);
        Parent_->ModificationsInProgress_ -= 1;
        if (Size_ > 0) {
            Parent_->BytesDownloading_ -= Size_;
        } else {
            Parent_->BytesFreeing_ -= -Size_;
        }
    }
    Parent_ = nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
