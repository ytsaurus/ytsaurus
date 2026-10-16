#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <functional>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TPendingDownloadsTracker
    : public TRefCounted
{
public:
    explicit TPendingDownloadsTracker(std::function<TFuture<i64>()> getRemainingSpace);

    class TPendingModification
    {
    public:
        TPendingModification(TPendingModification&&) noexcept = default;
        TPendingModification& operator=(const TPendingModification&) = delete;

        void Finish(std::optional<i64> actualSizeDelta = {}) &&;
        void Cancel() &&;

        ~TPendingModification()
        {
            if (Parent_) {
                std::move(*this).Cancel();
            }
        }

    private:
        TPendingDownloadsTrackerPtr Parent_ = nullptr;
        i64 Size_ = 0;

        friend class TPendingDownloadsTracker;
        TPendingModification(TPendingDownloadsTrackerPtr parent, i64 size);
    };

    struct TStats
    {
        i64 ModificationsInProgress;
        i64 RemainingSpaceLastObserved;
        i64 RemainingSpaceEstimateLow;
        i64 RemainingSpaceEstimateHigh;
        TInstant LastObservedAt;
    };

    void InitializeStats(i64 remainingSpace);
    TStats GetStats() const;

    TFuture<std::optional<TPendingModification>> TryBeginDownload(i64 size);
    TFuture<TPendingModification> BeginFree(i64 size);

private:
    const std::function<TFuture<i64>()> GetRemainingSpace_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, SpinLock_);
    TFuture<i64> RemainingSpaceFuture_;
    i64 RemainingSpaceLastObserved_ = 0;
    TInstant LastObservedAt_;
    bool ObservationInProgress_ = false;

    i64 UnconfirmedModifications_ = 0;
    i64 ModificationsInProgress_ = 0;

    i64 BytesDownloading_ = 0;
    i64 BytesFreeing_ = 0;

    i64 AccumulatedDelta_ = 0;

    friend class TPendingModification;
    TFuture<std::optional<TPendingModification>> TryBeginModification(i64 size);
};

DEFINE_REFCOUNTED_TYPE(TPendingDownloadsTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
