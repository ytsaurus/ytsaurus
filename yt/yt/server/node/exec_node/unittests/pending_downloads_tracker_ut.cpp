#include "yt/yt/server/node/exec_node/pending_downloads_tracker.h"

#include <yt/yt/core/concurrency/async_barrier.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <gtest/gtest.h>

namespace NYT::NExecNode {

using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TPendingDownloadsTrackerTest
    : public ::testing::Test
{
public:
    TPendingDownloadsTrackerTest(i64 initialFreeSpace = 0)
        : Tracker_(New<TPendingDownloadsTracker>([this] { return FreeSpacePromise_.Load().ToFuture(); }, initialFreeSpace))
    { }

protected:
    alignas(128) const TPendingDownloadsTrackerPtr Tracker_;

    NThreading::TAtomicObject<TPromise<i64>> FreeSpacePromise_ = NewPromise<i64>();
};

TEST_F(TPendingDownloadsTrackerTest, JustWorks)
{
    auto downloadFuture = Tracker_->TryBeginDownload(10);
    EXPECT_FALSE(downloadFuture.IsSet());
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateLow, 0);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateHigh, 0);

    FreeSpacePromise_.Load().Set(10);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceLastObserved, 10);
    EXPECT_EQ(Tracker_->GetStats().ModificationsInProgress, 1);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateLow, 0);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateHigh, 10);

    auto download = downloadFuture.AsUnique().GetOrCrash().ValueOrCrash().value();

    auto downloadFuture2 = Tracker_->TryBeginDownload(1);
    EXPECT_TRUE(downloadFuture2.IsSet());
    EXPECT_EQ(downloadFuture2.GetOrCrash().ValueOrCrash(), std::nullopt);

    Tracker_->BeginFree(1)
        .AsUnique()
        .GetOrCrash()
        .ValueOrCrash()
        .Finish();

    downloadFuture2 = Tracker_->TryBeginDownload(1);
    EXPECT_TRUE(downloadFuture2.IsSet());
    auto download2 = GetOrCrash(
        downloadFuture2
            .AsUnique()
            .GetOrCrash()
            .ValueOrCrash());

    EXPECT_EQ(Tracker_->GetStats().ModificationsInProgress, 2);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateLow, 0);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateHigh, 11);

    std::move(download).Finish();
    EXPECT_EQ(Tracker_->GetStats().ModificationsInProgress, 1);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateLow, 0);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateHigh, 1);

    std::move(download2).Cancel();
    EXPECT_EQ(Tracker_->GetStats().ModificationsInProgress, 0);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateLow, 1);
    EXPECT_EQ(Tracker_->GetStats().RemainingSpaceEstimateHigh, 1);
}

class TPendingDownloadsTrackerStressTest
    : public TPendingDownloadsTrackerTest
{
public:
    TPendingDownloadsTrackerStressTest()
        : TPendingDownloadsTrackerTest(InitialDiskSize_)
    { }

protected:
    struct TDownload
    {
        TPendingDownloadsTracker::TPendingModification Handle;
        int FullSize;
        int CurrentSize = 0;
        bool IsFinished = false;
        bool IsTombstone = false;
    };

    static constexpr int InitialDiskSize_ = 20;
    static constexpr int FileLimit_ = 5;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::minstd_rand Urbg_{std::random_device{}()};
    std::map<i64, TDownload> Downloads_;
    i64 IndexCounter_ = 0;
    bool DiskIsLagging_ = false;
    int DiskSize_ = InitialDiskSize_;
    bool RecentlyResized_ = false;
    int PendingModifications_ = 0;
    int PendingDownloadSize_ = 0;

    void InvalidateFreeSpace(const TGuard<NThreading::TSpinLock>&)
    {
        if (FreeSpacePromise_.Load().IsSet()) {
            FreeSpacePromise_.Store(NewPromise<i64>());
        }
    }

    int RecalculateFreeSpace(TGuard<NThreading::TSpinLock>&& guard)
    {
        int freeSpace = DiskSize_;
        for (const auto& [_, download] : Downloads_) {
            EXPECT_LE(download.CurrentSize, download.FullSize);
            freeSpace -= download.CurrentSize;
        }
        EXPECT_GE(freeSpace, 0);

        auto stats = Tracker_->GetStats();
        if (PendingModifications_ == 0 && !RecentlyResized_) {
            EXPECT_LE(stats.RemainingSpaceEstimateLow, freeSpace);
            EXPECT_GE(stats.RemainingSpaceEstimateHigh, freeSpace);
        }
        auto unfinishedCount = std::ranges::count_if(
            Downloads_,
            std::not_fn(&TDownload::IsFinished),
            &decltype(Downloads_)::value_type::second);
        EXPECT_GE(stats.ModificationsInProgress, unfinishedCount);
        EXPECT_LE(stats.ModificationsInProgress, unfinishedCount + PendingModifications_);

        InvalidateFreeSpace(guard);

        if (!DiskIsLagging_) {
            auto promise = FreeSpacePromise_.Load();
            promise.Set(freeSpace);
        }

        std::move(guard).Release();
        return freeSpace;
    }

    int GetMaxDownloadSize(const TGuard<NThreading::TSpinLock>&)
    {
        int freeSpace = DiskSize_ - PendingDownloadSize_;
        for (const auto& [_, download] : Downloads_) {
            freeSpace -= download.FullSize;
        }
        EXPECT_GE(freeSpace, 0);
        return freeSpace;
    }

    void ToggleDiskLag()
    {
        auto guard = Guard(Lock_);
        DiskIsLagging_ = !DiskIsLagging_;
        RecalculateFreeSpace(std::move(guard));
    }

    void AdvanceSomeDownload()
    {
        auto guard = Guard(Lock_);
        if (Downloads_.empty()) {
            return;
        }

        auto& candidate = std::next(
            Downloads_.begin(),
            std::uniform_int_distribution<int>(0, std::ssize(Downloads_) - 1)(Urbg_))
            ->second;

        if (candidate.IsFinished || candidate.IsTombstone) {
            return;
        }

        auto delta = std::uniform_int_distribution<int>(1, candidate.FullSize - candidate.CurrentSize)(Urbg_);
        candidate.CurrentSize += delta;

        EXPECT_LE(candidate.CurrentSize, candidate.FullSize);
        if (candidate.CurrentSize == candidate.FullSize) {
            candidate.IsFinished = true;

            InvalidateFreeSpace(guard);
            std::move(candidate.Handle).Finish();
        }

        RecalculateFreeSpace(std::move(guard));
    }

    void StartNewDownload(bool singleThreaded)
    {
        int size;
        int maxSize;
        int freeSpaceBefore;
        {
            auto guard = Guard(Lock_);
            maxSize = GetMaxDownloadSize(guard);
            if (std::ssize(Downloads_) >= FileLimit_ || maxSize == 0) {
                return;
            }
            size = std::uniform_int_distribution<int>(1, !singleThreaded || RecentlyResized_ ? maxSize : maxSize * 2)(Urbg_);
            PendingModifications_ += 1;
            PendingDownloadSize_ += size;
            freeSpaceBefore = RecalculateFreeSpace(std::move(guard));
        }

        auto oldStats = Tracker_->GetStats();

        auto maybeHandle = WaitFor(
            Tracker_->TryBeginDownload(size)
                .AsUnique())
            .ValueOrCrash();

        Yield();

        auto guard = Guard(Lock_);
        PendingModifications_ -= 1;
        PendingDownloadSize_ -= size;

        if (singleThreaded && !RecentlyResized_) {
            EXPECT_EQ(maybeHandle.has_value(), size <= maxSize);
        }

        if (Downloads_.empty()) {
            RecentlyResized_ = false;
        }

        if (singleThreaded) {
            auto newStats = Tracker_->GetStats();
            if (std::ranges::all_of(
                    Downloads_,
                    &TDownload::IsFinished,
                    &decltype(Downloads_)::value_type::second))
            {
                EXPECT_EQ(newStats.RemainingSpaceLastObserved, freeSpaceBefore);
            } else {
                EXPECT_EQ(oldStats.LastObservedAt, newStats.LastObservedAt);
            }
        }

        if (!maybeHandle) {
            return;
        }

        EmplaceOrCrash(
            Downloads_,
            IndexCounter_++,
            TDownload{
                .Handle = std::move(*maybeHandle),
                .FullSize = size,
            });

        RecalculateFreeSpace(std::move(guard));
    }

    void DeleteSomeFile(bool explicitCancel)
    {
        auto guard = Guard(Lock_);
        if (Downloads_.empty()) {
            return;
        }

        auto& [candidateIndex, candidate] = *std::next(
            Downloads_.begin(),
            std::uniform_int_distribution<int>(0, std::ssize(Downloads_) - 1)(Urbg_));

        if (candidate.IsTombstone) {
            return;
        }

        bool wasFinished = candidate.IsFinished;
        candidate.IsFinished = true;
        candidate.IsTombstone = true;

        PendingModifications_ += 1;
        RecalculateFreeSpace(std::move(guard));

        if (!wasFinished) {
            auto guard2 = Guard(Lock_);

            InvalidateFreeSpace(guard);
            if (explicitCancel) {
                std::move(candidate.Handle).Cancel();
            }

            PendingModifications_ -= 1;
            EraseOrCrash(Downloads_, candidateIndex);

            RecalculateFreeSpace(std::move(guard2));
            return;
        }

        auto handle = WaitFor(
            Tracker_->BeginFree(candidate.FullSize)
                .AsUnique())
            .ValueOrCrash();

        Yield();

        auto guard2 = Guard(Lock_);
        EraseOrCrash(Downloads_, candidateIndex);
        PendingModifications_ -= 1;

        InvalidateFreeSpace(guard);
        std::move(handle).Finish();

        RecalculateFreeSpace(std::move(guard2));
    }

    void ResizeDisk()
    {
        auto guard = Guard(Lock_);
        RecentlyResized_ = true;
        DiskSize_ = std::uniform_int_distribution<int>(std::max(InitialDiskSize_ / 2, DiskSize_ - GetMaxDownloadSize(guard)), InitialDiskSize_ * 3 / 2)(Urbg_);
        RecalculateFreeSpace(std::move(guard));
    }

    int RollD20()
    {
        auto guard = Guard(Lock_);
        return std::uniform_int_distribution<int>(1, 20)(Urbg_);
    }
};

TEST_F(TPendingDownloadsTrackerStressTest, StressSingleThreaded)
{
    constexpr auto timeLimit = TDuration::Seconds(10);
    const auto t0 = TInstant::Now();

    while (TInstant::Now() - t0 < timeLimit) {
        int d20 = RollD20();

        if (d20 == 20) {
            DeleteSomeFile(true);
        } else if (d20 == 19) {
            DeleteSomeFile(false);
        } else if (d20 >= 16) {
            StartNewDownload(true);
        } else if (d20 > 1) {
            AdvanceSomeDownload();
        } else {
            ResizeDisk();
        }
    }
}

TEST_F(TPendingDownloadsTrackerStressTest, StressMultiThreaded)
{
    constexpr auto timeLimit = TDuration::Seconds(10);
    const auto t0 = TInstant::Now();

    constexpr int threadCount = 4;
    auto threadPool = CreateThreadPool(threadCount, "test");

    auto barrier = TAsyncBarrier();

    for (int i = 0; i < threadCount; ++i) {
        threadPool->GetInvoker()->Invoke(BIND([&, i] (TAsyncBarrierCookie cookie) {
            while (TInstant::Now() - t0 < timeLimit) {
                int d20 = RollD20();

                if (d20 == 20 && i != 0) {
                    DeleteSomeFile(true);
                } else if (d20 == 19 && i != 0) {
                    DeleteSomeFile(false);
                } else if (d20 >= 16 && i != 0) {
                    StartNewDownload(false);
                } else if (d20 > 1) {
                    AdvanceSomeDownload();
                } else if (i == 0) {
                    ToggleDiskLag();
                }

                Yield();
            }

            if (i == 0) {
                auto guard = Guard(Lock_);
                DiskIsLagging_ = false;
                RecalculateFreeSpace(std::move(guard));
            }

            barrier.Remove(std::move(cookie));
        }, barrier.Insert()));
    }

    WaitFor(barrier.GetBarrierFuture())
        .ThrowOnError();

    EXPECT_EQ(PendingModifications_, 0);
    EXPECT_EQ(PendingDownloadSize_, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
