#include "block_flusher.h"

#include "block_store.h"
#include "config.h"
#include "dirty_block_pool.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NNbd::NJournal {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TBlockFlusher
    : public IBlockFlusher
{
public:
    TBlockFlusher(
        TJournalBlockFlusherConfigPtr config,
        IDirtyBlockPoolPtr dirtyPool,
        IBlockStorePtr blockStore,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , DirtyPool_(std::move(dirtyPool))
        , BlockStore_(std::move(blockStore))
        , Logger(std::move(logger))
        , Invoker_(CreateSerializedInvoker(std::move(invoker)))
        , FlushExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TBlockFlusher::OnFlush, MakeWeak(this)),
            Config_->FlushPeriod))
    { }

    void Start() final
    {
        FlushExecutor_->Start();
    }

    void Stop() final
    {
        YT_UNUSED_FUTURE(FlushExecutor_->Stop());
        Invoker_->Invoke(BIND(&TBlockFlusher::DoStop, MakeStrong(this)));
    }

    void RequestFlush(bool force) final
    {
        if (force || DirtyPool_->GetSize() > GetResidentTargetCount()) {
            FlushExecutor_->ScheduleOutOfBand();
        }
    }

    TFuture<void> RequestFlushBarrier() final
    {
        return BIND(&TBlockFlusher::DoRequestFlushBarrier, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TDirtyBlockPtr& block, TStoredBlockId storedBlockId), BlockFlushed);

private:
    const TJournalBlockFlusherConfigPtr Config_;
    const IDirtyBlockPoolPtr DirtyPool_;
    const IBlockStorePtr BlockStore_;
    const NLogging::TLogger Logger;
    const IInvokerPtr Invoker_;
    const TPeriodicExecutorPtr FlushExecutor_;

    //! Highest dirty id drained (EndDrained) so far, or -1.
    i64 LastDrainedId_ = -1;

    //! Set when a flush fails or the flusher is stopped. This stops any future attempts.
    TError FailedError_;

    struct TPendingFlushBarrier
    {
        i64 FlushBarrierId = -1;
        TPromise<void> Promise;
    };

    //! Flush barriers whose latched tail is not yet drained, in issue order.
    std::vector<TPendingFlushBarrier> PendingFlushBarriers_;

    int GetResidentTargetCount() const
    {
        return static_cast<int>(DirtyPool_->GetCapacity() * Config_->DirtyFractionThreshold);
    }

    void OnFlush()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        auto residentTargetCount = GetResidentTargetCount();

        while (FailedError_.IsOK()) {
            // While a flush barrier is pending -- its latched tail not yet drained -- keep nothing
            // resident; otherwise drain only the excess above the resident fraction.
            int targetDirtyCount = PendingFlushBarriers_.empty()
                ? residentTargetCount
                : 0;
            int excessCount = DirtyPool_->GetSize() - targetDirtyCount;
            if (excessCount <= 0) {
                break;
            }
            if (DrainBatch(excessCount) == 0) {
                break;
            }
        }
    }

    TFuture<void> DoRequestFlushBarrier()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        if (!FailedError_.IsOK()) {
            return MakeFuture(FailedError_);
        }

        int dirtyBlockCount = DirtyPool_->GetSize();
        if (dirtyBlockCount == 0) {
            return OKFuture;
        }

        // The barrier tail is the pool's current tail: head (= LastDrainedId_ + 1) plus the dirty count.
        i64 barrierId = LastDrainedId_ + 1 + dirtyBlockCount;
        auto promise = NewPromise<void>();
        PendingFlushBarriers_.push_back({
            .FlushBarrierId = barrierId,
            .Promise = promise,
        });
        FlushExecutor_->ScheduleOutOfBand();

        return promise.ToFuture();
    }

    void DoStop()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        // Keep an earlier flush error.
        if (FailedError_.IsOK()) {
            FailedError_ = TError("Block flusher is stopped");
        }
        FailPendingFlushBarriers(FailedError_);
        // Nothing will drain the pool again, so a parked writer would wait forever.
        DirtyPool_->Fail(FailedError_);
    }

    void SatisfyPendingFlushBarriers()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        auto headId = LastDrainedId_ + 1;
        std::erase_if(PendingFlushBarriers_, [&] (const TPendingFlushBarrier& barrier) {
            if (barrier.FlushBarrierId > headId) {
                return false;
            }
            barrier.Promise.TrySet();
            return true;
        });
    }

    void FailPendingFlushBarriers(const TError& error)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        auto barriers = std::exchange(PendingFlushBarriers_, {});
        for (const auto& barrier : barriers) {
            barrier.Promise.TrySet(error);
        }
    }

    int DrainBatch(int maxCount)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        auto drainResult = DirtyPool_->BeginDrain(maxCount);
        if (drainResult.empty()) {
            return 0;
        }

        std::vector<TSharedRef> payloads;
        payloads.reserve(drainResult.size());
        for (const auto& block : drainResult) {
            payloads.push_back(block->Payload);
        }

        YT_LOG_DEBUG("Flush started (BlockCount: %v)",
            drainResult.size());

        NProfiling::TWallTimer timer;
        auto blockIdsOrError = WaitFor(BlockStore_->WriteBlocks(payloads));
        if (!blockIdsOrError.IsOK()) {
            // Leave the drained blocks resident in the pool (not EndDrained) so reads still find them.
            YT_LOG_WARNING(blockIdsOrError, "Block flush failed");
            FailedError_ = TError("Block flush failed") << blockIdsOrError;
            FailPendingFlushBarriers(FailedError_);
            // The pool will never free space again, so a parked writer would wait forever.
            DirtyPool_->Fail(FailedError_);
            return 0;
        }

        const auto& storedBlockIds = blockIdsOrError.Value();
        YT_VERIFY(std::ssize(storedBlockIds) == std::ssize(drainResult));
        for (const auto& [block, storedBlockId] : Zip(drainResult, storedBlockIds)) {
            BlockFlushed_.Fire(block, storedBlockId);
        }

        DirtyPool_->EndDrain(drainResult);
        // The pool drains in id order, so the last block carries the highest id drained so far.
        LastDrainedId_ = drainResult.back()->BlockId.Underlying();
        SatisfyPendingFlushBarriers();

        YT_LOG_DEBUG("Flush finished (BlockCount: %v, ElapsedTime: %v)",
            drainResult.size(),
            timer.GetElapsedTime());

        return std::ssize(drainResult);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockFlusherPtr CreateBlockFlusher(
    TJournalBlockFlusherConfigPtr config,
    IDirtyBlockPoolPtr dirtyPool,
    IBlockStorePtr blockStore,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TBlockFlusher>(
        std::move(config),
        std::move(dirtyPool),
        std::move(blockStore),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
