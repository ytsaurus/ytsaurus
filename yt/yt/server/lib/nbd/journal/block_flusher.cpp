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
    }

    void RequestFlush(bool force) final
    {
        if (force || DirtyPool_->GetSize() > GetResidentTargetCount()) {
            FlushExecutor_->ScheduleOutOfBand();
        }
    }

    void RequestFlushBarrier() final
    {
        YT_UNUSED_FUTURE(BIND(&TBlockFlusher::DoRequestFlushBarrier, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run());
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

    //! Absolute tail latched by #RequestFlushBarrier: #OnFlush keeps nothing resident until every block below
    //! it has been drained. No eager flush is pending once it falls to LastDrainedId_ + 1 or below; it is
    //! only ever raised (never reset), so its initial -1 is just the first such "not pending" state.
    i64 FlushBarrierId_ = -1;

    //! Raised when flush fails. This stops any future attempts.
    bool Failed_ = false;

    int GetResidentTargetCount() const
    {
        return static_cast<int>(DirtyPool_->GetCapacity() * Config_->DirtyFractionThreshold);
    }

    void OnFlush()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        auto residentTargetCount = GetResidentTargetCount();

        while (!Failed_) {
            // While a flush barrier is pending -- its latched tail not yet drained (LastDrainedId_ +
            // 1 is the head) -- keep nothing resident; otherwise drain only the excess above the
            // resident fraction.
            int targetDirtyCount = FlushBarrierId_ > LastDrainedId_ + 1
                ? 0
                : residentTargetCount;
            int excessCount = DirtyPool_->GetSize() - targetDirtyCount;
            if (excessCount <= 0) {
                break;
            }
            if (DrainBatch(excessCount) == 0) {
                break;
            }
        }
    }

    void DoRequestFlushBarrier()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        // Tail = HeadIndex_ (= LastDrainedId_ + 1) + the resident count.
        FlushBarrierId_ = std::max(FlushBarrierId_, LastDrainedId_ + 1 + DirtyPool_->GetSize());
        FlushExecutor_->ScheduleOutOfBand();
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
            Failed_ = true;
            // TODO(babenko): the pool never frees space again, so writers already parked in
            // IDirtyBlockPool::Put wait forever -- their NBD requests hang instead of failing, and
            // SetError does not cancel them. Fail the outstanding waiters here.
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
