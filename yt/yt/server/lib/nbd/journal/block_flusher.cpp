#include "block_flusher.h"

#include "block_store.h"
#include "config.h"
#include "dirty_block_pool.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NNbd::NJournal {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

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
        , FlushExecutor_(New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TBlockFlusher::OnFlush, MakeWeak(this)),
            Config_->FlushPeriod))
    { }

    void Start() override
    {
        FlushExecutor_->Start();
    }

    void Stop() override
    {
        YT_UNUSED_FUTURE(FlushExecutor_->Stop());
    }

    void RequestFlush() override
    {
        FlushExecutor_->ScheduleOutOfBand();
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TDirtyBlockPtr& block, TStoredBlockId storedBlockId), BlockFlushed);

    void SubscribeFailed(const TCallback<void(const TError&)>& callback) override
    {
        Failed_.Subscribe(callback);
    }

    void UnsubscribeFailed(const TCallback<void(const TError&)>& callback) override
    {
        Failed_.Unsubscribe(callback);
    }

private:
    const TJournalBlockFlusherConfigPtr Config_;
    const IDirtyBlockPoolPtr DirtyPool_;
    const IBlockStorePtr BlockStore_;
    const NLogging::TLogger Logger;
    const TPeriodicExecutorPtr FlushExecutor_;

    TSingleShotCallbackList<void(const TError&)> Failed_;

    bool HasFailed() const
    {
        return Failed_.IsFired();
    }

    void OnFlush()
    {
        auto targetDirtyCount = static_cast<int>(
            DirtyPool_->GetCapacity() * Config_->DirtyFractionThreshold);

        while (!HasFailed()) {
            int excessCount = DirtyPool_->GetSize() - targetDirtyCount;
            if (excessCount <= 0) {
                break;
            }

            auto drainResult = DirtyPool_->BeginDrain(excessCount);
            if (drainResult.empty()) {
                break;
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
                // Leave the drained blocks resident in the pool (not EndDrained) so reads still find
                // them; the Failed signal lets the device fail itself.
                auto error = TError("Block flush failed") << blockIdsOrError;
                YT_LOG_WARNING(error);
                Failed_.Fire(error);
                return;
            }

            const auto& storedBlockIds = blockIdsOrError.Value();
            YT_VERIFY(std::ssize(storedBlockIds) == std::ssize(drainResult));
            for (const auto& [block, storedBlockId] : Zip(drainResult, storedBlockIds)) {
                BlockFlushed_.Fire(block, storedBlockId);
            }

            DirtyPool_->EndDrain(drainResult);

            YT_LOG_DEBUG("Flush finished (BlockCount: %v, ElapsedTime: %v)",
                drainResult.size(),
                timer.GetElapsedTime());
        }
    }
};

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
