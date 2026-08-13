#include "block_compactor.h"

#include "block_map.h"
#include "block_store.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/concurrency/retrying_periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

#include <util/generic/hash_set.h>

namespace NYT::NNbd::NJournal {

using namespace NChunkClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TBlockCompactor
    : public IBlockCompactor
{
public:
    TBlockCompactor(
        TJournalBlockCompactorConfigPtr config,
        IBlockMapPtr blockMap,
        IBlockStorePtr blockStore,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , BlockMap_(std::move(blockMap))
        , BlockStore_(std::move(blockStore))
        , Logger(std::move(logger))
        , CompactionInvoker_(std::move(invoker))
        , SerializedInvoker_(CreateSerializedInvoker(CompactionInvoker_))
        , Throttler_(CreateReconfigurableThroughputThrottler(Config_->ThroughputThrottler, Logger))
        , ScanExecutor_(New<TRetryingPeriodicExecutor>(
            SerializedInvoker_,
            BIND([weakThis = MakeWeak(this)] {
                auto this_ = weakThis.Lock();
                return this_ ? this_->OnScan() : TError();
            }),
            Config_->Backoff,
            Config_->ScanPeriod))
    { }

    void Start() final
    {
        ScanExecutor_->Start();
    }

    void Stop() final
    {
        YT_UNUSED_FUTURE(ScanExecutor_->Stop());
    }

private:
    const TJournalBlockCompactorConfigPtr Config_;
    const IBlockMapPtr BlockMap_;
    const IBlockStorePtr BlockStore_;
    const NLogging::TLogger Logger;
    //! Runs the chunk compactions concurrently; the scan (and all shared-state access) stays serialized
    //! on #SerializedInvoker_.
    const IInvokerPtr CompactionInvoker_;
    const IInvokerPtr SerializedInvoker_;
    const IThroughputThrottlerPtr Throttler_;
    const TRetryingPeriodicExecutorPtr ScanExecutor_;

    //! Chunk ids with a compaction in flight; kept off the candidate list and capped at
    //! #MaxConcurrentCompactions.
    THashSet<TChunkId> RunningCompactionChunkIds_;

    //! Outcome of the last finished compaction.
    TError LastCompactionError_;

    static double GetGarbageRatio(const TChunkInfo& info)
    {
        return info.WrittenBlockCount > 0
            ? 1.0 - static_cast<double>(info.ReferencedBlockCount) / info.WrittenBlockCount
            : 0.0;
    }

    //! A sealed chunk with live blocks worth relocating. Restored chunks qualify too: pinned by their
    //! snapshot, they never go fully dead on their own, so compaction is the only way to drain them.
    bool IsCompactable(const TChunkInfo& info) const
    {
        return
            info.SealState == EChunkSealState::Done &&
            info.ReferencedBlockCount > 0 &&
            GetGarbageRatio(info) >= Config_->GarbageRatioThreshold;
    }

    //! The compactable chunk with the most garbage that is not already being compacted, or null if none.
    std::optional<TChunkInfo> PickBestCompactionCandidate()
    {
        std::optional<TChunkInfo> best;
        for (const auto& info : BlockStore_->GetChunkInfos()) {
            if (RunningCompactionChunkIds_.contains(info.ChunkId)) {
                continue;
            }
            if (IsCompactable(info) && (!best || GetGarbageRatio(info) > GetGarbageRatio(*best))) {
                best = info;
            }
        }
        return best;
    }

    /*!
     *  Compactions run asynchronously, so the executor learns of a failed one only here, on a later tick.
     *  The latched error is reported until a compaction succeeds: the executor restarts #Backoff the
     *  moment a tick returns OK, so reporting a failure once would pin the delay at the minimum.
     */
    TError OnScan()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        bool launched = false;
        if (std::ssize(RunningCompactionChunkIds_) < Config_->MaxConcurrentCompactions) {
            if (auto candidate = PickBestCompactionCandidate()) {
                InsertOrCrash(RunningCompactionChunkIds_, candidate->ChunkId);
                BIND(&TBlockCompactor::CompactChunk, MakeStrong(this), *candidate)
                    .AsyncVia(CompactionInvoker_)
                    .Run()
                    .Subscribe(BIND(&TBlockCompactor::OnCompactionFinished, MakeStrong(this), candidate->ChunkId)
                        .Via(SerializedInvoker_));
                launched = true;
            }
        }
        // With nothing left to compact there is nothing to pace, so drop the latch rather than idle
        // at the maximum backoff forever.
        if (!launched && RunningCompactionChunkIds_.empty()) {
            LastCompactionError_ = {};
        }
        return LastCompactionError_;
    }

    void OnCompactionFinished(TChunkId chunkId, const TError& error)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(SerializedInvoker_);

        EraseOrCrash(RunningCompactionChunkIds_, chunkId);
        LastCompactionError_ = error;
        if (!error.IsOK()) {
            YT_LOG_WARNING(error, "Block store chunk compaction failed (ChunkId: %v)",
                chunkId);
        }
    }

    void CompactChunk(const TChunkInfo& chunkInfo)
    {
        auto blocks = BlockMap_->GetChunkBlocks(chunkInfo.ChunkIndex);

        YT_LOG_INFO("Started compacting block store chunk (ChunkId: %v, ReferencedBlockCount: %v, WrittenBlockCount: %v, "
            "GarbageRatio: %.2f, RelocatableBlockCount: %v)",
            chunkInfo.ChunkId,
            chunkInfo.ReferencedBlockCount,
            chunkInfo.WrittenBlockCount,
            GetGarbageRatio(chunkInfo),
            blocks.size());

        NProfiling::TWallTimer timer;
        int relocatedBlockCount = 0;
        for (int start = 0; start < std::ssize(blocks); start += Config_->MaxBlocksPerBatch) {
            int batchSize = std::min<int>(Config_->MaxBlocksPerBatch, std::ssize(blocks) - start);
            relocatedBlockCount += CompactBlockBatch(chunkInfo, TRange(blocks).Slice(start, start + batchSize));
        }

        YT_LOG_INFO("Finished compacting block store chunk (ChunkId: %v, RelocatedBlockCount: %v, ElapsedTime: %v)",
            chunkInfo.ChunkId,
            relocatedBlockCount,
            timer.GetElapsedTime());
    }

    //! Reads a batch of live blocks, rewrites them into fresh chunks, and repoints the map at the copies.
    //! Returns how many were actually relocated (a block a newer write superseded meanwhile is skipped).
    int CompactBlockBatch(const TChunkInfo& chunkInfo, TRange<std::pair<int, TStoredBlockId>> batch)
    {
        YT_LOG_DEBUG("Started compacting block batch (ChunkId: %v, BlockCount: %v)",
            chunkInfo.ChunkId,
            batch.size());

        std::vector<TStoredBlockId> oldBlockIds;
        oldBlockIds.reserve(batch.size());
        for (auto [blockIndex, storedBlockId] : batch) {
            oldBlockIds.push_back(storedBlockId);
        }

        auto payloads = WaitFor(BlockStore_->ReadBlocks(oldBlockIds, EWorkloadCategory::UserBatch))
            .ValueOrThrow();

        i64 batchBytes = 0;
        for (const auto& payload : payloads) {
            batchBytes += payload.Size();
        }
        WaitFor(Throttler_->Throttle(batchBytes))
            .ThrowOnError();

        auto newBlockIds = WaitFor(BlockStore_->WriteBlocks(payloads))
            .ValueOrThrow();
        YT_VERIFY(std::ssize(newBlockIds) == std::ssize(batch));

        int relocatedBlockCount = 0;
        for (int index = 0; index < std::ssize(batch); ++index) {
            if (BlockMap_->TryPutBlock(batch[index].first, ToMappedBlockId(oldBlockIds[index]), newBlockIds[index])) {
                ++relocatedBlockCount;
            }
        }

        YT_LOG_DEBUG("Finished compacting block batch (ChunkId: %v, RelocatedBlockCount: %v)",
            chunkInfo.ChunkId,
            relocatedBlockCount);

        return relocatedBlockCount;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockCompactorPtr CreateBlockCompactor(
    TJournalBlockCompactorConfigPtr config,
    IBlockMapPtr blockMap,
    IBlockStorePtr blockStore,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TBlockCompactor>(
        std::move(config),
        std::move(blockMap),
        std::move(blockStore),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TNullBlockCompactor
    : public IBlockCompactor
{
public:
    void Start() final
    { }

    void Stop() final
    { }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockCompactorPtr GetNullBlockCompactor()
{
    return LeakyRefCountedSingleton<TNullBlockCompactor>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
