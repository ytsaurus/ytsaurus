#include <yt/yt/server/lib/nbd/journal/block_compactor.h>
#include <yt/yt/server/lib/nbd/journal/block_map.h>
#include <yt/yt/server/lib/nbd/journal/block_store.h>
#include <yt/yt/server/lib/nbd/journal/block_store_helpers.h>
#include <yt/yt/server/lib/nbd/journal/config.h>
#include <yt/yt/server/lib/nbd/journal/public.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <utility>
#include <vector>

namespace NYT::NNbd::NJournal {
namespace {

using namespace NConcurrency;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("CompactorTest");

constexpr i64 BlockSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

//! Yields a fixed set of stored blocks and accepts every relocation.
class TMockBlockMap
    : public IBlockMap
{
public:
    explicit TMockBlockMap(std::vector<std::pair<int, TStoredBlockId>> blocks)
        : Blocks_(std::move(blocks))
    { }

    std::vector<std::pair<int, TStoredBlockId>> GetChunkBlocks(int chunkIndex) const final
    {
        std::vector<std::pair<int, TStoredBlockId>> blocks;
        for (const auto& [blockIndex, storedBlockId] : Blocks_) {
            if (ParseStoredBlockId(storedBlockId).ChunkIndex == chunkIndex) {
                blocks.emplace_back(blockIndex, storedBlockId);
            }
        }
        return blocks;
    }

    bool TryPutBlock(int /*blockIndex*/, TMappedBlockId /*expectedBlockId*/, TStoredBlockId /*storedBlockId*/) final
    {
        return true;
    }

    TMappedBlockId FindBlock(int /*blockIndex*/) final
    {
        YT_ABORT();
    }

    void PutBlock(int /*blockIndex*/, TDirtyBlockId /*blockId*/) final
    {
        YT_ABORT();
    }

    bool DiscardBlock(int /*blockIndex*/) final
    {
        YT_ABORT();
    }

    int GetUsedBlockCount() const final
    {
        YT_ABORT();
    }

    int GetBlockCount() const final
    {
        YT_ABORT();
    }

    void BeginSnapshot() final
    {
        YT_ABORT();
    }

    TBlockMapSnapshot ScanSnapshotPart(int /*beginBlockIndex*/, int /*endBlockIndex*/) final
    {
        YT_ABORT();
    }

    void EndSnapshot() final
    {
        YT_ABORT();
    }

    void BeginLoadSnapshot() final
    {
        YT_ABORT();
    }

    void LoadSnapshotPart(const TBlockMapSnapshot& /*snapshot*/) final
    {
        YT_ABORT();
    }

    void EndLoadSnapshot() final
    {
        YT_ABORT();
    }

    DEFINE_SIGNAL_OVERRIDE(void(TDirtyBlockId, TStoredBlockId), BlockFlushObserved);
    DEFINE_SIGNAL_OVERRIDE(void(TStoredBlockId), StoredBlockUnreferenced);

private:
    const std::vector<std::pair<int, TStoredBlockId>> Blocks_;
};

////////////////////////////////////////////////////////////////////////////////

//! Serves a fixed chunk-info set and records, per relocation, the chunk each read batch came from.
class TMockBlockStore
    : public IBlockStore
{
public:
    explicit TMockBlockStore(std::vector<TChunkInfo> chunkInfos)
        : ChunkInfos_(std::move(chunkInfos))
    { }

    void Start() final
    { }

    void Stop() final
    {
        YT_ABORT();
    }

    void SubscribeFailed(const TCallback<void(const TError&)>& /*callback*/) final
    { }

    void UnsubscribeFailed(const TCallback<void(const TError&)>& /*callback*/) final
    { }

    std::vector<TChunkInfo> GetChunkInfos() final
    {
        return ChunkInfos_;
    }

    TFuture<std::vector<TSharedRef>> ReadBlocks(
        TRange<TStoredBlockId> blockIds,
        EWorkloadCategory /*workloadCategory*/) final
    {
        {
            auto guard = Guard(Lock_);
            if (blockIds.size() > 0) {
                CompactedChunkIndices_.push_back(ParseStoredBlockId(blockIds[0]).ChunkIndex);
            }
        }
        std::vector<TSharedRef> result;
        result.reserve(blockIds.size());
        for (int index = 0; index < std::ssize(blockIds); ++index) {
            result.push_back(TSharedRef::FromString(std::string(BlockSize, 'x')));
        }
        return MakeFuture(std::move(result));
    }

    TFuture<std::vector<TStoredBlockId>> WriteBlocks(TRange<TSharedRef> blocks) final
    {
        auto guard = Guard(Lock_);
        std::vector<TStoredBlockId> ids;
        ids.reserve(blocks.size());
        for (int index = 0; index < std::ssize(blocks); ++index) {
            ids.push_back(MakeStoredBlockId({
                .ChunkIndex = RelocationChunkIndex,
                .RecordIndex = NextRecordIndex_++,
                .FragmentIndex = 0,
            }));
        }
        return MakeFuture(std::move(ids));
    }

    void ReleaseBlock(TStoredBlockId /*blockId*/) final
    { }

    std::vector<int> GetCompactedChunkIndices()
    {
        auto guard = Guard(Lock_);
        return CompactedChunkIndices_;
    }

    TFuture<void> SealChunks(TRange<NChunkClient::TChunkId> /*chunkIds*/) final
    {
        YT_ABORT();
    }

    std::vector<TStoredBlockRef> GetBlockRefs(TRange<TStoredBlockId> /*blockIds*/) final
    {
        YT_ABORT();
    }

    TFuture<void> BeginRestoreBlocks() final
    {
        YT_ABORT();
    }

    TFuture<std::vector<TStoredBlockId>> RestoreBlocks(std::vector<TSnapshotBlock> /*snapshotBlocks*/) final
    {
        YT_ABORT();
    }

    TFuture<void> EndRestoreBlocks(const TChunkBlockCounts& /*chunkBlockCounts*/) final
    {
        YT_ABORT();
    }

    void BeginSnapshot() final
    {
        YT_ABORT();
    }

    void EndSnapshot() final
    {
        YT_ABORT();
    }

private:
    // Relocated copies land in a chunk index that is not one of the compaction candidates.
    static constexpr int RelocationChunkIndex = 999;

    const std::vector<TChunkInfo> ChunkInfos_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    std::vector<int> CompactedChunkIndices_;
    int NextRecordIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TChunkInfo MakeChunkInfo(int chunkIndex, bool restored, EChunkSealState sealState, i64 referenced, i64 written)
{
    return {
        .ChunkId = NChunkClient::TChunkId::Create(),
        .ChunkIndex = chunkIndex,
        .RestoredFromSnapshot = restored,
        .SealState = sealState,
        .ReferencedBlockCount = referenced,
        .WrittenBlockCount = written,
    };
}

TEST(TBlockCompactorTest, PicksWorstEligibleChunk)
{
    auto actionQueue = New<TActionQueue>("CompactorTest");
    auto shutdownGuard = Finally([&] {
        actionQueue->Shutdown();
    });

    std::vector<TChunkInfo> chunkInfos{
        MakeChunkInfo(/*chunkIndex*/ 10, /*restored*/ true,  EChunkSealState::Done, /*referenced*/ 1, /*written*/ 10),   // garbage 0.90
        MakeChunkInfo(/*chunkIndex*/ 11, /*restored*/ false, EChunkSealState::Done, /*referenced*/ 5, /*written*/ 10),   // garbage 0.50
        MakeChunkInfo(/*chunkIndex*/ 12, /*restored*/ false, EChunkSealState::None, /*referenced*/ 1, /*written*/ 100),  // unsealed
        MakeChunkInfo(/*chunkIndex*/ 13, /*restored*/ true,  EChunkSealState::Done, /*referenced*/ 1, /*written*/ 0),    // total unknown
    };
    auto store = New<TMockBlockStore>(chunkInfos);

    std::vector<std::pair<int, TStoredBlockId>> blocks;
    for (int index = 0; index < std::ssize(chunkInfos); ++index) {
        blocks.emplace_back(
            index,
            MakeStoredBlockId({.ChunkIndex = chunkInfos[index].ChunkIndex, .RecordIndex = 0, .FragmentIndex = 0}));
    }
    auto blockMap = New<TMockBlockMap>(std::move(blocks));

    auto config = New<TJournalBlockCompactorConfig>();
    config->GarbageRatioThreshold = 0.5;
    config->ScanPeriod = TDuration::MilliSeconds(50);
    auto compactor = CreateBlockCompactor(config, blockMap, store, actionQueue->GetInvoker(), Logger);
    compactor->Start();
    auto stopCompactorGuard = Finally([&] {
        compactor->Stop();
    });

    WaitForPredicate(
        [&] { return !store->GetCompactedChunkIndices().empty(); },
        "compactor did not compact any chunk");

    // Chunk 10 is the worst *eligible* candidate: a restored chunk is allowed, while the higher-garbage
    // but unsealed chunk 12 and the unknown-total chunk 13 are skipped, and 11 has less garbage.
    EXPECT_EQ(store->GetCompactedChunkIndices().front(), 10);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNbd::NJournal
