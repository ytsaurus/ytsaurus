#include "column_block_manager.h"
#include "memory_helpers.h"
#include "dispatch_by_type.h"
#include "prepared_meta.h"
#include "read_span.h"

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_id.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>

#include <yt/yt/client/table_client/config.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/yt/core/misc/algorithm_helpers.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

using NProfiling::TCpuDurationIncrementingGuard;

using NChunkClient::EBlockType;

using NChunkClient::TChunkReaderMemoryManager;
using NChunkClient::TChunkReaderMemoryManagerOptions;
using NChunkClient::TClientChunkReadOptions;

using NChunkClient::IBlockCachePtr;
using NChunkClient::TBlockFetcherPtr;
using NChunkClient::IChunkReaderPtr;

using NTableClient::TChunkReaderConfigPtr;

static const auto& Logger = NTableClient::TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

// Need to declare move constructor to use in std::vector
TGroupBlockHolder::TGroupBlockHolder(TGroupBlockHolder&&)
{
    YT_ABORT();
}

TGroupBlockHolder::TGroupBlockHolder(
    TRange<ui32> blockIds,
    TRange<ui32> blockChunkRowCounts,
    TRange<TSharedRef> blockSegmentsMetas,
    const ui32* metaOffsetsInBlocks)
    : BlockIds_(blockIds)
    , BlockChunkRowCounts_(blockChunkRowCounts)
    , BlockSegmentsMetas_(blockSegmentsMetas)
    , MetaOffsetsInBlocks_(metaOffsetsInBlocks)
{ }

bool TGroupBlockHolder::NeedUpdateBlock(ui32 rowIndex) const
{
    return rowIndex >= BlockRowLimit_ && BlockIdIndex_ < BlockIds_.size();
}

TSharedRef TGroupBlockHolder::SetBlock(TSharedRef data)
{
    YT_VERIFY(BlockIdIndex_ < BlockIds_.size());

    if (MetaOffsetsInBlocks_) {
        auto blockId = BlockIds_[BlockIdIndex_];
        BlockSegmentsMeta = TRef(data.Begin() + MetaOffsetsInBlocks_[blockId], data.End());
        YT_VERIFY(BlockSegmentsMetas_[BlockIdIndex_].Size() == BlockSegmentsMeta.Size());
    } else {
        BlockSegmentsMeta = BlockSegmentsMetas_[BlockIdIndex_];
    }

    auto oldBlock = std::move(Block);

    Block = std::move(data);
    BlockRowLimit_ = BlockChunkRowCounts_[BlockIdIndex_];

    return oldBlock;
}

// TODO(lukyan): Use block row limits vector instead of blockMeta.
std::optional<ui32> TGroupBlockHolder::SkipToBlock(ui32 rowIndex)
{
    if (!NeedUpdateBlock(rowIndex)) {
        return std::nullopt;
    }

    // Need to find block with rowIndex.
    BlockIdIndex_ = ExponentialSearch<ui32>(BlockIdIndex_, BlockIds_.size(), [&] (ui32 blockIdIndex) {
        return BlockChunkRowCounts_[blockIdIndex] <= rowIndex;
    });

    // It is used for generating sentinel rows in lookup (for keys after end of chunk).
    if (BlockIdIndex_ == BlockIds_.size()) {
        return std::nullopt;
    }

    YT_VERIFY(BlockIdIndex_ < BlockIds_.size());
    return BlockIds_[BlockIdIndex_];
}

TRange<ui32> TGroupBlockHolder::GetBlockIds() const
{
    return BlockIds_;
}

std::vector<ui16> GetGroupsIds(
    const TPreparedChunkMeta& preparedChunkMeta,
    ui16 keyColumnCount,
    TRange<TColumnIdMapping> valuesIdMapping)
{
    std::vector<ui16> groupIds;
    groupIds.reserve(keyColumnCount + std::ssize(valuesIdMapping) + 1);
    for (int index = 0; index < keyColumnCount; ++index) {
        groupIds.push_back(preparedChunkMeta.ColumnIdToGroupId[index]);
    }

    for (auto [chunkSchemaIndex, readerSchemaIndex] : valuesIdMapping) {
        groupIds.push_back(preparedChunkMeta.ColumnIdToGroupId[chunkSchemaIndex]);
    }

    // TODO(lukyan): Or use first group for timestamp?
    auto timestampGroupIndex = preparedChunkMeta.ColumnGroups.size() - 1;
    groupIds.push_back(timestampGroupIndex);

    std::sort(groupIds.begin(), groupIds.end());
    groupIds.erase(std::unique(groupIds.begin(), groupIds.end()), groupIds.end());

    return groupIds;
}

// Create group block holders using set of reading groups' ids.
std::vector<TGroupBlockHolder> CreateGroupBlockHolders(
    const TPreparedChunkMeta& preparedChunkMeta,
    TRange<ui16> groupIds)
{
    std::vector<TGroupBlockHolder> groupHolders;
    groupHolders.reserve(std::ssize(groupIds));
    for (auto groupId : groupIds) {
        groupHolders.emplace_back(
            preparedChunkMeta.ColumnGroups[groupId].BlockIds,
            preparedChunkMeta.ColumnGroups[groupId].BlockChunkRowCounts,
            preparedChunkMeta.ColumnGroups[groupId].MergedMetas);
    }

    return groupHolders;
}

/////////////////////////////////////////////////////////////////////////////

std::vector<TBlockFetcher::TBlockInfo> BuildBlockInfos(
    std::vector<TRange<ui32>> groupBlockIndexes,
    TRange<TSpanMatching> windows,
    const TRefCountedDataBlockMetaPtr& blockMetas)
{
    auto groupCount = groupBlockIndexes.size();
    std::vector<ui32> perGroupBlockRowLimits(groupCount, 0);

    std::vector<TBlockFetcher::TBlockInfo> blockInfos;
    for (auto window : windows) {
        auto startRowIndex = window.Chunk.Lower;

        for (ui16 groupId = 0; groupId < groupCount; ++groupId) {
            if (startRowIndex < perGroupBlockRowLimits[groupId]) {
                continue;
            }

            auto& blockIndexes = groupBlockIndexes[groupId];

            auto blockIt = ExponentialSearch(blockIndexes.begin(), blockIndexes.end(), [&] (auto blockIt) {
                const auto& blockMeta = blockMetas->data_blocks(*blockIt);
                return blockMeta.chunk_row_count() <= startRowIndex;
            });

            blockIndexes = blockIndexes.Slice(blockIt - blockIndexes.begin(), blockIndexes.size());

            if (blockIt != blockIndexes.end()) {
                const auto& blockMeta = blockMetas->data_blocks(*blockIt);
                perGroupBlockRowLimits[groupId] = blockMeta.chunk_row_count();

                blockInfos.push_back({
                    .ReaderIndex = 0,
                    .BlockIndex = static_cast<int>(*blockIt),
                    .Priority = static_cast<int>(blockMeta.chunk_row_count() - blockMeta.row_count()),
                    .UncompressedDataSize = blockMeta.uncompressed_size()
                });
            }
        }
    }

    return blockInfos;
}

/////////////////////////////////////////////////////////////////////////////

class TAsyncBlockWindowManager
    : public IBlockManager
{
public:
    TAsyncBlockWindowManager(
        std::vector<TGroupBlockHolder> blockHolders,
        NChunkClient::TBlockFetcherPtr blockFetcher)
        : BlockHolders_(std::move(blockHolders))
        , BlockFetcher_(std::move(blockFetcher))
    { }

    void ClearUsedBlocks() override
    { }

    // Returns false if need wait for ready event.
    bool TryUpdateWindow(ui32 rowIndex, TReaderStatistics* readerStatistics) override
    {
        ++readerStatistics->TryUpdateWindowCallCount;
        TCpuDurationIncrementingGuard timingGuard(&readerStatistics->FetchBlockTime);

        if (FetchedBlocks_) {
            if (!FetchedBlocks_.IsSet()) {
                // Blocks have been already requested from previous Read but are not fetched yet.
                return false;
            }

            auto loadedBlocks = FetchedBlocks_.GetUnique()
                .ValueOrThrow();

            size_t index = 0;
            for (auto& blockHolder : BlockHolders_) {
                if (auto blockId = blockHolder.SkipToBlock(rowIndex)) {
                    ++readerStatistics->SetBlockCallCount;
                    YT_VERIFY(index < loadedBlocks.size());
                    blockHolder.SetBlock(std::move(loadedBlocks[index++].Data));
                }
            }
            YT_VERIFY(index == loadedBlocks.size());

            FetchedBlocks_.Reset();
            return true;
        }

        // Skip to window.
        std::vector<TFuture<TBlock>> pendingBlocks;
        pendingBlocks.reserve(BlockHolders_.size());

        readerStatistics->SkipToBlockCallCount += BlockHolders_.size();
        for (auto& blockHolder : BlockHolders_) {
            if (auto blockId = blockHolder.SkipToBlock(rowIndex)) {
                ++readerStatistics->FetchBlockCallCount;
                // N.B. Even if all futures are set we cannot use fast path and
                // set block to block holder. Blocks hold blob data and
                // current blocks must not be destructed until next call of Read method.
                pendingBlocks.push_back(BlockFetcher_->FetchBlock(*blockId));
            }
        }

        // Not every window switch causes block updates.
        // Read windows are built by all block last keys but here only reading block set is considered.
        if (pendingBlocks.empty()) {
            return true;
        }

        FetchedBlocks_ = AllSucceeded(std::move(pendingBlocks));
        ReadyEvent_ = FetchedBlocks_.As<void>();

        return false;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return ReadyEvent_;
    }

    bool IsFetchingCompleted() const override
    {
        return BlockFetcher_
            ? BlockFetcher_->IsFetchingCompleted()
            : true;
    }

    i64 GetUncompressedDataSize() const override
    {
        return BlockFetcher_
            ? BlockFetcher_->GetUncompressedDataSize()
            : 0;
    }

    i64 GetCompressedDataSize() const override
    {
        return BlockFetcher_
            ? BlockFetcher_->GetCompressedDataSize()
            : 0;
    }

    NChunkClient::TCodecDuration GetDecompressionTime() const override
    {
        return BlockFetcher_
            ? BlockFetcher_->GetDecompressionTime()
            : NChunkClient::TCodecDuration{};
    }

private:
    std::vector<TGroupBlockHolder> BlockHolders_;
    NChunkClient::TBlockFetcherPtr BlockFetcher_;
    TFuture<std::vector<NChunkClient::TBlock>> FetchedBlocks_;
    TFuture<void> ReadyEvent_ = VoidFuture;
};

TBlockManagerFactory CreateAsyncBlockWindowManagerFactory(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    TClientChunkReadOptions chunkReadOptions,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IInvokerPtr sessionInvoker)
{
    return [=] (std::vector<TGroupBlockHolder> blockHolders, TRange<TSpanMatching> windowsList) -> std::unique_ptr<IBlockManager> {
        std::vector<TRange<ui32>> groupBlockIds;
        for (const auto& blockHolder : blockHolders) {
            groupBlockIds.push_back(blockHolder.GetBlockIds());
        }

        size_t uncompressedBlocksSize = 0;
        size_t blockCount = 0;

        auto buildBlockInfosStartInstant = GetCpuInstant();
        auto blockInfos = BuildBlockInfos(std::move(groupBlockIds), windowsList, chunkMeta->DataBlockMeta());
        TDuration buildBlockInfosTime = CpuDurationToDuration(GetCpuInstant() - buildBlockInfosStartInstant);

        blockCount = blockInfos.size();
        for (const auto& blockInfo : blockInfos) {
            uncompressedBlocksSize += blockInfo.UncompressedDataSize;
        }

        TBlockFetcherPtr blockFetcher;
        if (!blockInfos.empty()) {
            auto createBlockFetcherStartInstant = GetCpuInstant();
            auto memoryManager = New<TChunkReaderMemoryManager>(TChunkReaderMemoryManagerOptions(config->WindowSize));

            auto compressedSize = chunkMeta->Misc().compressed_data_size();
            auto uncompressedSize = chunkMeta->Misc().uncompressed_data_size();

            blockFetcher = New<TBlockFetcher>(
                config,
                std::move(blockInfos),
                memoryManager,
                std::vector{underlyingReader},
                blockCache,
                CheckedEnumCast<NCompression::ECodec>(chunkMeta->Misc().compression_codec()),
                static_cast<double>(compressedSize) / uncompressedSize,
                chunkReadOptions,
                sessionInvoker);

            blockFetcher->Start();

            TDuration createBlockFetcherTime = CpuDurationToDuration(GetCpuInstant() - createBlockFetcherStartInstant);

            YT_LOG_DEBUG("Creating block manager (BlockCount: %v, UncompressedBlocksSize: %v, BuildBlockInfos: %v, CreateBlockFetcherTime: %v)",
                blockCount,
                uncompressedBlocksSize,
                buildBlockInfosTime,
                createBlockFetcherTime);
        }

        return std::make_unique<TAsyncBlockWindowManager>(std::move(blockHolders), std::move(blockFetcher));
    };
}

////////////////////////////////////////////////////////////////////////////////

class TSyncBlockWindowManager
    : public IBlockManager
{
public:
    TSyncBlockWindowManager(
        std::vector<TGroupBlockHolder> blockHolders,
        NChunkClient::IBlockCachePtr blockCache,
        TCachedVersionedChunkMetaPtr chunkMeta,
        NChunkClient::TChunkId chunkId)
        : BlockHolders_(std::move(blockHolders))
        , BlockCache_(std::move(blockCache))
        , ChunkId_(chunkId)
    {
        YT_VERIFY(TryEnumCast(chunkMeta->Misc().compression_codec(), &CodecId_));
    }

    void ClearUsedBlocks() override
    {
        UsedBlocks_.clear();
    }

    bool TryUpdateWindow(ui32 rowIndex, TReaderStatistics* readerStatistics = nullptr) override
    {
        ++readerStatistics->TryUpdateWindowCallCount;
        TCpuDurationIncrementingGuard timingGuard(&readerStatistics->FetchBlockTime);
        readerStatistics->SkipToBlockCallCount += BlockHolders_.size();

        for (auto& blockHolder : BlockHolders_) {
            if (auto blockId = blockHolder.SkipToBlock(rowIndex)) {
                ++readerStatistics->SetBlockCallCount;
                UsedBlocks_.push_back(blockHolder.SetBlock(GetBlock(*blockId)));
            }
        }

        return true;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return VoidFuture;
    }

    bool IsFetchingCompleted() const override
    {
        return true;
    }

    i64 GetUncompressedDataSize() const override
    {
        return 0;
    }

    i64 GetCompressedDataSize() const override
    {
        return 0;
    }

    NChunkClient::TCodecDuration GetDecompressionTime() const override
    {
        return NChunkClient::TCodecDuration{CodecId_, DecompressionDuration_};
    }

private:
    std::vector<TGroupBlockHolder> BlockHolders_;
    NChunkClient::IBlockCachePtr BlockCache_;
    NChunkClient::TChunkId ChunkId_;
    NCompression::ECodec CodecId_;
    std::vector<TSharedRef> UsedBlocks_;

    TDuration DecompressionDuration_;

    TSharedRef GetBlock(int blockIndex)
    {
        NChunkClient::TBlockId blockId(ChunkId_, blockIndex);

        auto cachedBlock = BlockCache_->FindBlock(blockId, EBlockType::UncompressedData).Block;
        if (cachedBlock) {
            return cachedBlock.Data;
        }

        auto compressedBlock = BlockCache_->FindBlock(blockId, EBlockType::CompressedData).Block;
        if (compressedBlock) {
            auto* codec = NCompression::GetCodec(CodecId_);

            NProfiling::TFiberWallTimer timer;
            auto uncompressedBlock = codec->Decompress(compressedBlock.Data);
            DecompressionDuration_ += timer.GetElapsedTime();

            if (CodecId_ != NCompression::ECodec::None) {
                BlockCache_->PutBlock(blockId, EBlockType::UncompressedData, TBlock(uncompressedBlock));
            }
            return uncompressedBlock;
        }

        YT_LOG_FATAL("Cached block is missing (BlockId: %v)", blockId);
    }
};

TBlockManagerFactory CreateSyncBlockWindowManagerFactory(
    IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    NChunkClient::TChunkId chunkId)
{
    return [=] (std::vector<TGroupBlockHolder> blockHolders, TRange<TSpanMatching> /*windowsList*/) -> std::unique_ptr<IBlockManager> {
        return std::make_unique<TSyncBlockWindowManager>(
            std::move(blockHolders),
            std::move(blockCache),
            std::move(chunkMeta),
            chunkId);
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
