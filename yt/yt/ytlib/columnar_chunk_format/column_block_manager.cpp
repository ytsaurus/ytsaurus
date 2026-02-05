#include "column_block_manager.h"
#include "prepared_meta.h"
#include "read_span.h"

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/yt/ytlib/chunk_client/block_id.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/private.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <library/cpp/yt/misc/range_formatters.h>

namespace NYT::NColumnarChunkFormat {

////////////////////////////////////////////////////////////////////////////////

using NProfiling::TCpuDurationIncrementingGuard;

using NChunkClient::EBlockType;

using NChunkClient::TChunkReaderMemoryManager;
using NChunkClient::TChunkReaderMemoryManagerOptions;
using NChunkClient::TChunkReaderMemoryManagerHolder;
using NChunkClient::TClientChunkReadOptions;

using NChunkClient::TBlock;
using NChunkClient::IBlockCachePtr;
using NChunkClient::TBlockFetcher;
using NChunkClient::TBlockFetcherPtr;
using NChunkClient::IChunkReaderPtr;

using NTableClient::TChunkReaderConfigPtr;

using namespace NTracing;

constinit const auto Logger = NTableClient::TableClientLogger;

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
    TRange<ui32> metaOffsetsInBlocks)
    : BlockIds_(blockIds)
    , BlockChunkRowCounts_(blockChunkRowCounts)
    , BlockSegmentsMetas_(blockSegmentsMetas)
    , MetaOffsetsInBlocks_(metaOffsetsInBlocks)
{ }

bool TGroupBlockHolder::NeedUpdateBlock(ui32 rowIndex) const
{
    return rowIndex >= BlockRowLimit_ && BlockIdIndex_ < BlockIds_.size();
}

TSharedRef TGroupBlockHolder::SwitchBlock(TSharedRef data)
{
    YT_VERIFY(BlockIdIndex_ < BlockIds_.size());

    if (MetaOffsetsInBlocks_.Empty()) {
        BlockSegmentsMeta = BlockSegmentsMetas_[BlockIdIndex_];
    } else {
        BlockSegmentsMeta = TRef(data.Begin() + MetaOffsetsInBlocks_[BlockIdIndex_], data.End());
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

TRange<ui32> TGroupBlockHolder::GetBlockChunkRowCounts() const
{
    return BlockChunkRowCounts_;
}

TCompactVector<ui16, 32> GetGroupsIds(
    const TPreparedChunkMeta& preparedChunkMeta,
    int chunkKeyColumnCount,
    ui16 readItemWidth,
    TRange<ui16> keyColumnIndexes,
    TRange<TColumnIdMapping> valuesIdMapping)
{
    int availableReadItemWidth = std::min<int>(readItemWidth, chunkKeyColumnCount);
    int extraKeyColumnCount = 0;
    for (auto id : keyColumnIndexes) {
        if (id >= availableReadItemWidth && id < chunkKeyColumnCount) {
            ++extraKeyColumnCount;
        }
    }

    TCompactVector<ui16, 32> groupIds;
    groupIds.resize(availableReadItemWidth + extraKeyColumnCount + std::ssize(valuesIdMapping) + 1);
    // Use raw data pointer because TCompactVector has branch in index operator.
    auto* groupIdsData = groupIds.data();

    for (int index = 0; index < availableReadItemWidth; ++index) {
        *groupIdsData++ = preparedChunkMeta.ColumnInfos[index].GroupId;
    }

    for (auto index : keyColumnIndexes) {
        if (index < availableReadItemWidth || index >= chunkKeyColumnCount) {
            continue;
        }
        *groupIdsData++ = preparedChunkMeta.ColumnInfos[index].GroupId;
    }

    for (auto [chunkSchemaIndex, _] : valuesIdMapping) {
        *groupIdsData++ = preparedChunkMeta.ColumnInfos[chunkSchemaIndex].GroupId;
    }

    // Timestamp group id.
    *groupIdsData++ = preparedChunkMeta.ColumnInfos.back().GroupId;

    YT_VERIFY(groupIdsData == groupIds.end());

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
            preparedChunkMeta.GroupInfos[groupId].BlockIds,
            preparedChunkMeta.GroupInfos[groupId].BlockChunkRowCounts,
            preparedChunkMeta.GroupInfos[groupId].MergedMetas,
            preparedChunkMeta.GroupInfos[groupId].SegmentMetaOffsets);
    }

    return groupHolders;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TBlockFetcher::TBlockInfo> BuildBlockInfos(
    std::vector<TRange<ui32>> groupBlockIndexes,
    std::vector<TRange<ui32>> groupBlockChunkRowCounts,
    TRange<TSpanMatching> windows,
    TRange<ui32> blockUncompressedSizes)
{
    auto groupCount = groupBlockIndexes.size();
    std::vector<ui32> perGroupBlockIndexes(groupCount, 0);
    std::vector<ui32> perGroupBlockRowLimits(groupCount, 0);

    std::vector<TBlockFetcher::TBlockInfo> blockInfos;
    for (auto window : windows) {
        auto startRowIndex = window.Chunk.Lower;

        for (ui16 groupId = 0; groupId < groupCount; ++groupId) {
            if (startRowIndex < perGroupBlockRowLimits[groupId]) {
                continue;
            }

            auto& groupBlockIndex = perGroupBlockIndexes[groupId];

            const auto& blockIndexes = groupBlockIndexes[groupId];
            const auto& blockChunkRowCountsInGroup = groupBlockChunkRowCounts[groupId];
            ui32 blockCountInGroup = std::size(blockIndexes);

            // NB: This reader can only read data blocks, hence in block infos we set block type to UncompressedData.
            YT_VERIFY(static_cast<int>(blockIndexes.Back()) < std::ssize(blockUncompressedSizes));
            groupBlockIndex = ExponentialSearch(groupBlockIndex, blockCountInGroup, [&] (auto index) {
                return blockChunkRowCountsInGroup[index] <= startRowIndex;
            });

            if (groupBlockIndex < blockCountInGroup) {
                auto chunkBlockIndex = blockIndexes[groupBlockIndex];
                perGroupBlockRowLimits[groupId] = blockChunkRowCountsInGroup[groupBlockIndex];

                ui32 startChunkRowCount = groupBlockIndex > 0
                    ? blockChunkRowCountsInGroup[groupBlockIndex - 1]
                    : 0;

                blockInfos.push_back({
                    .ReaderIndex = 0,
                    .BlockIndex = static_cast<int>(chunkBlockIndex),
                    .Priority = static_cast<int>(startChunkRowCount),
                    .UncompressedDataSize = blockUncompressedSizes[chunkBlockIndex],
                    .BlockType = EBlockType::UncompressedData,
                });
            }
        }
    }

    return blockInfos;
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncBlockWindowManager
    : public IBlockManager
{
public:
    TAsyncBlockWindowManager(
        std::vector<TGroupBlockHolder> blockHolders,
        NChunkClient::TBlockFetcherPtr blockFetcher,
        TTraceContextPtr traceContext)
        : BlockHolders_(std::move(blockHolders))
        , BlockFetcher_(std::move(blockFetcher))
        , TraceContext_(std::move(traceContext))
        , FinishGuard_(TraceContext_)
        , BlockCountStatistics_(BlockHolders_.size(), 0)
        , BlockSizeStatistics_(BlockHolders_.size(), 0)
    { }

    ~TAsyncBlockWindowManager()
    {
        YT_LOG_DEBUG(
            "Reader block statistics (Counts: %v, Sizes: %v)",
            BlockCountStatistics_,
            BlockSizeStatistics_);
    }

    // Does not need to keep and clear used blocks because every block switch in TAsyncBlockWindowManager
    // always interrupts reading.
    void ClearUsedBlocks() override
    { }

    // Returns false if need wait for ready event.
    bool TryUpdateWindow(ui32 rowIndex, TReaderStatistics* readerStatistics, bool onlyKeyBlocks) override
    {
        ++readerStatistics->TryUpdateWindowCallCount;
        TCpuDurationIncrementingGuard timingGuard(&readerStatistics->FetchBlockTime);

        if (FetchedBlocks_) {
            if (!FetchedBlocks_.IsSet()) {
                // Blocks have been already requested from previous Read but are not fetched yet.
                return false;
            }

            auto loadedBlocksOrError = FetchedBlocks_.AsUnique().Get();
            if (!loadedBlocksOrError.IsOK()) {
                return false;
            }

            auto loadedBlocks = std::move(loadedBlocksOrError).Value();

            size_t index = 0;
            for (auto& blockHolder : BlockHolders_) {
                if (onlyKeyBlocks && !blockHolder.GetContainsKeyColumn()) {
                    continue;
                }

                if (auto blockId = blockHolder.SkipToBlock(rowIndex)) {
                    ++readerStatistics->SetBlockCallCount;
                    YT_VERIFY(index < loadedBlocks.size());

                    ++BlockCountStatistics_[&blockHolder - BlockHolders_.data()];
                    BlockSizeStatistics_[&blockHolder - BlockHolders_.data()] += loadedBlocks[index].Data.Size();

                    blockHolder.SwitchBlock(std::move(loadedBlocks[index++].Data));
                }
            }
            YT_VERIFY(index == loadedBlocks.size());

            FetchedBlocks_.Reset();
            return true;
        }

        // Skip to window.
        std::vector<TFuture<TBlock>> pendingBlocks;
        pendingBlocks.reserve(BlockHolders_.size());

        TCurrentTraceContextGuard guard(TraceContext_);

        readerStatistics->SkipToBlockCallCount += BlockHolders_.size();
        for (auto& blockHolder : BlockHolders_) {
            if (onlyKeyBlocks && !blockHolder.GetContainsKeyColumn()) {
                continue;
            }

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
    TFuture<void> ReadyEvent_ = OKFuture;

    // TODO(lukyan): Move tracing to block fetcher or underlying chunk reader.
    const TTraceContextPtr TraceContext_;
    const TTraceContextFinishGuard FinishGuard_;

    std::vector<ui32> BlockCountStatistics_;
    std::vector<ui64> BlockSizeStatistics_;
};

TBlockManagerFactory CreateAsyncBlockWindowManagerFactory(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    TClientChunkReadOptions chunkReadOptions,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IInvokerPtr sessionInvoker,
    const std::optional<NChunkClient::TDataSourcePtr>& dataSource)
{
    return [=] (
        std::vector<TGroupBlockHolder> blockHolders,
        TRange<TSpanMatching> windowsList,
        TRange<ui32> blockUncompressedSizes
    ) -> std::unique_ptr<IBlockManager> {
        TTraceContextPtr traceContext;

        if (dataSource) {
            traceContext = CreateTraceContextFromCurrent("ChunkReader");
            PackBaggageForChunkReader(traceContext, *dataSource, NTableClient::MakeExtraChunkTags(chunkMeta->Misc()));
        }

        std::vector<TRange<ui32>> groupBlockIndexes;
        groupBlockIndexes.reserve(blockHolders.size());

        std::vector<TRange<ui32>> groupBlockChunkRowCounts;
        groupBlockChunkRowCounts.reserve(blockHolders.size());
        for (const auto& blockHolder : blockHolders) {
            groupBlockIndexes.push_back(blockHolder.GetBlockIds());
            groupBlockChunkRowCounts.push_back(blockHolder.GetBlockChunkRowCounts());
        }

        size_t uncompressedBlocksSize = 0;
        size_t blockCount = 0;

        auto buildBlockInfosStartInstant = GetCpuInstant();
        auto blockInfos = BuildBlockInfos(
            std::move(groupBlockIndexes),
            std::move(groupBlockChunkRowCounts),
            windowsList,
            blockUncompressedSizes);
        TDuration buildBlockInfosTime = CpuDurationToDuration(GetCpuInstant() - buildBlockInfosStartInstant);

        blockCount = blockInfos.size();
        for (const auto& blockInfo : blockInfos) {
            uncompressedBlocksSize += blockInfo.UncompressedDataSize;
        }

        TBlockFetcherPtr blockFetcher;
        if (!blockInfos.empty()) {
            TCurrentTraceContextGuard guard(traceContext);

            auto createBlockFetcherStartInstant = GetCpuInstant();
            auto memoryManagerHolder = TChunkReaderMemoryManager::CreateHolder(TChunkReaderMemoryManagerOptions(config->WindowSize));

            auto compressedSize = chunkMeta->Misc().compressed_data_size();
            auto uncompressedSize = chunkMeta->Misc().uncompressed_data_size();

            blockFetcher = New<TBlockFetcher>(
                config,
                std::move(blockInfos),
                memoryManagerHolder,
                std::vector{underlyingReader},
                blockCache,
                FromProto<NCompression::ECodec>(chunkMeta->Misc().compression_codec()),
                static_cast<double>(compressedSize) / uncompressedSize,
                chunkReadOptions,
                sessionInvoker);

            blockFetcher->Start();

            TDuration createBlockFetcherTime = CpuDurationToDuration(GetCpuInstant() - createBlockFetcherStartInstant);

            YT_LOG_DEBUG("Creating block manager "
                "(BlockCount: %v, UncompressedBlocksSize: %v, BuildBlockInfos: %v, CreateBlockFetcherTime: %v)",
                blockCount,
                uncompressedBlocksSize,
                buildBlockInfosTime,
                createBlockFetcherTime);
        }

        return std::make_unique<TAsyncBlockWindowManager>(std::move(blockHolders), std::move(blockFetcher), std::move(traceContext));
    };
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleAsyncBlockWindowManager
    : public IBlockManager
{
public:
    TSimpleAsyncBlockWindowManager(
        std::vector<TGroupBlockHolder> blockHolders,
        const TClientChunkReadOptions& chunkReadOptions,
        TRange<ui32> blockUncompressedSizes,
        double compressionRatio,
        IChunkReaderPtr underlyingReader,
        IInvokerPtr sessionInvoker,
        IBlockCachePtr blockCache,
        NCompression::ECodec codecId,
        TTraceContextPtr traceContext)
        : BlockHolders_(std::move(blockHolders))
        , ChunkReadOptions_(chunkReadOptions)
        , BlockUncompressedSizes_(blockUncompressedSizes.begin(), blockUncompressedSizes.end())
        , CompressionRatio_(compressionRatio)
        , UnderlyingReader_(std::move(underlyingReader))
        , SessionInvoker_(std::move(sessionInvoker))
        , BlockCache_(std::move(blockCache))
        , Codec_(NCompression::GetCodec(codecId))
        , TraceContext_(std::move(traceContext))
        , FinishGuard_(TraceContext_)
        , BlockCountStatistics_(BlockHolders_.size(), 0)
        , BlockSizeStatistics_(BlockHolders_.size(), 0)
    { }

    ~TSimpleAsyncBlockWindowManager()
    {
        YT_LOG_DEBUG(
            "Reader block statistics (Counts: %v, Sizes: %v)",
            BlockCountStatistics_,
            BlockSizeStatistics_);
    }

    void ClearUsedBlocks() override
    {
        UsedBlocks_.clear();
    }

    TSharedRef DecompressBlock(TBlock compressedBlock, int blockId)
    {
        TSharedRef uncompressedBlock;
        if (Codec_->GetId() == NCompression::ECodec::None) {
            uncompressedBlock = std::move(compressedBlock.Data);
        } else {
            NProfiling::TWallTimer timer;
            uncompressedBlock = Codec_->Decompress(compressedBlock.Data);

            DecompressionTime_ += timer.GetElapsedValue();
            UncompressedDataSize_ += uncompressedBlock.Size();
            CompressedDataSize_ += compressedBlock.Size();

            YT_VERIFY(std::ssize(uncompressedBlock) == BlockUncompressedSizes_[blockId]);
        }

        uncompressedBlock = TrackMemory(
            ChunkReadOptions_.MemoryUsageTracker,
            std::move(uncompressedBlock));

        BlockCache_->PutBlock(
            {UnderlyingReader_->GetChunkId(), blockId},
            EBlockType::UncompressedData,
            TBlock(uncompressedBlock));

        return uncompressedBlock;
    }

    // Returns false if need wait for ready event.
    bool TryUpdateWindow(ui32 rowIndex, TReaderStatistics* readerStatistics, bool onlyKeyBlocks) override
    {
        ++readerStatistics->TryUpdateWindowCallCount;
        TCpuDurationIncrementingGuard timingGuard(&readerStatistics->FetchBlockTime);

        if (FetchedBlocks_) {
            if (!FetchedBlocks_.IsSet()) {
                // Blocks have been already requested from previous Read but are not fetched yet.
                return false;
            }

            auto loadedBlocksOrError = FetchedBlocks_.AsUnique().Get();
            if (!loadedBlocksOrError.IsOK()) {
                return false;
            }

            auto periodicYielder = NConcurrency::CreatePeriodicYielder(TDuration::MilliSeconds(30));

            auto loadedBlocks = std::move(loadedBlocksOrError).Value();

            size_t index = 0;
            for (auto& blockHolder : BlockHolders_) {
                if (onlyKeyBlocks && !blockHolder.GetContainsKeyColumn()) {
                    continue;
                }

                if (auto blockId = blockHolder.SkipToBlock(rowIndex)) {
                    ++readerStatistics->SetBlockCallCount;
                    YT_VERIFY(index < loadedBlocks.size());

                    loadedBlocks[index].Data = DecompressBlock(loadedBlocks[index], *blockId);

                    periodicYielder.TryYield();

                    ++BlockCountStatistics_[&blockHolder - BlockHolders_.data()];
                    BlockSizeStatistics_[&blockHolder - BlockHolders_.data()] += loadedBlocks[index].Data.Size();

                    UsedBlocks_.push_back(blockHolder.SwitchBlock(std::move(loadedBlocks[index++].Data)));
                }
            }
            YT_VERIFY(index == loadedBlocks.size());

            FetchedBlocks_.Reset();
            return true;
        }

        // Skip to window.
        std::vector<int> pendingBlockIds;
        i64 uncompressedSize = 0;

        TCurrentTraceContextGuard guard(TraceContext_);

        readerStatistics->SkipToBlockCallCount += BlockHolders_.size();
        for (auto& blockHolder : BlockHolders_) {
            if (onlyKeyBlocks && !blockHolder.GetContainsKeyColumn()) {
                continue;
            }

            if (auto blockId = blockHolder.SkipToBlock(rowIndex)) {
                ++readerStatistics->FetchBlockCallCount;
                // N.B. Even if all futures are set we cannot use fast path and
                // set block to block holder. Blocks hold blob data and
                // current blocks must not be destructed until next call of Read method.

                auto cachedBlock = /*Config_->UseUncompressedBlockCache*/ true
                    ? BlockCache_->FindBlock({UnderlyingReader_->GetChunkId(), int(*blockId)}, EBlockType::UncompressedData)
                    : TBlock();
                if (cachedBlock) {
                    ChunkReadOptions_.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                        cachedBlock.Size(),
                        std::memory_order::relaxed);

                    cachedBlock.Data = TrackMemory(ChunkReadOptions_.MemoryUsageTracker, std::move(cachedBlock.Data));

                    UsedBlocks_.push_back(blockHolder.SwitchBlock(cachedBlock.Data));
                } else {
                    uncompressedSize += BlockUncompressedSizes_[*blockId];
                    pendingBlockIds.push_back(*blockId);
                }
            }
        }

        // Not every window switch causes block updates.
        // Read windows are built by all block last keys but here only reading block set is considered.
        if (pendingBlockIds.empty()) {
            return true;
        }

        FetchedBlocks_ = UnderlyingReader_->ReadBlocks(
            {
                .ClientOptions = ChunkReadOptions_,
                .EstimatedSize = static_cast<i64>(uncompressedSize * CompressionRatio_),
                .SessionInvoker = SessionInvoker_,
            },
            pendingBlockIds);

        ReadyEvent_ = FetchedBlocks_.As<void>();

        return false;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return ReadyEvent_;
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    i64 GetUncompressedDataSize() const override
    {
        return UncompressedDataSize_;
    }

    i64 GetCompressedDataSize() const override
    {
        return CompressedDataSize_;
    }

    NChunkClient::TCodecDuration GetDecompressionTime() const override
    {
        return {
            Codec_->GetId(),
            NProfiling::ValueToDuration(DecompressionTime_)
        };
    }

private:
    std::vector<TGroupBlockHolder> BlockHolders_;
    TClientChunkReadOptions ChunkReadOptions_;
    // Can keep TRange<ui32> but vector is more safe.
    std::vector<ui32> BlockUncompressedSizes_;

    const double CompressionRatio_;
    const IChunkReaderPtr UnderlyingReader_;
    const IInvokerPtr SessionInvoker_;
    const IBlockCachePtr BlockCache_;
    NCompression::ICodec* const Codec_;

    TFuture<std::vector<NChunkClient::TBlock>> FetchedBlocks_;
    TFuture<void> ReadyEvent_ = OKFuture;

    // TODO(lukyan): Move tracing to block fetcher or underlying chunk reader.
    const TTraceContextPtr TraceContext_;
    const TTraceContextFinishGuard FinishGuard_;

    std::vector<TSharedRef> UsedBlocks_;

    std::vector<ui32> BlockCountStatistics_;
    std::vector<ui64> BlockSizeStatistics_;

    i64 UncompressedDataSize_ = 0;
    i64 CompressedDataSize_ = 0;
    NProfiling::TCpuDuration DecompressionTime_ = 0;
};

TBlockManagerFactory CreateSimpleAsyncBlockWindowManagerFactory(
    TChunkReaderConfigPtr /*config*/,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    TClientChunkReadOptions chunkReadOptions,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IInvokerPtr sessionInvoker,
    const std::optional<NChunkClient::TDataSourcePtr>& dataSource)
{
    return [=] (
        std::vector<TGroupBlockHolder> blockHolders,
        TRange<TSpanMatching> /*windowsList*/,
        TRange<ui32> blockUncompressedSizes
    ) -> std::unique_ptr<IBlockManager> {
        TTraceContextPtr traceContext;

        if (dataSource) {
            traceContext = CreateTraceContextFromCurrent("ChunkReader");
            PackBaggageForChunkReader(traceContext, *dataSource, NTableClient::MakeExtraChunkTags(chunkMeta->Misc()));
        }

        auto compressedSize = chunkMeta->Misc().compressed_data_size();
        auto uncompressedSize = chunkMeta->Misc().uncompressed_data_size();

        return std::make_unique<TSimpleAsyncBlockWindowManager>(
            std::move(blockHolders),
            chunkReadOptions,
            blockUncompressedSizes,
            static_cast<double>(compressedSize) / uncompressedSize,
            std::move(underlyingReader),
            std::move(sessionInvoker),
            std::move(blockCache),
            FromProto<NCompression::ECodec>(chunkMeta->Misc().compression_codec()),
            std::move(traceContext));
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
        NChunkClient::TChunkId chunkId,
        NCompression::ECodec codecId)
        : BlockHolders_(std::move(blockHolders))
        , BlockCache_(std::move(blockCache))
        , ChunkId_(chunkId)
        , CodecId_(codecId)
    { }

    void ClearUsedBlocks() override
    {
        UsedBlocks_.clear();
    }

    bool TryUpdateWindow(ui32 rowIndex, TReaderStatistics* readerStatistics, bool /*onlyKeyBlocks*/) override
    {
        ++readerStatistics->TryUpdateWindowCallCount;
        TCpuDurationIncrementingGuard timingGuard(&readerStatistics->FetchBlockTime);
        readerStatistics->SkipToBlockCallCount += BlockHolders_.size();

        for (auto& blockHolder : BlockHolders_) {
            if (auto blockId = blockHolder.SkipToBlock(rowIndex)) {
                ++readerStatistics->SetBlockCallCount;
                UsedBlocks_.push_back(blockHolder.SwitchBlock(GetBlock(*blockId)));
            }
        }

        return true;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return OKFuture;
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
    const NChunkClient::IBlockCachePtr BlockCache_;
    const NChunkClient::TChunkId ChunkId_;
    const NCompression::ECodec CodecId_;

    // Need to keed used blocks within single Read call because
    // TryUpdateWindow always return true and does not interrupt reading when switching blocks.
    // In this case blocks can be switched multiple times in single Read call.
    std::vector<TSharedRef> UsedBlocks_;
    TDuration DecompressionDuration_;

    TSharedRef GetBlock(int blockIndex)
    {
        NChunkClient::TBlockId blockId(ChunkId_, blockIndex);

        if (auto block = BlockCache_->FindBlock(blockId, EBlockType::UncompressedData)) {
            return std::move(block.Data);
        }

        auto compressedBlock = BlockCache_->FindBlock(blockId, EBlockType::CompressedData);
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

        YT_LOG_FATAL("Cached block is missing (BlockId: %v)",
            blockId);
    }
};

TBlockManagerFactory CreateSyncBlockWindowManagerFactory(
    IBlockCachePtr blockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    NChunkClient::TChunkId chunkId)
{
    return [blockCache = std::move(blockCache), chunkMeta = std::move(chunkMeta), chunkId] (
        std::vector<TGroupBlockHolder> blockHolders,
        TRange<TSpanMatching> /*windowsList*/,
        TRange<ui32> /*blockUncompressedSizes*/
    ) mutable -> std::unique_ptr<IBlockManager> {
        return std::make_unique<TSyncBlockWindowManager>(
            std::move(blockHolders),
            std::move(blockCache),
            chunkId,
            FromProto<NCompression::ECodec>(chunkMeta->Misc().compression_codec()));
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat
