#include "column_block_manager.h"

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/yt/core/misc/algorithm_helpers.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

using NProfiling::TValueIncrementingTimingGuard;
using NProfiling::TWallTimer;

////////////////////////////////////////////////////////////////////////////////

std::vector<ui32> BuildColumnBlockSequence(TSegmentMetas segmentMetas)
{
    std::vector<ui32> blockSequence;
    ui32 segmentIndex = 0;

    while (segmentIndex < segmentMetas.size()) {
        auto blockIndex = segmentMetas[segmentIndex]->block_index();
        blockSequence.push_back(blockIndex);
        do {
            ++segmentIndex;
        } while (segmentIndex < segmentMetas.size() &&
             segmentMetas[segmentIndex]->block_index() == blockIndex);
    }
    return blockSequence;
}

TColumnBlockHolder::TColumnBlockHolder(TSharedSegmentMetas segmentMetas)
    : SegmentMetas_(std::move(segmentMetas))
    , BlockIds_(BuildColumnBlockSequence(SegmentMetas_))
{ }

const NProto::TSegmentMeta* TColumnBlockHolder::SkipToSegment(ui32 rowIndex)
{
    if (rowIndex < SegmentRowLimit_) {
        return nullptr;
    }

    auto segmentIt = ExponentialSearch(
        SegmentMetas_.begin() + SegmentStart_,
        SegmentMetas_.begin() + SegmentEnd_,
        [&] (auto segmentMetaIt) {
            return (*segmentMetaIt)->chunk_row_count() <= rowIndex;
        });

    if (segmentIt != SegmentMetas_.begin() + SegmentEnd_) {
        YT_VERIFY(Block_);
        SegmentRowLimit_ = (*segmentIt)->chunk_row_count();
        return *segmentIt;
    }

    return nullptr;
}

TRef TColumnBlockHolder::GetBlock() const
{
    return Block_;
}

TSegmentMetas TColumnBlockHolder::GetSegmentMetas() const
{
    return SegmentMetas_.Slice(SegmentStart_, SegmentEnd_);
}

bool TColumnBlockHolder::NeedUpdateBlock(ui32 rowIndex) const
{
    return rowIndex >= BlockRowLimit_ && BlockIdIndex_ < BlockIds_.size();
}

void TColumnBlockHolder::SetBlock(TSharedRef data, const TRefCountedBlockMetaPtr& blockMeta)
{
    YT_VERIFY(BlockIdIndex_ < BlockIds_.size());
    int blockId = BlockIds_[BlockIdIndex_];

    Block_ = data;

    auto segmentIt = ExponentialSearch(
        SegmentMetas_.begin() + SegmentStart_,
        SegmentMetas_.end(),
        [&] (auto segmentMetaIt) {
            return (*segmentMetaIt)->block_index() < blockId;
        });

    auto segmentItEnd = ExponentialSearch(
        segmentIt,
        SegmentMetas_.end(),
        [&] (auto segmentMetaIt) {
            return (*segmentMetaIt)->block_index() <= blockId;
        });

    SegmentStart_ = segmentIt - SegmentMetas_.begin();
    SegmentEnd_ = segmentItEnd - SegmentMetas_.begin();

    BlockRowLimit_ = blockMeta->blocks(blockId).chunk_row_count();
    YT_VERIFY(segmentIt != segmentItEnd);

    auto limitBySegment = segmentItEnd[-1]->chunk_row_count();
    YT_VERIFY(limitBySegment == BlockRowLimit_);
}

// TODO(lukyan): Use block row limits vector instead of blockMeta.
std::optional<ui32> TColumnBlockHolder::SkipToBlock(ui32 rowIndex, const TRefCountedBlockMetaPtr& blockMeta)
{
    if (!NeedUpdateBlock(rowIndex)) {
        return std::nullopt;
    }

    // Need to find block with rowIndex.
    while (BlockIdIndex_ < BlockIds_.size() &&
        blockMeta->blocks(BlockIds_[BlockIdIndex_]).chunk_row_count() <= rowIndex)
    {
        ++BlockIdIndex_;
    }

    // It is used for generating sentinel rows in lookup (for keys after end of chunk).
    if (BlockIdIndex_ == BlockIds_.size()) {
        return std::nullopt;
    }

    YT_VERIFY(BlockIdIndex_ < BlockIds_.size());
    return BlockIds_[BlockIdIndex_];
}

TRange<ui32> TColumnBlockHolder::GetBlockIds() const
{
    return BlockIds_;
}

std::vector<TColumnBlockHolder> CreateColumnBlockHolders(
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TRange<TColumnIdMapping> valueIdMapping)
{
    std::vector<TColumnBlockHolder> columns;
    const auto& columnMetas = chunkMeta->ColumnMeta();

    // Init columns.
    {
        int timestampReaderIndex = chunkMeta->ColumnMeta()->columns().size() - 1;
        const auto& columnMeta = columnMetas->columns(timestampReaderIndex);
        columns.emplace_back(TSharedSegmentMetas(MakeRange(columnMeta.segments()), columnMetas));
    }

    for (int index = 0; index < chunkMeta->GetChunkKeyColumnCount(); ++index) {
        const auto& columnMeta = columnMetas->columns(index);
        columns.emplace_back(TSharedSegmentMetas(MakeRange(columnMeta.segments()), columnMetas));
    }

    for (size_t index = 0; index < valueIdMapping.size(); ++index) {
        const auto& columnMeta = columnMetas->columns(valueIdMapping[index].ChunkSchemaIndex);
        columns.emplace_back(TSharedSegmentMetas(MakeRange(columnMeta.segments()), columnMetas));
    }

    return columns;
}

/////////////////////////////////////////////////////////////////////////////

TBlockWindowManager::TBlockWindowManager(
    std::vector<TColumnBlockHolder> columns,
    TRefCountedBlockMetaPtr blockMeta,
    TBlockFetcherPtr blockFetcher,
    TReaderTimeStatisticsPtr timeStatistics)
    : Columns_(std::move(columns))
    , BlockFetcher_(std::move(blockFetcher))
    , TimeStatistics_(timeStatistics)
    , BlockMeta_(std::move(blockMeta))
{ }

bool TBlockWindowManager::TryUpdateWindow(ui32 rowIndex)
{
    TValueIncrementingTimingGuard<TWallTimer> timingGuard(&TimeStatistics_->FetchBlockTime);
    if (FetchedBlocks_) {
        if (!FetchedBlocks_.IsSet()) {
            // Blocks has been already requested from previous Read but are not fetched yet.
            return true;
        }

        YT_VERIFY(FetchedBlocks_.IsSet());

        const auto& loadedBlocks = FetchedBlocks_.Get().ValueOrThrow();

        size_t index = 0;
        for (auto& column : Columns_) {
            if (column.NeedUpdateBlock(rowIndex)) {
                YT_VERIFY(index < loadedBlocks.size());
                column.SetBlock(loadedBlocks[index++].Data, BlockMeta_);
            }
        }
        YT_VERIFY(index == loadedBlocks.size());

        FetchedBlocks_.Reset();
        return false;
    }

    // Skip to window.
    std::vector<TFuture<TBlock>> pendingBlocks;
    for (auto& column : Columns_) {
        if (auto blockId = column.SkipToBlock(rowIndex, BlockMeta_)) {
            YT_VERIFY(column.NeedUpdateBlock(rowIndex));
            pendingBlocks.push_back(BlockFetcher_->FetchBlock(*blockId));
        }
    }

    // Not every window switch causes block updates.
    // Read windows are built by all block last keys but here only reading block set is considered.
    if (pendingBlocks.empty()) {
        return false;
    }

    FetchedBlocks_ = AllSucceeded(std::move(pendingBlocks));
    ReadyEvent_ = FetchedBlocks_.As<void>();

    return true;
}

TFuture<void> TBlockWindowManager::GetReadyEvent() const
{
    return ReadyEvent_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
