#include "column_block_manager.h"

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/yt/core/misc/algorithm_helpers.h>

#include "memory_helpers.h"
#include "dispatch_by_type.h"
#include "prepared_meta.h"

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

using NProfiling::TValueIncrementingTimingGuard;
using NProfiling::TWallTimer;

////////////////////////////////////////////////////////////////////////////////

TGroupBlockHolder::TGroupBlockHolder(TRange<ui32> blockIds, std::vector<TSharedRef> blockSegmentsMetas)
    : BlockIds_(blockIds)
    , BlockSegmentsMetas_(blockSegmentsMetas)
{ }

bool TGroupBlockHolder::NeedUpdateBlock(ui32 rowIndex) const
{
    return rowIndex >= BlockRowLimit_ && BlockIdIndex_ < BlockIds_.size();
}

void TGroupBlockHolder::SetBlock(TSharedRef data, const TRefCountedBlockMetaPtr& blockMeta)
{
    YT_VERIFY(BlockIdIndex_ < BlockIds_.size());
    int blockId = BlockIds_[BlockIdIndex_];

    BlockSegmentsMeta = BlockSegmentsMetas_[BlockIdIndex_];

    Block = data;
    BlockRowLimit_ = blockMeta->blocks(blockId).chunk_row_count();
}

// TODO(lukyan): Use block row limits vector instead of blockMeta.
std::optional<ui32> TGroupBlockHolder::SkipToBlock(ui32 rowIndex, const TRefCountedBlockMetaPtr& blockMeta)
{
    if (!NeedUpdateBlock(rowIndex)) {
        return std::nullopt;
    }

    // Need to find block with rowIndex.
    BlockIdIndex_ = ExponentialSearch<ui32>(BlockIdIndex_, BlockIds_.size(), [&] (ui32 blockIdIndex) {
        return blockMeta->blocks(BlockIds_[blockIdIndex]).chunk_row_count() <= rowIndex;
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
    for (int index = 0; index < keyColumnCount; ++index) {
        groupIds.push_back(preparedChunkMeta.GroupIdPerColumn[index]);
    }

    for (auto [chunkSchemaIndex, readerSchemaIndex] : valuesIdMapping) {
        groupIds.push_back(preparedChunkMeta.GroupIdPerColumn[chunkSchemaIndex]);
    }

    // TODO(lukyan): Or use first group for timestamp?
    auto timestampGroupIndex = preparedChunkMeta.ColumnGroups.size() - 1;
    groupIds.push_back(timestampGroupIndex);

    std::sort(groupIds.begin(), groupIds.end());
    groupIds.erase(std::unique(groupIds.begin(), groupIds.end()), groupIds.end());

    return groupIds;
}

// Create group block holders using set of reading groups' ids.
std::vector<std::unique_ptr<TGroupBlockHolder>> CreateGroupBlockHolders(
    const TPreparedChunkMeta& preparedChunkMeta,
    TRange<ui16> groupIds)
{
    std::vector<std::unique_ptr<TGroupBlockHolder>> groupHolders;
    for (auto groupId : groupIds) {
        groupHolders.emplace_back(std::make_unique<TGroupBlockHolder>(
            preparedChunkMeta.ColumnGroups[groupId].BlockIds,
            preparedChunkMeta.ColumnGroups[groupId].MergedMetas));
    }

    return groupHolders;
}

/////////////////////////////////////////////////////////////////////////////

TBlockWindowManager::TBlockWindowManager(
    std::vector<std::unique_ptr<TGroupBlockHolder>> blockHolders,
    TRefCountedBlockMetaPtr blockMeta,
    TBlockFetcherPtr blockFetcher,
    TReaderStatisticsPtr readerStatistics)
    : BlockHolders_(std::move(blockHolders))
    , BlockFetcher_(std::move(blockFetcher))
    , ReaderStatistics_(readerStatistics)
    , BlockMeta_(std::move(blockMeta))
{ }

bool TBlockWindowManager::TryUpdateWindow(ui32 rowIndex)
{
    ++ReaderStatistics_->TryUpdateWindowCallCount;

    TValueIncrementingTimingGuard<TWallTimer> timingGuard(&ReaderStatistics_->FetchBlockTime);
    if (FetchedBlocks_) {
        if (!FetchedBlocks_.IsSet()) {
            // Blocks has been already requested from previous Read but are not fetched yet.
            return false;
        }

        const auto& loadedBlocks = FetchedBlocks_.Get().ValueOrThrow();

        size_t index = 0;
        for (auto& blockHolder : BlockHolders_) {
            if (auto blockId = blockHolder->SkipToBlock(rowIndex, BlockMeta_)) {
                ++ReaderStatistics_->SetBlockCallCount;
                YT_VERIFY(index < loadedBlocks.size());
                blockHolder->SetBlock(loadedBlocks[index++].Data, BlockMeta_);
            }
        }
        YT_VERIFY(index == loadedBlocks.size());

        FetchedBlocks_.Reset();
        return true;
    }

    // Skip to window.
    std::vector<TFuture<TBlock>> pendingBlocks;
    for (auto& blockHolder : BlockHolders_) {
        ++ReaderStatistics_->SkipToBlockCallCount;
        if (auto blockId = blockHolder->SkipToBlock(rowIndex, BlockMeta_)) {
            ++ReaderStatistics_->FetchBlockCallCount;
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

TFuture<void> TBlockWindowManager::GetReadyEvent() const
{
    return ReadyEvent_;
}

const TBlockRef* TBlockWindowManager::GetBlockHolder(ui16 index)
{
    return BlockHolders_[index].get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
