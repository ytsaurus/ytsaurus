#pragma once

#include "public.h"
#include "block_ref.h"
#include "reader_statistics.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/chunk_client/block_fetcher.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

class TGroupBlockHolder
    : public TBlockRef
{
public:
    TGroupBlockHolder(const TGroupBlockHolder&) = delete;
    TGroupBlockHolder(TGroupBlockHolder&&) = default;

    TGroupBlockHolder(TRange<ui32> blockIds, std::vector<TSharedRef> blockSegmentsMetas);

    bool NeedUpdateBlock(ui32 rowIndex) const;

    void SetBlock(TSharedRef data, const TRefCountedBlockMetaPtr& blockMeta);

    // Returns block id.
    std::optional<ui32> SkipToBlock(ui32 rowIndex, const TRefCountedBlockMetaPtr& blockMeta);

    TRange<ui32> GetBlockIds() const;

private:
    const TRange<ui32> BlockIds_;
    std::vector<TSharedRef> BlockSegmentsMetas_;

    ui32 BlockRowLimit_ = 0;
    ui32 BlockIdIndex_ = 0;
};

// Returns ordered unique group ids. Can determine group index via binary search.
std::vector<ui16> GetGroupsIds(
    const TPreparedChunkMeta& preparedChunkMeta,
    ui16 keyColumnCount,
    TRange<TColumnIdMapping> valuesIdMapping);

std::vector<std::unique_ptr<TGroupBlockHolder>> CreateGroupBlockHolders(
    const TPreparedChunkMeta& preparedChunkMeta,
    TRange<ui16> groupIds);

////////////////////////////////////////////////////////////////////////////////

// Rows (and theirs indexes) in chunk are partitioned by block borders.
// Block borders are block last keys and corresponding block last indexes (block.chunk_row_count).
// Window is the range of row indexes between block borders.
// TBlockWindowManager updates blocks in column block holders for each window (range of row indexes between block).
class TBlockWindowManager
{
public:
    TBlockWindowManager(
        std::vector<std::unique_ptr<TGroupBlockHolder>> groups,
        TRefCountedBlockMetaPtr blockMeta,
        NChunkClient::TBlockFetcherPtr blockFetcher,
        TReaderStatisticsPtr readerStatistics);

    // Returns false if need wait for ready event.
    bool TryUpdateWindow(ui32 rowIndex);

    TFuture<void> GetReadyEvent() const;

    const TBlockRef* GetBlockHolder(ui16 index);

protected:
    std::vector<std::unique_ptr<TGroupBlockHolder>> BlockHolders_;
    NChunkClient::TBlockFetcherPtr BlockFetcher_;
    TReaderStatisticsPtr ReaderStatistics_;

private:
    const TRefCountedBlockMetaPtr BlockMeta_;
    TFuture<std::vector<NChunkClient::TBlock>> FetchedBlocks_;
    TFuture<void> ReadyEvent_ = VoidFuture;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
