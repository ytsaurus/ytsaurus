#pragma once

#include "public.h"
#include "reader_statistics.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/chunk_client/block_fetcher.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

using TSegmentMetas = TRange<const NProto::TSegmentMeta*>;
using TSharedSegmentMetas = TSharedRange<const NProto::TSegmentMeta*>;

////////////////////////////////////////////////////////////////////////////////

class TColumnBlockHolder
{
public:
    TColumnBlockHolder(const TColumnBlockHolder&) = delete;
    TColumnBlockHolder(TColumnBlockHolder&&) = default;

    explicit TColumnBlockHolder(TSharedSegmentMetas segmentMetas);

    const NProto::TSegmentMeta* SkipToSegment(ui32 rowIndex);

    TRef GetBlock() const;

    TSegmentMetas GetSegmentMetas() const;

    bool NeedUpdateBlock(ui32 rowIndex) const;

    void SetBlock(TSharedRef data, const TRefCountedBlockMetaPtr& blockMeta);

    // Returns block id.
    std::optional<ui32> SkipToBlock(ui32 rowIndex, const TRefCountedBlockMetaPtr& blockMeta);

    TRange<ui32> GetBlockIds() const;

private:
    const TSharedSegmentMetas SegmentMetas_;
    const std::vector<ui32> BlockIds_;

    // Blob value data is stored in blocks without capturing in TRowBuffer.
    // Therefore block must not change during from the current call of ReadRows till the next one.
    TSharedRef Block_;

    ui32 SegmentStart_ = 0;
    ui32 SegmentEnd_ = 0;

    ui32 BlockIdIndex_ = 0;
    ui32 BlockRowLimit_ = 0;

    // TODO(lukyan): Do not keep SegmentRowLimit_ here. Use value from IColumnBase.
    ui32 SegmentRowLimit_ = 0;
};

std::vector<TColumnBlockHolder> CreateColumnBlockHolders(
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TRange<TColumnIdMapping> valueIdMapping);

////////////////////////////////////////////////////////////////////////////////

// Rows (and theirs indexes) in chunk are partitioned by block borders.
// Block borders are block last keys and corresponding block last indexes (block.chunk_row_count).
// Window is the range of row indexes between block borders.
// TBlockWindowManager updates blocks in column block holders for each window (range of row indexes between block).
class TBlockWindowManager
{
public:
    TBlockWindowManager(
        std::vector<TColumnBlockHolder> columns,
        TRefCountedBlockMetaPtr blockMeta,
        NChunkClient::TBlockFetcherPtr blockFetcher,
        TReaderTimeStatisticsPtr timeStatistics);

    // Returns true if need wait for ready event.
    bool TryUpdateWindow(ui32 rowIndex);

    TFuture<void> GetReadyEvent() const;

protected:
    std::vector<TColumnBlockHolder> Columns_;
    NChunkClient::TBlockFetcherPtr BlockFetcher_;
    TReaderTimeStatisticsPtr TimeStatistics_;

private:
    const TRefCountedBlockMetaPtr BlockMeta_;
    TFuture<std::vector<NChunkClient::TBlock>> FetchedBlocks_;
    TFuture<void> ReadyEvent_ = VoidFuture;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
