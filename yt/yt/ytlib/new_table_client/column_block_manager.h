#pragma once

#include "public.h"
#include "block_ref.h"
#include "reader_statistics.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

class TGroupBlockHolder
    : public TBlockRef
{
public:
    TGroupBlockHolder(const TGroupBlockHolder&) = delete;
    TGroupBlockHolder(TGroupBlockHolder&&);

    TGroupBlockHolder(
        TRange<ui32> blockIds,
        TRange<ui32> blockChunkRowCounts,
        TRange<TSharedRef> blockSegmentsMetas,
        TRange<ui32> metaOffsetsInBlocks);

    bool NeedUpdateBlock(ui32 rowIndex) const;

    // Returns old block.
    TSharedRef SwitchBlock(TSharedRef data);

    // Returns block id.
    std::optional<ui32> SkipToBlock(ui32 rowIndex);

    TRange<ui32> GetBlockIds() const;

private:
    const TRange<ui32> BlockIds_;
    const TRange<ui32> BlockChunkRowCounts_;
    const TRange<TSharedRef> BlockSegmentsMetas_;
    const TRange<ui32> MetaOffsetsInBlocks_;

    ui32 BlockRowLimit_ = 0;
    ui32 BlockIdIndex_ = 0;
};

// Returns ordered unique group ids. Can determine group index via binary search.
TCompactVector<ui16, 32> GetGroupsIds(
    const TPreparedChunkMeta& preparedChunkMeta,
    ui16 keyColumnCount,
    TRange<TColumnIdMapping> valuesIdMapping);

std::vector<TGroupBlockHolder> CreateGroupBlockHolders(
    const TPreparedChunkMeta& preparedChunkMeta,
    TRange<ui16> groupIds);

////////////////////////////////////////////////////////////////////////////////

struct IBlockManager
{
    virtual ~IBlockManager() = default;

    virtual void ClearUsedBlocks() = 0;
    virtual bool TryUpdateWindow(ui32 rowIndex, TReaderStatistics* readerStatistics = nullptr) = 0;

    virtual TFuture<void> GetReadyEvent() const = 0;

    // TODO(lukyan): Remove this methods from here and from IReaderBase.
    virtual bool IsFetchingCompleted() const = 0;
    virtual i64 GetUncompressedDataSize() const = 0;
    virtual i64 GetCompressedDataSize() const = 0;
    virtual NChunkClient::TCodecDuration GetDecompressionTime() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
