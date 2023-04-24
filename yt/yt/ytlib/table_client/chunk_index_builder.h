#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/range.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkIndexBuilder
    : public TRefCounted
{
    struct TChunkIndexEntry
    {
        int BlockIndex;
        i64 RowOffset;
        i64 RowLength;
        TSharedRange<int> GroupOffsets;
        TSharedRange<int> GroupIndexes;
    };

    //! Processes new entry.
    virtual void ProcessRow(TVersionedRow row, TChunkIndexEntry entry) = 0;

    //! Builds an index and populates meta based on processed entries.
    //! Each vector item corresponds to a single system block that is to be appended to the chunk.
    virtual std::vector<TSharedRef> BuildIndex(
        TUnversionedValueRange lastKey,
        NProto::TSystemBlockMetaExt* systemBlockMetaExt) = 0;

    //! Same as above but with predefined start slot indexes for each entry. Intended for testing purposes
    virtual std::vector<TSharedRef> BuildIndex(
        TUnversionedValueRange lastKey,
        NProto::TSystemBlockMetaExt* systemBlockMetaExt,
        const std::vector<int>& rowToSlotIndex) = 0;

    virtual NChunkClient::EBlockType GetBlockType() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkIndexBuilder)

////////////////////////////////////////////////////////////////////////////////

IChunkIndexBuilderPtr CreateChunkIndexBuilder(
    const TChunkIndexesWriterConfigPtr& config,
    const TIndexedVersionedBlockFormatDetail& blockFormatDetail,
    const NLogging::TLogger& logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
