#pragma once

#include "public.h"
#include "chunk_meta_extensions.h"

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Options to insert null values with given column IDs into the resulting rows.
//!
//! This is required if the chunk schema has smaller key length than table schema. Usually, this is
//! the result of alter-table with key extension or chunk teleportation.
struct TKeyWideningOptions
{
    int InsertPosition = -1;
    std::vector<int> InsertedColumnIds;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<bool> GetCompositeColumnFlags(const TTableSchemaPtr& schema);
// COMPAT(babenko): the first two arguments are needed due to YT-19339.
std::vector<bool> GetHunkColumnFlags(
    NChunkClient::EChunkFormat chunkFormat,
    NChunkClient::EChunkFeatures chunkFeatures,
    const TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

class THorizontalBlockReader
    : public TNonCopyable
{
public:
    /*!
     *  For schemaless blocks id mapping must be of the chunk name table size.
     *  It maps chunk ids to reader ids.
     *  Column is omitted if reader id < 0.
     */
    THorizontalBlockReader(
        const TSharedRef& block,
        const NProto::TDataBlockMeta& meta,
        const std::vector<bool>& compositeColumnFlags,
        const std::vector<bool>& hunkColumnFlags,
        const std::vector<THunkChunkRef>& hunkChunkRefs,
        const std::vector<THunkChunkMeta>& hunkChunkMetas,
        const std::vector<int>& chunkToReaderIdMapping,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        const TKeyWideningOptions& keyWideningOptions,
        int extraColumnCount = 0);

    bool NextRow();

    bool SkipToRowIndex(i64 rowIndex);
    bool SkipToKeyBound(const TKeyBoundRef& lowerBound);
    bool SkipToKey(TUnversionedRow lowerBound);

    bool JumpToRowIndex(i64 rowIndex);

    TLegacyKey GetLegacyKey() const;
    TKey GetKey() const;
    TMutableUnversionedRow GetRow(TChunkedMemoryPool* memoryPool);

    i64 GetRowIndex() const;

protected:
    TMutableVersionedRow GetVersionedRow(TChunkedMemoryPool* memoryPool, TTimestamp timestamp);

private:
    const TSharedRef Block_;
    const NProto::TDataBlockMeta Meta_;

    // Maps chunk name table ids to client name table ids.
    const std::vector<int> ChunkToReaderIdMapping_;
    const std::vector<bool> CompositeColumnFlags_;
    const std::vector<bool> HunkColumnFlags_;

    const std::vector<THunkChunkRef>& HunkChunkRefs_;
    const std::vector<THunkChunkMeta>& HunkChunkMetas_;

    const TKeyWideningOptions KeyWideningOptions_;

    // If chunk key column count is smaller than key column count, key is extended with Nulls.
    // If chunk key column count is larger than key column count, key is trimmed.

    const TRange<ESortOrder> SortOrders_;
    const int CommonKeyPrefix_;

    // Count of extra row values, that are allocated and reserved
    // to be filled by upper levels (e.g. table_index).
    const int ExtraColumnCount_;

    TRef Data_;
    TRef Offsets_;

    i64 RowIndex_;
    const char* CurrentPointer_;
    ui32 ValueCount_;

    constexpr static size_t DefaultKeyBufferCapacity = 128;

    TCompactVector<char, DefaultKeyBufferCapacity> KeyBuffer_;
    TMutableUnversionedRow Key_;

    bool IsHunkValue(TUnversionedValue value);
    TUnversionedValue DecodeAnyValue(TUnversionedValue value);
    int GetChunkKeyColumnCount() const;
    int GetKeyColumnCount() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
