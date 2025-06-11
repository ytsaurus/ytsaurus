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

class IHorizontalBlockReader
    : public TNonCopyable
{
public:
    virtual ~IHorizontalBlockReader() = default;

    virtual bool NextRow() = 0;
    virtual bool SkipToRowIndex(i64 rowIndex) = 0;
    virtual bool SkipToKeyBound(const TKeyBoundRef& lowerBound) = 0;
    virtual bool SkipToKey(TUnversionedRow lowerBound) = 0;

    virtual bool JumpToRowIndex(i64 rowIndex) = 0;

    virtual TLegacyKey GetLegacyKey() const = 0;
    virtual TKey GetKey() const = 0;
    virtual TMutableUnversionedRow GetRow(TChunkedMemoryPool* memoryPool, bool remapIds) = 0;

    virtual i64 GetRowIndex() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class THorizontalBlockReader
    : public IHorizontalBlockReader
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
        int extraColumnCount = 0,
        bool decodeInlineHunkValues = false);

    bool NextRow() override;

    bool SkipToRowIndex(i64 rowIndex) override;
    bool SkipToKeyBound(const TKeyBoundRef& lowerBound) override;
    bool SkipToKey(TUnversionedRow lowerBound) override;

    bool JumpToRowIndex(i64 rowIndex) override;

    TLegacyKey GetLegacyKey() const override;
    TKey GetKey() const override;
    TMutableUnversionedRow GetRow(TChunkedMemoryPool* memoryPool, bool remapIds) override;

    i64 GetRowIndex() const override;

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

    const bool DecodeInlineHunkValues_;

    TRef Data_;
    TRef Offsets_;

    i64 RowIndex_;
    const char* CurrentPointer_;
    ui32 ValueCount_;

    static constexpr size_t DefaultKeyBufferCapacity = 128;

    TCompactVector<char, DefaultKeyBufferCapacity> KeyBuffer_;
    TMutableUnversionedRow Key_;

    bool IsHunkValue(TUnversionedValue value);
    TUnversionedValue DecodeAnyValue(TUnversionedValue value);
    int GetChunkKeyColumnCount() const;
    int GetKeyColumnCount() const;
};

////////////////////////////////////////////////////////////////////////////////

class TArrowHorizontalBlockReader
    : public IHorizontalBlockReader
{
public:
    TArrowHorizontalBlockReader(
        const TSharedRef& block,
        const NProto::TDataBlockMeta& dataBlockMeta,
        const TColumnarChunkMetaPtr& chunkMeta,
        const std::vector<bool>& compositeColumnFlags,
        const std::vector<bool>& hunkColumnFlags,
        const std::vector<THunkChunkRef>& hunkChunkRefs,
        const std::vector<THunkChunkMeta>& hunkChunkMetas,
        const std::vector<int>& chunkToReaderIdMapping,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        const TKeyWideningOptions& keyWideningOptions,
        int extraColumnCount = 0,
        bool decodeInlineHunkValues = false);

    bool NextRow() override;
    bool SkipToRowIndex(i64 rowIndex) override;
    bool SkipToKeyBound(const TKeyBoundRef& lowerBound) override;
    bool SkipToKey(TUnversionedRow lowerBound) override;

    bool JumpToRowIndex(i64 rowIndex) override;

    TLegacyKey GetLegacyKey() const override;
    TKey GetKey() const override;
    TMutableUnversionedRow GetRow(TChunkedMemoryPool* memoryPool, bool remapIds) override;

    i64 GetRowIndex() const override;

private:
    const TSharedRef Block_;
    const NProto::TDataBlockMeta DataBlockMeta_;
    const TColumnarChunkMetaPtr ChunkMeta_;

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

    const bool DecodeInlineHunkValues_;

    i64 RowIndex_ = 0;
    std::vector<TUnversionedOwningRow> Rows_;

    TCompactVector<char, 128> KeyBuffer_;
    TMutableUnversionedRow Key_;

    int GetChunkKeyColumnCount() const;
    int GetKeyColumnCount() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
