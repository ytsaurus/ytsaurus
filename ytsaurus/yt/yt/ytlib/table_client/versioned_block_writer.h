#pragma once

#include "public.h"
#include "private.h"
#include "block.h"
#include "chunk_index.h"
#include "chunk_meta_extensions.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/bitmap.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

int GetSimpleVersionedBlockPaddedKeySize(int keyColumnCount, int schemaColumnCount);

////////////////////////////////////////////////////////////////////////////////

class TVersionedBlockWriterBase
{
protected:
    TVersionedBlockWriterBase(
        TTableSchemaPtr schema,
        TMemoryUsageTrackerGuard guard);

public:
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MinTimestamp, MaxTimestamp);
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MaxTimestamp, MinTimestamp);

    DEFINE_BYVAL_RO_PROPERTY(int, RowCount, 0);

protected:
    const TTableSchemaPtr Schema_;
    const int SchemaColumnCount_;
    const int KeyColumnCount_;

    TMemoryUsageTrackerGuard MemoryGuard_;


    void UpdateMinMaxTimestamp(TTimestamp timestamp);

    TBlock FlushBlock(
        std::vector<TSharedRef> blockParts,
        NProto::TDataBlockMeta meta);

    bool IsInlineHunkValue(const TUnversionedValue& value) const;

private:
    const std::unique_ptr<bool[]> ColumnHunkFlags_;
};

////////////////////////////////////////////////////////////////////////////////

struct TSimpleVersionedBlockWriterTag
{ };

class TSimpleVersionedBlockWriter
    : public TVersionedBlockWriterBase
{
public:
    explicit TSimpleVersionedBlockWriter(
        TTableSchemaPtr schema,
        TMemoryUsageTrackerGuard guard = {});

    void WriteRow(TVersionedRow row);

    TBlock FlushBlock();

    i64 GetBlockSize() const;

private:
    TChunkedOutputStream KeyStream_{GetRefCountedTypeCookie<TSimpleVersionedBlockWriterTag>()};
    TBitmapOutput KeyNullFlags_;

    TChunkedOutputStream ValueStream_{GetRefCountedTypeCookie<TSimpleVersionedBlockWriterTag>()};
    TBitmapOutput ValueNullFlags_;
    std::optional<TBitmapOutput> ValueAggregateFlags_;

    TChunkedOutputStream TimestampStream_{GetRefCountedTypeCookie<TSimpleVersionedBlockWriterTag>()};

    TChunkedOutputStream StringDataStream_{GetRefCountedTypeCookie<TSimpleVersionedBlockWriterTag>()};

    i64 TimestampCount_ = 0;
    i64 ValueCount_ = 0;

    void WriteValue(
        TChunkedOutputStream& stream,
        TBitmapOutput& nullFlags,
        std::optional<TBitmapOutput>& aggregateFlags,
        const TUnversionedValue& value);
};

////////////////////////////////////////////////////////////////////////////////

struct TIndexedVersionedBlockWriterTag
{ };

class TIndexedVersionedBlockWriter
    : public TVersionedBlockWriterBase
{
public:
    TIndexedVersionedBlockWriter(
        TTableSchemaPtr schema,
        int blockIndex,
        const TIndexedVersionedBlockFormatDetail& blockFormatDetail,
        IChunkIndexBuilderPtr chunkIndexBuilder,
        TMemoryUsageTrackerGuard guard = {});

    void WriteRow(TVersionedRow row);

    TBlock FlushBlock();

    i64 GetBlockSize() const;

    static int GetBlockFormatVersion();

private:
    const TIndexedVersionedBlockFormatDetail& BlockFormatDetail_;
    const IChunkIndexBuilderPtr ChunkIndexBuilder_;
    const int BlockIndex_;
    const int GroupCount_;
    // TODO(akozhikhov): Consider having this flag per group to save space
    // if aggregate flag bitmaps are not needed for particular groups.
    const bool HasAggregateColumns_;
    const i64 SectorAlignmentSize_;
    const bool EnableGroupReordering_;

    TChunkedOutputStream Stream_{GetRefCountedTypeCookie<TIndexedVersionedBlockWriterTag>()};

    std::vector<i64> RowOffsets_;
    std::vector<int> GroupOffsets_;
    std::vector<int> GroupIndexes_;

    struct TRowData
    {
        struct TKeyData
        {
            i64 StringDataSize;
        };

        struct TValueData
        {
            struct TGroupData
            {
                i64 StringDataSize;
                int ValueCount;
                std::vector<std::pair<int, int>> ColumnValueRanges;
                i64 SectorAlignmentSize;
            };

            TCompactVector<TGroupData, IndexedRowTypicalGroupCount> Groups;
            TCompactVector<int, IndexedRowTypicalGroupCount> GroupOrder;
        };

        TKeyData KeyData;
        TValueData ValueData;

        TVersionedRow Row;

        i64 RowSectorAlignmentSize;
    };

    TRowData RowData_;


    void ResetRowData(TVersionedRow row);
    void ResetKeyData();
    void ResetValueData();

    i64 GetRowByteSize() const;
    i64 GetRowMetadataByteSize() const;
    i64 GetKeyDataByteSize() const;
    i64 GetTimestampDataByteSize() const;
    i64 GetValueDataByteSize() const;
    i64 GetValueGroupDataByteSize(int groupIndex, bool includeSectorAlignment) const;

    void EncodeRow(char* buffer);
    char* EncodeRowMetadata(char* buffer);
    char* EncodeKeyData(char* buffer) const;
    char* EncodeTimestampData(char* buffer);
    char* EncodeValueData(char* buffer);
    char* EncodeValueGroupData(char* buffer, int groupIndex) const;

    TCompactVector<int, IndexedRowTypicalGroupCount> ComputeGroupAlignmentAndReordering();

    void DoWriteValue(
        char*& buffer,
        char*& stringBuffer,
        int valueIndex,
        const TUnversionedValue& value,
        TMutableBitmap* nullFlagsBitmap,
        std::optional<TMutableBitmap>* aggregateFlagsBitmap) const;

    i64 GetUnalignedBlockSize() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
