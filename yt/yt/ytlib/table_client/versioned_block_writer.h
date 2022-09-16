#pragma once

#include "public.h"
#include "private.h"
#include "block.h"
#include "chunk_meta_extensions.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/memory_trackers/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/bitmap.h>
#include <yt/yt/core/misc/chunked_output_stream.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

constexpr int SimpleVersionedBlockValueSize = 16;
constexpr int SimpleVersionedBlockTimestampSize = 8;

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


    TBlock FlushBlock(
        std::vector<TSharedRef> blockParts,
        NProto::TDataBlockMeta meta);

    bool IsInlineHunkValue(const TUnversionedValue& value) const;

private:
    const std::unique_ptr<bool[]> ColumnHunkFlags_;
};

////////////////////////////////////////////////////////////////////////////////

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
    TChunkedOutputStream KeyStream_;
    TBitmapOutput KeyNullFlags_;

    TChunkedOutputStream ValueStream_;
    TBitmapOutput ValueNullFlags_;
    std::optional<TBitmapOutput> ValueAggregateFlags_;

    TChunkedOutputStream TimestampStream_;

    TChunkedOutputStream StringDataStream_;

    i64 TimestampCount_ = 0;
    i64 ValueCount_ = 0;


    void WriteValue(
        TChunkedOutputStream& stream,
        TBitmapOutput& nullFlags,
        std::optional<TBitmapOutput>& aggregateFlags,
        const TUnversionedValue& value);
};

////////////////////////////////////////////////////////////////////////////////

class TIndexedVersionedBlockWriter
    : public TVersionedBlockWriterBase
{
public:
    TIndexedVersionedBlockWriter(
        TTableSchemaPtr schema,
        int blockIndex,
        IChunkIndexBuilderPtr chunkIndexBuilder,
        TMemoryUsageTrackerGuard guard = {});

    void WriteRow(TVersionedRow row);

    TBlock FlushBlock();

    i64 GetBlockSize() const;

private:
    static constexpr int TypicalGroupCount = 1;

    const IChunkIndexBuilderPtr ChunkIndexBuilder_;
    const int BlockIndex_;
    const int GroupCount_;
    // TODO(akozhikhov): Consider having this flag per group to save space
    // if aggregate flag bitmaps are not needed for particular groups.
    const bool HasAggregateColumns_;
    const i64 SectorAlignmentSize_;
    const bool GroupReorderingEnabled_;

    TChunkedOutputStream Stream_;

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

            TCompactVector<TGroupData, TypicalGroupCount> Groups;
            TCompactVector<int, TypicalGroupCount> GroupOrder;
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

    TCompactVector<int, TypicalGroupCount> ComputeGroupAlignmentAndReordering();

    void DoWriteValue(
        char** buffer,
        char** stringBuffer,
        int valueIndex,
        const TUnversionedValue& value,
        TMutableBitmap* nullFlagsBitmap,
        std::optional<TMutableBitmap>* aggregateFlagsBitmap) const;

    i64 GetUnalignedBlockSize() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
