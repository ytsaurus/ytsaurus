#pragma once

#include "chunk_index.h"
#include "chunk_meta_extensions.h"
#include "public.h"
#include "schemaless_block_reader.h"

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/bitmap.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

/*

Row/block parsers incapsulate details of data layout on disk and determine how to parse row/block data
as well as how to transform the data into versioned rows.

Row/block readers use parsers to produce versioned rows,
find the row corresponding to a specific key, etc.

Simple format is compliant with block readers only.
Indexed format is compliant with either block or row readers. Row readers are used along with chunk index.

*/

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowParserBase
{
public:
    TVersionedRowParserBase(const TTableSchemaPtr& chunkSchema);

    TVersionedRowParserBase(const TVersionedRowParserBase& other) = delete;
    TVersionedRowParserBase(TVersionedRowParserBase&& other) = default;
    TVersionedRowParserBase& operator=(const TVersionedRowParserBase& other) = delete;
    TVersionedRowParserBase& operator=(TVersionedRowParserBase&& other) = default;

    struct TRowMetadata
    {
        constexpr static int DefaultKeyBufferCapacity = 128;
        TCompactVector<char, DefaultKeyBufferCapacity> KeyBuffer;

        TLegacyMutableKey Key;

        TRange<TTimestamp> WriteTimestamps;
        TRange<TTimestamp> DeleteTimestamps;

        ui32 ValueCount;
    };

protected:
    const int ChunkKeyColumnCount_;
    const int ChunkColumnCount_;

    std::unique_ptr<bool[]> ColumnHunkFlags_;
    std::unique_ptr<bool[]> ColumnAggregateFlags_;
    std::unique_ptr<EValueType[]> ColumnTypes_;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedBlockParser
    : public TVersionedRowParserBase
{
public:
    TSimpleVersionedBlockParser(
        TSharedRef block,
        const NProto::TDataBlockMeta& blockMeta,
        const TTableSchemaPtr& chunkSchema);

    DEFINE_BYREF_RO_PROPERTY(bool, Closed, false);
    DEFINE_BYREF_RO_PROPERTY(i64, RowCount);

    bool JumpToRowIndex(i64 rowIndex, TRowMetadata* rowMetadata);

    struct TColumnDescriptor
    {
        int ValueId;
        int ChunkSchemaId;

        int LowerValueIndex;
        int UpperValueIndex;

        bool Aggregate;
    };

    TColumnDescriptor GetColumnDescriptor(const TColumnIdMapping& mapping) const;

    void ReadValue(
        TVersionedValue* value,
        const TColumnDescriptor& columnDescriptor,
        int valueIndex) const;

    TTimestamp ReadValueTimestamp(
        const TColumnDescriptor& columnDescriptor,
        int valueIndex) const;

private:
    const TSharedRef Block_;

    TRef KeyData_;
    TRef ValueData_;
    TRef TimestampsData_;
    TRef StringData_;

    TReadOnlyBitmap KeyNullFlags_;
    TReadOnlyBitmap ValueNullFlags_;
    std::optional<TReadOnlyBitmap> ValueAggregateFlags_;

    i64 TimestampOffset_;
    i64 ValueOffset_;
    const char* ColumnValueCounts_;


    void ReadKeyValue(TUnversionedValue* value, int id, const char* ptr, i64 rowIndex) const;
    void ReadStringLike(TUnversionedValue* value, const char* ptr) const;
    ui32 GetColumnValueCount(int chunkSchemaId) const;
};

////////////////////////////////////////////////////////////////////////////////

class TIndexedVersionedRowParser
    : public TVersionedRowParserBase
{
public:
    TIndexedVersionedRowParser(
        const TTableSchemaPtr& chunkSchema,
        TCompactVector<int, IndexedRowTypicalGroupCount> groupIndexesToRead = {});

    struct TGroupInfo
    {
        bool Initialized = false;

        const char* GroupDataBegin;

        int ValueCount;

        const int* ColumnValueCounts;
        TReadOnlyBitmap NullFlags;
        std::optional<TReadOnlyBitmap> AggregateFlags;

        const char* ValuesBegin;
    };

    struct TColumnDescriptor
    {
        const TGroupInfo& GroupInfo;

        int ValueId;
        int ChunkSchemaId;

        int LowerValueIndex;
        int UpperValueIndex;

        bool Aggregate;
    };

    TColumnDescriptor GetColumnDescriptor(const TColumnIdMapping& mapping);

    void ReadValue(
        TVersionedValue* value,
        const TColumnDescriptor& columnDescriptor,
        int valueIndex) const;

    TTimestamp ReadValueTimestamp(
        const TColumnDescriptor& columnDescriptor,
        int valueIndex) const;

protected:
    const TIndexedVersionedBlockFormatDetail BlockFormatDetail_;
    const int GroupCount_;
    const bool HasAggregateColumns_;
    // NB: Used along with chunk index if a subset of row groups was read.
    const TCompactVector<int, IndexedRowTypicalGroupCount> GroupIndexesToRead_;

    bool GroupReorderingEnabled_;

    TReadOnlyBitmap KeyNullFlags_;
    TCompactVector<TGroupInfo, IndexedRowTypicalGroupCount> GroupInfos_;


    void PreprocessRow(
        const TCompactVector<TRef, IndexedRowTypicalGroupCount>& rowData,
        const int* groupOffsets,
        const int* groupIndexes,
        bool validateChecksums,
        TRowMetadata* rowMetadata);

    void ReadKeyValue(TUnversionedValue* value, int id, const char* ptr, const char** rowData) const;
    void ReadStringLike(TUnversionedValue* value, const char* ptr) const;

    const TGroupInfo& GetGroupInfo(int groupIndex, int columnCountInGroup);
};

class TIndexedVersionedBlockParser
    : public TIndexedVersionedRowParser
{
public:
    TIndexedVersionedBlockParser(
        TSharedRef block,
        const NProto::TDataBlockMeta& blockMeta,
        const TTableSchemaPtr& chunkSchema);

    DEFINE_BYREF_RO_PROPERTY(bool, Closed, false);
    DEFINE_BYREF_RO_PROPERTY(i64, RowCount);

    bool JumpToRowIndex(i64 rowIndex, TRowMetadata* rowMetadata);

private:
    const TSharedRef Block_;

    // NB: These are stored at the end of each block.
    const i64* RowOffsets_ = nullptr;
    const int* GroupOffsets_ = nullptr;
    const int* GroupIndexes_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRowParser>
class TVersionedRowReader
{
public:
    TVersionedRowReader(
        int keyColumnCount,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp,
        bool produceAllVersions,
        TRowParser rowParser);

    TVersionedRowReader(const TVersionedRowReader<TRowParser>& other) = delete;
    TVersionedRowReader(TVersionedRowReader<TRowParser>&& other) = delete;
    TVersionedRowReader& operator=(const TVersionedRowReader<TRowParser>& other) = delete;
    TVersionedRowReader& operator=(TVersionedRowReader<TRowParser>&& other) = delete;

    TLegacyKey GetKey() const;

protected:
    TVersionedRowParserBase::TRowMetadata RowMetadata_;

    TRowParser Parser_;


    // NB: Method is protected because it is intended for reads only via block reader.
    TMutableVersionedRow GetRow(TChunkedMemoryPool* memoryPool);

private:
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const int KeyColumnCount_;
    const std::vector<TColumnIdMapping>& SchemaIdMapping_;


    TMutableVersionedRow ReadAllVersions(TChunkedMemoryPool* memoryPool);
    TMutableVersionedRow ReadOneVersion(TChunkedMemoryPool* memoryPool);
};

template <typename TBlockParser>
class TVersionedBlockReader
    : public TVersionedRowReader<TBlockParser>
{
public:
    TVersionedBlockReader(
        TSharedRef block,
        const NProto::TDataBlockMeta& blockMeta,
        const TTableSchemaPtr& chunkSchema,
        int keyColumnCount,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        const TKeyComparer& keyComparer,
        TTimestamp timestamp,
        bool produceAllVersions,
        bool initialize);

    DEFINE_BYVAL_RO_PROPERTY(i64, RowIndex, -1);

    bool NextRow();

    bool SkipToRowIndex(i64 rowIndex);
    bool SkipToKey(TLegacyKey key);

    using TVersionedRowReader<TBlockParser>::GetKey;
    using TVersionedRowReader<TBlockParser>::GetRow;

private:
    using TVersionedRowReader<TBlockParser>::RowMetadata_;
    using TVersionedRowReader<TBlockParser>::Parser_;

    // NB: Chunk reader holds the comparer.
    const TKeyComparer& KeyComparer_;


    bool JumpToRowIndex(i64 rowIndex);
};

////////////////////////////////////////////////////////////////////////////////

using TSimpleVersionedBlockReader = TVersionedBlockReader<TSimpleVersionedBlockParser>;

using TIndexedVersionedBlockReader = TVersionedBlockReader<TIndexedVersionedBlockParser>;

using TIndexedVersionedRowReader = TVersionedRowReader<TIndexedVersionedRowParser>;

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessVersionedBlockReader
    : public THorizontalBlockReader
{
public:
    THorizontalSchemalessVersionedBlockReader(
        const TSharedRef& block,
        const NProto::TDataBlockMeta& blockMeta,
        const std::vector<bool>& compositeColumnFlags,
        const std::vector<int>& chunkToReaderIdMapping,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        TTimestamp timestamp);

    TLegacyKey GetKey() const;
    TMutableVersionedRow GetRow(TChunkedMemoryPool* memoryPool);

private:
    TTimestamp Timestamp_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define VERSIONED_BLOCK_READER_INL_H_
#include "versioned_block_reader-inl.h"
#undef VERSIONED_BLOCK_READER_INL_H_
