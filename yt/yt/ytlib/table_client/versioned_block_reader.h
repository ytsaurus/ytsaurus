#pragma once

#include "chunk_index.h"
#include "chunk_meta_extensions.h"
#include "public.h"
#include "schemaless_block_reader.h"

#include <yt/yt/ytlib/chunk_client/public.h>

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
    explicit TVersionedRowParserBase(const TTableSchemaPtr& chunkSchema);

    TVersionedRowParserBase(const TVersionedRowParserBase&) = delete;
    TVersionedRowParserBase(TVersionedRowParserBase&&) = delete;
    TVersionedRowParserBase& operator=(const TVersionedRowParserBase&) = delete;
    TVersionedRowParserBase& operator=(TVersionedRowParserBase&&) = delete;

protected:
    const int ChunkKeyColumnCount_;
    const int ChunkColumnCount_;

    // Indexed by chunk schema.
    TCompactVector<bool, TypicalColumnCount> ColumnHunkFlagsStorage_;
    bool* const ColumnHunkFlags_;

    // Indexed by chunk schema.
    TCompactVector<bool, TypicalColumnCount> ColumnAggregateFlagsStorage_;
    bool* const ColumnAggregateFlags_;

    // Indexed by chunk schema.
    TCompactVector<EValueType, TypicalColumnCount> PhysicalColumnTypesStorage_;
    EValueType* const PhysicalColumnTypes_;

    // Indexed by chunk schema.
    TCompactVector<ESimpleLogicalValueType, TypicalColumnCount> LogicalColumnTypesStorage_;
    ESimpleLogicalValueType* const LogicalColumnTypes_;
};

////////////////////////////////////////////////////////////////////////////////

struct TVersionedRowMetadata
{
    constexpr static int DefaultKeyBufferCapacity = 128;
    TCompactVector<char, DefaultKeyBufferCapacity> KeyBuffer;

    TLegacyMutableKey Key;

    TRange<TTimestamp> WriteTimestamps;
    TRange<TTimestamp> DeleteTimestamps;

    int ValueCount;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedBlockParser
    : public TVersionedRowParserBase
{
public:
    static constexpr NChunkClient::EChunkFormat ChunkFormat = NChunkClient::EChunkFormat::TableVersionedSimple;

    TSimpleVersionedBlockParser(
        TSharedRef block,
        const NProto::TDataBlockMeta& blockMeta,
        int blockFormatVersion,
        const TTableSchemaPtr& chunkSchema);

    bool IsValid() const;
    int GetRowCount() const;

    bool JumpToRowIndex(int rowIndex, TVersionedRowMetadata* rowMetadata);

    struct TColumnDescriptor
    {
        int ReaderSchemaId;
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
    const int RowCount_;

    bool Valid_ = false;

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


    void ReadKeyValue(TUnversionedValue* value, int id, const char* ptr, int rowIndex) const;
    void ReadStringLike(TUnversionedValue* value, const char* ptr) const;
    ui32 GetColumnValueCount(int chunkSchemaId) const;
};

////////////////////////////////////////////////////////////////////////////////

class TIndexedVersionedRowParser
    : public TVersionedRowParserBase
{
public:
    explicit TIndexedVersionedRowParser(
        const TTableSchemaPtr& chunkSchema,
        std::vector<int> groupIndexesToRead);

    struct TGroupInfo
    {
        bool Initialized = false;

        const char* GroupDataBegin = nullptr;

        int ValueCount;

        const int* ColumnValueCounts;
        TReadOnlyBitmap NullFlags;
        std::optional<TReadOnlyBitmap> AggregateFlags;

        const char* ValuesBegin;
    };

    struct TColumnDescriptor
    {
        const TGroupInfo& GroupInfo;

        int ReaderSchemaId;
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

    void ValidateRowDataChecksum(
        const TCompactVector<TRef, IndexedRowTypicalGroupCount>& rowData);

    void ProcessRow(
        const TCompactVector<TRef, IndexedRowTypicalGroupCount>& rowData,
        const int* groupOffsets,
        const int* groupIndexes,
        TVersionedRowMetadata* rowMetadata);

protected:
    const TIndexedVersionedBlockFormatDetail BlockFormatDetail_;
    const bool HasAggregateColumns_;
    // NB: Nonempty if a subset of row groups was read.
    const std::vector<int> GroupIndexesToRead_;

    const bool GroupReorderingEnabled_ = false;

    TReadOnlyBitmap KeyNullFlags_;
    TCompactVector<TGroupInfo, IndexedRowTypicalGroupCount> GroupInfos_;


    void ReadKeyValue(TUnversionedValue* value, int id, const char* ptr, const char** rowData) const;
    void ReadStringLike(TUnversionedValue* value, const char* ptr) const;

    const TGroupInfo& GetGroupInfo(int groupIndex, int columnCountInGroup);
};

////////////////////////////////////////////////////////////////////////////////

class TIndexedVersionedBlockParser
    : public TIndexedVersionedRowParser
{
public:
    static constexpr NChunkClient::EChunkFormat ChunkFormat = NChunkClient::EChunkFormat::TableVersionedIndexed;

    TIndexedVersionedBlockParser(
        TSharedRef block,
        const NProto::TDataBlockMeta& blockMeta,
        int blockFormatVersion,
        const TTableSchemaPtr& chunkSchema);

    bool IsValid() const;
    int GetRowCount() const;

    bool JumpToRowIndex(int rowIndex, TVersionedRowMetadata* rowMetadata);

private:
    const TSharedRef Block_;
    const int RowCount_;

    bool Valid_ = false;

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
    template <class... TParserArgs>
    TVersionedRowReader(
        int keyColumnCount,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp,
        bool produceAllVersions,
        TParserArgs&&... parserArgs);

    TVersionedRowReader(const TVersionedRowReader<TRowParser>& other) = delete;
    TVersionedRowReader(TVersionedRowReader<TRowParser>&& other) = delete;
    TVersionedRowReader& operator=(const TVersionedRowReader<TRowParser>& other) = delete;
    TVersionedRowReader& operator=(TVersionedRowReader<TRowParser>&& other) = delete;

    TMutableVersionedRow ProcessAndGetRow(
        const TCompactVector<TSharedRef, IndexedRowTypicalGroupCount>& owningRowData,
        const int* groupOffsets,
        const int* groupIndexes,
        TChunkedMemoryPool* memoryPool);

protected:
    TVersionedRowMetadata RowMetadata_;
    TRowParser Parser_;


    // NB: These methods are protected because they are intended for reads only via block reader.
    TLegacyKey GetKey() const;
    TMutableVersionedRow GetRow(TChunkedMemoryPool* memoryPool);

private:
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const int KeyColumnCount_;
    const std::vector<TColumnIdMapping>& SchemaIdMapping_;


    TMutableVersionedRow ReadRowAllVersions(TChunkedMemoryPool* memoryPool);
    TMutableVersionedRow ReadRowSingleVersion(TChunkedMemoryPool* memoryPool);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockParser>
class TVersionedBlockReader
    : public TVersionedRowReader<TBlockParser>
{
public:
    static constexpr NChunkClient::EChunkFormat ChunkFormat = TBlockParser::ChunkFormat;

    TVersionedBlockReader(
        TSharedRef block,
        const NProto::TDataBlockMeta& blockMeta,
        int blockFormatVersion,
        const TTableSchemaPtr& chunkSchema,
        int keyColumnCount,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        const TKeyComparer& keyComparer,
        TTimestamp timestamp,
        bool produceAllVersions);

    DEFINE_BYVAL_RO_PROPERTY(int, RowIndex, -1);

    bool NextRow();

    bool SkipToRowIndex(int rowIndex);
    bool SkipToKey(TLegacyKey key);

    using TVersionedRowReader<TBlockParser>::GetKey;
    TMutableVersionedRow GetRow(TChunkedMemoryPool* memoryPool);

private:
    using TVersionedRowReader<TBlockParser>::RowMetadata_;
    using TVersionedRowReader<TBlockParser>::Parser_;

    // NB: Chunk reader holds the comparer.
    const TKeyComparer& KeyComparer_;


    bool JumpToRowIndex(int rowIndex);
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
        const std::vector<bool>& hunkColumnFlags,
        const std::vector<THunkChunkMeta>& hunkChunkMetas,
        const std::vector<THunkChunkRef>& hunkChunkRefs,
        const std::vector<int>& chunkToReaderIdMapping,
        TRange<ESortOrder> sortOrders,
        int commonKeyPrefix,
        TTableSchemaPtr readerSchema,
        TTimestamp timestamp);

    TLegacyKey GetKey() const;
    TMutableVersionedRow GetRow(TChunkedMemoryPool* memoryPool);

private:
    const TTableSchemaPtr ReaderSchema_;
    const TTimestamp Timestamp_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define VERSIONED_BLOCK_READER_INL_H_
#include "versioned_block_reader-inl.h"
#undef VERSIONED_BLOCK_READER_INL_H_
