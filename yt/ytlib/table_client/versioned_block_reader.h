#pragma once

#include "chunk_meta_extensions.h"
#include "public.h"
#include "schema.h"
#include "schemaless_block_reader.h"
#include "unversioned_row.h"
#include "versioned_row.h"

#include <yt/core/misc/bitmap.h>
#include <yt/core/misc/ref.h>
#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IVersionedBlockReader
{
    virtual ~IVersionedBlockReader() = default;

    virtual bool NextRow() = 0;

    virtual bool SkipToRowIndex(i64 rowIndex) = 0;
    virtual bool SkipToKey(TKey key) = 0;

    virtual TKey GetKey() const = 0;
    virtual TVersionedRow GetRow(TChunkedMemoryPool* memoryPool) = 0;

    virtual i64 GetRowIndex() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedBlockReader
    : public IVersionedBlockReader
{
public:
    TSimpleVersionedBlockReader(
        const TSharedRef& block,
        const NProto::TBlockMeta& meta,
        const TTableSchema& chunkSchema,
        int chunkKeyColumnCount,
        int keyColumnCount,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        const TKeyComparer& keyComparer,
        TTimestamp timestamp,
        bool produceAllVersions,
        bool initialize);

    virtual bool NextRow() override;

    virtual bool SkipToRowIndex(i64 rowIndex) override;
    virtual bool SkipToKey(TKey key) override;

    virtual TKey GetKey() const override;
    virtual TVersionedRow GetRow(TChunkedMemoryPool* memoryPool) override;

    virtual i64 GetRowIndex() const override;

private:
    const TSharedRef Block_;
    typedef TReadOnlyBitmap<ui64> TBitmap;

    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const int ChunkKeyColumnCount_;
    const int KeyColumnCount_;

    const std::vector<TColumnIdMapping>& SchemaIdMapping_;
    const TTableSchema& ChunkSchema_;

    const NProto::TBlockMeta& Meta_;
    const NProto::TSimpleVersionedBlockMeta& VersionedMeta_;

    TRef KeyData_;
    TBitmap KeyNullFlags_;

    TRef ValueData_;
    TBitmap ValueNullFlags_;
    TNullable<TBitmap> ValueAggregateFlags_;

    TRef TimestampsData_;

    TRef StringData_;

    bool Closed_ = false;

    i64 RowIndex_;

    const static size_t DefaultKeyBufferCapacity = 256;
    SmallVector<char, DefaultKeyBufferCapacity> KeyBuffer_;
    TMutableKey Key_;

    const char* KeyDataPtr_;
    i64 TimestampOffset_;
    i64 ValueOffset_;
    ui16 WriteTimestampCount_;
    ui16 DeleteTimestampCount_;

    // NB: chunk reader holds the comparer.
    const TKeyComparer& KeyComparer_;

    bool JumpToRowIndex(i64 index);
    TVersionedRow ReadAllVersions(TChunkedMemoryPool* memoryPool);
    TVersionedRow ReadOneVersion(TChunkedMemoryPool* memoryPool);

    TTimestamp ReadTimestamp(int timestampIndex);
    void ReadValue(TVersionedValue* value, int valueIndex, int id, int chunkSchemaId);
    void ReadKeyValue(TUnversionedValue* value, int id);

    Y_FORCE_INLINE TTimestamp ReadValueTimestamp(int valueIndex);
    Y_FORCE_INLINE void ReadStringLike(TUnversionedValue* value, const char* ptr);

    ui32 GetColumnValueCount(int schemaColumnId) const;

};

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessVersionedBlockReader
    : public IVersionedBlockReader
{
public:
    THorizontalSchemalessVersionedBlockReader(
        const TSharedRef& block,
        const NProto::TBlockMeta& meta,
        const std::vector<TColumnIdMapping>& idMapping,
        int chunkKeyColumnCount,
        int keyColumnCount,
        TTimestamp timestamp);

    virtual bool NextRow() override;

    virtual bool SkipToRowIndex(i64 rowIndex) override;
    virtual bool SkipToKey(TKey key) override;

    virtual TKey GetKey() const override;
    virtual TVersionedRow GetRow(TChunkedMemoryPool* memoryPool) override;

    virtual i64 GetRowIndex() const override;

private:
    std::unique_ptr<THorizontalSchemalessBlockReader> UnderlyingReader_;
    TTimestamp Timestamp_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
