#pragma once

#include "public.h"

#include "chunk_meta_extensions.h"
#include "schema.h"
#include "versioned_row.h"

#include <core/misc/ref.h>
#include <core/misc/bitmap.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedBlockReader
{
public:
    TSimpleVersionedBlockReader(
        const TSharedRef& data,
        const NProto::TBlockMeta& meta,
        const TTableSchema& chunkSchema,
        const TKeyColumns& keyColumns,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp);

    bool NextRow();

    bool SkipToRowIndex(int rowIndex);
    bool SkipToKey(TKey key);
    
    TKey GetKey() const;
    TVersionedRow GetRow(TChunkedMemoryPool* memoryPool);

    static int FormatVersion;

private:
    typedef TReadOnlyBitmap<ui64> TBitmap;

    TSharedRef Data_;

    TTimestamp Timestamp_;
    const int KeyColumnCount_;

    const std::vector<TColumnIdMapping>& SchemaIdMapping_;
    const TTableSchema& ChunkSchema_;

    const NProto::TBlockMeta& Meta_;
    const NProto::TSimpleVersionedBlockMeta& VersionedMeta_;

    TRef KeyData_;
    TBitmap KeyNullFlags_;

    TRef ValueData_;
    TBitmap ValueNullFlags_;

    TRef TimestampsData_;

    TRef StringData_;

    bool Closed_ = false;

    int RowIndex_;

    TUnversionedRowBuilder KeyBuilder_;
    TKey Key_;

    char* KeyDataPtr_;
    i64 TimestampOffset_;
    i64 ValueOffset_;
    ui16 WriteTimestampCount_;
    ui16 DeleteTimestampCount_;

    bool JumpToRowIndex(int index);
    TVersionedRow ReadAllValues(TChunkedMemoryPool* memoryPool);
    TVersionedRow ReadValuesByTimestamp(TChunkedMemoryPool* memoryPool);

    TTimestamp ReadTimestamp(int timestampIndex);
    void ReadValue(TVersionedValue* value, int valueIndex, int id, int chunkSchemaId);
    TTimestamp ReadValueTimestamp(int valueIndex, int id);
    void ReadKeyValue(TUnversionedValue* value, int id);
    
    void ReadInt64(TUnversionedValue* value, char* ptr);
    void ReadUint64(TUnversionedValue* value, char* ptr);
    void ReadDouble(TUnversionedValue* value, char* ptr);
    void ReadBoolean(TUnversionedValue* value, char* ptr);
    void ReadStringLike(TUnversionedValue* value, char* ptr);

    ui32 GetColumnValueCount(int schemaColumnId) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
