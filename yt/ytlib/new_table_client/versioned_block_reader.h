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
        const std::vector<int>& schemaIdMapping,
        TTimestamp timestamp);

    bool NextRow();

    bool SkipToRowIndex(int rowIndex);
    bool SkipToKey(const TOwningKey& key);
    
    const TOwningKey& GetKey() const;
    TVersionedRow GetRow(TChunkedMemoryPool* memoryPool);

    int GetValueCount();
    int GetTimestampCount();

    static int FormatVersion;

private:
    typedef TReadOnlyBitMap<ui64> TBitMap;

    TTimestamp Timestamp_;
    const int KeyColumnCount_;

    const std::vector<int>& SchemaIdMapping_;
    const TTableSchema& Schema_;

    NProto::TBlockMeta Meta_;
    NProto::TSimpleVersionedBlockMeta VersionedMeta_;

    TRef KeyData_;
    TBitMap KeyNullFlags_;

    TRef ValueData_;
    TBitMap ValueNullFlags_;

    TRef TimestampsData_;

    TRef StringData_;

    bool Closed_;

    // Members describing current row.
    int RowIndex_;

    TOwningKey Key_;
    char* KeyDataPtr_;
    i64 TimestampOffset_;
    i64 ValueOffset_;
    ui32 TimestampCount_;

    bool JumpToRowIndex(int index);
    TVersionedRow ReadAllValues(TChunkedMemoryPool* memoryPool);
    TVersionedRow ReadValuesByTimestamp(TChunkedMemoryPool* memoryPool);

    TTimestamp ReadTimestamp(int timestampIndex);
    TVersionedValue ReadValue(int valueIndex, int id, int chunkSchemaId);
    TUnversionedValue ReadKeyValue(int id);
    TStringBuf ReadString(char* ptr);

    ui32 GetColumnValueCount(int schemaColumnId) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
