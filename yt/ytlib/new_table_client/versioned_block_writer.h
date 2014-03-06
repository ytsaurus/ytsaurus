#pragma once

#include "public.h"

#include "chunk_meta_extensions.h"
#include "private.h"
#include "schema.h"
#include "versioned_row.h"

#include <core/misc/bitmap.h>
#include <core/misc/chunked_output_stream.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedBlockWriter
{
    DEFINE_BYVAL_RO_PROPERTY(int, RowCount);

    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MinTimestamp);
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MaxTimestamp);

public:
    TSimpleVersionedBlockWriter(const TTableSchema& schema, const TKeyColumns& keyColumns);

    void WriteRow(
        TVersionedRow row,
        const TUnversionedValue* beginPrevKey,
        const TUnversionedValue* endPrevKey);

    TBlock FlushBlock();

    int GetBlockSize() const;

    static int GetKeySize(int keyColumnCount, int schemaColumnCount);
    static int GetPaddedKeySize(int keyColumnCount, int schemaColumnCount);

    static int FormatVersion;
    static int ValueSize;
    static int TimestampSize;

private:
    typedef TAppendOnlyBitMap<ui64> TBitMap;

    const int SchemaColumnCount_;
    const int KeyColumnCount_;

    TChunkedOutputStream KeyStream_;
    TBitMap KeyNullFlags_;

    TChunkedOutputStream ValueStream_;
    TBitMap ValueNullFlags_;

    TChunkedOutputStream TimestampsStream_;

    TChunkedOutputStream StringData_;

    i64 TimestampCount_;
    i64 ValueCount_;

    void WriteValue(
        TChunkedOutputStream& stream,
        TBitMap& nullFlags,
        const TUnversionedValue& value);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
