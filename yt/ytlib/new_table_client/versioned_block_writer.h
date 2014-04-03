#pragma once

#include "public.h"

#include "block_writer.h"
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
    : public IBlockWriter
{
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MinTimestamp);
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MaxTimestamp);

public:
    TSimpleVersionedBlockWriter(const TTableSchema& schema, const TKeyColumns& keyColumns);

    void WriteRow(
        TVersionedRow row,
        const TUnversionedValue* beginPrevKey,
        const TUnversionedValue* endPrevKey);

    virtual TBlock FlushBlock() override;

    virtual i64 GetBlockSize() const override;
    virtual i64 GetRowCount() const override;

    static int GetKeySize(int keyColumnCount, int schemaColumnCount);
    static int GetPaddedKeySize(int keyColumnCount, int schemaColumnCount);

    static const int FormatVersion;
    static const int ValueSize;
    static const int TimestampSize;

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
    i64 RowCount_;

    void WriteValue(
        TChunkedOutputStream& stream,
        TBitMap& nullFlags,
        const TUnversionedValue& value);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
