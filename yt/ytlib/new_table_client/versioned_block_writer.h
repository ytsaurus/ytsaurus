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
public:
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MinTimestamp);
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MaxTimestamp);

public:
    TSimpleVersionedBlockWriter(
        const TTableSchema& schema,
        const TKeyColumns& keyColumns);

    void WriteRow(
        TVersionedRow row,
        const TUnversionedValue* beginPrevKey,
        const TUnversionedValue* endPrevKey);

    virtual TBlock FlushBlock() override;

    virtual i64 GetBlockSize() const override;
    virtual i64 GetRowCount() const override;

    static int GetKeySize(int keyColumnCount, int schemaColumnCount);
    static int GetPaddedKeySize(int keyColumnCount, int schemaColumnCount);

    static const int FormatVersion = ETableChunkFormat::VersionedSimple;
    static const int ValueSize = 16;
    static const int TimestampSize = 8;

private:
    typedef TAppendOnlyBitmap<ui64> TBitmap;

    const TTableSchema& Schema_;

    const int SchemaColumnCount_;
    const int KeyColumnCount_;

    TChunkedOutputStream KeyStream_;
    TBitmap KeyNullFlags_;

    TChunkedOutputStream ValueStream_;
    TBitmap ValueNullFlags_;

    TChunkedOutputStream TimestampStream_;

    TChunkedOutputStream StringDataStream_;

    i64 TimestampCount_ = 0;
    i64 ValueCount_ = 0;
    i64 RowCount_ = 0;

    void WriteValue(
        TChunkedOutputStream& stream,
        TBitmap& nullFlags,
        const TUnversionedValue& value);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
