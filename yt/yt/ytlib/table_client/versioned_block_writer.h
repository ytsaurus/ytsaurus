#pragma once

#include "public.h"
#include "private.h"
#include "block_writer.h"
#include "chunk_meta_extensions.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/bitmap.h>
#include <yt/yt/core/misc/chunked_output_stream.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedBlockWriter
    : public IBlockWriter
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MinTimestamp);
    DEFINE_BYVAL_RO_PROPERTY(TTimestamp, MaxTimestamp);

public:
    explicit TSimpleVersionedBlockWriter(TTableSchemaPtr schema);

    void WriteRow(TVersionedRow row);

    virtual TBlock FlushBlock() override;

    virtual i64 GetBlockSize() const override;
    virtual i64 GetRowCount() const override;

    static int GetKeySize(int keyColumnCount, int schemaColumnCount);
    static int GetPaddedKeySize(int keyColumnCount, int schemaColumnCount);

    static const NChunkClient::EChunkFormat FormatVersion = NChunkClient::EChunkFormat::TableVersionedSimple;
    static const int ValueSize = 16;
    static const int TimestampSize = 8;

private:
    const TTableSchemaPtr Schema_;

    const int SchemaColumnCount_;
    const int KeyColumnCount_;
    const std::unique_ptr<bool[]> ColumnHunkFlags_;

    TChunkedOutputStream KeyStream_;
    TBitmapOutput KeyNullFlags_;

    TChunkedOutputStream ValueStream_;
    TBitmapOutput ValueNullFlags_;
    std::optional<TBitmapOutput> ValueAggregateFlags_;

    TChunkedOutputStream TimestampStream_;

    TChunkedOutputStream StringDataStream_;

    i64 TimestampCount_ = 0;
    i64 ValueCount_ = 0;
    i64 RowCount_ = 0;

    void WriteValue(
        TChunkedOutputStream& stream,
        TBitmapOutput& nullFlags,
        std::optional<TBitmapOutput>& aggregateFlags,
        const TUnversionedValue& value);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
