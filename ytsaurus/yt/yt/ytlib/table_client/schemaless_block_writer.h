#pragma once

#include "public.h"
#include "private.h"
#include "block.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/memory/chunked_output_stream.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class THorizontalBlockWriter
{
public:
    explicit THorizontalBlockWriter(TTableSchemaPtr schema, i64 reserveSize = 128_KB);

    void WriteRow(TUnversionedRow row);

    TBlock FlushBlock();

    i64 GetBlockSize() const;
    i64 GetRowCount() const;

    i64 GetCapacity() const;

    static const i64 MinReserveSize;
    static const i64 MaxReserveSize;

private:
    const i64 ReserveSize_;
    const int ColumnCount_;
    const std::unique_ptr<bool[]> ColumnHunkFlags_;

    TChunkedOutputStream Offsets_;
    TChunkedOutputStream Data_;

    i64 RowCount_ = 0;
    bool Closed_ = false;

    bool IsInlineHunkValue(const TUnversionedValue& value) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
