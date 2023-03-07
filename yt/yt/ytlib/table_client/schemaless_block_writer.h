#pragma once

#include "public.h"
#include "private.h"
#include "block_writer.h"

#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/chunked_output_stream.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class THorizontalBlockWriter
    : public IBlockWriter
{
public:
    THorizontalBlockWriter(i64 reserveSize = 2 * 64 * 1024);

    void WriteRow(TUnversionedRow row);

    virtual TBlock FlushBlock() override;

    virtual i64 GetBlockSize() const override;
    virtual i64 GetRowCount() const override;

    i64 GetCapacity() const;

    static const i64 MinReserveSize;
    static const i64 MaxReserveSize;

private:
    const i64 ReserveSize_;

    TChunkedOutputStream Offsets_;
    TChunkedOutputStream Data_;

    i64 RowCount_ = 0;
    bool Closed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
