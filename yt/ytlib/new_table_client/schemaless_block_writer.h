#pragma once

#include "public.h"

#include "block_writer.h"
#include "private.h"
#include "unversioned_row.h"

#include <core/misc/chunked_output_stream.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessBlockWriter
    : public IBlockWriter
{
public:
    THorizontalSchemalessBlockWriter(i64 reserveSize = 2 * 64 * 1024);

    void WriteRow(TUnversionedRow row);

    virtual TBlock FlushBlock() override;

    virtual i64 GetBlockSize() const override;
    virtual i64 GetRowCount() const override;

    i64 GetCapacity() const;

    static const ETableChunkFormat FormatVersion = ETableChunkFormat::SchemalessHorizontal;

    static const i64 MinReserveSize;
    static const i64 MaxReserveSize;

private:
    i64 RowCount_;
    bool Closed_;

    const i64 ReserveSize_;

    TChunkedOutputStream Offsets_;
    TChunkedOutputStream Data_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
