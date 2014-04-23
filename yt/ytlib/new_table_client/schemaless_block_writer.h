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
    THorizontalSchemalessBlockWriter();

    void WriteRow(TUnversionedRow row);

    virtual TBlock FlushBlock() override;

    virtual i64 GetBlockSize() const override;
    virtual i64 GetRowCount() const override;

    static const ETableChunkFormat FormatVersion = ETableChunkFormat::SchemalessHorizontal;

private:
    i64 RowCount_;
    bool Closed_;

    TChunkedOutputStream Offsets_;
    TChunkedOutputStream Data_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
