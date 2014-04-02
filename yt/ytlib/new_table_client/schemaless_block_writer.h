#pragma once

#include "public.h"

#include "private.h"
#include "unversioned_row.h"

#include <core/misc/chunked_output_stream.h>
#include <core/misc/property.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

class THorizontalSchemalessBlockWriter
{
    DEFINE_BYVAL_RO_PROPERTY(int, RowCount);

public:
    THorizontalSchemalessBlockWriter();
    THorizontalSchemalessBlockWriter(int keyColumnCount);

    void WriteRow(TUnversionedRow row);

    TBlock FlushBlock();

    int GetBlockSize() const;

    static int FormatVersion;

private:
    TChunkedOutputStream Offsets_;
    TChunkedOutputStream Data_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
