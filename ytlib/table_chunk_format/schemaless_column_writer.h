#pragma once

#include "column_writer.h"

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateSchemalessColumnWriter(
    int schemaColumnCount,
    TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
