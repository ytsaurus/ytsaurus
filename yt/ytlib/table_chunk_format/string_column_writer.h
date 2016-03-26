#pragma once

#include "column_writer.h"

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedStringColumnWriter(
    int columnId,
    bool aggregate,
    TDataBlockWriter* dataBlockWriter);

std::unique_ptr<IValueColumnWriter> CreateUnversionedStringColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormatc
} // namespace NYT
