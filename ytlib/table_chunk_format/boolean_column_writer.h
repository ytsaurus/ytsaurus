#pragma once

#include "column_writer.h"

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedBooleanColumnWriter(
    int columnId, 
    bool aggregate, 
    TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedBooleanColumnWriter(
    int columnIndex, 
    TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
