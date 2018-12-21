#pragma once

#include "column_writer.h"

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedDoubleColumnWriter(
    int columnIndex, 
    TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedDoubleColumnWriter(
    int columnId, 
    bool aggregate, 
    TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
