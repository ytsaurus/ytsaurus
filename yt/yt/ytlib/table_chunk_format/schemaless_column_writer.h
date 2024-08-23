#pragma once

#include "column_writer.h"

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateSchemalessColumnWriter(
    int schemaColumnCount,
    TDataBlockWriter* blockWriter,
    IMemoryUsageTrackerPtr memoryUsageTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
