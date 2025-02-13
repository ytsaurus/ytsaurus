#pragma once

#include "column_writer.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<IValueColumnWriter> CreateUnversionedFloatingPointColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    int maxValueCount = DefaultMaxSegmentValueCount);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedDoubleColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    int maxValueCount = DefaultMaxSegmentValueCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
