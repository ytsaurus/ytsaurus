#pragma once

#include "column_writer.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<IValueColumnWriter> CreateUnversionedFloatingPointColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<IValueColumnWriter> CreateVersionedFloatingPointColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
