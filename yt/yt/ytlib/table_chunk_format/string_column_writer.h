#pragma once

#include "column_writer.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedStringColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

std::unique_ptr<IValueColumnWriter> CreateVersionedAnyColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

std::unique_ptr<IValueColumnWriter> CreateVersionedCompositeColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedStringColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

std::unique_ptr<IValueColumnWriter> CreateUnversionedAnyColumnWriter(
    int columnIndex,
    TDataBlockWriter* dataBlockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

std::unique_ptr<IValueColumnWriter> CreateUnversionedCompositeColumnWriter(
    int columnIndex,
    TDataBlockWriter* dataBlockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
