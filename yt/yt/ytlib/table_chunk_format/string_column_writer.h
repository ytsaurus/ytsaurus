#pragma once

#include "column_writer.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedStringColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter);

std::unique_ptr<IValueColumnWriter> CreateVersionedAnyColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedStringColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter);

std::unique_ptr<IValueColumnWriter> CreateUnversionedAnyColumnWriter(
    int columnIndex,
    TDataBlockWriter* dataBlockWriter);

std::unique_ptr<IValueColumnWriter> CreateUnversionedComplexColumnWriter(
    int columnIndex,
    TDataBlockWriter* dataBlockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
