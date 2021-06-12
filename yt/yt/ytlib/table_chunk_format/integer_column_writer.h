#pragma once

#include "column_writer.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedInt64ColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter);

std::unique_ptr<IValueColumnWriter> CreateVersionedUint64ColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* dataBlockWriter);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedInt64ColumnWriter(
    int columnIndex,
    TDataBlockWriter* dataBlockWriter);

std::unique_ptr<IValueColumnWriter> CreateUnversionedUint64ColumnWriter(
    int columnIndex,
    TDataBlockWriter* dataBlockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
