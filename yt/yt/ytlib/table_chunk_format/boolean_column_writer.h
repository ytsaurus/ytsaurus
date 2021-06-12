#pragma once

#include "column_writer.h"

#include <yt/yt/ytlib/table_client/public.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateVersionedBooleanColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedBooleanColumnWriter(
    int columnIndex,
    TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
