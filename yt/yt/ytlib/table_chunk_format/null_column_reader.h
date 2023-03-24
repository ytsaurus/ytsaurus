#pragma once

#include "column_reader.h"

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

/// Creates reader that reads nulls from chunk.
std::unique_ptr<IUnversionedColumnReader> CreateUnversionedNullColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<NTableClient::ESortOrder> sortOrder,
    const NTableClient::TColumnSchema& columnSchema);

/// Creates reader that returns Nulls ifinitlely.
std::unique_ptr<IUnversionedColumnReader> CreateBlocklessUnversionedNullColumnReader(
    int columnIndex,
    int columnId,
    std::optional<NTableClient::ESortOrder> sortOrder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
