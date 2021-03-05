#pragma once

#include "column_reader.h"

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedBooleanColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    bool aggregate);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedBooleanColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<NTableClient::ESortOrder> sortOrder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
