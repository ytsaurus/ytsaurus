#pragma once

#include "column_reader.h"

#include <yt/yt/client/table_client/comparator.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<IVersionedColumnReader> CreateVersionedFloatingPointColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    bool aggregate);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<IUnversionedColumnReader> CreateUnversionedFloatingPointColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<NTableClient::ESortOrder> sortOrder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
