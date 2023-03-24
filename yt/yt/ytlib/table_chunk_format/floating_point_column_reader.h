#pragma once

#include "column_reader.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<IVersionedColumnReader> CreateVersionedFloatingPointColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    const NTableClient::TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<IUnversionedColumnReader> CreateUnversionedFloatingPointColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<NTableClient::ESortOrder> sortOrder,
    const NTableClient::TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
