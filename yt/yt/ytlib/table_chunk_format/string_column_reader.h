#pragma once

#include "column_reader.h"

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedStringColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    const NTableClient::TColumnSchema& columnSchema);

std::unique_ptr<IVersionedColumnReader> CreateVersionedAnyColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    const NTableClient::TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedStringColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<NTableClient::ESortOrder> sortOrder);

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedAnyColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<NTableClient::ESortOrder> sortOrder);

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedComplexColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<NTableClient::ESortOrder> sortOrder);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
