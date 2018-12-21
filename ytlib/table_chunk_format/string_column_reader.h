#pragma once

#include "column_reader.h"

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedStringColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    bool aggregate);

std::unique_ptr<IVersionedColumnReader> CreateVersionedAnyColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    bool aggregate);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedStringColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId);

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedAnyColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
