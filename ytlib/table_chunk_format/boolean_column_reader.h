#pragma once

#include "column_reader.h"

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedBooleanColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    bool aggregate);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedBooleanColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
