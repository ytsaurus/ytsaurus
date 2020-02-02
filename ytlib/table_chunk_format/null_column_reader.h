#pragma once

#include "column_reader.h"

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

/// Creates reader that reads nulls from chunk.
std::unique_ptr<IUnversionedColumnReader> CreateUnversionedNullColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId);

/// Creates reader that returns Nulls ifinitlely.
std::unique_ptr<IUnversionedColumnReader> CreateBlocklessUnversionedNullColumnReader(int columnIndex, int columnId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
