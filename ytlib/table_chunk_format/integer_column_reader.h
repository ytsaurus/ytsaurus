#pragma once

#include "public.h"

#include "column_reader.h"

#include <yt/ytlib/table_chunk_format/column_meta.pb.h>

#include <yt/ytlib/table_client/versioned_row.h>

namespace NYT {
namespace NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedInt64ColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    bool aggregate);

std::unique_ptr<IVersionedColumnReader> CreateVersionedUint64ColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    bool aggregate);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedInt64ColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId);

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedUint64ColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
