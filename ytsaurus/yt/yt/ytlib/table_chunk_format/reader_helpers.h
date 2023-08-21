#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/client/table_client/versioned_row.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

//! Updates row value count and attempts to free unused memory in #memoryPool.
void TrimRow(
    NTableClient::TMutableVersionedRow row,
    NTableClient::TVersionedValue* endValues,
    TChunkedMemoryPool* memoryPool);

//! Allocates a new row in #memoryPool and copies all existing keys and values
//! from existing #row into a new one.
//! NB: No timestamps are being copied.
NTableClient::TMutableVersionedRow IncreaseRowValueCount(
    NTableClient::TVersionedRow row,
    int newValueCount,
    TChunkedMemoryPool* memoryPool);

//! Attempts to free all memory allocated by #row in #memoryPool.
void FreeRow(
    NTableClient::TMutableVersionedRow row,
    TChunkedMemoryPool* memoryPool);

//! Sorts values in #row by |(index, timestsamp)| pair (by increasing index and
//! decreasing timestamp).
//! \see TVersionedRow
void SortRowValues(NTableClient::TMutableVersionedRow row);

//! Updates #row leaving just a single version per each non-aggregate column
//! (based on #aggregateFlags).
void FilterSingleRowVersion(
    NTableClient::TMutableVersionedRow row,
    const bool* aggregateFlags,
    TChunkedMemoryPool* memoryPool);

//! Computes the number of columns in reader schema.
int GetReaderSchemaColumnCount(TRange<NTableClient::TColumnIdMapping> schemaIdMapping);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
