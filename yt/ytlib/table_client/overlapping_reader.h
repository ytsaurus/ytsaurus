#pragma once

#include "public.h"

#include <functional>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

// NB: Rows are allocated in row merger buffer which is cleared on each Read() call.

constexpr int DefaultMinConcurrency = 5;

using TOverlappingReaderKeyComparer = std::function<int(
    const TUnversionedValue*,
    const TUnversionedValue*,
    const TUnversionedValue*,
    const TUnversionedValue*)>;

ISchemafulReaderPtr CreateSchemafulOverlappingLookupReader(
    std::unique_ptr<TSchemafulRowMerger> rowMerger,
    std::function<IVersionedReaderPtr()> readerFactory);

ISchemafulReaderPtr CreateSchemafulOverlappingRangeReader(
    const std::vector<TOwningKey>& boundaries,
    std::unique_ptr<TSchemafulRowMerger> rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    TOverlappingReaderKeyComparer keyComparer,
    int minConcurrency = DefaultMinConcurrency);

IVersionedReaderPtr CreateVersionedOverlappingRangeReader(
    const std::vector<TOwningKey>& boundaries,
    std::unique_ptr<TVersionedRowMerger> rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    TOverlappingReaderKeyComparer keyComparer,
    int minConcurrency = DefaultMinConcurrency);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
