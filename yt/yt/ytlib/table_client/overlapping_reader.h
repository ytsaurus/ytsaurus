#pragma once

#include "public.h"

#include <functional>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// NB: Rows are allocated in row merger buffer which is cleared on each Read() call.

constexpr int DefaultMinConcurrency = 5;

using TOverlappingReaderKeyComparer = std::function<int(TUnversionedValueRange, TUnversionedValueRange)>;

ISchemafulUnversionedReaderPtr CreateSchemafulOverlappingLookupReader(
    std::unique_ptr<TSchemafulRowMerger> rowMerger,
    std::function<IVersionedReaderPtr()> readerFactory);

ISchemafulUnversionedReaderPtr CreateSchemafulOverlappingRangeReader(
    const std::vector<TLegacyOwningKey>& boundaries,
    std::unique_ptr<TSchemafulRowMerger> rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    TOverlappingReaderKeyComparer keyComparer,
    int minConcurrency = DefaultMinConcurrency);

IVersionedReaderPtr CreateVersionedOverlappingRangeReader(
    const std::vector<TLegacyOwningKey>& boundaries,
    std::unique_ptr<IVersionedRowMerger> rowMerger,
    std::function<IVersionedReaderPtr(int index)> readerFactory,
    TOverlappingReaderKeyComparer keyComparer,
    int minConcurrency = DefaultMinConcurrency);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
