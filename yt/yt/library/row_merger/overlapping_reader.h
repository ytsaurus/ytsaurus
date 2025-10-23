#pragma once

#include <yt/yt/library/row_merger/public.h>

#include <yt/yt/client/table_client/public.h>

#include <functional>

namespace NYT::NRowMerger {

////////////////////////////////////////////////////////////////////////////////

// NB: Rows are allocated in row merger buffer which is cleared on each Read() call.

constexpr int DefaultMinConcurrency = 5;

using TOverlappingReaderKeyComparer = std::function<int(NTableClient::TUnversionedValueRange, NTableClient::TUnversionedValueRange)>;

NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulOverlappingRangeReader(
    const std::vector<NTableClient::TLegacyOwningKey>& boundaries,
    std::unique_ptr<NRowMerger::TSchemafulRowMerger> rowMerger,
    std::function<NTableClient::IVersionedReaderPtr(int index)> readerFactory,
    TOverlappingReaderKeyComparer keyComparer,
    int minConcurrency = DefaultMinConcurrency);

NTableClient::IVersionedReaderPtr CreateVersionedOverlappingRangeReader(
    const std::vector<NTableClient::TLegacyOwningKey>& boundaries,
    std::unique_ptr<NRowMerger::IVersionedRowMerger> rowMerger,
    std::function<NTableClient::IVersionedReaderPtr(int index)> readerFactory,
    TOverlappingReaderKeyComparer keyComparer,
    int minConcurrency = DefaultMinConcurrency);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowMerger
