#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/client/table_client/timestamped_schema_helpers.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <library/cpp/yt/memory/range.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Throws if corresponding distributed throttler is in overdraft.
void ThrowUponDistributedThrottlerOverdraft(
    ETabletDistributedThrottlerKind tabletThrottlerKind,
    const TTabletSnapshotPtr& tabletSnapshot,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions);

//! Throws if corresponding node throttler is in overdraft.
void ThrowUponNodeThrottlerOverdraft(
    std::optional<TInstant> requestStartTime,
    std::optional<TDuration> requestTimeout,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

struct TPartitionBounds
{
    std::vector<NTableClient::TRowRange> Bounds;
    int PartitionIndex;
};

NTableClient::ISchemafulUnversionedReaderPtr CreatePartitionScanReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TPartitionBounds>& partitionBounds,
    NTransactionClient::TReadTimestampRange timestampRange,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory,
    NTableClient::TTimestampReadOptions timestampReadOptions,
    bool mergeVersionedRows = true);

struct TPartitionKeys
{
    TRange<NTableClient::TUnversionedRow> Keys;
    int PartitionIndex;
};

NTableClient::ISchemafulUnversionedReaderPtr CreatePartitionLookupReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    int partitionIndex,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keys,
    NTransactionClient::TReadTimestampRange timestampRange,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory);

////////////////////////////////////////////////////////////////////////////////

//! Creates a range reader that merges data from the relevant stores and
//! returns a single version of each value.
/*!
 *  Decodes hunks.
 */
NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulSortedTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<NTableClient::TRowRange>& bounds,
    NTransactionClient::TReadTimestampRange timestampRange,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory,
    NTableClient::TTimestampReadOptions timestampReadOptions,
    bool mergeVersionedRows = true);

//! Creates a range reader that handles ordered stores.
/*!
 *  Decodes hunks.
 */
NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulOrderedTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    NTransactionClient::TReadTimestampRange timestampRange,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory);

//! Creates a range reader that handles both sorted and ordered tables.
/*!
 *  Decodes hunks.
 */
NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulRangeTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    NTransactionClient::TReadTimestampRange timestampRange,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory);

//! Creates a range reader that merges data from all given #stores and
//! returns all versions of each value.
/*!
 *  Can only handle sorted tables.
 *  Does not decode hunks, intentionally.
 */
NTableClient::IVersionedReaderPtr CreateCompactionTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    std::vector<ISortedStorePtr> stores,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    int minConcurrency,
    ETabletDistributedThrottlerKind tabletThrottlerKind,
    NConcurrency::IThroughputThrottlerPtr perTabletThrottler,
    std::optional<EWorkloadCategory> workloadCategory,
    IMemoryUsageTrackerPtr rowMergerMemoryTracker);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NTableClient::TSchemafulRowMerger> CreateQueryLatestTimestampRowMerger(
    NTableClient::TRowBufferPtr rowBuffer,
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    TTimestamp retentionTimestamp,
    const NTableClient::TTimestampReadOptions& timestampReadOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
