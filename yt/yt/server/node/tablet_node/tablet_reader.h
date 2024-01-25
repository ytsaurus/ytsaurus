#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/helpers.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/range.h>

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

//! Creates a lookup reader that merges data from the relevant stores and
//! returns a single version of each value.
/*!
 *  Can only handle sorted tables.
 *  Decodes hunks.
 */
NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulLookupTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keys,
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
    std::optional<EWorkloadCategory> workloadCategory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
