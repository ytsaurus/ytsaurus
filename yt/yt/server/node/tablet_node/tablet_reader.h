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

//! Throttles with corresponding tablet distributed throttler if it is in overdraft.
void ThrowUponThrottlerOverdraft(
    ETabletDistributedThrottlerKind tabletThrottlerKind,
    const TTabletSnapshotPtr& tabletSnapshot,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions);

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
    NTabletClient::TReadTimestampRange timestampRange,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory);

//! Creates a range reader that handles ordered stores.
/*!
 *  Decodes hunks.
 */
NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulOrderedTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TColumnFilter& columnFilter,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    NTabletClient::TReadTimestampRange timestampRange,
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
    NTabletClient::TReadTimestampRange timestampRange,
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
    NTabletClient::TReadTimestampRange timestampRange,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory);

//! Creates a range reader that merges data from all given #stores and
//! returns all versions of each value.
/*!
 *  Can only handle sorted tables.
 *  Does not decode hunks, intentionally.
 */
NTableClient::IVersionedReaderPtr CreateVersionedTabletReader(
    const TTabletSnapshotPtr& tabletSnapshot,
    std::vector<ISortedStorePtr> stores,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    int minConcurrency,
    std::optional<ETabletDistributedThrottlerKind> tabletThrottlerKind,
    std::optional<EWorkloadCategory> workloadCategory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
