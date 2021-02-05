#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/misc/range.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Creates a range reader that merges data from the relevant stores and
//! returns a single version of each value.

NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulSortedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<NTableClient::TRowRange>& bounds,
    TTimestamp timestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulOrderedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TTimestamp timestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

/*!
 *  Can handle both sorted and ordered tables.
 */
NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulRangeTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TTimestamp timestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

//! Creates a lookup reader that merges data from the relevant stores and
//! returns a single version of each value.
/*!
 *  Can only handle sorted tables.
 */
NTableClient::ISchemafulUnversionedReaderPtr CreateSchemafulLookupTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keys,
    TTimestamp timestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

//! Creates a range reader that merges data from all given #stores and
//! returns all versions of each value.
/*!
 *  Can only handle sorted tables.
 */
NTableClient::IVersionedReaderPtr CreateVersionedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    std::vector<ISortedStorePtr> stores,
    TLegacyOwningKey lowerBound,
    TLegacyOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    int minConcurrency,
    NConcurrency::IThroughputThrottlerPtr throttler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
