#pragma once

#include "public.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/misc/range.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Creates a range reader that merges data from the relevant stores and
//! returns a single version of each value.

 NTableClient::ISchemafulReaderPtr CreateSchemafulSortedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<NTableClient::TRowRange>& bounds,
    TTimestamp timestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions);

NTableClient::ISchemafulReaderPtr CreateSchemafulOrderedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions);

/*!
 *  Can handle both sorted and ordered tables.
 */

NTableClient::ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions);

//! Creates a lookup reader that merges data from the relevant stores and
//! returns a single version of each value.
/*!
 *  Can only handle sorted tables.
 */
NTableClient::ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    const TColumnFilter& columnFilter,
    const TSharedRange<TKey>& keys,
    TTimestamp timestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions);

//! Creates a range reader that merges data from all given #stores and
//! returns all versions of each value.
/*!
 *  Can only handle sorted tables.
 */
NTableClient::IVersionedReaderPtr CreateVersionedTabletReader(
    TTabletSnapshotPtr tabletSnapshot,
    std::vector<ISortedStorePtr> stores,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    int minConcurrency);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
