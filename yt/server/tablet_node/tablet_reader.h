#pragma once

#include "public.h"

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader that merges data from the relevant stores and
//! a single version of each value.
/*!
 *  Must be called from the automaton thread.
 */
NVersionedTableClient::ISchemafulReaderPtr CreateSchemafulTabletReader(
    TTablet* tablet,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp);

//! Creates a reader that merges data from the relevant stores
//! and provides all versions of each value.
/*!
 *  Must be called from the automaton thread.
 *  
 *  If #partition is |nullptr| then takes all data, otherwise just that
 *  contained in stores belonging to #partition.
 */
NVersionedTableClient::IVersionedReaderPtr CreateVersionedTabletReader(
    TTablet* tablet,
    std::vector<IStorePtr> stores,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
