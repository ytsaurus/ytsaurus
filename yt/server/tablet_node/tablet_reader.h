#pragma once

#include "public.h"

#include <core/actions/public.h>

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader that merges data from the relevant stores and
//! returns a single version of each value.
NVersionedTableClient::ISchemafulReaderPtr CreateSchemafulTabletReader(
    IInvokerPtr poolInvoker,
    TTabletSnapshotPtr tabletSnapshot,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp timestamp);

//! Creates a reader that merges data from all given #stores and
//! returns all versions of each value.
NVersionedTableClient::IVersionedReaderPtr CreateVersionedTabletReader(
    IInvokerPtr poolInvoker,
    TTabletSnapshotPtr tabletSnapshot,
    std::vector<IStorePtr> stores,
    TOwningKey lowerBound,
    TOwningKey upperBound,
    TTimestamp currentTimestamp,
    TTimestamp majorTimestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
