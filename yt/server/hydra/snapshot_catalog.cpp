#include "stdafx.h"
#include "snapshot_catalog.h"
#include "snapshot.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

ISnapshotStorePtr ISnapshotCatalog::GetStore(const TCellGuid& cellGuid)
{
    auto store = FindStore(cellGuid);
    YCHECK(store);
    return store;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

