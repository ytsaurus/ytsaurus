#include "stdafx.h"
#include "changelog_catalog.h"
#include "changelog.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

IChangelogStorePtr IChangelogCatalog::GetStore(const TCellGuid& cellGuid)
{
    auto store = FindStore(cellGuid);
    YCHECK(store);
    return store;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

