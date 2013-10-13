#include "stdafx.h"
#include "changelog.h"

#include <core/misc/error.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TChangelogCreateParams::TChangelogCreateParams()
    : PrevRecordCount(-1)
{ }

////////////////////////////////////////////////////////////////////////////////

IChangelogPtr IChangelogStore::OpenChangelogOrThrow(int id)
{
    auto changelog = TryOpenChangelog(id);
    if (!changelog) {
        THROW_ERROR_EXCEPTION(
            NHydra::EErrorCode::NoSuchChangelog,
            "No such changelog %d",
            id);
    }
    return changelog;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
