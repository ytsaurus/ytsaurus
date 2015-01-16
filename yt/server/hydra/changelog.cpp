#include "stdafx.h"
#include "changelog.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<IChangelogPtr> IChangelogStore::TryOpenChangelog(int id)
{
    return OpenChangelog(id).Apply(BIND([] (const TErrorOr<IChangelogPtr>& result) -> IChangelogPtr {
        if (!result.IsOK() && result.FindMatching(NHydra::EErrorCode::NoSuchChangelog)) {
            return IChangelogPtr(nullptr);
        }
        return result.ValueOrThrow();
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
