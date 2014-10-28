#include "stdafx.h"
#include "changelog.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<TErrorOr<IChangelogPtr>> IChangelogStore::TryOpenChangelog(int id)
{
    return OpenChangelog(id).Apply(BIND([] (const TErrorOr<IChangelogPtr>& result) -> TErrorOr<IChangelogPtr> {
        if (!result.IsOK() && result.GetCode() == NHydra::EErrorCode::NoSuchChangelog) {
            return IChangelogPtr(nullptr);
        }
        return result;
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
