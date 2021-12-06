#include "changelog.h"

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

TFuture<IChangelogPtr> IChangelogStore::TryOpenChangelog(int id)
{
    return OpenChangelog(id)
        .Apply(BIND([] (const TErrorOr<IChangelogPtr>& result) -> IChangelogPtr {
            if (!result.IsOK() && result.FindMatching(EErrorCode::NoSuchChangelog)) {
                return IChangelogPtr(nullptr);
            }
            return result.ValueOrThrow();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
