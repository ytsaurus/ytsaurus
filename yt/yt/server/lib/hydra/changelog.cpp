#include "changelog.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

int IChangelogStore::GetTermOrThrow() const
{
    auto optionalTerm = TryGetTerm();
    if (!optionalTerm) {
        THROW_ERROR_EXCEPTION("Cannot get term since changelog store is read-only");
    }
    return *optionalTerm;
}

int IChangelogStore::GetLatestChangelogIdOrThrow() const
{
    auto optionalChangelogId = TryGetLatestChangelogId();
    if (!optionalChangelogId) {
        THROW_ERROR_EXCEPTION("Cannot get latest changelog id since changelog store is read-only");
    }
    return *optionalChangelogId;
}

TFuture<IChangelogPtr> IChangelogStore::TryOpenChangelog(int id)
{
    return OpenChangelog(id)
        .Apply(BIND([] (const TErrorOr<IChangelogPtr>& result) -> IChangelogPtr {
            if (!result.IsOK() && result.FindMatching(NHydra::EErrorCode::NoSuchChangelog)) {
                return IChangelogPtr(nullptr);
            }
            return result.ValueOrThrow();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
