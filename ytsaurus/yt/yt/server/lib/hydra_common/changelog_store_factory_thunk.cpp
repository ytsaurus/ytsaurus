#include "changelog_store_factory_thunk.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<IChangelogStorePtr> TChangelogStoreFactoryThunk::Lock()
{
    return Underlying_.Acquire()->Lock();
}

void TChangelogStoreFactoryThunk::SetUnderlying(IChangelogStoreFactoryPtr underlying)
{
    Underlying_.Store(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
