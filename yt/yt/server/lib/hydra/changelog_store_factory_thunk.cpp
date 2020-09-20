#include "changelog_store_factory_thunk.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<IChangelogStorePtr> TChangelogStoreFactoryThunk::Lock()
{
    return GetUnderlying()->Lock();
}

void TChangelogStoreFactoryThunk::SetUnderlying(IChangelogStoreFactoryPtr underlying)
{
    TGuard<TAdaptiveLock> guard(SpinLock_);
    Underlying_ = underlying;
}

IChangelogStoreFactoryPtr TChangelogStoreFactoryThunk::GetUnderlying()
{
    TGuard<TAdaptiveLock> guard(SpinLock_);
    return Underlying_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
