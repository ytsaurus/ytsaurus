#pragma once

#include "changelog.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TChangelogStoreFactoryThunk
    : public IChangelogStoreFactory
{
public:
    TFuture<IChangelogStorePtr> Lock() override;

    void SetUnderlying(IChangelogStoreFactoryPtr underlying);

private:
    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    IChangelogStoreFactoryPtr Underlying_;


    IChangelogStoreFactoryPtr GetUnderlying();

};

DEFINE_REFCOUNTED_TYPE(TChangelogStoreFactoryThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
