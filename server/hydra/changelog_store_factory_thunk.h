#pragma once

#include "changelog.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TChangelogStoreFactoryThunk
    : public IChangelogStoreFactory
{
public:
    virtual TFuture<IChangelogStorePtr> Lock() override;

    void SetUnderlying(IChangelogStoreFactoryPtr underlying);

private:
    TSpinLock SpinLock_;
    IChangelogStoreFactoryPtr Underlying_;


    IChangelogStoreFactoryPtr GetUnderlying();

};

DEFINE_REFCOUNTED_TYPE(TChangelogStoreFactoryThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
