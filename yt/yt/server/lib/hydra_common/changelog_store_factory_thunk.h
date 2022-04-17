#pragma once

#include "changelog.h"

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TChangelogStoreFactoryThunk
    : public IChangelogStoreFactory
{
public:
    TFuture<IChangelogStorePtr> Lock() override;

    void SetUnderlying(IChangelogStoreFactoryPtr underlying);

private:
    TAtomicObject<IChangelogStoreFactoryPtr> Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TChangelogStoreFactoryThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
