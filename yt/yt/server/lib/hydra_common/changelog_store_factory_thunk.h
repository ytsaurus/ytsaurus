#pragma once

#include "changelog.h"

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TChangelogStoreFactoryThunk
    : public IChangelogStoreFactory
{
public:
    TFuture<IChangelogStorePtr> Lock() override;

    void SetUnderlying(IChangelogStoreFactoryPtr underlying);

private:
    TAtomicIntrusivePtr<IChangelogStoreFactory> Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TChangelogStoreFactoryThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
