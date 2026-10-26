#pragma once

#include "private.h"

#include <yt/yt/core/actions/signal.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TConfigWrapper
    : public TRefCounted
{
public:
    explicit TConfigWrapper(TDistributedHydraManagerConfigPtr config);

    void Set(TDistributedHydraManagerConfigPtr config);
    TDistributedHydraManagerConfigPtr Get() const;

    //! Fired synchronously by #Set.
    DEFINE_SIGNAL(void(const TDistributedHydraManagerConfigPtr& config), Changed);

private:
    TAtomicIntrusivePtr<TDistributedHydraManagerConfig> Config_;
};

DEFINE_REFCOUNTED_TYPE(TConfigWrapper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
