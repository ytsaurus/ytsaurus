#pragma once

#include "private.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct IBundleController
    : public TRefCounted
{
    virtual void Start() = 0;

    // TODO(capone212): CreateOrchidService
};

DEFINE_REFCOUNTED_TYPE(IBundleController)

////////////////////////////////////////////////////////////////////////////////

IBundleControllerPtr CreateBundleController(IBootstrap* bootstrap, TBundleControllerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
