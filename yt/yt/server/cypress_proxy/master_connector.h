#pragma once

#include "public.h"

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct IMasterConnector
    : public TRefCounted
{
    virtual void Start() = 0;

    // Thread affinity: any.
    virtual bool IsRegistered() = 0;
    virtual void ValidateRegistration() = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
