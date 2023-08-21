#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/rpc/service.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct IObjectService
    : public virtual TRefCounted
{
    virtual void Reconfigure(const TObjectServiceDynamicConfigPtr& config) = 0;

    virtual NRpc::IServicePtr GetService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IObjectService)

////////////////////////////////////////////////////////////////////////////////

IObjectServicePtr CreateObjectService(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
