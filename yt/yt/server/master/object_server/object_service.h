#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/rpc/service.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct IObjectService
    : public virtual NRpc::IService
{
    virtual NObjectClient::TObjectServiceCachePtr GetCache() = 0;
    virtual IInvokerPtr GetLocalReadOffloadInvoker() = 0;
};

DEFINE_REFCOUNTED_TYPE(IObjectService)

////////////////////////////////////////////////////////////////////////////////

IObjectServicePtr CreateObjectService(
    TObjectServiceConfigPtr config,
    NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
