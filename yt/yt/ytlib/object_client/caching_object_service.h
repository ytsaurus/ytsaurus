#pragma once

#include "public.h"

#include <yt/core/rpc/service.h>

#include <yt/core/actions/public.h>

#include <yt/core/logging/public.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

struct ICachingObjectService
    : public virtual NRpc::IService
{
    virtual void Reconfigure(const TCachingObjectServiceDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICachingObjectService)

ICachingObjectServicePtr CreateCachingObjectService(
    TCachingObjectServiceConfigPtr config,
    IInvokerPtr invoker,
    NRpc::IChannelPtr masterChannel,
    TObjectServiceCachePtr cache,
    NRpc::TRealmId masterCellId,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
