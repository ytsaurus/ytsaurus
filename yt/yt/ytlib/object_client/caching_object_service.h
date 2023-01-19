#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/logging/public.h>

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
    const NApi::NNative::IConnectionPtr& connection,
    TObjectServiceCachePtr cache,
    NRpc::TRealmId masterCellId,
    NLogging::TLogger logger,
    NRpc::IAuthenticatorPtr authenticator);

//! Accepts direct channel to master cache, used for
//! two-level master caches only.
ICachingObjectServicePtr CreateCachingObjectService(
    TCachingObjectServiceConfigPtr config,
    IInvokerPtr invoker,
    NRpc::IChannelPtr cacheChannel,
    TObjectServiceCachePtr cache,
    NRpc::TRealmId masterCellId,
    NLogging::TLogger logger,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
