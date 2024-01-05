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

////////////////////////////////////////////////////////////////////////////////

//! Creates a caching Object Service from a given #upstreamChannel.
/*!
 *  Throttling is applied to requests going through #upstreamChannel.
 */
ICachingObjectServicePtr CreateCachingObjectService(
    TCachingObjectServiceConfigPtr config,
    IInvokerPtr invoker,
    NRpc::IChannelPtr upstreamChannel,
    TObjectServiceCachePtr cache,
    NRpc::TRealmId masterCellId,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    NRpc::IAuthenticatorPtr authenticator);

//! Creates a channel to master to be used as an upstream for
//! Object Service cache.
NRpc::IChannelPtr CreateMasterChannelForCache(
    NApi::NNative::IConnectionPtr connection,
    NRpc::TRealmId masterCellId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
