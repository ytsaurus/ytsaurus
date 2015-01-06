#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Enables on-demand channel construction with dynamicablly discovered endpoint.
struct IRoamingChannelProvider
    : public virtual TRefCounted
{
    virtual NYTree::TYsonString GetEndpointDescription() const = 0;
    virtual TFuture<IChannelPtr> DiscoverChannel(IClientRequestPtr request) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRoamingChannelProvider)

//! Creates a channel with a dynamically discovered endpoint.
/*!
 *  Upon the first request to the roaming channel,
 *  the provider is called to discover the actual endpoint.
 *  This endpoint is cached and reused until some request fails with RPC error code.
 *  In the latter case the endpoint is rediscovered.
 */
IChannelPtr CreateRoamingChannel(
    IRoamingChannelProviderPtr provider,
    TCallback<bool(const TError&)> isChannelFailureError = BIND(&IsChannelFailureError));

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
