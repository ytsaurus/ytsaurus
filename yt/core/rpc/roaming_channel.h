#pragma once

#include "public.h"

#include <core/misc/error.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

typedef TCallback<TFuture<TErrorOr<IChannelPtr>>(IClientRequestPtr)> TChannelProducer;

//! Creates a channel with a dynamically discovered endpoint.
/*!
 *  Upon the first request to the roaming channel,
 *  the producer is called to discover the actual endpoint.
 *  This endpoint is cached and reused until some request fails with RPC error code.
 *  In the latter case the endpoint is rediscovered.
 */
IChannelPtr CreateRoamingChannel(TChannelProducer producer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
