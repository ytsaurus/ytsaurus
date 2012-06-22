#pragma once

#include "public.h"
#include "channel.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a scoped channel that wraps #underlyingChannel.
/*!
 *  The scoped channel forwards all requests to #underlyingChannel.
 *  It also tracks all outstanding requests.
 *  Calling #IChannel::Terminate blocks until all outstanding requests are replied.
 */ 
IChannelPtr CreateScopedChannel(IChannelPtr underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
