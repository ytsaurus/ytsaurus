#pragma once

#include "public.h"
#include "channel.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a serialized channel that wraps #underlyingChannel.
/*!
 *  The serialized channel forwards all requests to #underlyingChannel
 *  but only starts a new request once the previous one is completed.
 */ 
IChannelPtr CreateSerializedChannel(IChannelPtr underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
