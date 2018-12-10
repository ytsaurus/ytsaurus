#pragma once

#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Constructs a channel that limits request rate to the underlying channel.
IChannelPtr CreateThrottlingChannel(
    TThrottlingChannelConfigPtr config,
    IChannelPtr underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
