#pragma once

#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! The resulting channel initially forwards a request to #primaryChannel and
//! if no response comes within #delay, re-sends the request to #backupChannel.
//! Whatever underlying channel responds first is the winner.
IChannelPtr CreateLatencyTamingChannel(
    IChannelPtr primaryChannel,
    IChannelPtr backupChannel,
    TDuration delay);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
