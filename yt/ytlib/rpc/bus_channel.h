#pragma once

#include "public.h"
#include "channel.h"

#include <ytlib/bus/client.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via NBus.
IChannelPtr CreateBusChannel(
    NBus::IBusClientPtr client,
    TNullable<TDuration> defaultTimeout = Null);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
