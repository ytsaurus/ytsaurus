#pragma once

#include "public.h"
#include "channel.h"

#include <core/bus/client.h>

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
