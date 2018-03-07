#pragma once

#include "public.h"

#include <yt/core/bus/public.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via Bus.
IChannelPtr CreateBusChannel(NBus::IBusClientPtr client);

//! Creates a factory for creating Bus channels.
IChannelFactoryPtr CreateBusChannelFactory(NBus::TTcpBusConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
