#pragma once

#include "public.h"

#include <yt/core/bus/public.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via Bus.
IChannelPtr CreateBusChannel(NBus::IBusClientPtr client);

//! Returns the factory for creating Bus channels.
IChannelFactoryPtr GetBusChannelFactory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
