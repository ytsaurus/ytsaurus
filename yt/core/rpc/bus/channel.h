#pragma once

#include "public.h"

#include <yt/core/bus/public.h>

namespace NYT::NRpc::NBus {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel implemented via Bus.
IChannelPtr CreateBusChannel(NYT::NBus::IBusClientPtr client);

//! Creates a factory for creating Bus channels.
IChannelFactoryPtr CreateBusChannelFactory(NYT::NBus::TTcpBusConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NBus
