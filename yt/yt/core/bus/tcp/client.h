#pragma once

#include "public.h"

#include "packet.h"

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

//! Initializes a new client for communicating with a given address.
IBusClientPtr CreateTcpBusClient(
    TTcpBusClientConfigPtr config,
    std::unique_ptr<IPacketTranscoderFactory> packetTranscoderFactory = CreateYTPacketTranscoderFactory());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
