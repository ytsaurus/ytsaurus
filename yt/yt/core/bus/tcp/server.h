#pragma once

#include "public.h"

#include "packet.h"

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

IBusServerPtr CreateTcpBusServer(
    TTcpBusServerConfigPtr config,
    std::unique_ptr<IPacketTranscoderFactory> packetTranscoderFactory = CreateYTPacketTranscoderFactory());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
