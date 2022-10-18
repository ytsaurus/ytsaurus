#pragma once

#include "public.h"

#include "packet.h"

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

IBusServerPtr CreateTcpBusServer(
    TTcpBusServerConfigPtr config,
    IPacketTranscoderFactory* packetTranscoderFactory = GetYTPacketTranscoderFactory());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
