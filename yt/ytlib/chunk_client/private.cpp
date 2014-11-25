#include "stdafx.h"
#include "private.h"

#include <core/rpc/channel.h>
#include <core/rpc/caching_channel_factory.h>
#include <core/rpc/bus_channel.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkClientLogger("ChunkClient");

NRpc::IChannelFactoryPtr LightNodeChannelFactory(
    NRpc::CreateCachingChannelFactory(NRpc::GetBusChannelFactory()));

NRpc::IChannelFactoryPtr HeavyNodeChannelFactory(
    NRpc::CreateCachingChannelFactory(NRpc::GetBusChannelFactory()));

const char* const ChunkMetaSuffix = ".meta";

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

