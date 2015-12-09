#include "private.h"

#include <yt/core/rpc/bus_channel.h>
#include <yt/core/rpc/caching_channel_factory.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger ChunkClientLogger("ChunkClient");

NRpc::IChannelFactoryPtr LightNodeChannelFactory(
    NRpc::CreateCachingChannelFactory(NRpc::GetBusChannelFactory()));

NRpc::IChannelFactoryPtr HeavyNodeChannelFactory(
    NRpc::CreateCachingChannelFactory(NRpc::GetBusChannelFactory()));

const Stroka ChunkMetaSuffix(".meta");

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

