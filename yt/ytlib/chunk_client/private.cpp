#include "stdafx.h"
#include "private.h"

#include <core/rpc/caching_bus_channel_factory-inl.h>

#include <core/misc/singleton.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkClientLogger("ChunkClient");

struct THeavyNodeTag;
struct TLightNodeTag;

NRpc::IChannelFactoryPtr GetHeavyNodeChannelFactory()
{
    return NRpc::GetCachingBusChannelFactory<THeavyNodeTag>();
}

NRpc::IChannelFactoryPtr GetLightNodeChannelFactory()
{
    return NRpc::GetCachingBusChannelFactory<TLightNodeTag>();
}

const char* const ChunkMetaSuffix = ".meta";

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

