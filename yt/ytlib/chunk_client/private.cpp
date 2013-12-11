#include "stdafx.h"
#include "private.h"

#include <core/rpc/channel.h>
#include <core/rpc/caching_channel_factory.h>
#include <core/rpc/bus_channel.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkReaderLogger("ChunkReader");
NLog::TLogger ChunkWriterLogger("ChunkWriter");

// For light requests (e.g. SendBlocks, GetBlocks, etc).
NRpc::IChannelFactoryPtr LightNodeChannelFactory(NRpc::CreateCachingChannelFactory(NRpc::GetBusChannelFactory()));

// For heavy requests (e.g. PutBlocks).
NRpc::IChannelFactoryPtr HeavyNodeChannelFactory(NRpc::CreateCachingChannelFactory(NRpc::GetBusChannelFactory()));

const int MaxPrefetchWindow = 250;
const i64 ChunkReaderMemorySize = (i64) 16 * 1024;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

