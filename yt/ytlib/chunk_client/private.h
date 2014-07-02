#pragma once

#include <core/logging/log.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkClientLogger;

extern NRpc::IChannelFactoryPtr HeavyNodeChannelFactory;
extern NRpc::IChannelFactoryPtr LightNodeChannelFactory;

const int MaxPrefetchWindow = 250;
//! Estimated memory overhead per chunk reader.
const i64 ChunkReaderMemorySize = (i64) 16 * 1024;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

