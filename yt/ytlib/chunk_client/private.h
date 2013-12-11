#pragma once

#include <core/logging/log.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkReaderLogger;
extern NLog::TLogger ChunkWriterLogger;

extern NRpc::IChannelFactoryPtr HeavyNodeChannelFactory;
extern NRpc::IChannelFactoryPtr LightNodeChannelFactory;

extern const int MaxPrefetchWindow;

//! Estimated memory overhead per chunk reader.
extern const i64 ChunkReaderMemorySize;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

