#pragma once

#include <core/misc/lazy_ptr.h>

#include <core/rpc/channel_cache.h>

#include <core/logging/log.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkReaderLogger;
extern NLog::TLogger ChunkWriterLogger;

extern TLazyUniquePtr<NRpc::TChannelCache> HeavyNodeChannelCache;
extern TLazyUniquePtr<NRpc::TChannelCache> LightNodeChannelCache;

extern const int MaxPrefetchWindow;

//! Estimated memory overhead per chunk reader.
extern const i64 ChunkReaderMemorySize;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

