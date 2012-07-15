#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>
#include <ytlib/rpc/channel_cache.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkHolderLogger;
extern NProfiling::TProfiler ChunkHolderProfiler;
extern NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
