#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>
#include <ytlib/rpc/channel_cache.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger DataNodeLogger;
extern NProfiling::TProfiler DataNodeProfiler;
extern NRpc::TChannelCache ChannelCache;
extern Stroka CellGuidFileName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
