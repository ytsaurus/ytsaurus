#pragma once

#include <core/logging/log.h>
#include <core/profiling/profiler.h>
#include <core/rpc/channel_cache.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger DataNodeLogger;
extern NProfiling::TProfiler DataNodeProfiler;
extern NRpc::TChannelCache ChannelCache;
extern Stroka CellGuidFileName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
