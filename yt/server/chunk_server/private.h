#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkServerLogger;
extern NProfiling::TProfiler ChunkServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT