#pragma once

#include "public.h"

#include <core/logging/log.h>
#include <core/profiling/profiler.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger ChunkServerLogger;
extern NProfiling::TProfiler ChunkServerProfiler;

DECLARE_REFCOUNTED_STRUCT(IChunkVisitor)
DECLARE_REFCOUNTED_STRUCT(IChunkTraverserCallbacks)
DECLARE_REFCOUNTED_STRUCT(IChunkTreeBalancerCallbacks)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
