#pragma once

#include "public.h"

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkServerLogger;
extern NProfiling::TProfiler ChunkServerProfiler;

struct IChunkVisitor;
typedef TIntrusivePtr<IChunkVisitor> IChunkVisitorPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT