#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ReplicatorLogger;
extern const NProfiling::TProfiler ReplicatorProfilerRegistry;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
