#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, DistributedChunkSessionServiceLogger, "DistributedChunkSessionService");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
