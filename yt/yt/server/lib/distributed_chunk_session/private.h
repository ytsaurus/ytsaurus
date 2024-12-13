#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NDistributedChunkSession {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, DistributedChunkSessionServiceLogger, "DistributedChunkSessionService");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSession
