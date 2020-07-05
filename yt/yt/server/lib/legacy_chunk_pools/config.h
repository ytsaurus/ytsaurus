#pragma once

#include <yt/server/lib/chunk_pools/config.h>

namespace NYT::NLegacyChunkPools {

////////////////////////////////////////////////////////////////////////////////

// NB(gritukan): This config is used in CA, so we want to have one copy of it.
using TJobSizeAdjusterConfig = NYT::NChunkPools::TJobSizeAdjusterConfig;
using TJobSizeAdjusterConfigPtr = NYT::NChunkPools::TJobSizeAdjusterConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLegacyChunkPools
