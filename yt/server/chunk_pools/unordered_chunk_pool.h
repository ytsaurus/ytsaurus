#pragma once

#include "private.h"
#include "chunk_pool.h"

#include <yt/server/controller_agent/public.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPool> CreateUnorderedChunkPool(
    NControllerAgent::IJobSizeConstraintsPtr jobSizeConstraints,
    NControllerAgent::TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
