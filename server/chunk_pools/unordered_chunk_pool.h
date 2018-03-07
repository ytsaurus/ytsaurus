#pragma once

#include "private.h"
#include "chunk_pool.h"

#include <yt/server/controller_agent/public.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnorderedChunkPoolMode,
    (Normal)
    (AutoMerge)
);

std::unique_ptr<IChunkPool> CreateUnorderedChunkPool(
    NControllerAgent::IJobSizeConstraintsPtr jobSizeConstraints,
    NControllerAgent::TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig,
    EUnorderedChunkPoolMode mode = EUnorderedChunkPoolMode::Normal);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
