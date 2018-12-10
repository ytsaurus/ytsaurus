#pragma once

#include "private.h"
#include "chunk_pool.h"
#include "input_stream.h"

#include <yt/server/controller_agent/config.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnorderedChunkPoolMode,
    (Normal)
    (AutoMerge)
);

struct TUnorderedChunkPoolOptions
{
    EUnorderedChunkPoolMode Mode = EUnorderedChunkPoolMode::Normal;
    NControllerAgent::TJobSizeAdjusterConfigPtr JobSizeAdjusterConfig = nullptr;
    NControllerAgent::IJobSizeConstraintsPtr JobSizeConstraints = nullptr;
    //! Minimum uncompressed size to be teleported.
    i64 MinTeleportChunkSize = std::numeric_limits<i64>::max();
    //! Minimum data weight to be teleported/
    i64 MinTeleportChunkDataWeight = std::numeric_limits<i64>::max();
    bool SliceErasureChunksByParts = false;
    NScheduler::TOperationId OperationId;
    TString Task;

    void Persist(const TPersistenceContext& context);
};

std::unique_ptr<IChunkPool> CreateUnorderedChunkPool(
    const TUnorderedChunkPoolOptions& options,
    TInputStreamDirectory dataSourceDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
