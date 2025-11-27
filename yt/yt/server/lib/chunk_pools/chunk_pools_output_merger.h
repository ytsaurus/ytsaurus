#pragma once

#include "public.h"

#include <yt/yt/core/logging/serializable_logger.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

// Merges all job data into a single job.
IPersistentChunkPoolOutputPtr MergeChunkPoolsOutputs(
    std::vector<IPersistentChunkPoolOutputPtr> chunkPools,
    NLogging::TSerializableLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
