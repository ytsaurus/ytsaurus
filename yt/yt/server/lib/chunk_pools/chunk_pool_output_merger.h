#pragma once

#include "public.h"

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

// Merges all job data into a single job.
IPersistentChunkPoolOutputPtr MergeChunkPoolsOutputs(std::vector<IPersistentChunkPoolOutputPtr> chunkPools);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
