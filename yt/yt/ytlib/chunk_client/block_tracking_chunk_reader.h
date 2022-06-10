#pragma once

#include "chunk_reader.h"

#include <yt/yt/ytlib/memory_trackers/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateBlockTrackingChunkReader(
    IChunkReaderPtr underlying,
    IBlockTrackerPtr tracker,
    std::optional<EMemoryCategory> category);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
