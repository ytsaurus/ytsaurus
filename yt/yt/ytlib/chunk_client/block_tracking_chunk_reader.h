#pragma once

#include "chunk_reader.h"

#include <yt/yt/ytlib/memory_trackers/block_tracker.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateBlockTrackingChunkReader(
    IChunkReaderPtr underlying,
    IBlockTrackerPtr tracker,
    std::optional<NNodeTrackerClient::EMemoryCategory> category);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
