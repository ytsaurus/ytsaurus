#pragma once

#include "chunk_fragment_reader.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkFragmentReaderPtr CreateNewChunkFragmentReader(
    TChunkFragmentReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::INodeStatusDirectoryPtr nodeStatusDirectory,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
