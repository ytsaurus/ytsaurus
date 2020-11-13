#include "private.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger ChunkServerLogger("ChunkServer");
const NProfiling::TProfiler ChunkServerProfiler("/chunk_server");
const NProfiling::TRegistry ChunkServerProfilerRegistry("yt/chunk_server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
