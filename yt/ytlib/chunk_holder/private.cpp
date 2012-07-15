#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkHolderLogger("ChunkHolder");
NProfiling::TProfiler ChunkHolderProfiler("/chunk_holder");
NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
