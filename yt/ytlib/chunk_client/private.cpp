#include "stdafx.h"
#include "private.h"

#include <core/actions/bind.h>
#include <core/actions/bind_helpers.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkReaderLogger("ChunkReader");
NLog::TLogger ChunkWriterLogger("ChunkWriter");

// For light requests (e.g. SendBlocks, GetBlocks, etc).
TLazyUniquePtr<NRpc::TChannelCache> LightNodeChannelCache;

// For heavy requests (e.g. PutBlocks).
TLazyUniquePtr<NRpc::TChannelCache> HeavyNodeChannelCache;

const int MaxPrefetchWindow = 250;
const i64 ChunkReaderMemorySize = (i64) 16 * 1024;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

