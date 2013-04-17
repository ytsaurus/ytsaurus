#include "stdafx.h"
#include "private.h"

#include <ytlib/actions/bind.h>
#include <ytlib/actions/bind_helpers.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger ChunkReaderLogger("ChunkReader");
NLog::TLogger ChunkWriterLogger("ChunkWriter");

TLazyHolder<NRpc::TChannelCache> NodeChannelCache;

const int MaxPrefetchWindow = 250;
const i64 ChunkReaderMemorySize = (i64) 16 * 1024;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

