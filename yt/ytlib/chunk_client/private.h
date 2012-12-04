#pragma once

#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/rpc/channel_cache.h>

#include <ytlib/logging/log.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkReaderLogger;
extern NLog::TLogger ChunkWriterLogger;

extern TLazyHolder<NRpc::TChannelCache> NodeChannelCache;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

