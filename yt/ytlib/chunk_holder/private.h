#pragma once

#include <ytlib/logging/log.h>
#include <ytlib/rpc/channel_cache.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger ChunkHolderLogger;
extern NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
