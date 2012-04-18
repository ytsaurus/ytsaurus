#pragma once

#include "public.h"
#include "private.h"

#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateRemoteReader(
    TRemoteReaderConfigPtr config,
    IBlockCachePtr blockCache,
    NRpc::IChannel* masterChannel,
    const TChunkId& chunkId,
    const std::vector<Stroka>& seedAddresses = std::vector<Stroka>());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
