#pragma once

#include "public.h"
#include "async_reader.h"
#include "block_cache.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncReader::TPtr CreateRemoteReader(
    TRemoteReaderConfigPtr config,
    IBlockCache* blockCache,
    NRpc::IChannel* masterChannel,
    const TChunkId& chunkId,
    const yvector<Stroka>& seedAddresses = yvector<Stroka>());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
