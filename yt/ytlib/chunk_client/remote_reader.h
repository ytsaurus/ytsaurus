#pragma once

#include "public.h"
#include "chunk_replica.h"
#include "node_directory.h"

#include <ytlib/misc/nullable.h>

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateRemoteReader(
    TRemoteReaderConfigPtr config,
    IBlockCachePtr blockCache,
    NRpc::IChannelPtr masterChannel,
    TNodeDirectoryPtr nodeDirectory,
    const TNullable<TNodeDescriptor>& localDescriptor,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas = TChunkReplicaList());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
