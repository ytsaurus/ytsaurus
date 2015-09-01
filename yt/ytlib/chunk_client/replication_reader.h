#pragma once

#include "public.h"
#include "chunk_replica.h"
#include "client_block_cache.h"

#include <ytlib/api/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/misc/nullable.h>

#include <core/concurrency/throughput_throttler.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TNullable<NNodeTrackerClient::TNodeDescriptor>& localDescriptor,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas = TChunkReplicaList(),
    IBlockCachePtr blockCache = GetNullBlockCache(),
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
