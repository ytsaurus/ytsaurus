#pragma once

#include "public.h"
#include "client_block_cache.h"

#include <core/concurrency/throughput_throttler.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateReplicationWriter(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    const TChunkId& chunkId,
    const TChunkReplicaList& targets,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NRpc::IChannelPtr masterChannel,
    IBlockCachePtr blockCache = GetNullBlockCache(),
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
