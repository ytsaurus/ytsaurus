#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <ytlib/misc/nullable.h>

#include <ytlib/concurrency/throughput_throttler.h>

#include <ytlib/rpc/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    IBlockCachePtr blockCache,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TNullable<NNodeTrackerClient::TNodeDescriptor>& localDescriptor,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas = TChunkReplicaList(),
    EReadSessionType sessionType = EReadSessionType::User,
    IThroughputThrottlerPtr throttler = GetUnlimitedThrottler());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
