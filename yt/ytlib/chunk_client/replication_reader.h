#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <core/misc/nullable.h>

#include <core/concurrency/throughput_throttler.h>

#include <core/rpc/public.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    IBlockCachePtr compressedBlockCache,
    NRpc::IChannelPtr masterChannel,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TNullable<NNodeTrackerClient::TNodeDescriptor>& localDescriptor,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas = TChunkReplicaList(),
    const Stroka& networkName = NNodeTrackerClient::DefaultNetworkName,
    EReadSessionType sessionType = EReadSessionType::User,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
