#pragma once

#include "public.h"
#include "client_block_cache.h"
#include "chunk_reader_allowing_repair.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/optional.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderAllowingRepairPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    std::optional<NNodeTrackerClient::TNodeId> localNodeId,
    TChunkId chunkId,
    const TChunkReplicaList& seedReplicas,
    IBlockCachePtr blockCache,
    IClientChunkMetaCachePtr chunkMetaCache,
    TTrafficMeterPtr trafficMeter = nullptr,
    NNodeTrackerClient::INodeStatusDirectoryPtr nodeStatusDirectory = nullptr,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
