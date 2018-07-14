#pragma once

#include "public.h"
#include "chunk_replica.h"
#include "client_block_cache.h"
#include "chunk_reader_allowing_repair.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/misc/nullable.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkReaderAllowingRepairPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas = TChunkReplicaList(),
    IBlockCachePtr blockCache = GetNullBlockCache(),
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
