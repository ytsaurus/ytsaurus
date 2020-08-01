#pragma once

#include "public.h"
#include "client_block_cache.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/library/erasure/public.h>

#include <yt/core/misc/error.h>

#include <yt/core/rpc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

std::vector<IChunkReaderPtr> CreateErasurePartReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    TChunkId chunkId,
    const TChunkReplicaList& seedReplicas,
    const NErasure::ICodec* codec,
    const NErasure::TPartIndexList& partIndexList,
    IBlockCachePtr blockCache = GetNullBlockCache(),
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

std::vector<IChunkReaderPtr> CreateAllErasurePartReaders(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    TChunkId chunkId,
    const TChunkReplicaList& seedReplicas,
    const NErasure::ICodec* codec,
    IBlockCachePtr blockCache = GetNullBlockCache(),
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

