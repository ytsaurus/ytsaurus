#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/library/erasure/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::IChunkReaderPtr CreateChunkReader(
    TChunkReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::TChunkId chunkId,
    NErasure::ECodec codecId,
    const NChunkClient::TChunkReplicaList& replicas,
    NChunkClient::IBlockCachePtr blockCache,
    NChunkClient::TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler(),
    NConcurrency::IThroughputThrottlerPtr rpsThrottler = NConcurrency::GetUnlimitedThrottler());

NChunkClient::IChunkReaderPtr CreateChunkPartReader(
    TChunkReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::IBlockCachePtr blockCache,
    NChunkClient::TChunkId chunkId,
    NErasure::ECodec codecId,
    const NChunkClient::TChunkReplicaList& replicas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
