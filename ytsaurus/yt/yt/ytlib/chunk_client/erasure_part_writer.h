#pragma once

#include "public.h"
#include "client_block_cache.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/library/erasure/impl/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

std::vector<IChunkWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    TSessionId sessionId,
    NApi::NNative::IClientPtr client,
    const NErasure::TPartIndexList& partIndexList,
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    IBlockCachePtr blockCache = GetNullBlockCache(),
    TChunkReplicaWithMediumList targetReplicas = {});

std::vector<IChunkWriterPtr> CreateAllErasurePartWriters(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    TSessionId sessionId,
    NErasure::ICodec* codec,
    NApi::NNative::IClientPtr client,
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    IBlockCachePtr blockCache = GetNullBlockCache(),
    TChunkReplicaWithMediumList targetReplicas = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
