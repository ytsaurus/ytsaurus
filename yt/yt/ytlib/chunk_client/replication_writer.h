#pragma once

#include "public.h"
#include "client_block_cache.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateReplicationWriter(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    TSessionId sessionId,
    TChunkReplicaWithMediumList targets,
    NApi::NNative::IClientPtr client,
    TString localHostName,
    IBlockCachePtr blockCache = GetNullBlockCache(),
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
