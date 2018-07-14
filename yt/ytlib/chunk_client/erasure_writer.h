#pragma once

#include "public.h"
#include "client_block_cache.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/erasure/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    const TSessionId& sessionId,
    NErasure::ECodec codecId,
    NErasure::ICodec* codec,
    const std::vector<IChunkWriterPtr>& writers,
    const TWorkloadDescriptor& workloadDescriptor);

std::vector<IChunkWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    const TSessionId& sessionId,
    NErasure::ICodec* codec,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NApi::NNative::IClientPtr client,
    TTrafficMeterPtr trafficMeter = nullptr,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    IBlockCachePtr blockCache = GetNullBlockCache());

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

