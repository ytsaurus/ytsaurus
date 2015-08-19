#pragma once

#include "public.h"
#include "client_block_cache.h"

#include <ytlib/api/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/erasure/public.h>

#include <core/rpc/public.h>

#include <core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    const TChunkId& chunkId,
    NErasure::ICodec* codec,
    const std::vector<IChunkWriterPtr>& writers);

std::vector<IChunkWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    const TChunkId& chunkId,
    NErasure::ICodec* codec,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NApi::IClientPtr client,
    NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler(),
    IBlockCachePtr blockCache = GetNullBlockCache());

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

