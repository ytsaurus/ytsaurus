#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <vector>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

struct TDistributedChunkSessionSealSummary
{
    NChunkClient::TChunkId ChunkId;
    i64 RecordCount = 0;
    i64 CompressedDataSize = 0;
};

//! Fetches master-side seal summaries for distributed-session chunks.
/*!
 * Each call fetches the supplied chunks once. Polling policy belongs to the caller.
 * Only chunks observed as sealed are returned.
 * Chunk ids must be unique.
 * All chunks must belong to the same master cell.
 */
TFuture<std::vector<TDistributedChunkSessionSealSummary>> FetchDistributedChunkSessionSealSummaries(
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker,
    NChunkClient::TThrottlerManagerPtr throttlerManager,
    std::vector<NChunkClient::TChunkId> chunkIds,
    NLogging::TLogger logger = DistributedChunkSessionLogger());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
