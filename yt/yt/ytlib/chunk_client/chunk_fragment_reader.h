#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/library/erasure/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! A basic interface for reading chunk fragments from a suitable source.
struct IChunkFragmentReader
    : public virtual TRefCounted
{
    struct TChunkFragmentRequest
    {
        TChunkId ChunkId;
        NErasure::ECodec ErasureCodec = NErasure::ECodec::None;
        i64 Length = -1;
        int BlockIndex = -1;
        i64 BlockOffset = -1;
        // Only needed for erasure chunks.
        std::optional<i64> BlockSize;
    };

    struct TReadFragmentsResponse
    {
        std::vector<TSharedRef> Fragments;

        // NB: These statistics are used within hunk decoder.
        int BackendReadRequestCount = 0;
        int BackendHedgingReadRequestCount = 0;
        int BackendProbingRequestCount = 0;
    };

    //! Asynchronously reads a given set of chunk fragments.
    virtual TFuture<TReadFragmentsResponse> ReadFragments(
        TClientChunkReadOptions options,
        std::vector<TChunkFragmentRequest> requests) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkFragmentReader)

////////////////////////////////////////////////////////////////////////////////

using TThrottlerProvider = std::function<const NConcurrency::IThroughputThrottlerPtr& (EWorkloadCategory /*category*/)>;

IChunkFragmentReaderPtr CreateChunkFragmentReader(
    TChunkFragmentReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::INodeStatusDirectoryPtr nodeStatusDirectory,
    const NProfiling::TProfiler& profiler,
    NConcurrency::IThroughputThrottlerPtr mediumThrottler,
    TThrottlerProvider throttlerProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
