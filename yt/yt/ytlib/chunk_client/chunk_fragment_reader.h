#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/misc/workload.h>

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
        i64 Length;
        int BlockIndex;
        i64 BlockOffset;
        i64 BlockSize;
    };

    struct TReadFragmentsResponse
    {
        std::vector<TSharedRef> Fragments;

        i64 DataWeight = 0;
        int ChunkCount = 0;

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

IChunkFragmentReaderPtr CreateChunkFragmentReader(
    TChunkFragmentReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::INodeStatusDirectoryPtr nodeStatusDirectory,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
