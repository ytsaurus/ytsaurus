#pragma once

#include "chunk_reader_statistics.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! A basic interface for reading chunk fragments from a suitable source.
struct IChunkFragmentReader
    : public virtual TRefCounted
{
    struct TChunkFragmentRequest
    {
        TChunkId ChunkId;
        i64 Offset;
        i64 Size;
    };

    //! Asynchronously reads a given set of chunk fragments.
    virtual TFuture<std::vector<TSharedRef>> ReadFragments(
        const TClientChunkReadOptions& options,
        std::vector<TChunkFragmentRequest> requests) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkFragmentReader)

IChunkFragmentReaderPtr CreateChunkFragmentReader(
    TChunkFragmentReaderConfigPtr config,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::INodeStatusDirectoryPtr nodeStatusDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
