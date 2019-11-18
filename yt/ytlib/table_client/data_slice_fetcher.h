#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/fetcher.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/actions/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): this class looks redundant.
// Use chunk slice fetcher instead and call CombineVerionedChunkSlices where needed.

//! Fetches data slices for a bunch of table chunks by requesting
//! them directly from data nodes.
class TDataSliceFetcher
    : public TRefCounted
{
public:
    TDataSliceFetcher(
        NChunkClient::TFetcherConfigPtr config,
        i64 chunkSliceSize,
        const TKeyColumns& keyColumns,
        bool sliceByKeys,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        NChunkClient::IFetcherChunkScraperPtr chunkScraper,
        NApi::NNative::IClientPtr client,
        NTableClient::TRowBufferPtr rowBuffer,
        const NLogging::TLogger& logger);

    void AddChunk(NChunkClient::TInputChunkPtr chunk);
    TFuture<void> Fetch();
    std::vector<NChunkClient::TInputDataSlicePtr> GetDataSlices();
    void SetCancelableContext(TCancelableContextPtr cancelableContext);

private:
    const IChunkSliceFetcherPtr ChunkSliceFetcher_;
};

DEFINE_REFCOUNTED_TYPE(TDataSliceFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
