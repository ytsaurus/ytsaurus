#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/fetcher.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/phoenix.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkSliceFetcher
    : public virtual NChunkClient::IFetcher
{
    virtual std::vector<NChunkClient::TInputChunkSlicePtr> GetChunkSlices() = 0;

    virtual void AddChunkForSlicing(NChunkClient::TInputChunkPtr chunk, int keyColumnCount, bool sliceByKeys) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkSliceFetcher);

////////////////////////////////////////////////////////////////////////////////

IChunkSliceFetcherPtr CreateChunkSliceFetcher(
    NChunkClient::TFetcherConfigPtr config,
    i64 chunkSliceSize,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    NChunkClient::IFetcherChunkScraperPtr chunkScraper,
    NApi::NNative::IClientPtr client,
    NTableClient::TRowBufferPtr rowBuffer,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
