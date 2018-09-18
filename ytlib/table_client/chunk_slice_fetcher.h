#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/fetcher.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IChunkSliceFetcher
    : public virtual NChunkClient::IFetcher
{
    virtual std::vector<NChunkClient::TInputChunkSlicePtr> GetChunkSlices() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkSliceFetcher);

////////////////////////////////////////////////////////////////////////////////

IChunkSliceFetcherPtr CreateChunkSliceFetcher(
    NChunkClient::TFetcherConfigPtr config,
    i64 chunkSliceSize,
    const NTableClient::TKeyColumns& keyColumns,
    bool sliceByKeys,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    NChunkClient::IFetcherChunkScraperPtr chunkScraper,
    NApi::NNative::IClientPtr client,
    NTableClient::TRowBufferPtr rowBuffer,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
