#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/fetcher.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkSliceSizeFetcher
    : public NChunkClient::TFetcherBase
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<NChunkClient::TWeightedInputChunkPtr>, WeightedChunks);

public:
    TChunkSliceSizeFetcher(
        NChunkClient::TFetcherConfigPtr config,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        NChunkClient::IFetcherChunkScraperPtr chunkScraper,
        NApi::NNative::IClientPtr client,
        const NLogging::TLogger& logger);

private:
    void ProcessDynamicStore(int chunkIndex) override;

    TFuture<void> FetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes) override;

    TFuture<void> DoFetchFromNode(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> chunkIndexes);

    void OnResponse(
        NNodeTrackerClient::TNodeId nodeId,
        std::vector<int> requestedChunkIndexes,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspGetChunkSliceDataWeightsPtr& rspOrError);

    void OnFetchingStarted() override;
};

DEFINE_REFCOUNTED_TYPE(TChunkSliceSizeFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
