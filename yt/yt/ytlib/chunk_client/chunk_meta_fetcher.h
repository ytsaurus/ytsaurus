#pragma once

#include "fetcher.h"

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/client/misc/workload.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkMetaFetcher
    : public TFetcherBase
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TRefCountedChunkMetaPtr>, ChunkMetas);

public:
    TChunkMetaFetcher(
        TFetcherConfigPtr config,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        IFetcherChunkScraperPtr chunkScraper,
        NApi::NNative::IClientPtr client,
        const NLogging::TLogger& logger,
        TWorkloadDescriptor workloadDescriptor,
        std::function<void(NChunkClient::NProto::TReqGetChunkMeta&)> initializeRequest);

private:
    TWorkloadDescriptor WorkloadDescriptor_;

    std::function<void(NChunkClient::NProto::TReqGetChunkMeta&)> InitializeRequest_;

    void ProcessDynamicStore(int chunkIndex) override;

    TFuture<void> FetchFromNode(NNodeTrackerClient::TNodeId nodeId, std::vector<int> chunkIndexes) override;

    void OnFetchingStarted() override;

    void OnResponse(
        NNodeTrackerClient::TNodeId,
        std::vector<int> chunkIndexes,
        const TErrorOr<std::vector<TDataNodeServiceProxy::TRspGetChunkMetaPtr>>& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TChunkMetaFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
