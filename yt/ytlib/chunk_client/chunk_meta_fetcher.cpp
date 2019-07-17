#include "chunk_meta_fetcher.h"

#include "config.h"
#include "input_chunk.h"
#include "data_node_service_proxy.h"

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/core/rpc/public.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TChunkMetaFetcher::TChunkMetaFetcher(
    TFetcherConfigPtr config,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    IFetcherChunkScraperPtr chunkScraper,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& logger,
    TWorkloadDescriptor workloadDescriptor,
    std::function<void(TReqGetChunkMeta&)> initializeRequest)
    : TFetcherBase(
        std::move(config),
        std::move(nodeDirectory),
        std::move(invoker),
        std::move(chunkScraper),
        std::move(client),
        logger)
    , WorkloadDescriptor_(std::move(workloadDescriptor))
    , InitializeRequest_(std::move(initializeRequest))
{ }

TFuture<void> TChunkMetaFetcher::FetchFromNode(TNodeId nodeId, std::vector<int> chunkIndexes)
{
    TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    // TODO(max42): optimize it.

    std::vector<TFuture<TDataNodeServiceProxy::TRspGetChunkMetaPtr>> asyncResults;

    for (int index : chunkIndexes) {
        const auto& chunk = Chunks_[index];
        auto req = proxy.GetChunkMeta();
        // TODO(babenko): consider using light band instead when all metas become thin
        // CC: psushin@
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        req->set_enable_throttling(true);
        ToProto(req->mutable_chunk_id(), chunk->ChunkId());
        ToProto(req->mutable_workload_descriptor(), WorkloadDescriptor_);
        InitializeRequest_(*req);

        asyncResults.emplace_back(req->Invoke());
    }

    return Combine(std::move(asyncResults))
        .Apply(BIND(&TChunkMetaFetcher::OnResponse, MakeStrong(this), nodeId, chunkIndexes)
            .AsyncVia(Invoker_));
}

void TChunkMetaFetcher::OnResponse(
    NYT::NNodeTrackerClient::TNodeId nodeId,
    std::vector<int> requestedChunkIndexes,
    const TErrorOr<std::vector<TDataNodeServiceProxy::TRspGetChunkMetaPtr>>& rspOrError)
{
    if (!rspOrError.IsOK()) {
        YT_LOG_INFO("Failed to get chunk slices meta from node (Address: %v, NodeId: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            nodeId);

        OnNodeFailed(nodeId, requestedChunkIndexes);
        return;
    }

    const auto& responses = rspOrError.Value();

    for (int index = 0; index < requestedChunkIndexes.size(); ++index) {
        int chunkIndex = requestedChunkIndexes[index];
        auto& rsp = responses[index];
        if (ChunkMetas_.size() <= chunkIndex) {
            ChunkMetas_.resize(chunkIndex);
        }
        YT_VERIFY(!rsp->net_throttling());
        ChunkMetas_[chunkIndex] = New<TRefCountedChunkMeta>(std::move(rsp->chunk_meta()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
