#include "chunk_slice_size_fetcher.h"

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/rpc/channel.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NObjectClient;
using namespace NTabletClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TChunkSliceSizeFetcher::TChunkSliceSizeFetcher(
    NChunkClient::TFetcherConfigPtr config,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    NChunkClient::IFetcherChunkScraperPtr chunkScraper,
    NApi::NNative::IClientPtr client,
    const NLogging::TLogger& logger)
    : TFetcherBase(
        std::move(config),
        std::move(nodeDirectory),
        std::move(invoker),
        std::move(chunkScraper),
        std::move(client),
        logger)
{ }

void TChunkSliceSizeFetcher::AddChunk(TInputChunkPtr chunk, std::vector<TColumnStableName> columnStableNames)
{
    ChunkColumnFilterIds_[Chunks_.size()] = ColumnFilterDictionary_.GetIdOrRegisterAdmittedColumns(std::move(columnStableNames));
    TFetcherBase::AddChunk(std::move(chunk));
}

TFuture<void> TChunkSliceSizeFetcher::FetchFromNode(
    NNodeTrackerClient::TNodeId nodeId,
    std::vector<int> chunkIndexes)
{
    return BIND(&TChunkSliceSizeFetcher::DoFetchFromNode, MakeStrong(this), nodeId, Passed(std::move(chunkIndexes)))
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TChunkSliceSizeFetcher::DoFetchFromNode(
    NNodeTrackerClient::TNodeId nodeId,
    std::vector<int> chunkIndexes)
{
    TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.GetChunkSliceDataWeights();
    SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserInteractive));
    req->SetRequestHeavy(true);
    req->SetResponseHeavy(true);
    req->SetMultiplexingBand(EMultiplexingBand::Heavy);

    // We use this name table to replace all column names with their ids across the whole rpc request message.
    auto nameTable = New<TNameTable>();

    for (int index : chunkIndexes) {
        const auto& chunk = Chunks_[index];
        auto chunkId = chunk->EncodeReplica(nodeId);

        auto* weightedChunkRequest = req->add_chunk_requests();
        ToProto(weightedChunkRequest->mutable_chunk_id(), chunkId);
        if (chunk->LowerLimit()) {
            ToProto(weightedChunkRequest->mutable_lower_limit(), *chunk->LowerLimit());
        }
        if (chunk->UpperLimit()) {
            ToProto(weightedChunkRequest->mutable_upper_limit(), *chunk->UpperLimit());
        }

        if (auto columnStableNames = GetColumnStableNames(index)) {
            for (const auto& columnName : *columnStableNames) {
                auto columnId = nameTable->GetIdOrRegisterName(columnName.Underlying());
                weightedChunkRequest->mutable_column_filter()->add_indexes(columnId);
            }
        }
    }

    ToProto(req->mutable_name_table(), nameTable);

    if (req->chunk_requests_size() == 0) {
        return VoidFuture;
    }

    return req->Invoke().Apply(
        BIND(&TChunkSliceSizeFetcher::OnResponse, MakeStrong(this), nodeId, Passed(std::move(chunkIndexes)))
            .AsyncVia(Invoker_));
}

void TChunkSliceSizeFetcher::ProcessDynamicStore(int /*chunkIndex*/)
{
    YT_LOG_WARNING("Unable to get chunk size for dynamic store");
}

void TChunkSliceSizeFetcher::OnResponse(
    NNodeTrackerClient::TNodeId nodeId,
    std::vector<int> requestedChunkIndexes,
    const TDataNodeServiceProxy::TErrorOrRspGetChunkSliceDataWeightsPtr& rspOrError)
{
    YT_LOG_DEBUG("Node response received (NodeId: %v, ChunkIndexes: %v)",
        nodeId,
        requestedChunkIndexes);

    if (!rspOrError.IsOK()) {
        YT_LOG_INFO("Failed to get chunk slice size from node (Address: %v, NodeId: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            nodeId);

        OnNodeFailed(nodeId, requestedChunkIndexes);
        return;
    }

    const auto& responses = rspOrError.Value();

    YT_VERIFY(responses->chunk_responses_size() == std::ssize(requestedChunkIndexes));

    for (int index = 0; index < std::ssize(requestedChunkIndexes); ++index) {
        int chunkIndex = requestedChunkIndexes[index];
        const auto& rps = responses->chunk_responses(index);

        if (rps.has_error()) {
            auto error = FromProto<TError>(rps.error());
            OnChunkFailed(nodeId, requestedChunkIndexes[index], error);
            continue;
        }

        YT_VERIFY(chunkIndex < std::ssize(WeightedChunks_));
        WeightedChunks_[chunkIndex] = New<TWeightedInputChunk>(Chunks_[chunkIndex], rps.data_weight());
    }
}

void TChunkSliceSizeFetcher::OnFetchingStarted()
{
    WeightedChunks_.resize(Chunks_.size());
}

const std::optional<std::vector<TColumnStableName>> TChunkSliceSizeFetcher::GetColumnStableNames(int chunkIndex) const
{
    if (auto columnFilterIdIt = ChunkColumnFilterIds_.find(chunkIndex); columnFilterIdIt != ChunkColumnFilterIds_.end()) {
        return ColumnFilterDictionary_.GetAdmittedColumns(columnFilterIdIt->second);
    }

    return {};
}

void TChunkSliceSizeFetcher::ApplyBlockSelectivityFactors() const
{
    for (const auto& weightedChunk : WeightedChunks_) {
        auto totalDataWeight = weightedChunk->GetInputChunk()->GetTotalDataWeight();
        auto blockSelectivityFactor = std::min(static_cast<double>(weightedChunk->GetDataWeight()) / totalDataWeight, 1.0);
        weightedChunk->GetInputChunk()->SetBlockSelectivityFactor(blockSelectivityFactor);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
