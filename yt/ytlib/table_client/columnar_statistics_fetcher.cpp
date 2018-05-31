#include "columnar_statistics_fetcher.h"

#include <yt/ytlib/chunk_client/input_chunk.h>
#include <yt/ytlib/chunk_client/config.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TColumnarStatisticsFetcher::TColumnarStatisticsFetcher(
    TFetcherConfigPtr config,
    TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    IFetcherChunkScraperPtr chunkScraper,
    INativeClientPtr client,
    const TLogger& logger,
    std::vector<TString> columnNames)
    : TFetcherBase(
        std::move(config),
        std::move(nodeDirectory),
        std::move(invoker),
        std::move(chunkScraper),
        std::move(client),
        logger)
    , ColumnNames_(std::move(columnNames))
{ }

TFuture<void> TColumnarStatisticsFetcher::FetchFromNode(
    TNodeId nodeId,
    std::vector<int> chunkIndexes)
{
    return BIND(&TColumnarStatisticsFetcher::DoFetchFromNode, MakeStrong(this), nodeId, Passed(std::move(chunkIndexes)))
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<void> TColumnarStatisticsFetcher::DoFetchFromNode(TNodeId nodeId, std::vector<int> chunkIndexes)
{
    TDataNodeServiceProxy proxy(GetNodeChannel(nodeId));
    proxy.SetDefaultTimeout(Config_->NodeRpcTimeout);

    auto req = proxy.GetColumnarStatistics();
    NYT::ToProto(req->mutable_column_names(), ColumnNames_);
    for (int chunkIndex : chunkIndexes) {
        ToProto(req->add_chunk_ids(), Chunks_[chunkIndex]->ChunkId());
    }

    ToProto(req->mutable_workload_descriptor(), TWorkloadDescriptor(EWorkloadCategory::UserBatch));

    return req->Invoke().Apply(
        BIND(&TColumnarStatisticsFetcher::OnResponse, MakeStrong(this), nodeId, Passed(std::move(chunkIndexes)))
            .AsyncVia(Invoker_));
}

void TColumnarStatisticsFetcher::OnResponse(
    TNodeId nodeId,
    const std::vector<int>& chunkIndexes,
    const TDataNodeServiceProxy::TErrorOrRspGetColumnarStatisticsPtr& rspOrError)
{
    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Failed to get columnar statistics from node (Address: %v, NodeId: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            nodeId);
        OnNodeFailed(nodeId, chunkIndexes);
        return;
    }

    const auto& rsp = rspOrError.Value();

    for (int index = 0; index < chunkIndexes.size(); ++index) {
        int chunkIndex = chunkIndexes[index];
        const auto& statisticsResponse = rsp->statistics_responses(index);
        if (statisticsResponse.has_error()) {
            auto error = NYT::FromProto<TError>(statisticsResponse.error());
            if (error.FindMatching(NChunkClient::EErrorCode::ExtensionMissing)) {
                // This is an old chunk. Process it properly.
                ChunkStatistics_[chunkIndex].LegacyChunkDataWeight = Chunks_[chunkIndex]->GetDataWeight();
            } else {
                OnChunkFailed(nodeId, chunkIndex, error);
            }
        } else {
            ChunkStatistics_[chunkIndex].ColumnDataWeights = NYT::FromProto<std::vector<i64>>(statisticsResponse.data_weights());
            YCHECK(ChunkStatistics_[chunkIndex].ColumnDataWeights.size() == ColumnNames_.size());
        }
    }
}

TColumnarStatistics TColumnarStatisticsFetcher::GetColumnarStatistics() const
{
    auto statistics = GetEmptyStatistics();
    for (const auto& chunkStatistics : ChunkStatistics_) {
        statistics += chunkStatistics;
    }
    return statistics;
}

TFuture<void> TColumnarStatisticsFetcher::Fetch()
{
    ChunkStatistics_.resize(Chunks_.size(), GetEmptyStatistics());

    return TFetcherBase::Fetch();
}

TColumnarStatistics TColumnarStatisticsFetcher::GetEmptyStatistics() const
{
    return TColumnarStatistics{std::vector<i64>(ColumnNames_.size(), 0), 0};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
