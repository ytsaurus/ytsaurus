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
    ToProto(req->mutable_workload_descriptor(), TWorkloadDescriptor(EWorkloadCategory::UserBatch));

    for (int chunkIndex : chunkIndexes) {
        auto* subrequest = req->add_subrequests();
        NYT::ToProto(subrequest->mutable_column_names(), ChunkColumnNames_[chunkIndex]);
        ToProto(subrequest->mutable_chunk_id(), Chunks_[chunkIndex]->ChunkId());
    }

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
        const auto& subresponse = rsp->subresponses(index);
        if (subresponse.has_error()) {
            auto error = NYT::FromProto<TError>(subresponse.error());
            if (error.FindMatching(NChunkClient::EErrorCode::ExtensionMissing)) {
                // This is an old chunk. Process it somehow.
                ChunkStatistics_[chunkIndex].LegacyChunkDataWeight = Chunks_[chunkIndex]->GetDataWeight();
            } else {
                OnChunkFailed(nodeId, chunkIndex, error);
            }
        } else {
            ChunkStatistics_[chunkIndex].ColumnDataWeights = NYT::FromProto<std::vector<i64>>(subresponse.data_weights());
            YCHECK(ChunkStatistics_[chunkIndex].ColumnDataWeights.size() == ChunkColumnNames_[chunkIndex].size());
        }
    }
}

const std::vector<TColumnarStatistics>& TColumnarStatisticsFetcher::GetChunkStatistics() const
{
    return ChunkStatistics_;
}

void TColumnarStatisticsFetcher::SetColumnSelectivityFactors() const
{
    for (int index = 0; index < Chunks_.size(); ++index) {
        if (ChunkStatistics_[index].LegacyChunkDataWeight == 0) {
            // We have columnar statistics, so we can adjust input chunk data weight by setting column selectivity factor.
            // NB: we should add total row count to the column data weights because otherwise for the empty column list
            // there will be zero data weight which does not allow unordered pool to work properly.
            i64 totalColumnDataWeight = Chunks_[index]->GetTotalRowCount();
            for (i64 dataWeight : ChunkStatistics_[index].ColumnDataWeights) {
                totalColumnDataWeight += dataWeight;
            }
            auto totalDataWeight = Chunks_[index]->GetTotalDataWeight();
            Chunks_[index]->SetColumnSelectivityFactor(static_cast<double>(totalColumnDataWeight) / totalDataWeight);
        }
    }
}

TFuture<void> TColumnarStatisticsFetcher::Fetch()
{
    ChunkStatistics_.resize(Chunks_.size());

    return TFetcherBase::Fetch();
}

void TColumnarStatisticsFetcher::AddChunk(TInputChunkPtr chunk, std::vector<TString> columnNames)
{
    if (columnNames.empty()) {
        // Do not fetch anything. The less rpc requests, the better.
        Chunks_.emplace_back(std::move(chunk));
    } else {
        TFetcherBase::AddChunk(std::move(chunk));
    }
    ChunkColumnNames_.emplace_back(std::move(columnNames));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
