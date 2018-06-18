#include "columnar_statistics_fetcher.h"

#include "name_table.h"

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

    // Use nametable to replace all column names with their ids across the whole rpc request message.
    TNameTablePtr nameTable = New<TNameTable>();

    auto req = proxy.GetColumnarStatistics();
    ToProto(req->mutable_workload_descriptor(), TWorkloadDescriptor(EWorkloadCategory::UserBatch));

    for (int chunkIndex : chunkIndexes) {
        auto* subrequest = req->add_subrequests();
        for (const auto& columnName : ChunkColumnNames_[chunkIndex]) {
            auto columnId = nameTable->GetIdOrRegisterName(columnName);
            subrequest->add_column_ids(columnId);
        }
        ToProto(subrequest->mutable_chunk_id(), Chunks_[chunkIndex]->ChunkId());
    }

    ToProto(req->mutable_name_table(), nameTable);

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
            if (error.FindMatching(NChunkClient::EErrorCode::MissingExtension)) {
                // This is an old chunk. Process it somehow.
                ChunkStatistics_[chunkIndex] = TColumnarStatistics::MakeEmpty(ChunkColumnNames_[chunkIndex].size());
                ChunkStatistics_[chunkIndex].LegacyChunkDataWeight = Chunks_[chunkIndex]->GetDataWeight();
            } else {
                OnChunkFailed(nodeId, chunkIndex, error);
            }
        } else {
            ChunkStatistics_[chunkIndex].ColumnDataWeights = NYT::FromProto<std::vector<i64>>(subresponse.data_weights());
            if (subresponse.has_timestamp_total_weight()) {
                ChunkStatistics_[chunkIndex].TimestampTotalWeight = subresponse.timestamp_total_weight();
            }
            YCHECK(ChunkStatistics_[chunkIndex].ColumnDataWeights.size() == ChunkColumnNames_[chunkIndex].size());
        }
    }
}

const std::vector<TColumnarStatistics>& TColumnarStatisticsFetcher::GetChunkStatistics() const
{
    return ChunkStatistics_;
}

void TColumnarStatisticsFetcher::ApplyColumnSelectivityFactors() const
{
    for (int index = 0; index < Chunks_.size(); ++index) {
        const auto& chunk = Chunks_[index];
        const auto& statistics = ChunkStatistics_[index];
        if (statistics.LegacyChunkDataWeight == 0) {
            // We have columnar statistics, so we can adjust input chunk data weight by setting column selectivity factor.
            i64 totalColumnDataWeight = 0;
            if (chunk->GetTableChunkFormat() == ETableChunkFormat::SchemalessHorizontal ||
                chunk->GetTableChunkFormat() == ETableChunkFormat::UnversionedColumnar)
            {
                // NB: we should add total row count to the column data weights because otherwise for the empty column list
                // there will be zero data weight which does not allow unordered pool to work properly.
                totalColumnDataWeight += chunk->GetTotalRowCount();
            } else if (
                chunk->GetTableChunkFormat() == ETableChunkFormat::VersionedSimple ||
                chunk->GetTableChunkFormat() == ETableChunkFormat::VersionedColumnar)
            {
                // Default value of sizeof(TTimestamp) = 8 is used for versioned chunks that were written before
                // we started to save the timestamp statistics to columnar statistics extension.
                totalColumnDataWeight += statistics.TimestampTotalWeight.Get(sizeof(TTimestamp));
            } else {
                THROW_ERROR_EXCEPTION("Cannot apply column selectivity factor for chunk of an old table format")
                    << TErrorAttribute("chunk_id", chunk->ChunkId())
                    << TErrorAttribute("table_chunk_format", chunk->GetTableChunkFormat());
            }
            for (i64 dataWeight : statistics.ColumnDataWeights) {
                totalColumnDataWeight += dataWeight;
            }
            auto totalDataWeight = chunk->GetTotalDataWeight();
            chunk->SetColumnSelectivityFactor(std::min(static_cast<double>(totalColumnDataWeight) / totalDataWeight, 1.0));
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
