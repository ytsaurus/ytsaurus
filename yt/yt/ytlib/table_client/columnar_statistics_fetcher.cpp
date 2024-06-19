#include "columnar_statistics_fetcher.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TColumnarStatisticsFetcher::TColumnarStatisticsFetcher(
    IInvokerPtr invoker,
    NNative::IClientPtr client,
    TOptions options)
    : TFetcherBase(
        options.Config,
        options.NodeDirectory,
        invoker,
        options.ChunkScraper,
        client,
        options.Logger)
    , Options_(std::move(options))
    , ColumnFilterDictionary_(/*sortColumns=*/false)
{ }

void TColumnarStatisticsFetcher::ProcessDynamicStore(int chunkIndex)
{
    auto statistics = TColumnarStatistics::MakeEmpty(GetColumnStableNames(chunkIndex).size());
    if (Options_.StoreChunkStatistics) {
        ChunkStatistics_[chunkIndex] = std::move(statistics);
    } else {
        LightweightChunkStatistics_[chunkIndex] = statistics.MakeLightweightStatistics();
    }
}

THashSet<int> TColumnarStatisticsFetcher::GetChunkIndexesToFetch()
{
    THashSet<int> indexes;
    for (const auto& [chunkIndex, chunk] : Enumerate(Chunks_)) {
        bool needFetch = NeedFetchFromNode_[chunk];
        if (chunk->IsDynamicStore()) {
            ProcessDynamicStore(chunkIndex);
        } else if (needFetch) {
            indexes.insert(chunkIndex);
        }
    }

    return indexes;
}

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

    // Use name table to replace all column names with their ids across the whole rpc request message.
    TNameTablePtr nameTable = New<TNameTable>();

    auto req = proxy.GetColumnarStatistics();
    SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserBatch));
    req->set_enable_early_finish(Options_.EnableEarlyFinish);

    for (int chunkIndex : chunkIndexes) {
        auto* subrequest = req->add_subrequests();
        for (const auto& columnName : GetColumnStableNames(chunkIndex)) {
            auto columnId = nameTable->GetIdOrRegisterName(columnName.Underlying());
            subrequest->add_column_ids(columnId);
        }

        auto chunkId = EncodeChunkId(Chunks_[chunkIndex], nodeId);
        ToProto(subrequest->mutable_chunk_id(), chunkId);
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
        YT_LOG_INFO(rspOrError, "Failed to get columnar statistics from node (Address: %v, NodeId: %v)",
            NodeDirectory_->GetDescriptor(nodeId).GetDefaultAddress(),
            nodeId);
        OnNodeFailed(nodeId, chunkIndexes);
        return;
    }

    const auto& rsp = rspOrError.Value();

    for (int index = 0; index < std::ssize(chunkIndexes); ++index) {
        int chunkIndex = chunkIndexes[index];
        const auto& subresponse = rsp->subresponses(index);
        TColumnarStatistics statistics;
        if (subresponse.has_error()) {
            auto error = NYT::FromProto<TError>(subresponse.error());
            if (error.FindMatching(NChunkClient::EErrorCode::MissingExtension)) {
                // This is an old chunk. Process it somehow.
                statistics = TColumnarStatistics::MakeLegacy(
                    GetColumnStableNames(chunkIndex).size(),
                    Chunks_[chunkIndex]->GetDataWeight(),
                    Chunks_[chunkIndex]->GetTotalRowCount());
            } else {
                OnChunkFailed(nodeId, chunkIndex, error);
            }
        } else {
            if (subresponse.has_columnar_statistics()) {
                FromProto(&statistics, subresponse.columnar_statistics(), Chunks_[chunkIndex]->GetTotalRowCount());
            } else {
                // COMPAT(denvid): Delete this with deleting deprecated fields from TRspGetColumnarStatistics.TSubresponse.
                statistics.ColumnDataWeights = NYT::FromProto<std::vector<i64>>(subresponse.column_data_weights());
                YT_VERIFY(statistics.ColumnDataWeights.size() == GetColumnStableNames(chunkIndex).size());
                if (subresponse.has_timestamp_total_weight()) {
                    statistics.TimestampTotalWeight = subresponse.timestamp_total_weight();
                }
                statistics.ChunkRowCount = Chunks_[chunkIndex]->GetTotalRowCount();
            }
        }
        if (Options_.StoreChunkStatistics) {
            ChunkStatistics_[chunkIndex] = statistics;
        } else {
            LightweightChunkStatistics_[chunkIndex] = statistics.MakeLightweightStatistics();
        }
        if (Options_.AggregatePerTableStatistics) {
            auto tableIndex = Chunks_[chunkIndex]->GetTableIndex();
            YT_VERIFY(tableIndex < std::ssize(TableStatistics_));
            TableStatistics_[tableIndex] += statistics;
        }
    }
}

const std::vector<TColumnarStatistics>& TColumnarStatisticsFetcher::GetChunkStatistics() const
{
    YT_VERIFY(Options_.StoreChunkStatistics);

    return ChunkStatistics_;
}

const std::vector<TColumnarStatistics>& TColumnarStatisticsFetcher::GetTableStatistics() const
{
    YT_VERIFY(Options_.AggregatePerTableStatistics);

    return TableStatistics_;
}

void TColumnarStatisticsFetcher::ApplyColumnSelectivityFactors() const
{
    for (int index = 0; index < std::ssize(Chunks_); ++index) {
        const auto& chunk = Chunks_[index];
        TLightweightColumnarStatistics statistics;
        if (Options_.StoreChunkStatistics) {
            statistics = ChunkStatistics_[index].MakeLightweightStatistics();
        } else {
            statistics = LightweightChunkStatistics_[index];
        }
        if (statistics.LegacyChunkDataWeight == 0) {
            // We have columnar statistics, so we can adjust input chunk data weight by setting column selectivity factor.
            i64 totalColumnDataWeight = 0;
            switch (chunk->GetChunkFormat()) {
                case EChunkFormat::TableUnversionedSchemalessHorizontal:
                case EChunkFormat::TableUnversionedColumnar:
                    // NB: we should add total row count to the column data weights because otherwise for the empty column list
                    // there will be zero data weight which does not allow unordered pool to work properly.
                    totalColumnDataWeight += chunk->GetTotalRowCount();
                    break;
                case EChunkFormat::TableVersionedSimple:
                case EChunkFormat::TableVersionedColumnar:
                case EChunkFormat::TableVersionedIndexed:
                case EChunkFormat::TableVersionedSlim:
                    // Default value of sizeof(TTimestamp) = 8 is used for versioned chunks that were written before
                    // we started to save the timestamp statistics to columnar statistics extension.
                    totalColumnDataWeight += statistics.TimestampTotalWeight.value_or(sizeof(TTimestamp));
                    break;
                default:
                    THROW_ERROR_EXCEPTION("Cannot apply column selectivity factor for chunk of an unexpected format")
                        << TErrorAttribute("chunk_id", chunk->GetChunkId())
                        << TErrorAttribute("chunk_format", chunk->GetChunkFormat());
            }
            totalColumnDataWeight += statistics.ColumnDataWeightsSum;
            auto totalDataWeight = chunk->GetTotalDataWeight();
            chunk->SetColumnSelectivityFactor(std::min(static_cast<double>(totalColumnDataWeight) / totalDataWeight, 1.0));
        }
    }
}

TFuture<void> TColumnarStatisticsFetcher::Fetch()
{
    if (Options_.Mode == EColumnarStatisticsFetcherMode::FromMaster) {
        OnFetchingStarted();
        return VoidFuture;
    }

    return TFetcherBase::Fetch();
}

void TColumnarStatisticsFetcher::OnFetchingStarted()
{
    if (Options_.StoreChunkStatistics) {
        ChunkStatistics_.resize(Chunks_.size());
    } else {
        LightweightChunkStatistics_.resize(Chunks_.size());
    }
}

void TColumnarStatisticsFetcher::AddChunk(TInputChunkPtr chunk, std::vector<TColumnStableName> columnStableNames)
{
    if (!NeedFetchFromNode_.emplace(chunk, true).second) {
        // We already know about this chunk.
        return;
    }

    if (Options_.AggregatePerTableStatistics) {
        if (chunk->GetTableIndex() >= std::ssize(TableStatistics_)) {
            TableStatistics_.resize(chunk->GetTableIndex() + 1);
        }
    }

    if (columnStableNames.empty()) {
        // Do not fetch anything. The less rpc requests, the better.
        Chunks_.emplace_back(std::move(chunk));
        NeedFetchFromNode_[chunk] = false;
    } else {
        const NProto::THeavyColumnStatisticsExt* heavyColumnStatistics = nullptr;
        if (Options_.Mode == EColumnarStatisticsFetcherMode::FromMaster ||
            Options_.Mode == EColumnarStatisticsFetcherMode::Fallback)
        {
            heavyColumnStatistics = chunk->HeavyColumnarStatisticsExt().get();
        }
        if (heavyColumnStatistics || Options_.Mode == EColumnarStatisticsFetcherMode::FromMaster) {
            TColumnarStatistics columnarStatistics;
            if (heavyColumnStatistics) {
                columnarStatistics = GetColumnarStatistics(*heavyColumnStatistics, columnStableNames, chunk->GetTotalRowCount());
            } else {
                YT_VERIFY(Options_.Mode == EColumnarStatisticsFetcherMode::FromMaster);
                columnarStatistics = TColumnarStatistics::MakeLegacy(columnStableNames.size(), chunk->GetDataWeight(), chunk->GetTotalRowCount());
            }
            Chunks_.emplace_back(chunk);
            NeedFetchFromNode_[chunk] = false;
            if (Options_.StoreChunkStatistics) {
                ChunkStatistics_.resize(Chunks_.size());
                ChunkStatistics_.back() = columnarStatistics;
            } else {
                LightweightChunkStatistics_.resize(Chunks_.size());
                LightweightChunkStatistics_.back() = columnarStatistics.MakeLightweightStatistics();
            }
            if (Options_.AggregatePerTableStatistics) {
                YT_VERIFY(chunk->GetTableIndex() < std::ssize(TableStatistics_));
                TableStatistics_[chunk->GetTableIndex()] += columnarStatistics;
            }
        } else {
            TFetcherBase::AddChunk(std::move(chunk));
        }
    }

    ChunkColumnFilterIds_.emplace_back(ColumnFilterDictionary_.GetIdOrRegisterAdmittedColumns(columnStableNames));
}

const std::vector<TColumnStableName>& TColumnarStatisticsFetcher::GetColumnStableNames(int chunkIndex) const
{
    return ColumnFilterDictionary_.GetAdmittedColumns(ChunkColumnFilterIds_[chunkIndex]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
