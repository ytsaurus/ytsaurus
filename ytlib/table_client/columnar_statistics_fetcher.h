#pragma once

#include "private.h"
#include "column_filter_dictionary.h"

#include <yt/client/table_client/columnar_statistics.h>

#include <yt/ytlib/chunk_client/fetcher.h>

#include <yt/ytlib/chunk_client/data_node_service_proxy.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Fetches columnary statistics for a bunch of table chunks by requesting
//! them directly from data nodes.
class TColumnarStatisticsFetcher
    : public NChunkClient::TFetcherBase
{
public:
    TColumnarStatisticsFetcher(
        NChunkClient::TFetcherConfigPtr config,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        NChunkClient::IFetcherChunkScraperPtr chunkScraper,
        NApi::NNative::IClientPtr client,
        const NLogging::TLogger& logger,
        bool storeChunkStatistics = true);

    //! Return per-chunk columnar statistics.
    const std::vector<TColumnarStatistics>& GetChunkStatistics() const;
    //! Set column selectivity factor for all processed chunks according to the fetched columnar statistics.
    void ApplyColumnSelectivityFactors() const;

    TFuture<void> Fetch() override;

    void AddChunk(NChunkClient::TInputChunkPtr chunk, std::vector<TString> columnNames);

private:
    bool StoreChunkStatistics_;

    std::vector<TColumnarStatistics> ChunkStatistics_;
    std::vector<TLightweightColumnarStatistics> LightweightChunkStatistics_;

    std::vector<int> ChunkColumnFilterIds_;

    TColumnFilterDictionary ColumnFilterDictionary_;

    // Columnar statistics fetcher does not support adding pure chunks as each chunk should be provided with
    // a column list to fetch statistics for.
    using TFetcherBase::AddChunk;

    virtual TFuture<void> FetchFromNode(NNodeTrackerClient::TNodeId nodeId, std::vector<int> chunkIndexes) override;

    TFuture<void> DoFetchFromNode(NNodeTrackerClient::TNodeId nodeId, std::vector<int> chunkIndexes);

    const std::vector<TString>& GetColumnNames(int chunkIndex) const;

    void OnResponse(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspGetColumnarStatisticsPtr& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TColumnarStatisticsFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
