#pragma once

#include "private.h"
#include "column_filter_dictionary.h"

#include <yt/yt/client/table_client/columnar_statistics.h>

#include <yt/yt/ytlib/chunk_client/fetcher.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnarStatisticsFetcherOptions
{
    NChunkClient::TFetcherConfigPtr Config;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
    NChunkClient::IFetcherChunkScraperPtr ChunkScraper;
    EColumnarStatisticsFetcherMode Mode = EColumnarStatisticsFetcherMode::Fallback;
    bool StoreChunkStatistics = false;
    bool AggregatePerTableStatistics = false;
    bool EnableEarlyFinish = false;
    NLogging::TLogger Logger = TableClientLogger;
};

//! Fetches columnary statistics for a bunch of table chunks by requesting
//! them directly from data nodes.
class TColumnarStatisticsFetcher
    : public NChunkClient::TFetcherBase
{
public:
    using TOptions = TColumnarStatisticsFetcherOptions;

    TColumnarStatisticsFetcher(
        IInvokerPtr invoker,
        NApi::NNative::IClientPtr client,
        TOptions options = TOptions());

    //! Return per-chunk columnar statistics.
    const std::vector<TColumnarStatistics>& GetChunkStatistics() const;

    //! Return per-table columnar statistics.
    const std::vector<TColumnarStatistics>& GetTableStatistics() const;

    //! Set column selectivity factor for all processed chunks according to the fetched columnar statistics.
    void ApplyColumnSelectivityFactors() const;

    TFuture<void> Fetch() override;

    void AddChunk(NChunkClient::TInputChunkPtr chunk, std::vector<TColumnStableName> columnStableNames);

private:
    TOptions Options_;

    //! This map serves two purposes:
    //! 1. We do not want to apply selectivity factor twice for the same chunk object as well as fetching
    //!    same statistics multiple times.
    //! 2. We do not want to fetch data from nodes, when the user did not ask for it
    //!    (see TColumnarStatisticsFetcherOptions::Mode).
    THashMap<NChunkClient::TInputChunkPtr, bool> NeedFetchFromNode_;

    std::vector<TColumnarStatistics> ChunkStatistics_;
    std::vector<TLightweightColumnarStatistics> LightweightChunkStatistics_;

    std::vector<TColumnarStatistics> TableStatistics_;

    std::vector<int> ChunkColumnFilterIds_;

    TColumnStableNameFilterDictionary ColumnFilterDictionary_;

    // Columnar statistics fetcher does not support adding pure chunks as each chunk should be provided with
    // a column list to fetch statistics for.
    using TFetcherBase::AddChunk;

    void ProcessDynamicStore(int chunkIndex) override;

    THashSet<int> GetChunkIndexesToFetch() override;

    TFuture<void> FetchFromNode(NNodeTrackerClient::TNodeId nodeId, std::vector<int> chunkIndexes) override;

    void OnFetchingStarted() override;

    TFuture<void> DoFetchFromNode(NNodeTrackerClient::TNodeId nodeId, std::vector<int> chunkIndexes);

    const std::vector<TColumnStableName>& GetColumnStableNames(int chunkIndex) const;

    void OnResponse(
        NNodeTrackerClient::TNodeId nodeId,
        const std::vector<int>& chunkIndexes,
        const NChunkClient::TDataNodeServiceProxy::TErrorOrRspGetColumnarStatisticsPtr& rspOrError);
};

DEFINE_REFCOUNTED_TYPE(TColumnarStatisticsFetcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
